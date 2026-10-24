package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

// This acceptance test uses distinct OS processes, production WSS/mTLS and node
// keys. The only injected network fault is a transparent TCP transport gate;
// leases, authentication, recovery and acknowledgments use production code.
func TestProcessClusterNetworkPartitionAndKilledLeaderRecovery(t *testing.T) {
	processClusterNetworkPartitionAndKilledLeaderRecovery(t, nil)
}

type processClusterLoad struct {
	Profile                      string
	EvidencePath                 string
	Readers, Concurrent, Batches int
	HookDelay                    time.Duration
	Observe                      func(processClusterMeasurement)
}

type processClusterMeasurement struct {
	Jobs                                                    int
	Accepted, Completed                                     int
	Passed                                                  bool
	Reads, HealthyErrors, FaultWindowErrors, CanceledErrors int64
	Elapsed, FailoverRTO, MaxDispatch                       time.Duration
}

func processClusterNetworkPartitionAndKilledLeaderRecovery(t *testing.T, configure func(...*processFixture)) {
	processClusterWithLoad(t, configure, processClusterLoad{})
}

func processClusterWithLoad(t *testing.T, configure func(...*processFixture), load processClusterLoad) {
	startedAt := time.Now()
	var metrics processClusterMeasurement
	load.EvidencePath = processClusterEvidencePath(load.Profile)
	endMeasurement := registerProcessClusterFinalEvidence(t, load, &metrics, startedAt)
	defer endMeasurement()
	if load.Concurrent == 0 {
		load.Concurrent = 4
	}
	if load.Batches == 0 {
		load.Batches = 1
	}
	var metricsMu sync.Mutex
	completedIDs := map[string]bool{}
	var evidenceMu sync.Mutex
	checkpoint := func() {
		evidenceMu.Lock()
		defer evidenceMu.Unlock()
		metricsMu.Lock()
		partial := metrics
		metricsMu.Unlock()
		partial.Elapsed = time.Since(startedAt)
		writeProcessClusterEvidence(t, load, partial)
	}
	a, b, w := newProcessFixture(t), newProcessFixture(t), newProcessFixture(t)
	a.Config.Hostname = "acceptance-a"
	b.Config.Hostname = "acceptance-b"
	w.Config.Hostname = "acceptance-worker"
	for _, f := range []*processFixture{a, b, w} {
		f.Config.StorageBaseDir = a.Config.StorageBaseDir
		f.Config.SharedStorage = true
		f.Config.DeadJobTimeout = 90
	}
	if configure != nil {
		configure(a, b, w)
	}
	configureProcessTLS(t, a, b, w)
	gate := newTCPGate(t, fmt.Sprintf("127.0.0.1:%d", w.Config.HTTPPort))
	var hooksMu sync.Mutex
	hooks := map[string]int{}
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if load.HookDelay > 0 {
			time.Sleep(load.HookDelay)
		}
		var data map[string]any
		if json.NewDecoder(r.Body).Decode(&data) != nil {
			rw.WriteHeader(400)
			return
		}
		if data["action"] == "job_complete" {
			id, _ := data["id"].(string)
			hooksMu.Lock()
			hooks[id]++
			hooksMu.Unlock()
		}
		rw.WriteHeader(204)
	}))
	defer receiver.Close()
	for _, f := range []*processFixture{a, b, w} {
		f.Config.WebHookAllowedHosts = []string{"127.0.0.1"}
	}
	store := a.Store(t)
	ctx := context.Background()
	seed := func(list, key string, v any) {
		a.Put(t, key, v)
		if err := store.ListPush(ctx, list, v); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []*processFixture{a, b, w} {
		ip := fmt.Sprintf("127.0.0.1:%d", f.Config.HTTPPort)
		if f == w {
			ip = gate.Addr()
		}
		seed("servers", "servers:"+f.Config.Hostname, &types.Server{Hostname: f.Config.Hostname, IP: ip})
	}
	seed("server_groups", "server_groups:masters", &types.ServerGroup{ID: "masters", Master: true, Regexp: "^acceptance-[ab]$"})
	runDir := filepath.Join(a.Dir, "barriers")
	if err := os.MkdirAll(runDir, 0700); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(a.Dir, "cluster-plugin.sh")
	// Every dispatch leaves its ID in a durable append-only execution witness.
	// Release is per job, so the test can hold work across arbitrary fault windows.
	content := `IFS= read -r input
id=$(printf '%s' "$input" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
test -n "$id" || exit 3
printf '%s\n' "$id" >> "$RUN_DIR/executions"
printf 'begin-%s\n' "$id"
: > "$RUN_DIR/$id.started"
while test ! -f "$RUN_DIR/$id.release"; do sleep 0.05; done
printf 'end-%s\n' "$id"
printf '%s\n' '{"complete":true,"code":0}'
`
	if err := os.WriteFile(script, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	seed("plugins", "plugins:barrier", &types.Plugin{ID: "barrier", Title: "Process barrier", Enabled: true, Command: "/bin/sh " + script, Env: map[string]string{"RUN_DIR": runDir}})
	event := &types.Event{ID: "process-cluster", Title: "Process cluster", Plugin: "barrier", Target: w.Config.Hostname, Timing: &types.Timing{}, MaxChildren: 12, WebHook: receiver.URL}
	seed("events", "events:"+event.ID, event)
	ap := a.Start(t)
	wp := w.Start(t)
	_ = b.Start(t)
	awaitProcess(t, 15*time.Second, "initial elected leader", func() bool { return a.Request(t, "/api/app/status", nil)["master"] == true })
	readLoad := startProcessClusterReaders(t, load.Readers, a, b, w)
	defer func() {
		readLoad.Stop()
		metrics.Reads, metrics.HealthyErrors, metrics.FaultWindowErrors, metrics.CanceledErrors = readLoad.Stats()
	}()
	var failureOnce sync.Once
	captureFailure := func(stage, id string, observation any) {
		t.Logf("cluster failure stage=%s accepted_job=%s", stage, id)
		failureOnce.Do(func() {
			t.Logf("first cluster failure stage=%s accepted_job=%s observation=%#v", stage, id, observation)
			for _, node := range []*processFixture{a, b, w} {
				node.captureStacks(t)
			}
			if id != "" {
				processClusterRecoveryDiagnostics(t, store, id)
			}
			for _, node := range []*processFixture{a, b, w} {
				t.Logf("node=%s status=%#v servers=%#v", node.Config.Hostname, node.Request(t, "/api/app/status", nil), node.Request(t, "/api/app/list_servers", nil))
			}
		})
	}
	run := func(f *processFixture) string {
		stage, id, succeeded := "worker connection readiness", "", false
		defer func() {
			if !succeeded {
				captureFailure(stage, id, nil)
			}
		}()
		dispatchAt := time.Now()
		awaitProcess(t, 15*time.Second, "authenticated worker connection", func() bool {
			r := f.Request(t, "/api/app/list_servers", nil)
			servers, _ := r["servers"].(map[string]any)
			worker, _ := servers[w.Config.Hostname].(map[string]any)
			return worker != nil && worker["disabled"] == false
		})
		stage = "dispatch request"
		r := f.Request(t, "/api/app/run_event", map[string]any{"id": event.ID})
		ids, _ := r["ids"].([]any)
		if r["code"] != float64(0) || len(ids) != 1 {
			t.Fatalf("dispatch failed: %#v", r)
		}
		id, _ = ids[0].(string)
		metricsMu.Lock()
		metrics.Accepted++
		metricsMu.Unlock()
		checkpoint()
		stage = "accepted job worker start"
		awaitProcess(t, 8*time.Second, "worker started "+id, func() bool { _, err := os.Stat(filepath.Join(runDir, id+".started")); return err == nil })
		metricsMu.Lock()
		if elapsed := time.Since(dispatchAt); elapsed > metrics.MaxDispatch {
			metrics.MaxDispatch = elapsed
		}
		metricsMu.Unlock()
		succeeded = true
		return id
	}
	release := func(id string) {
		if err := os.WriteFile(filepath.Join(runDir, id+".release"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	outboxHas := func(id string) bool {
		rows, err := store.ListGet(ctx, "remote_completions", 0, 0)
		if err != nil {
			return false
		}
		for _, row := range rows {
			m, _ := row.(map[string]any)
			if m["id"] == id {
				return true
			}
		}
		return false
	}
	assertFinal := func(f *processFixture, id string, deadline time.Time) time.Time {
		var last map[string]any
		converged := false
		defer func() {
			if !converged {
				captureFailure("job/log/history/notification convergence", id, last)
			}
		}()
		observedAt, ok := awaitProcessClusterUntil(deadline, func(waitCtx context.Context) bool {
			r, requestErr := processClusterRequestContext(waitCtx, f, "/api/app/get_job_status", map[string]any{"id": id})
			if requestErr != nil {
				last = map[string]any{"api_error": requestErr.Error()}
				return false
			}
			job, _ := r["job"].(map[string]any)
			pending := false
			rows, outboxErr := store.ListGet(waitCtx, "remote_completions", 0, 0)
			for _, row := range rows {
				m, _ := row.(map[string]any)
				if m["id"] == id {
					pending = true
				}
			}
			last = map[string]any{"api_code": r["code"], "api_complete": r["complete"], "job_complete": job["complete"], "job_code": job["code"], "job_hostname": job["hostname"], "completion_outbox": pending}
			raw, manifest, err := storage.JobLogRead(waitCtx, store, id, 0, 1<<20)
			begin, end := strings.Contains(string(raw), "begin-"+id), strings.Contains(string(raw), "end-"+id)
			last["log_manifest"], last["log_bytes"], last["log_begin"], last["log_end"], last["log_error"] = manifest, len(raw), begin, end, fmt.Sprintf("%v", err)
			hooksMu.Lock()
			count := hooks[id]
			hooksMu.Unlock()
			last["completion_hooks"] = count
			if !(r["complete"] == true && job["code"] == float64(0) && !pending && outboxErr == nil && err == nil && manifest.Final && begin && end && count == 1) {
				return false
			}
			history, historyErr := storage.HistoryJobs(waitCtx, store)
			historyCount := 0
			for _, row := range history {
				if row.ID == id {
					historyCount++
				}
			}
			last["history_copies"] = historyCount
			return historyErr == nil && historyCount == 1
		})
		if !ok {
			t.Fatalf("job, log, history and notification missed common deadline: %s", id)
		}
		metricsMu.Lock()
		if !completedIDs[id] {
			completedIDs[id] = true
			metrics.Completed++
		}
		metricsMu.Unlock()
		checkpoint()
		converged = true
		return observedAt
	}
	first := run(a)
	gate.Pause()
	awaitProcess(t, 10*time.Second, "isolated worker preserves running task after heartbeat timeout", func() bool {
		r := w.Request(t, "/api/app/status", nil)
		return r["master_hostname"] == "" && r["active_jobs"] == float64(1)
	})
	release(first)
	awaitProcess(t, 8*time.Second, "partitioned worker persisted completion outbox", func() bool { return outboxHas(first) })
	readLoad.Fault(w, true)
	wp.Kill(t)
	wp = w.Start(t)
	readLoad.Fault(w, false)
	if !outboxHas(first) {
		t.Fatal("restarted isolated worker lost its durable completion outbox")
	}
	gate.Resume()
	assertFinal(a, first, time.Now().Add(15*time.Second))

	// Keep worker execution alive while the leader itself is SIGKILLed. The backup
	// must wait for the real expired lease and adopt the reservation after restart.
	second := run(a)
	var leaseBefore map[string]any
	if !a.Get(t, "global/master_lease", &leaseBefore) {
		t.Fatal("missing initial lease")
	}
	failoverAt := time.Now()
	readLoad.Fault(a, true)
	ap.Kill(t)
	awaitProcess(t, 18*time.Second, "backup naturally acquired expired leader lease", func() bool { return b.Request(t, "/api/app/status", nil)["master"] == true })
	var leaseAfter map[string]any
	if !b.Get(t, "global/master_lease", &leaseAfter) || leaseAfter["hostname"] != b.Config.Hostname || leaseAfter["owner"] == leaseBefore["owner"] {
		t.Fatalf("lease did not transfer: %#v", leaseAfter)
	}
	_ = a.Start(t)
	readLoad.Fault(a, false)
	awaitProcess(t, 8*time.Second, "old leader restarted as follower", func() bool {
		r := a.Request(t, "/api/app/status", nil)
		return r["master"] == false && r["master_hostname"] == b.Config.Hostname
	})
	var leaseRestarted map[string]any
	if !b.Get(t, "global/master_lease", &leaseRestarted) || leaseRestarted["owner"] != leaseAfter["owner"] {
		t.Fatal("restarted old leader replaced the new owner")
	}
	staleDispatch := a.Request(t, "/api/app/run_event", map[string]any{"id": event.ID})
	if staleDispatch["code"] == float64(0) {
		t.Fatalf("old leader accepted a dispatch after takeover: %#v", staleDispatch)
	}
	release(second)
	assertFinal(b, second, time.Now().Add(15*time.Second))
	// Parallel HTTP requests exercise dispatch and completion while the restarted
	// peer participates in the authenticated cluster again.
	all := []string{first, second}
	for batch := 0; batch < load.Batches; batch++ {
		ids := make(chan string, load.Concurrent)
		var wg sync.WaitGroup
		for i := 0; i < load.Concurrent; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); ids <- run(b) }()
		}
		wg.Wait()
		close(ids)
		var completed []string
		releaseAt := time.Now()
		deadline := releaseAt.Add(15 * time.Second)
		for id := range ids {
			all = append(all, id)
			completed = append(completed, id)
			release(id)
		}
		observations := make(chan time.Time, len(completed))
		var observeWG sync.WaitGroup
		for _, id := range completed {
			observeWG.Add(1)
			go func(id string) { defer observeWG.Done(); observations <- assertFinal(b, id, deadline) }(id)
		}
		go func() { observeWG.Wait(); close(observations) }()
		for observedAt := range observations {
			rto := observedAt.Sub(failoverAt)
			metricsMu.Lock()
			if metrics.FailoverRTO == 0 || rto < metrics.FailoverRTO {
				metrics.FailoverRTO = rto
			}
			metricsMu.Unlock()
		}
	}
	metrics.Jobs, metrics.Elapsed = len(all), time.Since(startedAt)
	if load.Profile != "" && metrics.FailoverRTO > 30*time.Second {
		t.Errorf("failover to first newly dispatched converged completion took %s, limit 30s", metrics.FailoverRTO)
	}
	t.Logf("capacity sample jobs=%d parallel_jobs=%d readers=%d hook_delay=%s elapsed=%s max_dispatch=%s failover_to_first_new_completion=%s", metrics.Jobs, load.Concurrent, load.Readers, load.HookDelay, metrics.Elapsed, metrics.MaxDispatch, metrics.FailoverRTO)
	if load.Observe != nil {
		load.Observe(metrics)
	}
	raw, err := os.ReadFile(filepath.Join(runDir, "executions"))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, id := range strings.Fields(string(raw)) {
		counts[id]++
	}
	if len(counts) != len(all) {
		t.Fatalf("unexpected dispatches: %v", counts)
	}
	for _, id := range all {
		if counts[id] != 1 {
			t.Fatalf("job %s executed %d times across partition/takeover/restart", id, counts[id])
		}
	}
	hooksMu.Lock()
	defer hooksMu.Unlock()
	for _, id := range all {
		if hooks[id] != 1 {
			t.Fatalf("job %s received %d completion notifications", id, hooks[id])
		}
	}
}

// Only emit bounded, non-secret state on failure. Notification payloads, tokens,
// configuration and user job parameters are deliberately excluded.
func processClusterRecoveryDiagnostics(t *testing.T, store storage.Storage, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, key := range []string{"global/master_lease", "jobs:" + id} {
		raw, err := store.Get(ctx, key)
		var record map[string]any
		if err == nil {
			err = json.Unmarshal(raw, &record)
		}
		out := map[string]any{}
		for _, field := range []string{"hostname", "owner", "expires", "id", "complete", "code", "pid", "time_start", "time_end"} {
			if value, ok := record[field]; ok {
				out[field] = value
			}
		}
		t.Logf("persisted %s: state=%#v error=%v", key, out, err)
	}
	for _, key := range []string{"active_jobs", "remote_completions"} {
		rows, err := store.ListGet(ctx, key, 0, 0)
		matches := []map[string]any{}
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			if row["id"] == id {
				matches = append(matches, map[string]any{"id": row["id"], "hostname": row["hostname"], "complete": row["complete"], "code": row["code"]})
			}
		}
		t.Logf("persisted %s: count=%d matching=%#v error=%v", key, len(rows), matches, err)
	}
	history, err := storage.HistoryJobs(ctx, store)
	count := 0
	for _, job := range history {
		if job.ID == id {
			count++
		}
	}
	t.Logf("history job=%s count=%d error=%v", id, count, err)
	if lister, ok := store.(storage.PrefixKeyLister); ok {
		keys, err := lister.ListKeysPrefix(ctx, "notifications/")
		t.Logf("notification records=%d list_error=%v", len(keys), err)
		for i, key := range keys {
			if i >= 24 {
				break
			}
			raw, err := store.Get(ctx, key)
			var row map[string]any
			if err == nil {
				err = json.Unmarshal(raw, &row)
			}
			t.Logf("notification %s state=%v attempts=%v next_attempt=%v lease_until=%v error=%v", key, row["state"], row["attempts"], row["next_attempt"], row["lease_until"], err)
		}
	}
}
