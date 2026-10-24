package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

type processLoadProfile struct {
	events    []string
	nextEvent int
	f         *processFixture
	p         *daemonProcess
	m         *processLoadMetrics
	witness   string
	hooksMu   sync.Mutex
	hooks     map[string]int
	ids       map[string]time.Time
}

func newProcessLoadProfile(t *testing.T, name string, concurrency, events int) *processLoadProfile {
	t.Helper()
	l := &processLoadProfile{f: newProcessFixture(t), m: newProcessLoadMetrics(t, name), hooks: map[string]int{}, ids: map[string]time.Time{}}
	l.m.EventCount = events
	l.m.Concurrency = concurrency
	l.m.Backend = "Filesystem"
	backend := os.Getenv("OWLCRON_LOAD_BACKEND")
	if backend != "" && backend != "Filesystem" {
		variable := ""
		switch backend {
		case "S3":
			variable = "OWLCRON_REMOTE_S3_CONFIG"
		case "Couchbase":
			variable = "OWLCRON_REMOTE_COUCHBASE_CONFIG"
		default:
			t.Fatal("invalid OWLCRON_LOAD_BACKEND")
		}
		options := map[string]any{}
		if json.Unmarshal([]byte(os.Getenv(variable)), &options) != nil || options == nil {
			t.Fatalf("%s must contain storage options (redacted)", variable)
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			t.Fatal(err)
		}
		prefix, _ := options["prefix"].(string)
		options["prefix"] = strings.TrimRight(prefix, "/:") + "/load-acceptance/" + hex.EncodeToString(nonce[:]) + "/"
		l.f.Config.StorageEngine = backend
		l.m.Backend = backend
		l.f.Config.StorageOptions = options
		l.f.Put(t, "api_keys:"+l.f.APIKey, &types.APIKey{ID: l.f.APIKey, Key: l.f.APIKey, Enabled: true, Privileges: types.Privileges{"admin": true}})
		remote := l.f.Store(t)
		t.Cleanup(func() { cleanupRemoteProcessNamespace(t, remote) })
	}
	l.f.Config.JobDataExpireDays = 1
	l.f.Config.JobLogExpireDays = 1
	l.f.Config.HistoryCleanupInterval = 1
	l.f.Config.HistoryCleanupBatchSize = 32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var data map[string]any
		if json.NewDecoder(r.Body).Decode(&data) != nil {
			w.WriteHeader(400)
			return
		}
		if data["action"] == "job_complete" {
			id, _ := data["id"].(string)
			l.hooksMu.Lock()
			l.hooks[id]++
			l.hooksMu.Unlock()
		}
		w.WriteHeader(204)
	}))
	t.Cleanup(receiver.Close)
	l.f.Config.WebHookAllowedHosts = []string{"127.0.0.1"}
	l.witness = filepath.Join(l.f.Dir, "executions")
	script := filepath.Join(l.f.Dir, "load.sh")
	content := `IFS= read -r input
id=$(printf '%s' "$input" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
test -n "$id" || exit 3
printf '%s\n' "$id" >> '` + l.witness + `'
printf 'load-begin-%s\n' "$id"
sleep 0.02
printf 'load-end-%s\n' "$id"
printf '%s\n' '{"complete":true,"code":0}'
`
	if err := os.WriteFile(script, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	store := l.f.Store(t)
	ctx := context.Background()
	seed := func(list, key string, v any) {
		l.f.Put(t, key, v)
		if err := store.ListPush(ctx, list, v); err != nil {
			t.Fatal(err)
		}
	}
	seed("servers", "servers:"+l.f.Config.Hostname, &types.Server{Hostname: l.f.Config.Hostname, IP: fmt.Sprintf("127.0.0.1:%d", l.f.Config.HTTPPort)})
	seed("server_groups", "server_groups:load", &types.ServerGroup{ID: "load", Master: true, Regexp: ".*"})
	if name == "soak" {
		if duration, err := time.ParseDuration(os.Getenv("OWLCRON_SOAK_DURATION")); err == nil && duration > 0 {
			l.f.ChildTimeout = duration + time.Minute
			if l.f.ChildTimeout > 48*time.Hour {
				l.f.ChildTimeout = 48 * time.Hour
			}
		}
	}
	seed("plugins", "plugins:load", &types.Plugin{ID: "load", Title: "Load", Enabled: true, Command: "/bin/sh " + script})
	for i := 0; i < events; i++ {
		id := fmt.Sprintf("load-%06d", i)
		l.events = append(l.events, id)
		seed("events", "events:"+id, &types.Event{ID: id, Title: "Load profile", Plugin: "load", Target: "local", Timing: &types.Timing{}, MaxChildren: concurrency + 1, WebHook: receiver.URL})
	}
	l.p = l.f.Start(t)
	awaitProcess(t, 15*time.Second, "load leader", func() bool { return l.f.Request(t, "/api/app/status", nil)["master"] == true })
	l.m.SetupSeconds = time.Since(l.m.Started).Seconds()
	l.m.measurementStart = time.Now()
	return l
}
func (l *processLoadProfile) batch(t *testing.T, count, concurrency int) {
	t.Helper()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	next := 0
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next >= count {
					mu.Unlock()
					return
				}
				next++
				event := l.events[l.nextEvent%len(l.events)]
				l.nextEvent++
				mu.Unlock()
				started := time.Now()
				response, err := processLoadRequest(l.f, l.m, "/api/app/run_event", map[string]any{"id": event})
				var id string
				if err == nil {
					ids, _ := response["ids"].([]any)
					if len(ids) != 1 {
						err = fmt.Errorf("dispatch IDs: %v", response)
					} else {
						id, _ = ids[0].(string)
					}
				}
				if err == nil {
					l.m.mu.Lock()
					l.m.Accepted++
					l.m.mu.Unlock()
					mu.Lock()
					l.ids[id] = started
					mu.Unlock()
					deadline := time.Now().Add(45 * time.Second)
					for {
						response, err = processLoadRequest(l.f, l.m, "/api/app/get_job_status", map[string]any{"id": id})
						if err != nil {
							break
						}
						if response["complete"] == true {
							job, _ := response["job"].(map[string]any)
							if job["code"] != float64(0) {
								err = fmt.Errorf("job %s failed: %v", id, job)
							}
							break
						}
						if time.Now().After(deadline) {
							err = fmt.Errorf("job %s completion timeout", id)
							break
						}
						l.m.sampleRSS(l.p)
						time.Sleep(20 * time.Millisecond)
					}
					if err == nil {
						l.m.mu.Lock()
						l.m.Completed++
						processLoadObserve(&l.m.jobHistogram, time.Since(started))
						l.m.mu.Unlock()
					}
				}
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					l.m.mu.Lock()
					l.m.Errors++
					l.m.mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	l.m.sampleRSS(l.p)
	if len(errs) > 0 {
		t.Fatalf("profile errors (%d): %v", len(errs), errs)
	}
}
func (l *processLoadProfile) verify(t *testing.T) {
	t.Helper()
	ids := processLoadSortedIDs(l.ids)
	awaitProcess(t, 30*time.Second, "load logs and notifications", func() bool {
		l.hooksMu.Lock()
		defer l.hooksMu.Unlock()
		for _, id := range ids {
			if l.hooks[id] != 1 {
				return false
			}
			raw, manifest, err := storage.JobLogRead(context.Background(), l.f.Store(t), id, 0, 1<<20)
			if err != nil || !manifest.Final || strings.Count(string(raw), "load-begin-"+id) != 1 || strings.Count(string(raw), "load-end-"+id) != 1 {
				return false
			}
		}
		return true
	})
	var logBytes int64
	for _, id := range ids {
		raw, _, err := storage.JobLogRead(context.Background(), l.f.Store(t), id, 0, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		logBytes += int64(len(raw))
	}
	l.m.mu.Lock()
	l.m.LogBytes = logBytes
	l.m.mu.Unlock()
	counts, err := processLoadWitness(l.witness)
	if err != nil {
		t.Fatal(err)
	}
	if len(counts) != len(ids) {
		t.Fatalf("witness IDs %d != accepted %d", len(counts), len(ids))
	}
	history, err := storage.HistoryJobs(context.Background(), l.f.Store(t))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, j := range history {
		seen[j.ID]++
	}
	for _, id := range ids {
		if counts[id] != 1 || seen[id] != 1 {
			t.Fatalf("job %s executions=%d history=%d", id, counts[id], seen[id])
		}
	}
}

// This certifies only the selected offered-load profile, never maximum capacity.
func TestProcessCapacityProfile(t *testing.T) {
	jobs := processLoadEnvInt(t, "OWLCRON_CAPACITY_JOBS", 24, 100000)
	clients := processLoadEnvInt(t, "OWLCRON_CAPACITY_CONCURRENCY", 4, 256)
	events := processLoadEnvInt(t, "OWLCRON_CAPACITY_EVENTS", 8, 100000)
	l := newProcessLoadProfile(t, "capacity", clients, events)
	defer l.m.finish(t)
	l.batch(t, jobs, clients)
	l.verify(t)
}
