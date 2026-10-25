package engine

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
	"go.uber.org/zap"
)

func newSchedulingTestEngine(t *testing.T, hostname string) *Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := NewEngine(&types.Config{Hostname: hostname, ClusterAllowInsecure: true, SharedStorage: true, StorageBaseDir: filepath.Join(dir, "data"), LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), SecretKey: strings.Repeat("node-key-", 5), SchedulerStartupGrace: 60, MasterPingFreq: 1, MasterPingTimeout: 5, ChildKillTimeout: 1}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err = e.createDirectories(); err != nil {
		t.Fatal(err)
	}
	if err = e.comm.Startup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Shutdown() })
	return e
}
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
func setMasterForTest(e *Engine) {
	e.mu.Lock()
	e.multi.Master = true
	e.multi.MasterHostname = e.Hostname()
	e.mu.Unlock()
	if !e.EnsureLeadership() {
		panic("test master could not acquire lease")
	}
}
func saveSchedulingFixture(t *testing.T, e *Engine, id string) *types.Event {
	t.Helper()
	script := filepath.Join(e.config.LogDir, "fixture.sh")
	if err := os.WriteFile(script, []byte("echo scheduled-output\nprintf '%s\\n' '{\"progress\":0.5}' '{\"complete\":true,\"code\":0}'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.SavePluginToStorage(&types.Plugin{ID: "complete", Enabled: true, Command: "/bin/sh " + script}); err != nil {
		t.Fatal(err)
	}
	event := &types.Event{ID: id, Title: id, Enabled: true, Plugin: "complete", Target: "local", Timing: &types.Timing{}, MaxChildren: 10}
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	return event
}
func TestSchedulerCursorAndCatchUpCompatibility(t *testing.T) {
	e := newSchedulingTestEngine(t, "scheduler-node")
	setMasterForTest(e)
	event := saveSchedulingFixture(t, e, "minute")
	at := time.Date(2026, 10, 7, 0, 3, 0, 0, time.UTC)
	e.scheduler.MinuteTickAt(at)
	e.scheduler.MinuteTickAt(at)
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
	if cursor := e.GetState().Cursors[event.ID]; cursor != at.Unix() {
		t.Fatalf("cursor=%d", cursor)
	}
	event.CatchUp = true
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	e.scheduler.MinuteTickAt(at.Add(3 * time.Minute))
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 4 })
	seen := map[int64]bool{}
	for _, job := range e.GetDeadJobs() {
		if seen[job.Now] {
			t.Fatalf("duplicate scheduled minute %d", job.Now)
		}
		seen[job.Now] = true
	}
	raw, err := e.storage.Get(context.Background(), "global/state")
	if err != nil {
		t.Fatal(err)
	}
	var state types.State
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if state.Cursors[event.ID] != at.Add(3*time.Minute).Unix() {
		t.Fatal("cursor was not persisted")
	}
	e.UpdateMasterState(false)
	e.scheduler.MinuteTickAt(at.Add(4 * time.Minute))
	if e.GetState().Cursors[event.ID] != at.Add(3*time.Minute).Unix() {
		t.Fatal("disabled scheduler advanced cursor")
	}
}
func TestSchedulerGraceStartsScheduling(t *testing.T) {
	e := newSchedulingTestEngine(t, "grace-node")
	saveSchedulingFixture(t, e, "grace-event")
	e.config.SchedulerStartupGrace = 0
	e.goMaster()
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
	e.scheduler.MinuteTickAt(time.Now())
	if len(e.GetDeadJobs()) != 1 {
		t.Fatal("startup and manual tick duplicated the same minute")
	}
}

func TestSchedulerRoleLifecycleAndGraceCancellation(t *testing.T) {
	e := newSchedulingTestEngine(t, "node-z")
	event := saveSchedulingFixture(t, e, "role-event")
	e.goMaster()
	first := e.scheduler.done
	e.goMaster()
	if first != e.scheduler.done {
		t.Fatal("duplicate master created a second timer")
	}
	if !e.AcceptMasterPing("node-a", time.Now().Unix()) {
		t.Fatal("preferred authenticated master not accepted")
	}
	if e.IsMaster() || e.scheduler.cancel != nil {
		t.Fatal("demotion did not stop scheduler")
	}
	select {
	case <-first:
	default:
		t.Fatal("scheduler grace loop leaked")
	}
	before := e.GetState().Cursors[event.ID]
	e.scheduler.MinuteTickAt(time.Now())
	if e.GetState().Cursors[event.ID] != before {
		t.Fatal("slave scheduled event")
	}
	e.goMaster()
	if e.scheduler.done == first {
		t.Fatal("promotion did not create fresh scheduler lifecycle")
	}
	e.goIdle()
	if e.scheduler.cancel != nil {
		t.Fatal("idle role retained scheduler")
	}
}
func TestMinuteTickDrainsQueueWithoutLockReentry(t *testing.T) {
	e := newSchedulingTestEngine(t, "queue-node")
	setMasterForTest(e)
	event := saveSchedulingFixture(t, e, "queue-event")
	event.Queue = true
	event.Timing = nil
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	timestamp := int64(1791331260)
	if err := e.storage.ListPush(context.Background(), "global/event_queue/"+event.ID, map[string]interface{}{"event_id": event.ID, "timestamp": timestamp}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.eventQueue[event.ID] = 1
	e.mu.Unlock()
	done := make(chan struct{})
	go func() { e.scheduler.MinuteTickAt(time.Now()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("minute tick deadlocked while draining queue")
	}
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
	for _, job := range e.GetDeadJobs() {
		if job.Now != timestamp {
			t.Fatalf("queue timestamp lost: %d", job.Now)
		}
	}
	if e.GetEventQueues()[event.ID] != 0 {
		t.Fatal("queue count did not drain")
	}
}
func TestQueueProcessesDelayedSnapshotAndUniqueIDs(t *testing.T) {
	e := newSchedulingTestEngine(t, "queue-node")
	job := &types.Job{ID: "delayed", Command: "/usr/bin/true", Hostname: e.Hostname(), When: float64(time.Now().Unix() + 3600)}
	e.queue.EnqueueInternal(&types.QueueTask{Action: "launchLocalJob", Data: map[string]interface{}{"job": job}})
	done := make(chan struct{})
	go func() { e.queue.processInternalQueue(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queue execution cannot reenqueue")
	}
	if active := e.GetActiveJobs()[job.ID]; active == nil {
		t.Fatal("delayed snapshot was not reserved as an active job")
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.queue.EnqueueInternal(&types.QueueTask{Action: "unknown", When: float64(time.Now().Unix() + 3600)})
		}()
	}
	wg.Wait()
	e.queue.mu.RLock()
	count := len(e.queue.internalQueue)
	e.queue.mu.RUnlock()
	if count != 100 {
		t.Fatalf("queue IDs collided: count=%d", count)
	}
}
func TestTimingCompatibilityAcrossTimezoneAndDST(t *testing.T) {
	e := newSchedulingTestEngine(t, "timing-node")
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	event := &types.Event{Timezone: loc.String(), Timing: &types.Timing{Minutes: []int{30}, Hours: []int{1}, Weekdays: []int{0}, Days: []int{1}, Months: []int{11}, Years: []int{2026}}}
	for _, utc := range []string{"2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"} {
		at, _ := time.Parse(time.RFC3339, utc)
		if !e.scheduler.checkEventTiming(event, at.Unix()) {
			t.Errorf("Cronicle repeated DST hour did not match %s", utc)
		}
	}
	at, _ := time.Parse(time.RFC3339, "2026-11-01T07:30:00Z")
	if e.scheduler.checkEventTiming(event, at.Unix()) {
		t.Fatal("unexpected match outside local hour")
	}
}
func TestClusterAuthenticatedDispatchAndReconnect(t *testing.T) {
	for _, backend := range []string{"Filesystem", "S3", "Couchbase", "Redis"} {
		t.Run(backend, func(t *testing.T) { exerciseClusterDispatchAndReconnect(t, backend) })
	}
}
func configureSchedulingBackend(t *testing.T, e *Engine, backend string) {
	t.Helper()
	if backend == "Filesystem" {
		return
	}
	options := map[string]interface{}{"bucket": "owlcron-integration", "prefix": fmt.Sprintf("cluster-%d", time.Now().UnixNano())}
	switch backend {
	case "S3":
		endpoint := os.Getenv("OWLCRON_TEST_S3_ENDPOINT")
		if endpoint == "" {
			t.Skip("set OWLCRON_TEST_S3_ENDPOINT for real cluster tests")
		}
		options["endpoint"] = endpoint
		options["access_key"] = "owlcron-test"
		options["secret_key"] = "owlcron-test-secret"
	case "Couchbase":
		connection := os.Getenv("OWLCRON_TEST_COUCHBASE_CONNECTION")
		if connection == "" {
			t.Skip("set OWLCRON_TEST_COUCHBASE_CONNECTION for real cluster tests")
		}
		options["connection_string"] = connection
		options["username"] = "Administrator"
		options["password"] = "owlcron-test-secret"
	case "Redis":
		addr := os.Getenv("OWLCRON_TEST_REDIS_ADDR")
		if addr == "" {
			t.Skip("set OWLCRON_TEST_REDIS_ADDR for Redis 8.4+ cluster tests")
		}
		options["addr"] = addr
		options["username"] = os.Getenv("OWLCRON_TEST_REDIS_USERNAME")
		options["password"] = os.Getenv("OWLCRON_TEST_REDIS_PASSWORD")
	}
	store, err := storage.NewStorage(&storage.StorageConfig{Engine: backend, Options: options})
	if err != nil {
		t.Fatal(err)
	}
	e.storage = store
	e.config.StorageEngine = backend
	e.config.StorageOptions = options
}
func exerciseClusterDispatchAndReconnect(t *testing.T, backend string) {
	master := newSchedulingTestEngine(t, "node-a")
	worker := newSchedulingTestEngine(t, "node-b")
	configureSchedulingBackend(t, master, backend)
	shareSchedulingStore(t, master, worker)
	setMasterForTest(master)
	mux := http.NewServeMux()
	worker.comm.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	if err := master.comm.ConnectToSlave(worker.Hostname(), address); err != nil {
		t.Fatal(err)
	}
	master.comm.SendMasterPings()
	waitForCondition(t, time.Second, func() bool { return worker.GetMulti().MasterHostname == master.Hostname() })
	event := saveSchedulingFixture(t, master, "remote-event")
	event.Target = worker.Hostname()
	if err := master.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := master.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 3*time.Second, func() bool { return len(master.GetDeadJobs()) == 1 })
	for _, job := range master.GetDeadJobs() {
		logData, err := readAllStoredLog(context.Background(), master.storage, job.ID)
		if err != nil || !strings.Contains(string(logData), "scheduled-output") {
			t.Fatalf("remote log not transferred: %v %s", err, logData)
		}
		if job.Hostname != worker.Hostname() || job.Code != 0 {
			t.Fatalf("remote completion=%+v", job)
		}
	}
	peer, _ := master.comm.GetSlaveConnection(worker.Hostname())
	if peer.Socket == nil {
		t.Fatal("peer not connected")
	}
	first := peer.Socket
	_ = first.Close()
	waitForCondition(t, 4*time.Second, func() bool {
		peer, exists := master.comm.GetSlaveConnection(worker.Hostname())
		return exists && peer.Socket != nil && peer.Socket != first
	})
	master.comm.SendMasterPings()
	if err := master.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 3*time.Second, func() bool { return len(master.GetDeadJobs()) == 2 })
	// A worker process continues through a transport break. Its durable result
	// must reconcile with the master's existing reservation after reconnect.
	script := filepath.Join(master.config.LogDir, "fixture.sh")
	if err := os.WriteFile(script, []byte("sleep 0.4\necho recovered-output\nprintf '%s\\n' '{\"complete\":true,\"code\":0}'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := master.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, func() bool { return len(worker.GetActiveJobs()) == 1 })
	peer, _ = master.comm.GetSlaveConnection(worker.Hostname())
	_ = peer.Socket.Close()
	if len(master.GetDeadJobs()) != 2 || len(master.GetActiveJobs()) != 1 {
		t.Fatal("disconnect falsely completed running job")
	}
	waitForCondition(t, 4*time.Second, func() bool { return len(master.GetDeadJobs()) == 3 })
	waitForCondition(t, time.Second, func() bool {
		items, _ := worker.storage.ListGet(context.Background(), "remote_completions", 0, 0)
		return len(items) == 0
	})
	// A registered target with the wrong hostname must fail mutual authentication.
	if err := master.comm.ConnectToSlave("wrong-identity", address); err == nil {
		t.Fatal("accepted unexpected node identity")
	}
}
func TestFailoverFirstCandidateDoesNotReenterEngineLock(t *testing.T) {
	e := newSchedulingTestEngine(t, "a-first")
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: e.Hostname()}); err != nil {
		t.Fatal(err)
	}
	if err := e.SaveServerGroupToStorage(&types.ServerGroup{ID: "masters", Master: true, Regexp: ".*"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { e.masterFailover(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("first candidate failover deadlocked")
	}
	if !e.IsMaster() {
		t.Fatal("first eligible candidate did not become master")
	}
	if err := e.comm.DeleteServerFromStorage(e.Hostname()); err != nil {
		t.Fatal(err)
	}
	servers, err := e.comm.loadServerList()
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 0 {
		t.Fatal(fmt.Sprintf("deleted server remains in list: %v", servers))
	}
}

func TestFailoverUsesLeaseInsteadOfAnonymousHTTPStatus(t *testing.T) {
	e := newSchedulingTestEngine(t, "node-b")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"master":true}`))
	}))
	address := strings.TrimPrefix(server.URL, "http://")
	for _, peer := range []*types.Server{{Hostname: "node-a", IP: address}, {Hostname: "node-b"}} {
		if err := e.comm.SaveServerToStorage(peer); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.SaveServerGroupToStorage(&types.ServerGroup{ID: "masters", Master: true, Regexp: ".*"}); err != nil {
		t.Fatal(err)
	}
	lease := leadershipLease{Hostname: "node-a", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := e.storage.Put(e.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	e.masterFailover()
	if e.IsMaster() {
		t.Fatal("elected while another node owned an active lease")
	}
	lease.Expires = time.Now().Add(-time.Second).UnixNano()
	raw, _ = json.Marshal(lease)
	if err := e.storage.Put(e.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	e.masterFailover()
	if !e.IsMaster() {
		t.Fatal("anonymous HTTP master claim blocked takeover after lease expiry")
	}
}
func TestDisconnectedWorkerRecoveryTimeout(t *testing.T) {
	e := newSchedulingTestEngine(t, "node-a")
	setMasterForTest(e)
	e.config.DeadJobTimeout = 5
	now := time.Now()
	e.comm.mu.Lock()
	e.comm.slaves["node-b"] = &SlaveConnection{Hostname: "node-b", LastSeen: now.Unix() - 3, ActiveJobs: map[string]*types.Job{}}
	e.comm.mu.Unlock()
	job := &types.Job{ID: "missing-remote", Hostname: "node-b", Event: "timeout-event", TimeStart: float64(now.Unix() - 10), Retries: 3}
	e.mu.Lock()
	e.activeJobs[job.ID] = job
	e.mu.Unlock()
	e.comm.ExpireDisconnectedJobs(now)
	if e.GetActiveJobs()[job.ID] == nil {
		t.Fatal("reservation released before recovery timeout")
	}
	e.comm.ExpireDisconnectedJobs(now.Add(3 * time.Second))
	completed, ok := e.LookupJob(job.ID)
	if !ok || !completed.Complete || completed.Code == 0 || completed.AbortReason == "" {
		t.Fatalf("missing remote timeout completion: %+v", completed)
	}
	if len(e.GetActiveJobs()) != 0 {
		t.Fatal("uncertain execution was retried")
	}
}

func TestCatchUpRetriesOriginalMinuteAfterLaunchFailure(t *testing.T) {
	e := newSchedulingTestEngine(t, "retry-minute")
	setMasterForTest(e)
	event := saveSchedulingFixture(t, e, "catch-up-failure")
	event.CatchUp = true
	event.MaxChildren = 1
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 7, 0, 2, 0, 0, time.UTC)
	e.mu.Lock()
	e.activeJobs["blocker"] = &types.Job{ID: "blocker", Event: event.ID, Hostname: "other-node"}
	e.state.Cursors[event.ID] = at.Add(-time.Minute).Unix()
	e.mu.Unlock()
	e.scheduler.MinuteTickAt(at)
	if got := e.GetState().Cursors[event.ID]; got != at.Add(-time.Minute).Unix() {
		t.Fatalf("failed minute advanced cursor: %d", got)
	}
	e.mu.Lock()
	delete(e.activeJobs, "blocker")
	e.mu.Unlock()
	e.scheduler.MinuteTickAt(at)
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
	for _, job := range e.GetDeadJobs() {
		if job.Now != at.Unix() {
			t.Fatal("retried a different scheduled minute")
		}
	}
}

func shareSchedulingStore(t *testing.T, leader, follower *Engine) {
	t.Helper()
	backend := leader.config.StorageEngine
	if backend == "" {
		backend = "Filesystem"
	}
	store, err := storage.NewStorage(&storage.StorageConfig{Engine: backend, BaseDir: leader.config.StorageBaseDir, Options: leader.config.StorageOptions})
	if err != nil {
		t.Fatal(err)
	}
	follower.storage = store
	follower.config.StorageBaseDir = leader.config.StorageBaseDir
	follower.config.StorageEngine = backend
	follower.config.StorageOptions = leader.config.StorageOptions
	follower.config.SharedStorage = true

}
func TestFollowerShutdownCannotOverwriteLeaderCursors(t *testing.T) {
	leader := newSchedulingTestEngine(t, "node-a")
	follower := newSchedulingTestEngine(t, "node-b")
	shareSchedulingStore(t, leader, follower)
	leader.goMaster()
	leader.mu.Lock()
	leader.state.Cursors["cursor"] = 123456
	leader.mu.Unlock()
	if err := leader.saveStateToStorage(); err != nil {
		t.Fatal(err)
	}
	follower.mu.Lock()
	follower.state.Cursors["cursor"] = 1
	follower.state.Enabled = false
	follower.mu.Unlock()
	if err := follower.Shutdown(); err != nil {
		t.Fatal(err)
	}
	raw, err := leader.storage.Get(context.Background(), "global/state")
	if err != nil {
		t.Fatal(err)
	}
	var state types.State
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if state.Cursors["cursor"] != 123456 || !state.Enabled {
		t.Fatalf("follower overwrote shared state: %+v", state)
	}
}
func TestLeadershipLeaseFencesAnotherInstanceAfterExpiry(t *testing.T) {
	first := newSchedulingTestEngine(t, "same-hostname")
	second := newSchedulingTestEngine(t, "same-hostname")
	first.config.MasterPingTimeout = 1
	second.config.MasterPingTimeout = 1
	shareSchedulingStore(t, first, second)
	first.goMaster()
	second.goMaster()
	if !first.IsMaster() || second.IsMaster() {
		t.Fatal("two processes acquired the same active lease")
	}
	if first.LeadershipToken() == second.LeadershipToken() {
		t.Fatal("instance fencing tokens collided")
	}
	time.Sleep(1100 * time.Millisecond)
	second.goMaster()
	if !second.IsMaster() {
		t.Fatal("expired lease could not be acquired")
	}
	if first.EnsureLeadership() {
		t.Fatal("replaced owner was allowed to renew or dispatch")
	}
	if first.ValidateNodeLeadership(first.Hostname(), first.LeadershipToken()) {
		t.Fatal("replaced node token remained trusted")
	}
	if !second.ValidateNodeLeadership(second.Hostname(), second.LeadershipToken()) {
		t.Fatal("current lease token is not recognized")
	}
}
func TestPrimaryOutageAutomaticTakeoverPreservesCursor(t *testing.T) {
	for _, backend := range []string{"Filesystem", "S3", "Couchbase", "Redis"} {
		t.Run(backend, func(t *testing.T) { exercisePrimaryOutage(t, backend) })
	}
}
func exercisePrimaryOutage(t *testing.T, backend string) {
	primary := newSchedulingTestEngine(t, "node-a")
	secondary := newSchedulingTestEngine(t, "node-b")
	configureSchedulingBackend(t, primary, backend)
	shareSchedulingStore(t, primary, secondary)
	primary.config.MasterPingTimeout = 2
	secondary.config.MasterPingTimeout = 2
	makeServer := func(e *Engine) *httptest.Server {
		mux := http.NewServeMux()
		e.comm.RegisterRoutes(mux)
		mux.HandleFunc("/api/app/status", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0, "master": e.IsMaster()})
		})
		return httptest.NewServer(mux)
	}
	primaryServer := makeServer(primary)
	secondaryServer := makeServer(secondary)
	defer primaryServer.Close()
	defer secondaryServer.Close()
	peers := []*types.Server{{Hostname: primary.Hostname(), IP: strings.TrimPrefix(primaryServer.URL, "http://")}, {Hostname: secondary.Hostname(), IP: strings.TrimPrefix(secondaryServer.URL, "http://")}}
	for _, peer := range peers {
		if err := primary.comm.SaveServerToStorage(peer); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SaveServerGroupToStorage(&types.ServerGroup{ID: "masters", Master: true, Regexp: "^node-"}); err != nil {
		t.Fatal(err)
	}
	event := saveSchedulingFixture(t, primary, "takeover-event")
	event.CatchUp = true
	if err := primary.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := primary.Startup(); err != nil {
		t.Fatal(err)
	}
	if err := secondary.Startup(); err != nil {
		t.Fatal(err)
	}
	if !primary.IsMaster() || secondary.IsMaster() {
		t.Fatal("initial leader was not selected")
	}
	at := time.Now().Truncate(time.Minute)
	primary.scheduler.MinuteTickAt(at)
	waitForCondition(t, time.Second, func() bool { return len(primary.GetDeadJobs()) == 1 })
	primaryServer.Close()
	// Abrupt process loss: stop its loops and listener without running Shutdown
	// or releasing the shared lease. The standby must wait for lease expiry.
	primary.cancel()
	waitForCondition(t, 8*time.Second, func() bool { return secondary.IsMaster() })
	if primary.EnsureLeadership() || secondary.ValidateNodeLeadership(primary.Hostname(), primary.LeadershipToken()) {
		t.Fatal("expired primary remained eligible to dispatch or acknowledge worker messages")
	}
	if err := primary.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if secondary.GetState().Cursors[event.ID] != at.Unix() {
		t.Fatal("takeover lost committed cursor")
	}
	secondary.scheduler.MinuteTickAt(at)
	history, err := readStoredHistory(context.Background(), secondary.storage, event.ID)
	if err != nil || len(history) != 1 {
		t.Fatalf("same minute duplicated after actual outage: %v %+v", err, history)
	}
	secondary.scheduler.MinuteTickAt(at.Add(time.Minute))
	waitForCondition(t, time.Second, func() bool {
		items, _ := readStoredHistory(context.Background(), secondary.storage, event.ID)
		return len(items) == 2
	})
	for _, job := range secondary.GetDeadJobs() {
		if job.Hostname != secondary.Hostname() {
			t.Fatalf("takeover job executed on wrong host: %+v", job)
		}
	}
}

func TestPreparedQueueAdmissionSurvivesRestartAndDoesNotDuplicate(t *testing.T) {
	first := newSchedulingTestEngine(t, "queue-owner")
	setMasterForTest(first)
	event := saveSchedulingFixture(t, first, "durable-queue")
	event.Queue = true
	event.Timing = nil
	if err := first.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	timestamp := int64(1791331260)
	token := "stable-queue-admission"
	queued := map[string]interface{}{"queue_token": token, "event_id": event.ID, "timestamp": timestamp, "event": event}
	queueKey := "global/event_queue/" + event.ID
	if err := first.storage.ListPush(context.Background(), queueKey, queued); err != nil {
		t.Fatal(err)
	}
	plug, err := first.jobManager.GetPluginFromStorage(event.Plugin)
	if err != nil {
		t.Fatal(err)
	}
	prepared := &types.Job{ID: "prepared-job-id", Event: event.ID, EventTitle: event.Title, Plugin: event.Plugin, Hostname: first.Hostname(), Target: "local", Command: plug.Command, Now: timestamp, TimeStart: float64(time.Now().Unix())}
	if err := first.jobManager.stageQueueAdmission(token, []*types.Job{prepared}); err != nil {
		t.Fatal(err)
	}
	// Simulate the exact crash boundary: immutable admission committed, no active
	// reservation or process created, and the queue head remains durable.
	if len(first.GetActiveJobs()) != 0 || len(first.GetDeadJobs()) != 0 {
		t.Fatal("preparation started execution")
	}
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	restarted := newSchedulingTestEngine(t, "queue-owner")
	shareSchedulingStore(t, first, restarted)
	restarted.goMaster()
	restarted.scheduler.checkEventQueues()
	waitForCondition(t, time.Second, func() bool { return len(restarted.GetDeadJobs()) == 1 })
	complete, ok := restarted.LookupJob(prepared.ID)
	if !ok || complete.Code != 0 || complete.Now != timestamp {
		t.Fatalf("prepared job not recovered: %+v", complete)
	}
	items, err := restarted.storage.ListGet(context.Background(), queueKey, 0, 0)
	if err != nil || len(items) != 0 {
		t.Fatalf("admitted queue head retained: %v %v", err, items)
	}
	// Simulate a second boundary: execution finished before the head removal was
	// committed. Replaying the stable token consumes the head without a new job.
	if err := restarted.storage.ListPush(context.Background(), queueKey, queued); err != nil {
		t.Fatal(err)
	}
	restarted.mu.Lock()
	restarted.eventQueue[event.ID] = 1
	restarted.mu.Unlock()
	restarted.scheduler.checkEventQueues()
	history, err := readStoredHistory(context.Background(), restarted.storage, event.ID)
	if err != nil || len(history) != 1 || len(restarted.GetActiveJobs()) != 0 {
		t.Fatalf("queue replay duplicated admission: %v %v", err, history)
	}
}
