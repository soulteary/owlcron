package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gorillaWS "github.com/gorilla/websocket"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

func overloadSocketPair(t *testing.T) (*gorillaWS.Conn, *gorillaWS.Conn) {
	t.Helper()
	accepted := make(chan *gorillaWS.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&gorillaWS.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			accepted <- conn
		}
	}))
	t.Cleanup(server.Close)
	client, _, err := gorillaWS.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := <-accepted
	t.Cleanup(func() { client.Close(); peer.Close() })
	return client, peer
}

func TestClusterReadTimeoutExcludesLocalHandlerDelay(t *testing.T) {
	e := newSchedulingTestEngine(t, "overload-master")
	setMasterForTest(e)
	// Only the socket's idle timeout is short. Keep the independently fenced
	// lease alive while deliberately delaying local message processing.
	e.config.MasterPingTimeout = 1
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
		t.Fatal(err)
	}
	conn, remote := overloadSocketPair(t)
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	peer := &SlaveConnection{Hostname: "worker", Socket: conn}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	e.comm.dispatchMu.Lock()
	locked := true
	defer func() {
		if locked {
			e.comm.dispatchMu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() { defer close(done); e.comm.readSlaveLoop(ctx, peer, conn) }()
	if err := remote.WriteJSON(map[string]any{"type": "local-delay"}); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, time.Second, func() bool { e.comm.mu.RLock(); defer e.comm.mu.RUnlock(); return peer.LastSeen > 0 })
	time.Sleep(1200 * time.Millisecond)
	e.comm.dispatchMu.Unlock()
	locked = false
	// Give the read loop time to encounter an already-expired deadline in the
	// old implementation, then send a fresh frame within the actual idle bound.
	time.Sleep(50 * time.Millisecond)
	if err := remote.WriteJSON(map[string]any{"type": "status", "data": map[string]any{"marker": "after-delay"}}); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 500*time.Millisecond, func() bool { e.comm.mu.RLock(); defer e.comm.mu.RUnlock(); return peer.Data["marker"] == "after-delay" })
	cancel()
	conn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("read loop did not stop")
	}
}

type slowPeerValidationStore struct {
	storage.Storage
	reads            atomic.Int64
	entered, release chan struct{}
	once             sync.Once
}

func (s *slowPeerValidationStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "global/master_lease" && s.reads.Add(1) == 2 {
		s.once.Do(func() { close(s.entered); <-s.release })
	}
	return s.Storage.Get(ctx, key)
}
func TestClusterSlowPeerValidationDoesNotHoldGlobalPeerLock(t *testing.T) {
	e := newSchedulingTestEngine(t, "overload-master")
	setMasterForTest(e)
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
		t.Fatal(err)
	}
	conn, remote := overloadSocketPair(t)
	peer := &SlaveConnection{Hostname: "worker", Socket: conn}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	store := &slowPeerValidationStore{Storage: e.storage, entered: make(chan struct{}), release: make(chan struct{})}
	e.storage = store
	released := false
	defer func() {
		if !released {
			close(store.release)
		}
	}()
	sent := make(chan error, 1)
	go func() { sent <- e.comm.SendToSlave(peer.Hostname, "master_ping", map[string]any{}) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("send did not reach slow validation")
	}
	progress := make(chan struct{})
	go func() { e.comm.mu.Lock(); peer.LastSeen = time.Now().Unix(); e.comm.mu.Unlock(); close(progress) }()
	select {
	case <-progress:
	case <-time.After(200 * time.Millisecond):
		t.Error("one slow peer blocked global heartbeat state")
	}
	close(store.release)
	released = true
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := remote.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame["type"] != "master_ping" {
		t.Fatalf("unexpected frame: %#v", frame)
	}
	<-progress
}

func TestClusterHeartbeatCoalescesSlowPeerWithoutBlockingLeaseLoop(t *testing.T) {
	e := newSchedulingTestEngine(t, "overload-master")
	setMasterForTest(e)
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
		t.Fatal(err)
	}
	conn, remote := overloadSocketPair(t)
	peer := &SlaveConnection{Hostname: "worker", Socket: conn}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	peer.writeMu.Lock()
	locked := true
	defer func() {
		if locked {
			peer.writeMu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() {
		for range 100 {
			e.comm.SendMasterPings()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("slow peer blocked heartbeat caller and lease loop")
	}
	e.comm.mu.RLock()
	pending := peer.pingPending
	e.comm.mu.RUnlock()
	if !pending {
		t.Fatal("heartbeat was not retained while worker send was busy")
	}
	// Lease verification and global peer state remain available while this peer
	// is stalled. No new goroutine is queued for each of the 100 ticks.
	if !e.EnsureLeadership() {
		t.Fatal("could not renew leadership while a peer was stalled")
	}
	peer.writeMu.Unlock()
	locked = false
	remote.SetReadDeadline(time.Now().Add(time.Second))
	var frame map[string]any
	if err := remote.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame["type"] != "master_ping" {
		t.Fatalf("unexpected frame %#v", frame)
	}
	waitForCondition(t, time.Second, func() bool { e.comm.mu.RLock(); defer e.comm.mu.RUnlock(); return !peer.pingPending })
	remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if err := remote.ReadJSON(&frame); err == nil {
		t.Fatal("busy peer accumulated duplicate heartbeat tasks")
	}
	// A pending task whose generation was retired must exit without retaining
	// any global lock, even while another peer has the same logical hostname.
	peer.writeMu.Lock()
	locked = true
	e.comm.SendMasterPings()
	e.comm.ResetCluster()
	replacement := &SlaveConnection{Hostname: "worker", pingPending: true}
	e.comm.mu.Lock()
	e.comm.slaves[replacement.Hostname] = replacement
	e.comm.mu.Unlock()
	peer.writeMu.Unlock()
	locked = false
	e.comm.wg.Wait()
	e.comm.mu.RLock()
	replacementPending := replacement.pingPending
	e.comm.mu.RUnlock()
	if !replacementPending {
		t.Fatal("retired heartbeat cleared replacement generation state")
	}
}

type countedCompletionStore struct {
	storage.Storage
	writes    atomic.Int64
	failFirst bool
	failUntil atomic.Int64
}

func (s *countedCompletionStore) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if strings.HasPrefix(key, "jobs:") {
		n := s.writes.Add(1)
		if (s.failFirst && n == 1) || n <= s.failUntil.Load() {
			return fmt.Errorf("injected completion commit interruption")
		}
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func TestClusterCompletionCommitsOnceAndRetriesFailedCommit(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("interrupted_%v", failFirst), func(t *testing.T) {
			e := newSchedulingTestEngine(t, "completion-master")
			setMasterForTest(e)
			if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
				t.Fatal(err)
			}
			conn, remote := overloadSocketPair(t)
			peer := &SlaveConnection{Hostname: "worker", Socket: conn}
			e.comm.mu.Lock()
			e.comm.slaves[peer.Hostname] = peer
			e.comm.mu.Unlock()
			live := &types.Job{ID: "bounded-commit", Event: "missing", Hostname: "worker", TimeStart: 1}
			e.mu.Lock()
			e.activeJobs[live.ID] = types.CloneJob(live)
			e.mu.Unlock()
			final := types.CloneJob(live)
			final.Complete = true
			final.TimeEnd = 2
			raw, _ := json.Marshal(final)
			var data map[string]any
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatal(err)
			}
			store := &countedCompletionStore{Storage: e.storage, failFirst: failFirst}
			e.storage = store
			e.comm.handleSlaveFinishJob(peer, data)
			expected := int64(1)
			if failFirst {
				expected = 2
			}
			if writes := store.writes.Load(); writes != expected {
				t.Fatalf("canonical completion attempts=%d want %d", writes, expected)
			}
			remote.SetReadDeadline(time.Now().Add(time.Second))
			var ack map[string]any
			if err := remote.ReadJSON(&ack); err != nil {
				t.Fatal(err)
			}
			if ack["type"] != "finish_job_ack" {
				t.Fatalf("unexpected acknowledgement %#v", ack)
			}
			if _, ok := e.GetDeadJob(live.ID); !ok {
				t.Fatal("completion was not published")
			}
			if _, ok := e.GetActiveJobs()[live.ID]; ok {
				t.Fatal("completed job remained active")
			}
			// A lost acknowledgement must not turn periodic replays into
			// repeated history/notification/active-list durable commits.
			e.mu.Lock()
			e.activeJobs[live.ID] = types.CloneJob(live)
			e.mu.Unlock()
			data["code"] = float64(99)
			data["log"] = "must not overwrite a committed log"
			e.comm.handleSlaveFinishJob(peer, data)
			if err := remote.ReadJSON(&ack); err != nil {
				t.Fatal(err)
			}
			if ack["type"] != "finish_job_ack" {
				t.Fatalf("unexpected replay acknowledgement %#v", ack)
			}
			if writes := store.writes.Load(); writes != expected {
				t.Fatalf("ack replay rewrote completion: %d attempts", writes)
			}
			if _, ok := e.GetActiveJobs()[live.ID]; ok {
				t.Fatal("ack replay left a stale active reservation")
			}
			snapshot, ok := e.GetDeadJob(live.ID)
			if !ok || snapshot.Code != 0 {
				t.Fatal("ack replay replaced the first completed snapshot")
			}
		})
	}
}

// This handler authenticates a synthetic master only for a transport unit test;
// the production mTLS/Ed25519 boundary is exercised by the separate cluster tests.
type completionFrameReceiver struct{}

func (completionFrameReceiver) OnConnect(client *ws.Client) {
	client.SetAuthentication("master", true, true, nil, "node", "test-lease", 0)
}
func (completionFrameReceiver) OnDisconnect(*ws.Client)           {}
func (completionFrameReceiver) OnError(*ws.Client, error)         {}
func (completionFrameReceiver) OnMessage(*ws.Client, *ws.Message) {}
func TestClusterCompletionAdmissionSendsLinearFramesAndRecoveryReplaysUnacked(t *testing.T) {
	e := newSchedulingTestEngine(t, "worker")
	e.comm.wsServer = ws.NewServer(completionFrameReceiver{}, zap.NewNop())
	server := httptest.NewServer(http.HandlerFunc(e.comm.wsServer.HandleWebSocket))
	defer server.Close()
	conn, _, err := gorillaWS.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	frames := make(chan string, 100)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			var msg ws.Message
			if conn.ReadJSON(&msg) != nil {
				return
			}
			var body struct {
				ID string `json:"id"`
			}
			if msg.Type == "finish_job" && json.Unmarshal(msg.Data, &body) == nil {
				frames <- body.ID
			}
		}
	}()
	waitForCondition(t, time.Second, func() bool {
		for _, client := range e.comm.wsServer.GetClients() {
			if client.IsNodeMaster() {
				return true
			}
		}
		return false
	})
	for i := range 8 {
		job := &types.Job{ID: fmt.Sprintf("linear-%d", i), Hostname: e.Hostname(), Complete: true}
		if err := e.jobManager.enqueueRemoteCompletion(job); err != nil {
			t.Fatal(err)
		}
	}
	readBatch := func() {
		t.Helper()
		seen := map[string]bool{}
		for range 8 {
			select {
			case id := <-frames:
				if seen[id] {
					t.Fatalf("duplicate completion frame for %s", id)
				}
				seen[id] = true
			case <-time.After(time.Second):
				t.Fatal("completion frame missing")
			}
		}
	}
	readBatch()
	select {
	case id := <-frames:
		t.Fatalf("admission replayed earlier envelope %s", id)
	case <-time.After(100 * time.Millisecond):
	}
	// No acknowledgement was sent. Periodic recovery must still replay all
	// eight stable envelopes rather than silently dropping them after admission.
	e.jobManager.RetryRemoteCompletions()
	readBatch()
	rows, err := e.storage.ListGet(context.Background(), "remote_completions", 0, 0)
	if err != nil || len(rows) != 8 {
		t.Fatalf("unacknowledged outbox was lost: %d, %v", len(rows), err)
	}
	conn.Close()
	<-readDone
}

func TestClusterFailedCompletionNeverAcknowledgesBeforeRecovery(t *testing.T) {
	e := newSchedulingTestEngine(t, "completion-master")
	setMasterForTest(e)
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
		t.Fatal(err)
	}
	conn, remote := overloadSocketPair(t)
	peer := &SlaveConnection{Hostname: "worker", Socket: conn}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	live := &types.Job{ID: "interrupted-completion", Event: "missing", Hostname: "worker", TimeStart: 1}
	e.mu.Lock()
	e.activeJobs[live.ID] = types.CloneJob(live)
	e.mu.Unlock()
	final := types.CloneJob(live)
	final.Complete = true
	final.TimeEnd = 2
	raw, _ := json.Marshal(final)
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	store := &countedCompletionStore{Storage: e.storage}
	store.failUntil.Store(2)
	e.storage = store
	ack := make(chan error, 1)
	go func() {
		var frame map[string]any
		err := remote.ReadJSON(&frame)
		if err == nil && frame["type"] != "finish_job_ack" {
			err = fmt.Errorf("unexpected frame %#v", frame)
		}
		ack <- err
	}()
	e.comm.handleSlaveFinishJob(peer, data)
	if writes := store.writes.Load(); writes != 2 {
		t.Fatalf("initial commit attempts=%d", writes)
	}
	select {
	case err := <-ack:
		t.Fatalf("acknowledged failed durable commit: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cached, ok := e.GetDeadJob(live.ID)
	if !ok || cached.StorageError == "" {
		t.Fatal("pending completion did not retain storage error")
	}
	e.comm.handleSlaveFinishJob(peer, data)
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered commit was not acknowledged")
	}
	if writes := store.writes.Load(); writes != 3 {
		t.Fatalf("recovery commit attempts=%d", writes)
	}
	cached, ok = e.GetDeadJob(live.ID)
	if !ok || cached.StorageError != "" {
		t.Fatal("successful recovery did not clear pending status")
	}
}

func TestClusterQueuedSendRechecksRevokedTarget(t *testing.T) {
	for _, mode := range []string{"peer_disabled", "registry_disabled", "registry_removed"} {
		t.Run(mode, func(t *testing.T) {
			e := newSchedulingTestEngine(t, "revocation-master")
			setMasterForTest(e)
			if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
				t.Fatal(err)
			}
			e.config.ClusterAllowInsecure = false
			conn, remote := overloadSocketPair(t)
			peer := &SlaveConnection{Hostname: "worker", Socket: conn}
			e.comm.mu.Lock()
			e.comm.slaves[peer.Hostname] = peer
			e.comm.mu.Unlock()
			release := make(chan struct{})
			close(release)
			base := e.storage
			observer := &slowPeerValidationStore{Storage: base, entered: make(chan struct{}), release: release}
			e.storage = observer
			peer.writeMu.Lock()
			locked := true
			defer func() {
				if locked {
					peer.writeMu.Unlock()
				}
			}()
			sent := make(chan error, 1)
			go func() { sent <- e.comm.SendToSlave(peer.Hostname, "launch_job", map[string]any{"id": "must-not-send"}) }()
			waitForCondition(t, time.Second, func() bool { return observer.reads.Load() >= 1 })
			switch mode {
			case "peer_disabled":
				e.comm.mu.Lock()
				peer.Disabled = true
				e.comm.mu.Unlock()
			case "registry_disabled":
				raw, _ := json.Marshal(&types.Server{Hostname: "worker", Disabled: true})
				if err := base.Put(context.Background(), "servers:worker", raw); err != nil {
					t.Fatal(err)
				}
			case "registry_removed":
				if err := base.Delete(context.Background(), "servers:worker"); err != nil {
					t.Fatal(err)
				}
			}
			peer.writeMu.Unlock()
			locked = false
			select {
			case err := <-sent:
				if err == nil {
					t.Fatal("queued send accepted a revoked target")
				}
			case <-time.After(time.Second):
				t.Fatal("revoked queued send did not exit")
			}
			remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			var frame map[string]any
			if err := remote.ReadJSON(&frame); err == nil {
				t.Fatalf("revoked target received frame %#v", frame)
			}
		})
	}
}
