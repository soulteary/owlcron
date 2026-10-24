package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

// A previous TCP reader may finish decoding after a leadership transition. Its
// peer object must not regain authority when this process becomes master again.
func TestClusterStalePeerCannotMutateAfterLeadershipRecovery(t *testing.T) {
	e := newSchedulingTestEngine(t, "recovering-master")
	setMasterForTest(e)
	old := &SlaveConnection{Hostname: "worker", ActiveJobs: map[string]*types.Job{}, Data: map[string]interface{}{}}
	e.comm.mu.Lock()
	e.comm.slaves[old.Hostname] = old
	e.comm.mu.Unlock()
	e.mu.Lock()
	e.activeJobs["running"] = &types.Job{ID: "running", Hostname: "worker", Progress: 0.1}
	e.mu.Unlock()
	e.comm.ResetCluster()
	replacement := &SlaveConnection{Hostname: "worker", ActiveJobs: map[string]*types.Job{}, Data: map[string]interface{}{}}
	e.comm.mu.Lock()
	e.comm.slaves[replacement.Hostname] = replacement
	e.comm.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.comm.handleSlaveMessage(old, map[string]interface{}{"type": "job_update", "data": map[string]interface{}{"id": "running", "progress": 0.9}})
		}()
	}
	wg.Wait()
	e.mu.RLock()
	progress := e.activeJobs["running"].Progress
	e.mu.RUnlock()
	if progress != 0.1 {
		t.Fatalf("stale peer modified recovered job: %v", progress)
	}
}

func TestClusterExpiredLeadershipCannotAcceptWorkerUpdate(t *testing.T) {
	e := newSchedulingTestEngine(t, "expired-master")
	setMasterForTest(e)
	peer := &SlaveConnection{Hostname: "worker", ActiveJobs: map[string]*types.Job{}, Data: map[string]interface{}{}}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	e.mu.Lock()
	e.activeJobs["running"] = &types.Job{ID: "running", Hostname: "worker", Progress: 0.1}
	e.mu.Unlock()
	lease := leadershipLease{Hostname: "new-master", Owner: "replacement", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := e.storage.Put(e.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	e.comm.handleSlaveMessage(peer, map[string]interface{}{"type": "job_update", "data": map[string]interface{}{"id": "running", "progress": 0.9}})
	e.mu.RLock()
	progress := e.activeJobs["running"].Progress
	e.mu.RUnlock()
	if progress != 0.1 {
		t.Fatalf("expired master applied worker update: %v", progress)
	}
}

func TestClusterWorkerStatusCannotPromoteItselfOrDemoteMaster(t *testing.T) {
	e := newSchedulingTestEngine(t, "node-z")
	setMasterForTest(e)
	peer := &SlaveConnection{Hostname: "node-a", ActiveJobs: map[string]*types.Job{}, Data: map[string]interface{}{}}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.comm.handleSlaveMessage(peer, map[string]interface{}{"type": "status", "data": map[string]interface{}{"master": true, "epoch": float64(time.Now().Unix())}})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker role claim deadlocked cluster reset")
	}
	if !e.IsMaster() || !e.ValidateNodeLeadership(e.Hostname(), e.LeadershipToken()) {
		t.Fatal("unsigned worker status demoted lease owner")
	}
}

func TestClusterProbeTrustsLeaseWithoutContactingPeerURL(t *testing.T) {
	e := newSchedulingTestEngine(t, "node-b")
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.Write([]byte(`{"master":true}`)) }))
	defer server.Close()
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "node-a", IP: server.URL}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		owner   string
		expires int64
		want    bool
	}{{"owner", time.Now().Add(time.Minute).UnixNano(), true}, {"owner", time.Now().Add(-time.Second).UnixNano(), false}, {"", time.Now().Add(time.Minute).UnixNano(), false}} {
		raw, _ := json.Marshal(leadershipLease{Hostname: "node-a", Owner: test.owner, Expires: test.expires})
		if err := e.storage.Put(e.ctx, "global/master_lease", raw); err != nil {
			t.Fatal(err)
		}
		if got := e.probeServer("node-a"); got != test.want {
			t.Errorf("probe=%v want %v", got, test.want)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("leadership probe contacted untrusted peer URL")
	}
}
