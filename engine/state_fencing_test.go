package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

type pausedStateStorage struct {
	storage.Storage
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (s *pausedStateStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if key == "global/state" {
		s.once.Do(func() { close(s.entered); <-s.resume })
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func TestPausedOldMasterCannotOverwriteNewStateGeneration(t *testing.T) {
	old := newSchedulingTestEngine(t, "node-a")
	next := newSchedulingTestEngine(t, "node-b")
	old.config.MasterPingTimeout = 1
	next.config.MasterPingTimeout = 1
	shareSchedulingStore(t, old, next)
	old.goMaster()
	old.mu.Lock()
	old.state.Enabled = false
	old.state.JobCodes["event"] = 1
	old.state.Stats["jobs_completed"] = 10
	old.mu.Unlock()
	paused := &pausedStateStorage{Storage: old.storage, entered: make(chan struct{}), resume: make(chan struct{})}
	old.storage = paused
	errCh := make(chan error, 1)
	go func() { errCh <- old.saveStateToStorage() }()
	select {
	case <-paused.entered:
	case <-time.After(time.Second):
		t.Fatal("old write did not reach the pause boundary")
	}
	time.Sleep(1100 * time.Millisecond)
	next.goMaster()
	if !next.IsMaster() {
		close(paused.resume)
		t.Fatal("new generation failed to take over")
	}
	next.mu.Lock()
	next.state.Enabled = true
	next.state.JobCodes["event"] = 0
	next.state.Stats["jobs_completed"] = 20
	next.mu.Unlock()
	if err := next.saveStateToStorage(); err != nil {
		close(paused.resume)
		t.Fatal(err)
	}
	close(paused.resume)
	if err := <-errCh; !errors.Is(err, errStaleStateOwner) {
		t.Fatalf("stale write was accepted: %v", err)
	}
	raw, err := next.storage.Get(context.Background(), "global/state")
	if err != nil {
		t.Fatal(err)
	}
	var stored types.State
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Enabled || stored.JobCodes["event"] != 0 || stored.Stats["jobs_completed"] != 20 {
		t.Fatalf("old generation replaced new state: %+v", stored)
	}
}

func TestInterruptedPromotionDoesNotStrandGlobalState(t *testing.T) {
	first := newSchedulingTestEngine(t, "node-a")
	middle := newSchedulingTestEngine(t, "node-b")
	another := newSchedulingTestEngine(t, "node-c")
	last := newSchedulingTestEngine(t, "node-d")
	for _, e := range []*Engine{first, middle, another, last} {
		e.config.MasterPingTimeout = 1
	}
	shareSchedulingStore(t, first, middle)
	shareSchedulingStore(t, first, another)
	shareSchedulingStore(t, first, last)
	first.goMaster()
	time.Sleep(1100 * time.Millisecond)
	if err := middle.claimLeadership(); err != nil {
		t.Fatal(err)
	}
	// The middle candidate dies after claiming the lease, before installing a
	// state ownership generation. The third candidate still must recover A.
	middle.cancel()
	time.Sleep(1100 * time.Millisecond)
	if err := another.claimLeadership(); err != nil {
		t.Fatal(err)
	}
	another.cancel()
	time.Sleep(1100 * time.Millisecond)
	last.goMaster()
	if !last.IsMaster() {
		t.Fatal("interrupted promotion permanently stranded the previous global state")
	}
	raw, err := last.storage.Get(context.Background(), "global/master_lease")
	var lease leadershipLease
	if err != nil || json.Unmarshal(raw, &lease) != nil || len(lease.PreviousOwners) != 0 {
		t.Fatal("committed generation retained obsolete predecessor chain")
	}
}

func TestRemovedEventStateIsNotRestoredByCursorMerge(t *testing.T) {
	e := newSchedulingTestEngine(t, "removed-event")
	e.goMaster()
	event := saveSchedulingFixture(t, e, "remove-state")
	e.mu.Lock()
	e.state.Cursors[event.ID] = 1791331260
	e.state.JobCodes[event.ID] = 1
	e.state.Robins[event.ID] = 2
	e.mu.Unlock()
	if err := e.saveStateToStorage(); err != nil {
		t.Fatal(err)
	}
	if err := e.scheduler.RemoveEvent(event.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.saveStateToStorage(); err != nil {
		t.Fatal(err)
	}
	if err := e.loadStateFromStorage(); err != nil {
		t.Fatal(err)
	}
	state := e.GetState()
	if len(state.Cursors) != 0 || len(state.JobCodes) != 0 || len(state.Robins) != 0 {
		t.Fatalf("removed event metadata restored: %+v", state)
	}
}
