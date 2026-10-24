package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestMetricsSnapshotUsesOnlyMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No storage is configured: any storage access would fail this test.
	e := &Engine{ctx: ctx, multi: &types.MultiServer{Master: true}, state: &types.State{Enabled: true}, activeJobs: map[string]*types.Job{
		"sensitive-id": {Pending: false}, "secret-id": {Pending: true},
	}, queue: &Queue{internalQueue: map[string]*types.QueueTask{"private-task": {}}}}
	snapshot := e.MetricsSnapshot()
	if !snapshot.Master || snapshot.Worker || !snapshot.SchedulerEnabled || snapshot.ActiveJobs != 1 || snapshot.PendingJobs != 1 || snapshot.InternalQueueTasks != 1 || snapshot.ShutdownRequested {
		t.Fatalf("bad snapshot: %+v", snapshot)
	}
	e.leaseUntil = time.Now().Add(-time.Second).UnixNano()
	if e.MetricsSnapshot().Master {
		t.Fatal("expired lease reported as master")
	}
	cancel()
	if !e.MetricsSnapshot().ShutdownRequested {
		t.Fatal("shutdown missing")
	}
}

func TestMetricsSnapshotConcurrentUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &Engine{ctx: ctx, multi: &types.MultiServer{}, state: &types.State{}, activeJobs: map[string]*types.Job{}, queue: &Queue{internalQueue: map[string]*types.QueueTask{}}}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			e.mu.Lock()
			e.multi.Slave = i%2 == 0
			e.state.Enabled = i%2 == 0
			e.activeJobs["one"] = &types.Job{Pending: i%2 == 0}
			e.mu.Unlock()
			e.queue.mu.Lock()
			e.queue.internalQueue["one"] = &types.QueueTask{}
			e.queue.mu.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		s := e.MetricsSnapshot()
		if s.ActiveJobs+s.PendingJobs > 1 || s.InternalQueueTasks > 1 {
			t.Fatalf("invalid snapshot %+v", s)
		}
	}
	wg.Wait()
}
