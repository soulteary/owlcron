package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
)

type readinessStorage struct {
	*storage.FilesystemStorage
	mu      sync.Mutex
	failure error
	block   chan struct{}
	calls   int
}

func (s *readinessStorage) Exists(ctx context.Context, key string) (bool, error) {
	s.mu.Lock()
	failure, block := s.failure, s.block
	s.calls++
	s.mu.Unlock()
	if block != nil {
		<-block
	} // Deliberately model a backend ignoring cancellation.
	if failure != nil {
		return false, failure
	}
	return s.FilesystemStorage.Exists(ctx, key)
}
func TestReadinessLifecycleAndStorage(t *testing.T) {
	e := newSchedulingTestEngine(t, "ready-worker")
	backend := &readinessStorage{FilesystemStorage: e.storage.(*storage.FilesystemStorage)}
	e.storage = backend
	if e.Readiness(context.Background()) == nil {
		t.Fatal("unstarted engine ready")
	}
	if err := e.Startup(); err != nil {
		t.Fatal(err)
	}
	// The probe must remain healthy on a non-master worker.
	e.mu.Lock()
	e.multi.Master = false
	e.multi.Slave = true
	e.mu.Unlock()
	if err := e.Readiness(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.failure = errors.New("private backend detail")
	backend.mu.Unlock()
	if e.Readiness(context.Background()) == nil {
		t.Fatal("unavailable backend ready")
	}
	backend.mu.Lock()
	backend.failure = nil
	backend.mu.Unlock()
	if err := e.Readiness(context.Background()); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	e.RequestShutdown()
	if e.Readiness(context.Background()) == nil {
		t.Fatal("draining engine ready")
	}
	if err := e.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if e.Readiness(context.Background()) == nil {
		t.Fatal("closed engine ready")
	}
}
func TestReadinessBoundsUncooperativeBackend(t *testing.T) {
	e := newSchedulingTestEngine(t, "ready-blocked")
	e.ready.Store(true)
	blocked := make(chan struct{})
	backend := &readinessStorage{FilesystemStorage: e.storage.(*storage.FilesystemStorage), block: blocked}
	e.storage = backend
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		if err := e.Readiness(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout: %v", err)
		}
		cancel()
	}
	start := time.Now()
	if err := e.Readiness(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("default timeout: %v", err)
	}
	if time.Since(start) > ReadinessTimeout+time.Second {
		t.Fatal("probe exceeded its deadline")
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("blocked backend spawned %d probes", calls)
	}
	close(blocked)
	deadline := time.Now().Add(time.Second)
	for {
		e.readinessMu.Lock()
		done := e.readinessProbe == nil
		e.readinessMu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("probe did not drain")
		}
		time.Sleep(time.Millisecond)
	}
	backend.mu.Lock()
	backend.block = nil
	backend.mu.Unlock()
	if err := e.Readiness(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReadinessFailedStartup(t *testing.T) {
	e := newSchedulingTestEngine(t, "failed-startup")
	if err := e.storage.Put(context.Background(), "global/state", []byte("invalid JSON")); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(); err == nil {
		t.Fatal("startup unexpectedly succeeded")
	}
	if err := e.Readiness(context.Background()); err == nil {
		t.Fatal("failed startup was ready")
	}
}
