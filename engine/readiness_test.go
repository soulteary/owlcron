package engine

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
)

type readinessStorage struct {
	*storage.FilesystemStorage
	mu                 sync.Mutex
	failure            error
	block              chan struct{}
	calls              int
	keys               []string
	started            chan context.Context
	ignoreCancellation bool
}

func (s *readinessStorage) Exists(ctx context.Context, key string) (bool, error) {
	s.mu.Lock()
	failure, block := s.failure, s.block
	started, ignoreCancellation := s.started, s.ignoreCancellation
	s.keys = append(s.keys, key)
	s.calls++
	s.mu.Unlock()
	if started != nil {
		started <- ctx
	}
	if block != nil {
		<-block
	} // Deliberately model a backend ignoring cancellation.
	if failure != nil {
		return false, failure
	}
	if ignoreCancellation {
		return true, nil
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
	if err := e.initializeReadiness(); err != nil {
		t.Fatal(err)
	}
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

// Real Exists implementations read the requested object. Verify readiness does
// not request the growing state object or rewrite the startup marker.
func TestReadinessDoesNotReadOrModifyApplicationState(t *testing.T) {
	e := newSchedulingTestEngine(t, "ready-small-probe")
	if err := e.initializeReadiness(); err != nil {
		t.Fatal(err)
	}
	e.ready.Store(true)
	original := bytes.Repeat([]byte("state"), 1024*1024)
	if err := e.storage.Put(context.Background(), "global/state", original); err != nil {
		t.Fatal(err)
	}
	backend := &readinessStorage{FilesystemStorage: e.storage.(*storage.FilesystemStorage)}
	e.storage = backend
	for i := 0; i < 2; i++ {
		if err := e.Readiness(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	backend.mu.Lock()
	keys := append([]string(nil), backend.keys...)
	backend.mu.Unlock()
	if len(keys) != 2 {
		t.Fatalf("probe calls: %v", keys)
	}
	for _, key := range keys {
		if key != readinessProbeKey {
			t.Fatalf("read application key %q", key)
		}
	}
	actual, err := backend.Get(context.Background(), "global/state")
	if err != nil || !bytes.Equal(actual, original) {
		t.Fatalf("application state changed: %v", err)
	}
	marker, err := backend.Get(context.Background(), readinessProbeKey)
	if err != nil || string(marker) != "1" {
		t.Fatalf("probe changed marker: %q, %v", marker, err)
	}
	if err := backend.Delete(context.Background(), readinessProbeKey); err != nil {
		t.Fatal(err)
	}
	if err := e.Readiness(context.Background()); err == nil {
		t.Fatal("missing marker reported ready")
	}
}

func TestReadinessRejectsLateSuccessfulProbe(t *testing.T) {
	e := newSchedulingTestEngine(t, "ready-late-result")
	if err := e.initializeReadiness(); err != nil {
		t.Fatal(err)
	}
	e.ready.Store(true)
	blocked := make(chan struct{})
	started := make(chan context.Context, 1)
	backend := &readinessStorage{FilesystemStorage: e.storage.(*storage.FilesystemStorage), block: blocked, started: started, ignoreCancellation: true}
	e.storage = backend
	// Start the shared probe, but cancel only this caller's wait.
	caller, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- e.Readiness(caller) }()
	probeCtx := <-started
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation: %v", err)
	}
	<-probeCtx.Done()
	// Done is evaluated only after the caller joins the shared probe.
	waiter := &readinessWaitContext{Context: context.Background(), entered: make(chan struct{})}
	later := make(chan error, 1)
	go func() { later <- e.Readiness(waiter) }()
	<-waiter.entered
	close(blocked)
	if err := <-later; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late success was accepted: %v", err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("shared expired probe was replaced: %d calls", calls)
	}
}

// Readiness evaluates the caller's Done channel when it begins waiting.
type readinessWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *readinessWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

type readinessInitializationStorage struct {
	*storage.FilesystemStorage
	failure error
	block   chan struct{}
	started chan context.Context
}

func (s *readinessInitializationStorage) Put(ctx context.Context, key string, data []byte) error {
	if key != readinessProbeKey {
		return s.FilesystemStorage.Put(ctx, key, data)
	}
	if s.started != nil {
		s.started <- ctx
	}
	if s.block != nil {
		<-s.block
	}
	return s.failure
}

func TestReadinessMarkerInitializationFailure(t *testing.T) {
	e := newSchedulingTestEngine(t, "ready-marker-failure")
	e.storage = &readinessInitializationStorage{FilesystemStorage: e.storage.(*storage.FilesystemStorage), failure: errors.New("marker write denied")}
	if err := e.Startup(); err == nil {
		t.Fatal("startup accepted failed marker write")
	}
	if err := e.Readiness(context.Background()); err == nil {
		t.Fatal("failed startup reported ready")
	}
}

func TestReadinessMarkerInitializationBoundsUncooperativeBackend(t *testing.T) {
	e := newSchedulingTestEngine(t, "ready-marker-blocked")
	blocked := make(chan struct{})
	started := make(chan context.Context, 1)
	e.storage = &readinessInitializationStorage{FilesystemStorage: e.storage.(*storage.FilesystemStorage), block: blocked, started: started}
	result := make(chan error, 1)
	go func() { result <- e.initializeReadiness() }()
	ctx := <-started
	<-ctx.Done()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("initialization timeout: %v", err)
		}
	case <-time.After(time.Second):
		close(blocked)
		t.Fatal("initialization waited for uncooperative backend")
	}
	close(blocked) // Release a deliberately late successful write.
	if err := e.Readiness(context.Background()); err == nil {
		t.Fatal("timed-out initialization reported ready")
	}
}
