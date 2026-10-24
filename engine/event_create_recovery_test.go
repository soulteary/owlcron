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

type eventCreateFailureStorage struct {
	storage.Storage
	failKey string
}

func (s *eventCreateFailureStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if key == s.failKey {
		return errors.New("injected event creation failure")
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

func TestCreateEventIndexFailureDoesNotReserveID(t *testing.T) {
	e := jobTestEngine(t)
	store := &eventCreateFailureStorage{Storage: e.storage, failKey: "events"}
	e.storage = store
	event := &types.Event{ID: "retry-index", Title: "original"}
	if err := e.scheduler.CreateEvent(event); err == nil {
		t.Fatal("index failure was hidden")
	}
	if _, err := store.Get(e.ctx, "events:"+event.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed index write reserved ID: %v", err)
	}
	if len(e.scheduler.events) != 0 {
		t.Fatal("failed creation entered scheduler")
	}
	store.failKey = ""
	// Reload first to exercise a retry after losing all in-memory state.
	if err := e.scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	if err := e.scheduler.CreateEvent(event); err != nil {
		t.Fatalf("retry could not create event: %v", err)
	}
	if err := e.scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	if got := e.scheduler.events[event.ID]; got == nil || got.Title != event.Title {
		t.Fatalf("successful retry not loadable: %+v", got)
	}
	items, err := store.ListGet(e.ctx, "events", 0, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("retry duplicated index: %v %v", items, err)
	}
}

func TestCreateEventCanonicalFailureLeavesRetryableIndex(t *testing.T) {
	e := jobTestEngine(t)
	event := &types.Event{ID: "retry-canonical", Title: "original"}
	store := &eventCreateFailureStorage{Storage: e.storage, failKey: "events:" + event.ID}
	e.storage = store
	if err := e.scheduler.CreateEvent(event); err == nil {
		t.Fatal("canonical failure was hidden")
	}
	if err := e.scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	if len(e.scheduler.events) != 0 {
		t.Fatal("dangling index loaded as an event")
	}
	store.failKey = ""
	if err := e.scheduler.CreateEvent(event); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if err := e.scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	if len(e.scheduler.events) != 1 {
		t.Fatal("retry did not produce one loadable event")
	}
}

func TestCreateEventRejectsExistingDurableRecordWithoutChangingIndex(t *testing.T) {
	e := jobTestEngine(t)
	event := &types.Event{ID: "existing", Title: "original"}
	if err := e.scheduler.CreateEvent(event); err != nil {
		t.Fatal(err)
	}
	// Simulate another scheduler that has not loaded the persisted event yet.
	delete(e.scheduler.events, event.ID)
	before, err := e.storage.Get(e.ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []*types.Event{event, {ID: event.ID, Title: "replacement"}} {
		if err := e.scheduler.CreateEvent(candidate); err == nil {
			t.Fatal("duplicate creation accepted")
		}
	}
	after, err := e.storage.Get(e.ctx, "events")
	if err != nil || string(before) != string(after) {
		t.Fatalf("duplicate changed index: %s %v", after, err)
	}
	raw, err := e.storage.Get(e.ctx, "events:"+event.ID)
	if err != nil {
		t.Fatal(err)
	}
	var got types.Event
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Title != "original" {
		t.Fatalf("duplicate overwrote canonical record: %+v", got)
	}
}

// Force both schedulers to observe an absent canonical record before either
// reaches CAS, rather than relying on goroutine scheduling for this race.
type concurrentEventCreateStorage struct {
	storage.Storage
	mu    sync.Mutex
	reads int
	ready chan struct{}
}

func (s *concurrentEventCreateStorage) Get(ctx context.Context, key string) ([]byte, error) {
	raw, err := s.Storage.Get(ctx, key)
	if key == "events:concurrent" {
		s.mu.Lock()
		s.reads++
		n := s.reads
		if n == 2 {
			close(s.ready)
		}
		s.mu.Unlock()
		if n <= 2 {
			select {
			case <-s.ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return raw, err
}
func (s *concurrentEventCreateStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func TestCreateEventConcurrentSchedulersCannotReplaceWinner(t *testing.T) {
	e := jobTestEngine(t)
	e.storage = &concurrentEventCreateStorage{Storage: e.storage, ready: make(chan struct{})}
	other, err := NewScheduler(e, e.logger)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for i, scheduler := range []*Scheduler{e.scheduler, other} {
		event := &types.Event{ID: "concurrent", Title: []string{"first", "second"}[i]}
		go func(s *Scheduler, event *types.Event) { results <- s.CreateEvent(event) }(scheduler, event)
	}
	successes := 0
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				successes++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent create deadlocked")
		}
	}
	if successes != 1 {
		t.Fatalf("wanted exactly one winner, got %d", successes)
	}
	if err := e.scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	if len(e.scheduler.events) != 1 {
		t.Fatal("winning event not loadable")
	}
	items, err := e.storage.ListGet(e.ctx, "events", 0, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("concurrent index duplicated: %v %v", items, err)
	}
	data, err := json.Marshal(items[0])
	if err != nil {
		t.Fatal(err)
	}
	var index map[string]interface{}
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if len(index) != 1 || index["id"] != "concurrent" {
		t.Fatalf("index retained competing payload: %s", data)
	}
}
