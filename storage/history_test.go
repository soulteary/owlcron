package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/soulteary/owlcron/types"
)

type historyFaultStore struct {
	Storage
	PrefixKeyLister
	failPut          string
	failDelete       string
	failRecordDelete string
}

func (s *historyFaultStore) Delete(ctx context.Context, key string) error {
	if key == s.failRecordDelete {
		return errors.New("injected record delete failure")
	}
	return s.Storage.Delete(ctx, key)
}

func TestHistoryGarbageCollectionPropagatesDeleteFailure(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	if err := s.Put(ctx, historyKey("missing"), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	fault := &historyFaultStore{Storage: s, PrefixKeyLister: s, failRecordDelete: historyKey("missing")}
	if _, _, err := HistoryPage(ctx, fault, "", 10); err == nil {
		t.Fatal("swallowed GC delete failure")
	}
	if err := s.Put(ctx, "deleted_jobs:missing", []byte(`{"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := HistoryUpsert(ctx, fault, &types.Job{ID: "missing"}); err == nil {
		t.Fatal("swallowed upsert GC delete failure")
	}
}

type historyBlockedPutStore struct {
	Storage
	PrefixKeyLister
	blocked chan struct{}
	release chan struct{}
}

func (s *historyBlockedPutStore) Put(ctx context.Context, key string, data []byte) error {
	if strings.HasPrefix(key, historyPrefix) {
		close(s.blocked)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Storage.Put(ctx, key, data)
}

func TestHistoryUpsertCleansIndexWhenDeletionCommitsDuringPut(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	j := &types.Job{ID: "racing", Complete: true}
	saveHistoryJob(t, s, j)
	b := &historyBlockedPutStore{Storage: s, PrefixKeyLister: s, blocked: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- HistoryUpsert(ctx, b, j) }()
	<-b.blocked
	if err := s.Put(ctx, "deleted_jobs:"+j.ID, []byte(`{"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := HistoryRemove(ctx, s, j.ID); err != nil {
		t.Fatal(err)
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if exists, err := s.Exists(ctx, historyKey(j.ID)); err != nil || exists {
		t.Fatalf("residual index exists=%v err=%v", exists, err)
	}
}

func TestHistoryPageGarbageCollectsStaleIndices(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	for _, id := range []string{"missing", "deleted", "marker"} {
		if err := s.Put(ctx, historyKey(id), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put(ctx, "jobs:deleted", []byte(`{"id":"deleted","deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	saveHistoryJob(t, s, &types.Job{ID: "marker", Complete: true})
	if err := s.Put(ctx, "deleted_jobs:marker", []byte(`{"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	jobs, _, err := HistoryPage(ctx, s, "", 10)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	keys, err := s.ListKeysPrefix(ctx, historyPrefix)
	if err != nil || len(keys) != 0 {
		t.Fatalf("residual keys=%v err=%v", keys, err)
	}
}

func (s *historyFaultStore) Put(ctx context.Context, key string, data []byte) error {
	if key == s.failPut {
		return errors.New("injected put failure")
	}
	return s.Storage.Put(ctx, key, data)
}
func (s *historyFaultStore) ListDelete(ctx context.Context, key string, recursive bool) error {
	if key == s.failDelete {
		return errors.New("injected delete failure")
	}
	return s.Storage.ListDelete(ctx, key, recursive)
}

func historyStore(t *testing.T) *FilesystemStorage {
	t.Helper()
	s, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func saveHistoryJob(t *testing.T, s Storage, j *types.Job) {
	t.Helper()
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Put(context.Background(), "jobs:"+j.ID, b); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryMigrationRecovery(t *testing.T) {
	for _, stage := range []string{"copy", "global-delete", "event-delete"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			s := historyStore(t)
			for _, id := range []string{"one", "two"} {
				j := &types.Job{ID: id, Event: "event", TimeEnd: 10, Complete: true}
				saveHistoryJob(t, s, j)
				if err := s.ListPush(ctx, "logs/completed", j); err != nil {
					t.Fatal(err)
				}
				if err := s.ListPush(ctx, "logs/events/event", j); err != nil {
					t.Fatal(err)
				}
			}
			fault := &historyFaultStore{Storage: s, PrefixKeyLister: s}
			switch stage {
			case "copy":
				fault.failPut = historyKey("two")
			case "global-delete":
				fault.failDelete = "logs/completed"
			case "event-delete":
				fault.failDelete = "logs/events/event"
			}
			if err := HistoryMigrate(ctx, fault); err == nil {
				t.Fatal("expected failure")
			}
			// A new completion is independent of the interrupted migration.
			j := &types.Job{ID: "new", TimeStart: 20, TimeEnd: 20, Complete: true}
			saveHistoryJob(t, s, j)
			if err := HistoryUpsert(ctx, s, j); err != nil {
				t.Fatal(err)
			}
			if err := HistoryMigrate(ctx, s); err != nil {
				t.Fatal(err)
			}
			if err := HistoryMigrate(ctx, s); err != nil {
				t.Fatal(err)
			}
			jobs, err := HistoryJobs(ctx, s)
			if err != nil || len(jobs) != 3 {
				t.Fatalf("jobs=%v err=%v", jobs, err)
			}
			if jobs[0].ID != "new" {
				t.Fatal("not completion sorted")
			}
			for _, key := range []string{"logs/completed", "logs/events/event"} {
				exists, err := s.Exists(ctx, key)
				if err != nil || exists {
					t.Fatalf("legacy index retained: %s %v", key, err)
				}
			}
		})
	}
}

func TestHistoryConcurrentUpsertsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	const count = 80
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		j := &types.Job{ID: fmt.Sprintf("job-%03d", i), TimeEnd: float64(i), Complete: true}
		saveHistoryJob(t, s, j)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 3; n++ {
				if err := HistoryUpsert(ctx, s, j); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	jobs, err := HistoryJobs(ctx, s)
	if err != nil || len(jobs) != count {
		t.Fatalf("count=%d err=%v", len(jobs), err)
	}
	keys, err := s.ListKeysPrefix(ctx, historyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		b, err := s.Get(ctx, key)
		if err != nil || len(b) > 64 {
			t.Fatalf("unbounded entry: %d %v", len(b), err)
		}
	}
	if exists, _ := s.Exists(ctx, "logs/completed"); exists {
		t.Fatal("legacy array written")
	}
}

func TestHistoryDeleteCannotBeResurrectedByMigrationOrCompletion(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	j := &types.Job{ID: "deleted", Complete: true}
	saveHistoryJob(t, s, j)
	if err := s.ListPush(ctx, "logs/completed", j); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "jobs:deleted", []byte(`{"id":"deleted","complete":true,"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := HistoryUpsert(ctx, s, j); err != nil {
				t.Error(err)
			}
			if err := HistoryRemove(ctx, s, j.ID); err != nil {
				t.Error(err)
			}
		}()
	}
	if err := HistoryMigrate(ctx, s); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	// Even an index recreated after deletion stays invisible.
	if err := HistoryUpsert(ctx, s, j); err != nil {
		t.Fatal(err)
	}
	jobs, err := HistoryJobs(ctx, s)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("resurrected: %v %v", jobs, err)
	}
}

func TestHistoryReadFailsClosedOnCorruptCanonicalAndListFailure(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	j := &types.Job{ID: "bad"}
	if err := HistoryUpsert(ctx, s, j); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "jobs:bad", []byte("broken")); err != nil {
		t.Fatal(err)
	}
	if _, err := HistoryJobs(ctx, s); err == nil {
		t.Fatal("accepted corruption")
	}
	if err := s.Put(ctx, "logs/completed", []byte("broken")); err != nil {
		t.Fatal(err)
	}
	if err := HistoryMigrate(ctx, s); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("migration error: %v", err)
	}
	if exists, _ := s.Exists(ctx, "logs/completed"); !exists {
		t.Fatal("deleted corrupt migration source")
	}
}

func TestHistoryMigrationSeedsEventOnlyRecords(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	j := &types.Job{ID: "event-only", Event: "orphan", Complete: true}
	saveHistoryJob(t, s, j)
	if err := s.ListPush(ctx, "logs/events/orphan", j); err != nil {
		t.Fatal(err)
	}
	if err := HistoryMigrate(ctx, s); err != nil {
		t.Fatal(err)
	}
	jobs, err := HistoryJobs(ctx, s)
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
}

func TestHistoryPageAdvancesOverMissingDeletedAndRemovedCursor(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	for _, id := range []string{"a", "b", "c", "d"} {
		j := &types.Job{ID: id, Complete: true}
		if err := HistoryUpsert(ctx, s, j); err != nil {
			t.Fatal(err)
		}
		if id != "a" {
			saveHistoryJob(t, s, j)
		}
	}
	if err := s.Put(ctx, "deleted_jobs:b", []byte(`{"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	// A corrupt record after this batch must not be read by the first page.
	if err := s.Put(ctx, "jobs:d", []byte("bad")); err != nil {
		t.Fatal(err)
	}
	jobs, next, err := HistoryPage(ctx, s, "", 2)
	if err != nil || len(jobs) != 0 || next != historyKey("b") {
		t.Fatalf("first page=%v next=%q err=%v", jobs, next, err)
	}
	if err := HistoryRemove(ctx, s, "b"); err != nil {
		t.Fatal(err)
	}
	jobs, next, err = HistoryPage(ctx, s, next, 1)
	if err != nil || len(jobs) != 1 || jobs[0].ID != "c" || next != historyKey("c") {
		t.Fatalf("second page=%v next=%q err=%v", jobs, next, err)
	}
	saveHistoryJob(t, s, &types.Job{ID: "d", Complete: true})
	jobs, next, err = HistoryPage(ctx, s, next, 1)
	if err != nil || len(jobs) != 1 || next != "" {
		t.Fatalf("last page=%v next=%q err=%v", jobs, next, err)
	}
}

func TestHistoryRejectsStorageWithoutPrefixEnumeration(t *testing.T) {
	ctx := context.Background()
	store := struct{ Storage }{historyStore(t)}
	if err := HistoryMigrate(ctx, store); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("migration: %v", err)
	}
	if _, err := HistoryJobs(ctx, store); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("history: %v", err)
	}
	if _, _, err := HistoryPage(ctx, store, "", 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("page: %v", err)
	}
}

func TestHistorySkipsIncompleteCanonicalWithoutRemovingIndex(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	j := &types.Job{ID: "pending", Complete: false}
	saveHistoryJob(t, s, j)
	if err := HistoryUpsert(ctx, s, j); err != nil {
		t.Fatal(err)
	}
	jobs, err := HistoryJobs(ctx, s)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("history=%v err=%v", jobs, err)
	}
	jobs, _, err = HistoryPage(ctx, s, "", 10)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("page=%v err=%v", jobs, err)
	}
	if exists, err := s.Exists(ctx, historyKey(j.ID)); err != nil || !exists {
		t.Fatalf("incomplete index removed: %v %v", exists, err)
	}
	j.Complete = true
	saveHistoryJob(t, s, j)
	jobs, err = HistoryJobs(ctx, s)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("completed history=%v err=%v", jobs, err)
	}
}
