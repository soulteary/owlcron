package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

type eventIndexRecoveryStorage struct {
	storage.Storage
	failEnumeration   bool
	failIndex         bool
	deleteAfterList   string
	deleteBeforeIndex string
}

func (s *eventIndexRecoveryStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	if s.failEnumeration {
		return nil, errors.New("injected enumeration failure")
	}
	keys, err := s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
	if err == nil && s.deleteAfterList != "" {
		err = s.Storage.Delete(ctx, "events:"+s.deleteAfterList)
	}
	return keys, err
}

func (s *eventIndexRecoveryStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if key == "events" && s.failIndex {
		return errors.New("injected index recovery failure")
	}
	if key == "events" && s.deleteBeforeIndex != "" {
		if err := s.Storage.Delete(ctx, "events:"+s.deleteBeforeIndex); err != nil {
			return err
		}
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

func TestLegacyEventRepairDoesNotResurrectConcurrentlyDeletedEvent(t *testing.T) {
	for _, stage := range []string{"after enumeration", "before index commit"} {
		t.Run(stage, func(t *testing.T) {
			e := jobTestEngine(t)
			writeOrphanEvent(t, e.storage, "deleted")
			store := &eventIndexRecoveryStorage{Storage: e.storage}
			if stage == "after enumeration" {
				store.deleteAfterList = "deleted"
			} else {
				store.deleteBeforeIndex = "deleted"
			}
			s := freshEventScheduler(t, store)
			if err := s.LoadEventsFromStorage(); err != nil {
				t.Fatal(err)
			}
			if got := s.GetAllEvents(); len(got) != 0 {
				t.Fatalf("repair resurrected deleted event: %#v", got)
			}
			if _, err := e.storage.Get(context.Background(), "events:deleted"); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("repair recreated deleted canonical record: %v", err)
			}
		})
	}
}

func writeOrphanEvent(t *testing.T, store storage.Storage, id string) {
	t.Helper()
	raw, err := json.Marshal(&types.Event{ID: id, Title: "original " + id, Category: "private", Params: map[string]interface{}{"value": "preserved"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), "events:"+id, raw); err != nil {
		t.Fatal(err)
	}
}

func freshEventScheduler(t *testing.T, store storage.Storage) *Scheduler {
	t.Helper()
	s, err := NewScheduler(&Engine{storage: store}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestLoadEventsRepairsLegacyOrphansAfterRestart(t *testing.T) {
	e := jobTestEngine(t)
	ctx := context.Background()
	indexed := &types.Event{ID: "indexed", Title: "existing"}
	if err := e.scheduler.CreateEvent(indexed); err != nil {
		t.Fatal(err)
	}
	writeOrphanEvent(t, e.storage, "orphan-a")
	writeOrphanEvent(t, e.storage, "orphan-b")
	before, err := e.storage.Get(ctx, "events:orphan-a")
	if err != nil {
		t.Fatal(err)
	}
	// A stale index left by an interrupted new create is not a runnable event.
	if err := upsertStorageList(ctx, e.storage, "events", "uncommitted", map[string]string{"id": "uncommitted"}, false); err != nil {
		t.Fatal(err)
	}
	s := freshEventScheduler(t, e.storage)
	for attempt := 0; attempt < 2; attempt++ {
		if err := s.LoadEventsFromStorage(); err != nil {
			t.Fatal(err)
		}
		if got := s.GetAllEvents(); len(got) != 3 || got["orphan-a"].Category != "private" || got["orphan-b"].Params["value"] != "preserved" {
			t.Fatalf("canonical events were not recovered: %#v", got)
		}
		items, err := e.storage.ListGet(ctx, "events", 0, 0)
		if err != nil || len(items) != 4 {
			t.Fatalf("index repair duplicated or removed entries: %d, %v", len(items), err)
		}
		// The repaired index must survive another scheduler instance.
		s = freshEventScheduler(t, e.storage)
	}
	after, err := e.storage.Get(ctx, "events:orphan-a")
	if err != nil || string(before) != string(after) {
		t.Fatal("repair changed the canonical event")
	}
}

func TestLegacyEventRepairFailuresPreserveScheduleAndRemainRetryable(t *testing.T) {
	for _, stage := range []string{"enumeration", "index"} {
		t.Run(stage, func(t *testing.T) {
			e := jobTestEngine(t)
			writeOrphanEvent(t, e.storage, "orphan")
			store := &eventIndexRecoveryStorage{Storage: e.storage, failEnumeration: stage == "enumeration", failIndex: stage == "index"}
			s := freshEventScheduler(t, store)
			s.events["previous"] = &types.Event{ID: "previous"}
			if err := s.LoadEventsFromStorage(); err == nil {
				t.Fatal("expected injected repair failure")
			}
			if got := s.GetAllEvents(); len(got) != 1 || got["previous"] == nil {
				t.Fatal("failed recovery replaced the previous schedule")
			}
			store.failEnumeration, store.failIndex = false, false
			if err := s.LoadEventsFromStorage(); err != nil {
				t.Fatal(err)
			}
			if got := s.GetAllEvents(); len(got) != 1 || got["orphan"] == nil {
				t.Fatal("recovery did not succeed after storage recovered")
			}
		})
	}
}

func TestLegacyEventRepairRejectsInvalidCanonicalWithoutPartialIndex(t *testing.T) {
	for i, raw := range []string{`{`, `{"id":"other"}`, `{"title":"missing ID"}`} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			e := jobTestEngine(t)
			writeOrphanEvent(t, e.storage, "a-valid")
			if err := e.storage.Put(context.Background(), "events:z-invalid", []byte(raw)); err != nil {
				t.Fatal(err)
			}
			s := freshEventScheduler(t, e.storage)
			if err := s.LoadEventsFromStorage(); err == nil {
				t.Fatal("invalid orphan accepted")
			}
			items, err := e.storage.ListGet(context.Background(), "events", 0, 0)
			if err != nil || len(items) != 0 {
				t.Fatalf("failed validation partially repaired index: %#v, %v", items, err)
			}
		})
	}
}
