package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Model the list slicing contract shared by filesystem and remote storage without
// thousands of disk writes obscuring the scheduler's pagination behavior.
type eventLoadingStorage struct {
	storage.Storage
	items      []interface{}
	records    map[string][]byte
	offsets    []int
	failOffset int
	failID     string
}

func (s *eventLoadingStorage) ListGet(_ context.Context, key string, offset, limit int) ([]interface{}, error) {
	if key != "events" {
		return nil, fmt.Errorf("unexpected list %q", key)
	}
	s.offsets = append(s.offsets, offset)
	if s.failOffset >= 0 && offset >= s.failOffset {
		return nil, errors.New("injected page failure")
	}
	if offset >= len(s.items) {
		return nil, nil
	}
	end := offset + limit
	if limit <= 0 || end > len(s.items) {
		end = len(s.items)
	}
	return s.items[offset:end], nil
}
func (s *eventLoadingStorage) Get(_ context.Context, key string) ([]byte, error) {
	if key == "events:"+s.failID {
		return nil, errors.New("injected record failure")
	}
	data, ok := s.records[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return data, nil
}
func newEventLoadingScheduler(t *testing.T, count int) (*Scheduler, *eventLoadingStorage) {
	t.Helper()
	store := &eventLoadingStorage{records: make(map[string][]byte), failOffset: -1}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("e%05d", i)
		store.items = append(store.items, map[string]interface{}{"id": id, "title": "stale title"})
		raw, err := json.Marshal(&types.Event{ID: id, Title: "canonical " + id})
		if err != nil {
			t.Fatal(err)
		}
		store.records["events:"+id] = raw
	}
	scheduler, err := NewScheduler(&Engine{storage: store}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	scheduler.events["previous"] = &types.Event{ID: "previous"}
	return scheduler, store
}
func TestLoadEventsFromStorageReadsEveryPage(t *testing.T) {
	for _, count := range []int{0, 1000, 10001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			scheduler, store := newEventLoadingScheduler(t, count)
			if err := scheduler.LoadEventsFromStorage(); err != nil {
				t.Fatal(err)
			}
			if len(scheduler.events) != count {
				t.Fatalf("loaded %d events, want %d", len(scheduler.events), count)
			}
			wantCalls := count/1000 + 1
			if len(store.offsets) != wantCalls {
				t.Fatalf("page offsets %v, want %d calls", store.offsets, wantCalls)
			}
			for i, offset := range store.offsets {
				if offset != i*1000 {
					t.Fatalf("unexpected page offsets: %v", store.offsets)
				}
			}
			if count > 0 && scheduler.events[fmt.Sprintf("e%05d", count-1)].Title != "canonical "+fmt.Sprintf("e%05d", count-1) {
				t.Fatal("canonical event did not override index")
			}
		})
	}
}
func TestLoadEventsFromStorageCountsStaleAndDuplicateIndexEntries(t *testing.T) {
	scheduler, store := newEventLoadingScheduler(t, 1001)
	// The first page includes both a tombstoned record and a duplicate. Neither
	// may move the next page's offset backwards or cause early termination.
	delete(store.records, "events:e00000")
	store.items[1] = store.items[2]
	if err := scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.events) != 999 || scheduler.events["e01000"] == nil {
		t.Fatalf("stale/duplicate entries interrupted loading: %d", len(scheduler.events))
	}
}
func TestLoadEventsFromStorageFailurePreservesPreviousSchedule(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*eventLoadingStorage)
		want   string
	}{
		{"later page", func(s *eventLoadingStorage) { s.failOffset = 1000 }, "offset 1000"},
		{"later record", func(s *eventLoadingStorage) { s.failID = "e01000" }, "e01000"},
		{"malformed index", func(s *eventLoadingStorage) { s.items[1000] = 42 }, "entry 1000"},
		{"missing index ID", func(s *eventLoadingStorage) { s.items[1000] = map[string]interface{}{} }, "has no ID"},
		{"malformed canonical", func(s *eventLoadingStorage) { s.records["events:e01000"] = []byte("{") }, "decode event"},
		{"missing canonical ID", func(s *eventLoadingStorage) { s.records["events:e01000"] = []byte(`{"title":"invalid"}`) }, "ID mismatch"},
		{"mismatched canonical ID", func(s *eventLoadingStorage) { s.records["events:e01000"] = []byte(`{"id":"other"}`) }, "ID mismatch"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			scheduler, store := newEventLoadingScheduler(t, 1001)
			tt.mutate(store)
			err := scheduler.LoadEventsFromStorage()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %v, want %q", err, tt.want)
			}
			if len(scheduler.events) != 1 || scheduler.events["previous"] == nil {
				t.Fatal("failed load published partial schedule")
			}
		})
	}
}
