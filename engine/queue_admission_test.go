package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

type queueFailureStorage struct {
	storage.Storage
	mu         sync.Mutex
	failPrefix string
}

func (s *queueFailureStorage) fail(prefix string) {
	s.mu.Lock()
	s.failPrefix = prefix
	s.mu.Unlock()
}
func (s *queueFailureStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	s.mu.Lock()
	fail := s.failPrefix != "" && strings.HasPrefix(key, s.failPrefix)
	s.mu.Unlock()
	if fail {
		return fmt.Errorf("injected queue storage failure")
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

func TestQueueAdmissionStorageFailuresRemainReplayable(t *testing.T) {
	e := newSchedulingTestEngine(t, "durable-queue")
	setMasterForTest(e)
	event := saveSchedulingFixture(t, e, "durable-event")
	event.Queue = true
	event.Timing = nil
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	store := &queueFailureStorage{Storage: e.storage}
	e.storage = store
	key := "global/event_queue/" + event.ID
	push := func(token string) {
		if err := store.ListPush(context.Background(), key, map[string]interface{}{"queue_token": token, "event_id": event.ID, "timestamp": int64(1791331260), "event": event}); err != nil {
			t.Fatal(err)
		}
		e.mu.Lock()
		e.eventQueue[event.ID]++
		e.mu.Unlock()
	}
	push("before-admission")
	store.fail("queue_admissions:")
	e.scheduler.checkEventQueues()
	info, err := store.ListGetInfo(context.Background(), key)
	if err != nil || info.Length != 1 || len(e.GetActiveJobs()) != 0 || len(e.GetDeadJobs()) != 0 {
		t.Fatalf("failed preparation consumed or executed queue: %+v %v", info, err)
	}
	store.fail("")
	e.scheduler.checkEventQueues()
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
	e.jobManager.Wait()
	push("after-admission")
	store.fail(key)
	e.scheduler.checkEventQueues()
	waitForCondition(t, time.Second, func() bool {
		e.scheduler.checkEventQueues()
		return len(e.GetDeadJobs()) == 2
	})
	e.jobManager.Wait()
	info, err = store.ListGetInfo(context.Background(), key)
	if err != nil || info.Length != 1 {
		t.Fatalf("failed dequeue did not retain head: %+v %v", info, err)
	}
	store.fail("")
	waitForCondition(t, time.Second, func() bool {
		e.scheduler.checkEventQueues()
		current, err := store.ListGetInfo(context.Background(), key)
		return err == nil && current.Length == 0 && e.GetEventQueues()[event.ID] == 0
	})
	history, err := readStoredHistory(context.Background(), store, event.ID)
	info, infoErr := store.ListGetInfo(context.Background(), key)
	if err != nil || infoErr != nil || len(history) != 2 || info.Length != 0 || e.GetEventQueues()[event.ID] != 0 {
		t.Fatalf("dequeue replay duplicated admission: %v %v %+v", err, infoErr, history)
	}
	push("flush-survives-restart")
	if count, err := e.scheduler.FlushEventQueue(event.ID); err != nil || count != 1 {
		t.Fatalf("flush=%d %v", count, err)
	}
	e.scheduler.preloadEventQueues()
	info, err = store.ListGetInfo(context.Background(), key)
	if err != nil || info.Length != 0 || e.GetEventQueues()[event.ID] != 0 {
		t.Fatal("flushed queue was restored from storage")
	}
}

func TestLegacyQueueSnapshotPreservesOverrides(t *testing.T) {
	e := newSchedulingTestEngine(t, "legacy-queue")
	setMasterForTest(e)
	event := saveSchedulingFixture(t, e, "legacy-event")
	plug, err := e.jobManager.GetPluginFromStorage(event.Plugin)
	if err != nil {
		t.Fatal(err)
	}
	plug.Params = []*types.PluginParam{{ID: "message", Type: "text", Default: "default"}}
	if err := e.jobManager.SavePluginToStorage(plug); err != nil {
		t.Fatal(err)
	}
	event.Queue = true
	event.Timing = nil
	event.Params = map[string]interface{}{"message": "current"}
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	// The original 0.9.102 queue stores a flat event, including manual overrides
	// and its logical schedule time, rather than a nested event object.
	raw, _ := json.Marshal(event)
	var legacy map[string]interface{}
	_ = json.Unmarshal(raw, &legacy)
	legacy["params"] = map[string]interface{}{"message": "queued-override"}
	legacy["title"] = "queued snapshot"
	legacy["now"] = int64(1791331260)
	if err := e.storage.ListPush(context.Background(), "global/event_queue/"+event.ID, legacy); err != nil {
		t.Fatal(err)
	}
	e.scheduler.preloadEventQueues()
	e.scheduler.checkEventQueues()
	waitForCondition(t, time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
	for _, job := range e.GetDeadJobs() {
		if job.Now != 1791331260 || job.EventTitle != "queued snapshot" || job.Params["message"] != "queued-override" {
			t.Fatalf("legacy queued overrides lost: %+v", job)
		}
	}
}
