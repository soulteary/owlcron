package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func retentionLog(t *testing.T, e *Engine, id string, now time.Time) string {
	t.Helper()
	path := filepath.Join(e.config.LogDir, "jobs", id+".log")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep until expiry"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, now.Add(-72*time.Hour), now.Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRetentionLocalLogProtectionAndConcurrentPasses(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.config.JobLogExpireDays = 1
	now := time.Now()
	ids := []string{"active", "spooled", "outbox", "incomplete", "expired", "recent"}
	for _, id := range ids {
		retentionLog(t, e, id, now)
	}
	e.mu.Lock()
	e.activeJobs["active"] = &types.Job{ID: "active"}
	e.mu.Unlock()
	if err := e.jobManager.writeCompletionSpool(&types.Job{ID: "spooled", Complete: true}); err != nil {
		t.Fatal(err)
	}
	if err := e.storage.ListPush(context.Background(), "remote_completions", map[string]interface{}{"id": "outbox"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"incomplete", "expired", "recent"} {
		job := &types.Job{ID: id, Complete: id != "incomplete", TimeEnd: float64(now.Add(-48 * time.Hour).Unix())}
		if id == "recent" {
			job.TimeEnd = float64(now.Unix())
		}
		raw, _ := json.Marshal(job)
		if err := e.storage.Put(context.Background(), "jobs:"+id, raw); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.RunHistoryMaintenance(context.Background(), now); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for _, id := range ids {
		_, err := os.Stat(filepath.Join(e.config.LogDir, "jobs", id+".log"))
		if id == "expired" {
			if !os.IsNotExist(err) {
				t.Fatalf("expired log survived: %v", err)
			}
		} else if err != nil {
			t.Fatalf("protected %s log removed: %v", id, err)
		}
	}
}

func TestRetentionDisabledAndCanceled(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	now := time.Now()
	path := retentionLog(t, e, "retained", now)
	if err := e.RunHistoryMaintenance(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	e.config.JobLogExpireDays = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.RunHistoryMaintenance(ctx, now); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionLocalLogSymlinkDoesNotDeleteTarget(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.config.JobLogExpireDays = 1
	now := time.Now()
	outside := filepath.Join(t.TempDir(), "outside.log")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(e.config.LogDir, "jobs")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "unsafe.log")); err != nil {
		t.Fatal(err)
	}
	if err := e.RunHistoryMaintenance(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionSharedBatchCursorResumesAfterRestart(t *testing.T) {
	e := jobTestEngine(t)
	e.config.JobDataExpireDays = 1
	e.config.HistoryCleanupBatchSize = 2
	now := time.Now()
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		job := &types.Job{ID: id, Event: "evt", Complete: true, TimeEnd: float64(now.Add(-48 * time.Hour).Unix())}
		raw, _ := json.Marshal(job)
		if err := e.storage.Put(ctx, "jobs:"+id, raw); err != nil {
			t.Fatal(err)
		}
		if err := storage.HistoryUpsert(ctx, e.storage, job); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.RunHistoryMaintenance(ctx, now); err != nil {
		t.Fatal(err)
	}
	jobs, err := storage.HistoryJobs(ctx, e.storage)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 {
		t.Fatalf("batch size was not respected: %d", len(jobs))
	}
	// A fresh engine with the same backend must resume the persisted index cursor.
	restarted := jobTestEngine(t)
	restarted.storage = e.storage
	restarted.config.JobDataExpireDays = 1
	restarted.config.HistoryCleanupBatchSize = 2
	e.mu.Lock()
	e.multi.Master = false
	e.mu.Unlock()
	if err := e.releaseLeadership(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := restarted.RunHistoryMaintenance(ctx, now); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err = storage.HistoryJobs(ctx, e.storage)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("restart did not finish retention: %d %v", len(jobs), err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		deleted, err := e.IsJobHistoryDeleted(id)
		if err != nil || !deleted {
			t.Fatalf("missing durable tombstone %s: %v", id, err)
		}
	}
}

type retentionReadFailure struct{ storage.Storage }

func (s retentionReadFailure) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "jobs:retry" {
		return nil, errors.New("injected backend read failure")
	}
	return s.Storage.Get(ctx, key)
}
func TestRetentionLocalReadFailureIsRetryable(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.config.JobLogExpireDays = 1
	now := time.Now()
	path := retentionLog(t, e, "retry", now)
	raw, _ := json.Marshal(&types.Job{ID: "retry", Complete: true, TimeEnd: float64(now.Add(-48 * time.Hour).Unix())})
	if err := e.storage.Put(context.Background(), "jobs:retry", raw); err != nil {
		t.Fatal(err)
	}
	base := e.storage
	e.storage = retentionReadFailure{base}
	if err := e.RunHistoryMaintenance(context.Background(), now); err == nil {
		t.Fatal("backend outage was hidden")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("backend outage deleted log", err)
	}
	e.storage = base
	if err := e.RunHistoryMaintenance(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("recovery did not remove expired log: %v", err)
	}
}

type retentionBlockedStorage struct {
	storage.Storage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *retentionBlockedStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "deleted_jobs:cached" {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.Storage.Get(ctx, key)
}
func (s *retentionBlockedStorage) Exists(ctx context.Context, key string) (bool, error) {
	if key == "deleted_jobs:cached" {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.Storage.Exists(ctx, key)
}
func (s *retentionBlockedStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func TestRetentionAsyncSingleFlightAndShutdownWait(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.AddDeadJob(&types.Job{ID: "cached", Complete: true})
	blocked := &retentionBlockedStorage{Storage: e.storage, entered: make(chan struct{}), release: make(chan struct{})}
	e.storage = blocked
	returned := make(chan struct{})
	go func() { e.runHistoryMaintenanceIfDue(time.Now()); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("maintenance launch blocked tick")
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not reach backend")
	}
	// An elapsed interval must not queue another maintenance worker or wait for
	// the mutex held by the running pass.
	for i := 0; i < 20; i++ {
		e.runHistoryMaintenanceIfDue(time.Now().Add(time.Hour))
	}
	stopped := make(chan struct{})
	go func() { _ = e.Shutdown(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("shutdown abandoned running maintenance")
	case <-time.After(30 * time.Millisecond):
	}
	close(blocked.release)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not await maintenance completion")
	}
	e.runHistoryMaintenanceIfDue(time.Now().Add(2 * time.Hour))
	e.maintenanceLaunchMu.Lock()
	running := e.maintenanceRunning
	e.maintenanceLaunchMu.Unlock()
	if running {
		t.Fatal("maintenance launched after shutdown")
	}
}

func TestRetentionEvictsRemoteDeletedCachedHistory(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.config.HistoryCleanupBatchSize = 1
	for _, id := range []string{"a", "b", "c"} {
		e.AddDeadJob(&types.Job{ID: id, Complete: true})
		if err := e.storage.Put(context.Background(), "deleted_jobs:"+id, []byte(`{"deleted":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	e.deadJobs["b"].StorageError = "pending"
	e.mu.Unlock()
	for i := 0; i < 5; i++ {
		if err := e.RunHistoryMaintenance(context.Background(), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	jobs := e.GetDeadJobs()
	if len(jobs) != 1 || jobs["b"] == nil {
		t.Fatalf("cache reconciliation removed pending completion or retained tombstones: %#v", jobs)
	}
}

func TestCompletionRecoverySingleFlightDoesNotBlockTick(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.jobManager.finishMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			e.jobManager.finishMu.Unlock()
		}
	}()
	e.runCompletionRecovery()
	tickDone := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			e.tick()
		}
		close(tickDone)
	}()
	select {
	case <-tickDone:
	case <-time.After(time.Second):
		t.Fatal("tick blocked behind pending completion recovery")
	}
	e.maintenanceLaunchMu.Lock()
	running := e.completionRecoveryRunning
	e.maintenanceLaunchMu.Unlock()
	if !running {
		t.Fatal("completion recovery did not start")
	}
	stopped := make(chan struct{})
	go func() { _ = e.Shutdown(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("shutdown abandoned pending completion recovery")
	case <-time.After(30 * time.Millisecond):
	}
	e.jobManager.finishMu.Unlock()
	unlocked = true
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not await completion recovery")
	}
	e.runCompletionRecovery()
	e.maintenanceLaunchMu.Lock()
	running = e.completionRecoveryRunning
	e.maintenanceLaunchMu.Unlock()
	if running {
		t.Fatal("completion recovery started after shutdown")
	}
}

func TestRetentionFollowerDeletesFreshTombstonedLogsWithTTLDisabled(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	now := time.Now()
	for _, id := range []string{"history", "log", "ordinary", "active"} {
		path := retentionLog(t, e, id, now)
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	for _, key := range []string{"deleted_jobs:history", "deleted_job_logs:log", "deleted_jobs:active"} {
		if err := e.storage.Put(ctx, key, []byte(`{"deleted":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	e.activeJobs["active"] = &types.Job{ID: "active"}
	e.mu.Unlock()
	if err := e.RunHistoryMaintenance(ctx, now); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"history", "log", "ordinary", "active"} {
		_, err := os.Stat(filepath.Join(e.config.LogDir, "jobs", id+".log"))
		if id == "history" || id == "log" {
			if !os.IsNotExist(err) {
				t.Fatalf("fresh tombstoned %s log survived with TTL disabled: %v", id, err)
			}
		} else if err != nil {
			t.Fatalf("protected %s log removed: %v", id, err)
		}
	}
}
