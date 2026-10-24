package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

type cleanupTestStorage struct {
	storage.Storage
	blockOp, blockPrefix string
	entered, release     chan struct{}
	once                 sync.Once
	failOp, failPrefix   string
}

func (s *cleanupTestStorage) intercept(op, key string) error {
	if op == s.blockOp && strings.HasPrefix(key, s.blockPrefix) {
		s.once.Do(func() { close(s.entered); <-s.release })
	}
	if op == s.failOp && strings.HasPrefix(key, s.failPrefix) {
		return fmt.Errorf("injected cleanup failure: %s %s", op, key)
	}
	return nil
}
func (s *cleanupTestStorage) Put(ctx context.Context, key string, data []byte) error {
	if err := s.intercept("put", key); err != nil {
		return err
	}
	return s.Storage.Put(ctx, key, data)
}
func (s *cleanupTestStorage) Delete(ctx context.Context, key string) error {
	if err := s.intercept("delete", key); err != nil {
		return err
	}
	return s.Storage.Delete(ctx, key)
}
func (s *cleanupTestStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if err := s.intercept("update", key); err != nil {
		return err
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func (s *cleanupTestStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}

func cleanupJob(t *testing.T, e *Engine, id string) *types.Job {
	t.Helper()
	job := &types.Job{ID: id, Event: "retention-event", Hostname: e.Hostname(), Complete: true, TimeStart: 1, TimeEnd: 2, LogFileSize: 10}
	if err := e.jobManager.finishJobStorage(job); err != nil {
		t.Fatal(err)
	}
	if err := e.writeJobLog(context.Background(), id, []byte("durable log")); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestHistoryDeletionFencesOtherEngineLateWrites(t *testing.T) {
	for _, stage := range []string{"log", "index"} {
		t.Run(stage, func(t *testing.T) {
			a, b := jobTestEngine(t), jobTestEngine(t)
			base := a.storage
			b.storage = base
			job := cleanupJob(t, a, "late-"+stage)
			gate := &cleanupTestStorage{Storage: base, entered: make(chan struct{}), release: make(chan struct{})}
			if stage == "log" {
				gate.blockOp = "update"
				gate.blockPrefix = "job_log_chunks/"
			} else {
				gate.blockOp = "put"
				gate.blockPrefix = "history/completed/"
			}
			a.storage = gate
			done := make(chan error, 1)
			go func() {
				if stage == "log" {
					_, err := storage.JobLogAppend(context.Background(), a.storage, job.ID, 0, []byte("durable log"), true, a.logHardLimit())
					done <- err
				} else {
					done <- a.jobManager.finishJobStorage(job)
				}
			}()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("late writer never reached gate")
			}
			if err := b.DeleteJobHistory(job.ID); err != nil {
				close(gate.release)
				t.Fatal(err)
			}
			close(gate.release)
			if err := <-done; err != nil && !errors.Is(err, ErrJobHistoryDeleted) && !errors.Is(err, storage.ErrJobLogDeleted) {
				t.Fatal(err)
			}
			if _, ok := b.LookupJob(job.ID); ok {
				t.Fatal("late writer restored deleted job")
			}
			raw, err := base.Get(context.Background(), "job_logs:"+job.ID)
			if err != nil || !bytes.Equal(raw, deletedLogMarker) {
				t.Fatalf("late writer restored log: %q %v", raw, err)
			}
			items, err := storage.HistoryJobs(context.Background(), base)
			if err != nil || len(items) != 0 {
				t.Fatalf("deleted history visible: %v %v", items, err)
			}
			keys, err := base.(storage.PrefixKeyLister).ListKeysPrefix(context.Background(), "history/completed/")
			if err != nil || len(keys) != 0 {
				t.Fatalf("late index retained after cleanup: %v %v", keys, err)
			}
		})
	}
}

func TestHistoryCleanupIntentRecoversEveryStorageFailure(t *testing.T) {
	stages := []struct{ op, prefix string }{{"put", "deleted_jobs:"}, {"update", "jobs:"}, {"put", "deleted_job_logs:"}, {"update", "job_logs:"}, {"delete", "job_logs:"}, {"update", "job_log_manifest/"}, {"update", "job_log_chunks/"}, {"delete", "history/completed/"}, {"delete", historyCleanupPrefix}}
	for _, stage := range stages {
		t.Run(stage.op+stage.prefix, func(t *testing.T) {
			a, b := jobTestEngine(t), jobTestEngine(t)
			base := a.storage
			b.storage = base
			job := cleanupJob(t, a, "recover-cleanup")
			a.storage = &cleanupTestStorage{Storage: base, failOp: stage.op, failPrefix: stage.prefix}
			if err := a.DeleteJobHistory(job.ID); err == nil {
				t.Fatal("cleanup failure was hidden")
			}
			if exists, err := base.Exists(context.Background(), historyCleanupKey(job.ID, true)); err != nil || !exists {
				t.Fatalf("recovery intent lost: %v %v", exists, err)
			}
			if err := b.RetryHistoryDeletions(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := b.RetryHistoryDeletions(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, ok := b.LookupJob(job.ID); ok {
				t.Fatal("deleted history survived recovery")
			}
			for _, key := range []string{"deleted_jobs:" + job.ID, "deleted_job_logs:" + job.ID, "jobs:" + job.ID, "job_logs:" + job.ID} {
				raw, err := base.Get(context.Background(), key)
				if err != nil || len(raw) > 256 {
					t.Fatalf("small tombstone missing: %s %d %v", key, len(raw), err)
				}
			}
			if exists, err := base.Exists(context.Background(), historyCleanupKey(job.ID, true)); err != nil || exists {
				t.Fatalf("completed intent retained: %v %v", exists, err)
			}
		})
	}
}

func TestHistoryCleanupProtectsSpoolAndRemoteOutbox(t *testing.T) {
	for _, pending := range []string{"spool", "outbox", "active"} {
		t.Run(pending, func(t *testing.T) {
			e := jobTestEngine(t)
			job := cleanupJob(t, e, "pending-cleanup")
			switch pending {
			case "spool":
				if err := e.jobManager.writeCompletionSpool(job); err != nil {
					t.Fatal(err)
				}
			case "outbox":
				if err := upsertStorageList(context.Background(), e.storage, "remote_completions", job.ID, map[string]interface{}{"id": job.ID, "job": job}, false); err != nil {
					t.Fatal(err)
				}
			case "active":
				if err := upsertStorageList(context.Background(), e.storage, "active_jobs", job.ID, job, false); err != nil {
					t.Fatal(err)
				}
			}
			for _, history := range []bool{false, true} {
				if err := e.startHistoryCleanup(job.ID, history); !errors.Is(err, ErrHistoryCleanupPending) {
					t.Fatalf("pending %s allowed cleanup: %v", pending, err)
				}
			}
			if exists, err := e.storage.Exists(context.Background(), "deleted_job_logs:"+job.ID); err != nil || exists {
				t.Fatalf("pending completion lost log: %v %v", exists, err)
			}
			if pending == "spool" {
				if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExpiredLogCannotReturnThroughCompletionSpool(t *testing.T) {
	e := jobTestEngine(t)
	job := cleanupJob(t, e, "expired-log-replay")
	if err := e.DeleteJobLog(job.ID); err != nil {
		t.Fatal(err)
	}
	// Simulate an old worker's local file and recovery spool arriving after the
	// shared log has expired. Metadata recovery must succeed without that log.
	job.LogFile = filepath.Join(e.config.LogDir, "jobs", job.ID+".log")
	if err := e.jobManager.createLogFile(job); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(job.LogFile, []byte("stale worker log"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.writeCompletionSpool(job); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.LookupJob(job.ID); !ok {
		t.Fatal("log expiry removed job metadata")
	}
	raw, err := e.storage.Get(context.Background(), "job_logs:"+job.ID)
	if err != nil || !bytes.Equal(raw, deletedLogMarker) {
		t.Fatalf("spool restored expired log: %q %v", raw, err)
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("successful metadata replay retained spool: %v", err)
	}
}

func TestHistoryCleanupRetainsIntentAfterLocalRemovalFailure(t *testing.T) {
	e := jobTestEngine(t)
	job := cleanupJob(t, e, "local-cleanup-failure")
	path := filepath.Join(e.config.LogDir, "jobs", job.ID+".log")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(path, "prevent-remove")
	if err := os.WriteFile(child, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteJobHistory(job.ID); err == nil {
		t.Fatal("local removal failure was hidden")
	}
	if exists, err := e.storage.Exists(context.Background(), historyCleanupKey(job.ID, true)); err != nil || !exists {
		t.Fatalf("local failure discarded cleanup intent: %v %v", exists, err)
	}
	if err := os.Remove(child); err != nil {
		t.Fatal(err)
	}
	if err := e.RetryHistoryDeletions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("local removal did not recover: %v", err)
	}
	if exists, err := e.storage.Exists(context.Background(), historyCleanupKey(job.ID, true)); err != nil || exists {
		t.Fatalf("successful retry retained intent: %v %v", exists, err)
	}
}
