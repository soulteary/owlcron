package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

type completionOutageStorage struct{ storage.Storage }

func (s *completionOutageStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if strings.HasPrefix(key, "jobs:") {
		return errors.New("injected completion storage outage")
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

func TestUnspooledCompletionRecoversQuotaBeforeBackendAndRestart(t *testing.T) {
	e := jobTestEngine(t)
	jobsDir := filepath.Join(e.config.LogDir, "jobs")
	// A different, earlier completion cannot upload its missing log. It must
	// not prevent another result from becoming durable as capacity recovers.
	blocked := &types.Job{ID: "a-missing-log", Complete: true, LogFile: filepath.Join(jobsDir, "a-missing-log.log"), LogFileSize: 100}
	if err := e.jobManager.writeCompletionSpool(blocked); err != nil {
		t.Fatal(err)
	}
	filler := filepath.Join(jobsDir, "old.log")
	if err := os.WriteFile(filler, []byte(strings.Repeat("x", 4096)), 0600); err != nil {
		t.Fatal(err)
	}
	used, err := spoolTreeBytes(context.Background(), jobsDir)
	if err != nil {
		t.Fatal(err)
	}
	e.spoolBudget = newNodeSpoolBudget(used+1, 80, []string{jobsDir})
	job := &types.Job{ID: "b-unspooled", Event: "event", Hostname: e.Hostname(), Code: 7, Description: "actual plugin result", TimeStart: 1, TimeEnd: 2}
	if err := e.jobManager.persistActiveJob(job); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.activeJobs[job.ID] = types.CloneJob(job)
	e.mu.Unlock()
	base := e.storage
	e.storage = &completionOutageStorage{Storage: base}
	e.jobManager.finishLocalJob(job)
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("quota unexpectedly allowed initial completion spool: %v", err)
	}
	if finished, ok := e.GetDeadJob(job.ID); !ok || finished.StorageError == "" || finished.Code != 7 {
		t.Fatalf("unspooled completion lost retry state: %+v", finished)
	}
	if err := e.jobManager.RetryPendingCompletions(); !errors.Is(err, ErrSpoolCapacityExceeded) {
		t.Fatalf("quota failure was hidden during retry: %v", err)
	}
	actual, err := spoolTreeBytes(context.Background(), jobsDir)
	if err != nil || actual > used+1 {
		t.Fatalf("recovery exceeded hard spool quota: %d %v", actual, err)
	}
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("backend outage was hidden")
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
		t.Fatalf("earlier failed completion starved the recovered spool write: %v", err)
	}
	if finished, _ := e.GetDeadJob(job.ID); finished.StorageError == "" {
		t.Fatal("spool write cleared the pending backend error")
	}
	// Only the newly durable spool, not the old engine's memory, survives.
	fresh, err := NewEngine(e.config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	fresh.multi.Master = true
	t.Cleanup(func() { fresh.cancel(); fresh.jobManager.Wait() })
	if err := fresh.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("the unrelated missing log should still require recovery")
	}
	finished, ok := fresh.GetDeadJob(job.ID)
	if !ok || finished.Code != 7 || finished.Description != "actual plugin result" || finished.StorageError != "" {
		t.Fatalf("restart lost the actual completed result: %+v", finished)
	}
	if _, err := os.Stat(fresh.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("successful recovery retained the spool: %v", err)
	}
	items, err := base.ListGet(context.Background(), "active_jobs", 0, 0)
	if err != nil || len(items) != 0 {
		t.Fatalf("completion recovery retained its active reservation: %v %v", items, err)
	}
	history, err := readStoredHistory(context.Background(), base, "")
	if err != nil || len(history) != 1 || history[0].ID != job.ID {
		t.Fatalf("completion recovery duplicated or lost history: %+v %v", history, err)
	}
	if e.state.Stats["jobs_completed"] != 1 || e.state.Stats["jobs_failed"] != 1 {
		t.Fatalf("recovery repeated completion side effects: %+v", e.state.Stats)
	}
}

func TestUnspooledCompletionRetriesWhenLocalSpoolRemainsUnavailable(t *testing.T) {
	e := jobTestEngine(t)
	if err := os.MkdirAll(e.config.LogDir, 0700); err != nil {
		t.Fatal(err)
	}
	// A regular file obstructs the spool directory, independently of quota.
	if err := os.WriteFile(filepath.Join(e.config.LogDir, "jobs"), []byte("unavailable"), 0600); err != nil {
		t.Fatal(err)
	}
	base := e.storage
	e.storage = &completionOutageStorage{Storage: base}
	job := &types.Job{ID: "unwritable-spool", Event: "event", Hostname: e.Hostname(), Code: 0, Description: "finished successfully", TimeStart: 1, TimeEnd: 2}
	e.mu.Lock()
	e.activeJobs[job.ID] = types.CloneJob(job)
	e.mu.Unlock()
	e.jobManager.finishLocalJob(job)
	if finished, ok := e.GetDeadJob(job.ID); !ok || finished.StorageError == "" {
		t.Fatalf("filesystem failure lost the retry snapshot: %+v", finished)
	}
	if err := e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("continued backend outage was hidden")
	}
	e.storage = base
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatalf("recovered backend could not persist the memory fallback: %v", err)
	}
	finished, _ := e.GetDeadJob(job.ID)
	if finished.StorageError != "" || !finished.Complete || finished.Description != job.Description {
		t.Fatalf("fallback result did not recover: %+v", finished)
	}
	raw, err := base.Get(context.Background(), "jobs:"+job.ID)
	var stored types.Job
	if err != nil || json.Unmarshal(raw, &stored) != nil || !stored.Complete || stored.Code != 0 {
		t.Fatalf("fallback result was not durable: %s %v", raw, err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatalf("successful memory fallback remained pending: %v", err)
	}
}

func TestUnspooledCompletionRetriesBackendWhileQuotaFull(t *testing.T) {
	e := jobTestEngine(t)
	jobsDir := filepath.Join(e.config.LogDir, "jobs")
	e.spoolBudget = newNodeSpoolBudget(1, 80, []string{jobsDir})
	base := e.storage
	e.storage = &completionOutageStorage{Storage: base}
	job := &types.Job{ID: "backend-recovered-first", Event: "event", Hostname: e.Hostname(), Code: 3, TimeStart: 1, TimeEnd: 2}
	e.jobManager.finishLocalJob(job)
	if finished, ok := e.GetDeadJob(job.ID); !ok || finished.StorageError == "" {
		t.Fatalf("unspooled result lost its pending state: %+v", finished)
	}
	e.storage = base
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatalf("quota failure prevented recovered backend persistence: %v", err)
	}
	finished, _ := e.GetDeadJob(job.ID)
	if finished.StorageError != "" || finished.Code != 3 {
		t.Fatalf("actual result did not recover: %+v", finished)
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("completion unexpectedly bypassed the quota: %v", err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatalf("durable completion remained pending: %v", err)
	}
	history, err := readStoredHistory(context.Background(), base, "")
	if err != nil || len(history) != 1 {
		t.Fatalf("backend fallback duplicated history: %+v %v", history, err)
	}
}

func TestCompletionQuotaDoesNotPersistTransientSpoolError(t *testing.T) {
	e := jobTestEngine(t)
	e.spoolBudget = newNodeSpoolBudget(1, 80, []string{filepath.Join(e.config.LogDir, "jobs")})
	job := &types.Job{ID: "healthy-backend-full-spool", Event: "event", Hostname: e.Hostname(), TimeStart: 1, TimeEnd: 2}
	e.jobManager.finishLocalJob(job)
	raw, err := e.storage.Get(context.Background(), "jobs:"+job.ID)
	var stored types.Job
	if err != nil || json.Unmarshal(raw, &stored) != nil || !stored.Complete || stored.StorageError != "" {
		t.Fatalf("transient spool failure incorrectly marked durable history pending: %s %v", raw, err)
	}
	if finished, _ := e.GetDeadJob(job.ID); finished.StorageError != "" {
		t.Fatalf("durable completion remained pending: %+v", finished)
	}
}

func TestUnspooledRemoteCompletionRetainsFailedOutbox(t *testing.T) {
	e := jobTestEngine(t)
	e.multi.Master = false
	e.multi.Slave = true
	jobsDir := filepath.Join(e.config.LogDir, "jobs")
	e.spoolBudget = newNodeSpoolBudget(1, 80, []string{jobsDir})
	base := e.storage
	e.storage = &injectedStorage{Storage: base, failOutbox: true}
	job := &types.Job{ID: "unspooled-worker-result", Event: "event", Hostname: e.Hostname(), Code: 0, TimeStart: 1, TimeEnd: 2}
	e.jobManager.finishLocalJob(job)
	if finished, ok := e.GetDeadJob(job.ID); !ok || finished.StorageError == "" {
		t.Fatalf("failed delivery of unspooled result was hidden: %+v", finished)
	}
	if err := e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("continued outbox failure was hidden")
	}
	e.spoolBudget.mu.Lock()
	e.spoolBudget.maximum = 1 << 20
	e.spoolBudget.mu.Unlock()
	if err := e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("spool recovery incorrectly cleared the outbox failure")
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
		t.Fatalf("failed delivery did not retain its new durable spool: %v", err)
	}
	if finished, _ := e.GetDeadJob(job.ID); finished.StorageError == "" {
		t.Fatal("result stopped being retryable before durable delivery")
	}
	e.storage = base
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	if finished, _ := e.GetDeadJob(job.ID); finished.StorageError != "" {
		t.Fatalf("successful delivery retained the pending error: %+v", finished)
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("durable delivery retained its spool: %v", err)
	}
	items, err := base.ListGet(context.Background(), "remote_completions", 0, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("outbox recovery lost or duplicated delivery: %+v %v", items, err)
	}
	if e.state.Stats["jobs_completed"] != 1 {
		t.Fatal("delivery retries repeated completion side effects")
	}
}
