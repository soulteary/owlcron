package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func TestStartupMigratesLegacyHistoryAndRecoversPendingDeletion(t *testing.T) {
	e := jobTestEngine(t)
	ctx := context.Background()
	job := &types.Job{ID: "legacy-startup", Event: "legacy-event", Complete: true, TimeStart: 1, TimeEnd: 2}
	raw, _ := json.Marshal(job)
	if err := e.storage.Put(ctx, "jobs:"+job.ID, raw); err != nil {
		t.Fatal(err)
	}
	if err := e.storage.ListPush(ctx, "logs/completed", job); err != nil {
		t.Fatal(err)
	}
	if err := e.storage.ListPush(ctx, "logs/events/"+job.Event, job); err != nil {
		t.Fatal(err)
	}
	cleanupJob(t, e, "pending-delete")
	intent := historyCleanupIntent{ID: "pending-delete", History: true}
	raw, _ = json.Marshal(intent)
	if err := e.storage.Put(ctx, historyCleanupKey(intent.ID, true), raw); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Shutdown() })
	jobs, err := storage.HistoryJobs(ctx, e.storage)
	if err != nil || len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("startup history: %#v, %v", jobs, err)
	}
	if exists, err := e.storage.Exists(ctx, "logs/completed"); err != nil || exists {
		t.Fatalf("legacy index remains: %v %v", exists, err)
	}
	if deleted, err := e.IsJobHistoryDeleted(intent.ID); err != nil || !deleted {
		t.Fatalf("pending deletion was not recovered: %v %v", deleted, err)
	}
}

func TestMissingCompletionLogRetainsRecoverySpool(t *testing.T) {
	e := jobTestEngine(t)
	job := &types.Job{ID: "missing-completion-log", Complete: true, TimeStart: 1, TimeEnd: 2, LogFileSize: 100, LogFile: filepath.Join(e.config.LogDir, "jobs", "missing-completion-log.log")}
	if err := e.jobManager.writeCompletionSpool(job); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("missing log was treated as durable completion")
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
		t.Fatal("failed recovery discarded spool")
	}
	if err := os.WriteFile(job.LogFile, []byte("recovered original log"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("truncated recovery log was accepted")
	}
	if err := os.WriteFile(job.LogFile, []byte(strings.Repeat("x", int(job.LogFileSize))), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("successful recovery retained spool: %v", err)
	}
}
