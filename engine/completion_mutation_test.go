package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func reserveMutationJob(t *testing.T) (*Engine, *types.Job) {
	t.Helper()
	e := splitTestEngine(t)
	job := &types.Job{ID: "runtime-reservation", Event: "event", EventTitle: "original title", Hostname: e.Hostname(), TimeStart: 1}
	e.jobManager.finishMu.Lock()
	err := e.jobManager.checkpointRuntimeJob(job)
	e.jobManager.finishMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.persistActiveJob(job); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.activeJobs[job.ID] = types.CloneJob(job)
	e.mu.Unlock()
	return e, job
}

func fillMutationSpool(t *testing.T, e *Engine) string {
	t.Helper()
	if err := e.spoolBudget.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	used, maximum, _ := e.spoolBudget.snapshot()
	path := filepath.Join(e.config.LogDir, "jobs", "pressure.log")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", int(maximum-used))), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.spoolBudget.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRuntimeMetadataGrowthReservesSpaceBeforeAcceptance(t *testing.T) {
	e, original := reserveMutationJob(t)
	filler := fillMutationSpool(t, e)
	title := strings.Repeat("expanded title", 32*1024)
	if _, err := e.UpdateActiveJob(original.ID, map[string]interface{}{"title": title}); !errors.Is(err, ErrSpoolCapacityExceeded) {
		t.Fatalf("larger metadata accepted without recovery capacity: %v", err)
	}
	current := e.GetActiveJobs()[original.ID]
	if current.EventTitle != original.EventTitle || current.UpdateVersion != original.UpdateVersion {
		t.Fatal("rejected update changed the active job")
	}
	journal, err := readCompletionJournal(e.jobManager.completionJournalPath(original.ID))
	if err != nil || journal.checkpoint.Job.EventTitle != original.EventTitle {
		t.Fatalf("rejected update changed the durable snapshot: %v", err)
	}
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if _, err := e.UpdateActiveJob(original.ID, map[string]interface{}{"title": title}); err != nil {
		t.Fatalf("recovered capacity did not allow metadata growth: %v", err)
	}
	journal, err = readCompletionJournal(e.jobManager.completionJournalPath(original.ID))
	if err != nil || journal.checkpoint.Job.EventTitle != title || journal.checkpoint.Job.UpdateVersion != 1 {
		t.Fatalf("accepted runtime metadata was not checkpointed: %v", err)
	}
}

func TestAbortRemainsDurableWhenSpoolCannotGrow(t *testing.T) {
	e, job := reserveMutationJob(t)
	fillMutationSpool(t, e)
	var cancelled atomic.Bool
	e.jobManager.mu.Lock()
	e.jobManager.running[job.ID] = func() { cancelled.Store(true) }
	e.jobManager.mu.Unlock()
	if err := e.jobManager.AbortJob(job.ID, strings.Repeat("abort detail ", 32*1024)); err != nil {
		t.Fatalf("spool pressure prevented cancellation: %v", err)
	}
	if !cancelled.Load() {
		t.Fatal("abort did not cancel execution")
	}
	current := e.GetActiveJobs()[job.ID]
	if len(current.AbortReason) == 0 || len(current.AbortReason) > 256 {
		t.Fatal("abort did not retain a bounded recovery diagnostic")
	}
	current.Complete = true
	current.Code = 1
	current.Description = "Job Aborted: " + current.AbortReason
	current.TimeEnd = 2
	e.jobManager.finishMu.Lock()
	err := e.jobManager.writeCompletionCheckpoint(current, completionPhaseSealed)
	e.jobManager.finishMu.Unlock()
	if err != nil {
		t.Fatalf("reserved terminal margin could not retain abort result: %v", err)
	}
	journal, err := readCompletionJournal(e.jobManager.completionJournalPath(job.ID))
	if err != nil || journal.checkpoint.Job.Code != 1 || journal.checkpoint.Job.Description != current.Description {
		t.Fatalf("abort result was not durable at the hard limit: %v", err)
	}
	actual, err := spoolTreeBytes(context.Background(), filepath.Join(e.config.LogDir, "jobs"))
	if err != nil || actual > e.spoolBudget.maximum {
		t.Fatalf("abort exceeded the spool budget: %d %v", actual, err)
	}
}
