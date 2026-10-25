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

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func TestReservedCompletionsSurviveConcurrentFinishAtHardQuota(t *testing.T) {
	e := jobTestEngine(t)
	jobsDir := filepath.Join(e.config.LogDir, "jobs")
	e.spoolBudget = newNodeSpoolBudget(2<<20, 80, []string{jobsDir})
	base := e.storage
	jobs := make([]*types.Job, 2)
	for i := range jobs {
		job := &types.Job{ID: fmt.Sprintf("reserved-%d", i), Event: "event", Hostname: e.Hostname(), TimeStart: 1}
		e.jobManager.finishMu.Lock()
		err := e.jobManager.ensureCompletionReservation(job)
		e.jobManager.finishMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if err := e.jobManager.persistActiveJob(job); err != nil {
			t.Fatal(err)
		}
		jobs[i] = job
	}
	used, maximum, _ := e.spoolBudget.snapshot()
	if err := e.writeSpoolFile(filepath.Join(jobsDir, "pressure.log"), bytes.Repeat([]byte("x"), int(maximum-used)), ".pressure-*"); err != nil {
		t.Fatal(err)
	}
	e.storage = &completionOutageStorage{Storage: base}
	var wg sync.WaitGroup
	for i, job := range jobs {
		job.Code = 7 + i
		// This valid result exceeds the former 16 KiB headroom. The complete
		// envelope was reserved before execution, not when this report arrived.
		job.Description = fmt.Sprintf("actual-%d-", i) + strings.Repeat("result", 20*1024)
		job.TimeEnd = 2
		wg.Add(1)
		go func() { defer wg.Done(); e.jobManager.finishLocalJob(job) }()
	}
	wg.Wait()
	for _, job := range jobs {
		journal, err := readCompletionJournal(e.jobManager.completionJournalPath(job.ID))
		if err != nil || journal.checkpoint.Phase != completionPhaseSealed || journal.checkpoint.Job.Code != job.Code || journal.checkpoint.Job.Description != job.Description {
			t.Fatalf("full quota lost completed result %s: %+v %v", job.ID, journal, err)
		}
	}
	actual, err := spoolTreeBytes(context.Background(), jobsDir)
	if err != nil || actual != maximum {
		t.Fatalf("terminal commits changed the reserved byte count: %d/%d %v", actual, maximum, err)
	}
	// Restart immediately, without freeing quota or recovering the backend.
	fresh, err := NewEngine(e.config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	fresh.multi.Master = true
	fresh.storage = &completionOutageStorage{Storage: base}
	t.Cleanup(func() { fresh.cancel(); fresh.jobManager.Wait() })
	if err := fresh.jobManager.RecoverJobs(); err == nil {
		t.Fatal("continuing backend outage was hidden")
	}
	for _, job := range jobs {
		finished, ok := fresh.GetDeadJob(job.ID)
		if !ok || finished.Code != job.Code || finished.Description != job.Description || finished.StorageError == "" {
			t.Fatalf("restart lost an unpersisted actual result: %+v", finished)
		}
	}
	fresh.storage = base
	if err := fresh.jobManager.RecoverJobs(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.jobManager.RecoverJobs(); err != nil {
		t.Fatal(err)
	}
	history, err := readStoredHistory(context.Background(), base, "")
	if err != nil || len(history) != len(jobs) {
		t.Fatalf("replay lost or duplicated history: %+v %v", history, err)
	}
	if e.state.Stats["jobs_completed"] != float64(len(jobs)) {
		t.Fatalf("recovery repeated completion effects: %+v", e.state.Stats)
	}
}

func TestCompletionReservationRejectsExecutionBeforeSideEffects(t *testing.T) {
	e := jobTestEngine(t)
	marker := filepath.Join(t.TempDir(), "executed")
	event := installTestPlugin(t, e, "read input\nprintf 'executed' > '"+marker+"'\nprintf '%s\\n' '{\"complete\":1,\"code\":7}'\n")
	e.spoolBudget = newNodeSpoolBudget(64<<10, 80, []string{filepath.Join(e.config.LogDir, "jobs")})
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	if job.Code == 0 || !strings.Contains(job.Description, "capacity") {
		t.Fatalf("insufficient reservation was hidden: %+v", job)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("command ran before recovery capacity was reserved: %v", err)
	}
}

func TestCompletionJournalRetainsPriorCheckedSlot(t *testing.T) {
	e := jobTestEngine(t)
	job := &types.Job{ID: "checked-slots", Hostname: e.Hostname(), TimeStart: 1}
	if err := e.jobManager.ensureCompletionReservation(job); err != nil {
		t.Fatal(err)
	}
	job.Complete, job.Code, job.Description = true, 7, "durable report"
	if err := e.jobManager.writeCompletionCheckpoint(job, completionPhaseReported); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.writeCompletionCheckpoint(job, completionPhaseSealed); err != nil {
		t.Fatal(err)
	}
	path := e.jobManager.completionJournalPath(job.ID)
	latest, err := readCompletionJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a torn new payload/header. The other slot remains checked.
	_, err = file.WriteAt([]byte("torn"), int64(latest.slot)*(latest.capacity+completionJournalHeader)+completionJournalHeader)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	recovered, err := readCompletionJournal(path)
	if err != nil || recovered.generation != latest.generation-1 || recovered.checkpoint.Phase != completionPhaseReported || recovered.checkpoint.Job.Code != 7 || recovered.checkpoint.Job.Description != "durable report" {
		t.Fatalf("torn checkpoint destroyed the previous valid result: %+v %v", recovered, err)
	}
}

func TestCompletionResultLimitRejectsBeforeChangingCheckpoint(t *testing.T) {
	e := jobTestEngine(t)
	job := &types.Job{ID: "bounded-result", Hostname: e.Hostname(), TimeStart: 1}
	if err := e.jobManager.publishJob(job); err != nil {
		t.Fatal(err)
	}
	candidate := types.CloneJob(job)
	candidate.Complete = true
	candidate.Description = strings.Repeat("x", maxCompletionResultBytes)
	if err := e.jobManager.publishJob(candidate); !errors.Is(err, ErrCompletionResultTooLarge) {
		t.Fatalf("oversized mutable result accepted: %v", err)
	}
	journal, err := readCompletionJournal(e.jobManager.completionJournalPath(job.ID))
	if err != nil || journal.checkpoint.Phase != completionPhaseActive || journal.checkpoint.Job.Description != "" {
		t.Fatalf("rejected result changed the durable snapshot: %+v %v", journal, err)
	}
	if active := e.GetActiveJobs()[job.ID]; active.Complete || active.Description != "" {
		t.Fatalf("rejected result changed the active snapshot: %+v", active)
	}
}
