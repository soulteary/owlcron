package engine

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func TestCompletionJournalRestartPreservesDurableAbort(t *testing.T) {
	for _, reported := range []bool{false, true} {
		name := "active execution"
		if reported {
			name = "reported success"
		}
		t.Run(name, func(t *testing.T) {
			e := splitTestEngine(t)
			job := &types.Job{ID: "durable-abort", Event: "event", Hostname: e.Hostname(), TimeStart: 1}
			e.jobManager.finishMu.Lock()
			err := e.jobManager.ensureCompletionReservation(job)
			e.jobManager.finishMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if reported {
				job.Complete = true
				job.Code = 0
				job.Description = "plugin reported success before cancellation"
				e.jobManager.finishMu.Lock()
				err = e.jobManager.writeCompletionCheckpoint(job, completionPhaseReported)
				e.jobManager.finishMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			// Shared state deliberately predates the abort. Only the local checked
			// journal can preserve the cancellation across a daemon restart.
			if err := e.jobManager.persistActiveJob(job); err != nil {
				t.Fatal(err)
			}
			e.mu.Lock()
			e.activeJobs[job.ID] = types.CloneJob(job)
			e.mu.Unlock()
			var cancelled atomic.Bool
			e.jobManager.mu.Lock()
			e.jobManager.running[job.ID] = func() { cancelled.Store(true) }
			e.jobManager.mu.Unlock()
			reason := "operator cancelled after checking the result"
			if err := e.jobManager.AbortJob(job.ID, reason); err != nil {
				t.Fatal(err)
			}
			if !cancelled.Load() {
				t.Fatal("abort did not cancel the owned execution")
			}
			journal, err := readCompletionJournal(e.jobManager.completionJournalPath(job.ID))
			if err != nil || journal.checkpoint.Job.AbortReason != reason {
				t.Fatalf("abort was not durably checkpointed: %+v %v", journal, err)
			}

			// The fresh engine has neither the old active snapshot nor its cancel
			// function. Recovery must resolve the durable abort before persistence.
			fresh, err := NewEngine(e.config, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			fresh.multi.Master = true
			t.Cleanup(func() {
				fresh.cancel()
				fresh.jobManager.Wait()
				_ = fresh.blobStores.Close()
			})
			if err := fresh.jobManager.RecoverJobs(); err != nil {
				t.Fatal(err)
			}
			finished, ok := fresh.GetDeadJob(job.ID)
			if !ok || !finished.Complete || finished.Code != 1 || finished.AbortReason != reason || finished.Description != "Job Aborted: "+reason || finished.StorageError != "" {
				t.Fatalf("restart changed the durable cancellation into success or a generic restart failure: %+v", finished)
			}
			raw, err := fresh.storage.Get(context.Background(), "jobs:"+job.ID)
			var stored types.Job
			if err != nil || json.Unmarshal(raw, &stored) != nil || stored.Code != 1 || stored.AbortReason != reason || stored.Description != finished.Description {
				t.Fatalf("recovered cancellation was not durably committed: %s %v", raw, err)
			}
			rows, err := fresh.storage.ListGet(context.Background(), "active_jobs", 0, 0)
			if err != nil || len(rows) != 0 {
				t.Fatalf("recovered abort retained its execution reservation: %+v %v", rows, err)
			}
			if _, err := os.Stat(fresh.jobManager.completionJournalPath(job.ID)); !os.IsNotExist(err) {
				t.Fatalf("committed abort retained its recovery journal: %v", err)
			}
		})
	}
}
