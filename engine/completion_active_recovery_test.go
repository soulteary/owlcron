package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func TestActiveJournalRecoveryPreservesCompletionBookkeeping(t *testing.T) {
	for _, sharedReservation := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_reservation_%t", sharedReservation), func(t *testing.T) {
			e := splitTestEngine(t)
			job := &types.Job{ID: "interrupted-reserved-job", Event: "interrupted-event", Hostname: e.Hostname(), TimeStart: 1}
			e.jobManager.finishMu.Lock()
			err := e.jobManager.checkpointRuntimeJob(job)
			e.jobManager.finishMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if sharedReservation {
				if err := e.jobManager.persistActiveJob(job); err != nil {
					t.Fatal(err)
				}
			}
			staleTemp := filepath.Join(e.config.LogDir, "jobs", ".completion-reserve-interrupted")
			if err := os.WriteFile(staleTemp, make([]byte, 1<<20), 0600); err != nil {
				t.Fatal(err)
			}
			fresh, err := NewEngine(e.config, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = fresh.Shutdown() })
			fresh.multi.Master = true
			for range 2 {
				if err := fresh.jobManager.RecoverJobs(); err != nil {
					t.Fatal(err)
				}
				state := fresh.GetState()
				if state.Stats["jobs_completed"] != 1 || state.Stats["jobs_failed"] != 1 {
					t.Fatalf("interrupted execution lost or duplicated completion accounting: %+v", state.Stats)
				}
				finished, ok := fresh.GetDeadJob(job.ID)
				if !ok || finished.Code != 1 || finished.AbortReason != "Server restarted while job was active" {
					t.Fatalf("interrupted reserved execution was not archived: %+v", finished)
				}
				if _, err := os.Stat(staleTemp); !os.IsNotExist(err) {
					t.Fatalf("interrupted reservation allocation was not reclaimed: %v", err)
				}
			}
		})
	}
}
