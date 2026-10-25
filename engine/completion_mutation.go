package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/soulteary/owlcron/types"
)

// Runtime changes share the completion fence: recovery space and the local
// checkpoint must commit before the larger snapshot becomes visible.
func (jm *JobManager) checkpointRuntimeJob(job *types.Job) error {
	if err := jm.ensureCompletionReservation(job); err != nil {
		return err
	}
	if job.Hostname != "local" && job.Hostname != jm.getHostname() {
		return nil
	}
	phase := completionPhaseActive
	if job.Complete {
		phase = completionPhaseReported
	}
	return jm.writeCompletionCheckpoint(job, phase)
}

func (jm *JobManager) prepareJobAbort(id, reason string) (*types.Job, error) {
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	jm.engine.mu.RLock()
	job := types.CloneJob(jm.engine.activeJobs[id])
	jm.engine.mu.RUnlock()
	if job == nil {
		return nil, fmt.Errorf("job not found: %s", id)
	}
	job.AbortReason = reason
	reserveErr := jm.ensureCompletionReservation(job)
	if reserveErr != nil {
		// Cancellation must remain possible at the hard limit. The terminal
		// margin covers bounded diagnostics even when no growth is available.
		job.AbortReason = boundedCompletionDiagnostic(reason)
		terminal := types.CloneJob(job)
		terminal.Complete = true
		terminal.Code = 1
		terminal.Description = "Job Aborted: " + job.AbortReason
		if fitErr := jm.completionReservationFits(terminal); fitErr == nil {
			reserveErr = nil
		} else {
			reserveErr = errors.Join(reserveErr, fitErr)
		}
	}
	if job.Hostname == "local" || job.Hostname == jm.getHostname() {
		phase := completionPhaseActive
		if job.Complete {
			phase = completionPhaseReported
		}
		reserveErr = errors.Join(reserveErr, jm.writeCompletionCheckpoint(job, phase))
	}
	jm.engine.mu.Lock()
	if current := jm.engine.activeJobs[id]; current != nil {
		current.AbortReason = job.AbortReason
		job = types.CloneJob(current)
	}
	jm.engine.mu.Unlock()
	return job, reserveErr
}

// Startup recovers interrupted executions through the normal completion path,
// preserving its counters, activity, error chains, and queue wakeups. Reported
// results use the idempotent replay path instead of repeating those effects.
func (jm *JobManager) recoverActiveCompletionJournals() error {
	if err := jm.cleanCompletionReservationTemps(); err != nil {
		return err
	}
	paths, err := filepath.Glob(filepath.Join(jm.engine.config.LogDir, "jobs", "*.completion-wal"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		job, err := jm.recoverInterruptedJournal(path)
		if err != nil {
			return err
		}
		if job != nil {
			jm.finishLocalJob(job)
		}
	}
	return nil
}

func (jm *JobManager) cleanCompletionReservationTemps() error {
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	root, err := os.OpenRoot(filepath.Join(jm.engine.config.LogDir, "jobs"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	removed := false
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".completion-reserve-") {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if err := root.Remove(entry.Name()); err != nil && !os.IsNotExist(err) {
				return err
			}
			removed = true
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if removed {
		if err := dir.Sync(); err != nil {
			return err
		}
		if jm.engine.spoolBudget != nil {
			return jm.engine.spoolBudget.refresh(jm.engine.ctx)
		}
	}
	return nil
}

func (jm *JobManager) recoverInterruptedJournal(path string) (*types.Job, error) {
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	journal, err := readCompletionJournal(path)
	if err != nil {
		return nil, err
	}
	if journal.checkpoint.Phase != completionPhaseActive {
		return nil, nil
	}
	job := types.CloneJob(journal.checkpoint.Job)
	if jm.completionJournalPath(job.ID) != path || job.Hostname != "local" && job.Hostname != jm.getHostname() {
		return nil, fmt.Errorf("invalid active completion journal job")
	}
	jm.mu.RLock()
	_, running := jm.running[job.ID]
	jm.mu.RUnlock()
	if running {
		return nil, nil
	}
	if err := stopRecoveredJobProcess(job); err != nil {
		return nil, err
	}
	job.Code = 1
	if job.AbortReason == "" {
		job.AbortReason = "Server restarted while job was active"
		job.Description = job.AbortReason
	} else {
		job.AbortReason = boundedCompletionDiagnostic(job.AbortReason)
		job.Description = "Job Aborted: " + job.AbortReason
	}
	job.Retries = 0
	return job, nil
}
