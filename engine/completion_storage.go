package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/soulteary/owlcron/types"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.uber.org/zap"
)

var ErrJobHistoryDeleted = errors.New("job history was deleted")

func (e *Engine) IsJobHistoryDeleted(id string) (bool, error) {
	if e.storage == nil {
		return false, fmt.Errorf("storage not initialized")
	}
	return e.storage.Exists(context.Background(), "deleted_jobs:"+id)
}

func (jm *JobManager) legacyCompletionSpoolPath(id string) string {
	return filepath.Join(jm.engine.config.LogDir, "jobs", id+".completed.json")
}
func (jm *JobManager) completionSpoolPath(id string) string {
	if _, err := os.Lstat(jm.completionJournalPath(id)); err == nil {
		return jm.completionJournalPath(id)
	}
	return jm.legacyCompletionSpoolPath(id)
}
func (jm *JobManager) writeCompletionSpool(job *types.Job) error {
	if _, err := os.Lstat(jm.completionJournalPath(job.ID)); err == nil {
		return jm.writeCompletionCheckpoint(job, completionPhaseSealed)
	} else if !os.IsNotExist(err) {
		return err
	}
	path := jm.legacyCompletionSpoolPath(job.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return jm.engine.writeSpoolFile(path, raw, ".complete-*")
}
func (jm *JobManager) persistCompletion(job *types.Job) error {
	started := time.Now()
	stage := started
	var logTime, historyTime, notificationTime, removalTime time.Duration
	defer func() {
		if elapsed := time.Since(started); elapsed >= time.Second {
			jm.logger.Warn("Slow completion persistence", zap.String("job_id", job.ID), zap.Duration("elapsed", elapsed), zap.Duration("log", logTime), zap.Duration("history", historyTime), zap.Duration("notifications", notificationTime), zap.Duration("active_removal", removalTime))
		}
	}()
	if deleted, err := jm.engine.IsJobHistoryDeleted(job.ID); err != nil {
		return err
	} else if deleted {
		return ErrJobHistoryDeleted
	}
	sanitizeJobReports(job)
	if err := jm.uploadJobLogToStorage(job); err != nil {
		return err
	}
	logTime = time.Since(stage)
	stage = time.Now()
	if err := jm.finishJobStorage(job); err != nil {
		return err
	}
	historyTime = time.Since(stage)
	stage = time.Now()
	if err := jm.enqueueCompletionNotifications(job); err != nil {
		return err
	}
	notificationTime = time.Since(stage)
	stage = time.Now()
	if jm.engine.storage != nil {
		err := removeStorageList(context.Background(), jm.engine.storage, "active_jobs", job.ID)
		removalTime = time.Since(stage)
		return err
	}
	return nil
}

// RetryPendingCompletions is idempotent and also runs before crash recovery. The
// local spool survives a backend outage after a plugin has already completed.
// If even the first spool write failed, the finished in-memory snapshot remains
// retryable. Try to make it durable before contacting the backend again, so a
// recovered local disk protects the result across a continued backend outage.
func (jm *JobManager) RetryPendingCompletions() error {
	return jm.retryPendingCompletions(false)
}

func (jm *JobManager) retryPendingCompletions(startup bool) error {
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	paths, err := filepath.Glob(filepath.Join(jm.engine.config.LogDir, "jobs", "*.completed.json"))
	if err != nil {
		return err
	}
	journals, err := filepath.Glob(filepath.Join(jm.engine.config.LogDir, "jobs", "*.completion-wal"))
	if err != nil {
		return err
	}
	paths = append(journals, paths...)
	pending := make(map[string]*types.Job)
	jm.engine.mu.RLock()
	for id, job := range jm.engine.deadJobs {
		if job.Complete && job.StorageError != "" {
			pending[id] = types.CloneJob(job)
		}
	}
	jm.engine.mu.RUnlock()
	var retryErrors []error
	seen := make(map[string]bool)
	for _, path := range paths {
		var snapshot *types.Job
		if isCompletionJournal(path) {
			snapshot, err = jm.recoverCompletionJournal(path, startup)
			if err != nil {
				retryErrors = append(retryErrors, fmt.Errorf("recover completion journal %s: %w", path, err))
				continue
			}
			if snapshot == nil {
				continue
			}
		} else {
			raw, err := os.ReadFile(path)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				retryErrors = append(retryErrors, err)
				continue
			}
			snapshot = &types.Job{}
			if err = json.Unmarshal(raw, snapshot); err != nil {
				retryErrors = append(retryErrors, fmt.Errorf("invalid completion spool %s: %w", path, err))
				continue
			}
			if !snapshot.Complete || snapshot.ID == "" || jm.legacyCompletionSpoolPath(snapshot.ID) != path {
				retryErrors = append(retryErrors, fmt.Errorf("invalid completion spool job: %s", path))
				continue
			}
		}
		if seen[snapshot.ID] {
			continue
		}
		seen[snapshot.ID] = true
		spooled := true
		if current := pending[snapshot.ID]; current != nil {
			snapshot = current
			delete(pending, snapshot.ID)
			spooled = false
		}
		if err := jm.retryPendingCompletion(snapshot, spooled); err != nil {
			retryErrors = append(retryErrors, err)
		}
	}
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := jm.retryPendingCompletion(pending[id], false); err != nil {
			retryErrors = append(retryErrors, err)
		}
	}
	return errors.Join(retryErrors...)
}

// The caller holds finishMu, so a retry never repeats completion counters,
// event updates, or chained jobs, nor races a runtime update of the same job.
func (jm *JobManager) retryPendingCompletion(job *types.Job, spooled bool) error {
	job.StorageError = ""
	var spoolErr error
	if !spooled {
		spoolErr = jm.writeCompletionSpool(job)
		spooled = spoolErr == nil
		if !spooled {
			if info, err := os.Lstat(jm.completionSpoolPath(job.ID)); err == nil && info.Mode().IsRegular() {
				spooled = true
			}
		}
	}
	err := jm.persistCompletion(job)
	if errors.Is(err, ErrJobHistoryDeleted) {
		jm.engine.DeleteDeadJob(job.ID)
		jm.AckRemoteCompletion(job.ID)
	} else {
		if err == nil && !jm.engine.IsMaster() {
			err = jm.enqueueRemoteCompletion(job)
		}
		if err != nil {
			err = errors.Join(spoolErr, err)
			job.StorageError = err.Error()
			jm.engine.AddDeadJob(job)
			return fmt.Errorf("retry completion %s: %w", job.ID, err)
		}
		jm.engine.AddDeadJob(job)
	}
	if spooled {
		if err = jm.removeCompletionSpool(job.ID); err != nil {
			return err
		}
	}
	return nil
}
