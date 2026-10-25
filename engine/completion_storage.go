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

func (jm *JobManager) completionSpoolPath(id string) string {
	return filepath.Join(jm.engine.config.LogDir, "jobs", id+".completed.json")
}
func (jm *JobManager) writeCompletionSpool(job *types.Job) error {
	path := jm.completionSpoolPath(job.ID)
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
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	paths, err := filepath.Glob(filepath.Join(jm.engine.config.LogDir, "jobs", "*.completed.json"))
	if err != nil {
		return err
	}
	pending := make(map[string]*types.Job)
	jm.engine.mu.RLock()
	for id, job := range jm.engine.deadJobs {
		if job.Complete && job.StorageError != "" {
			pending[id] = types.CloneJob(job)
		}
	}
	jm.engine.mu.RUnlock()
	var retryErrors []error
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			retryErrors = append(retryErrors, err)
			continue
		}
		var job types.Job
		if err = json.Unmarshal(raw, &job); err != nil {
			retryErrors = append(retryErrors, fmt.Errorf("invalid completion spool %s: %w", path, err))
			continue
		}
		if !job.Complete || job.ID == "" || jm.completionSpoolPath(job.ID) != path {
			retryErrors = append(retryErrors, fmt.Errorf("invalid completion spool job: %s", path))
			continue
		}
		snapshot := &job
		if current := pending[job.ID]; current != nil {
			snapshot = current
			delete(pending, job.ID)
		}
		if err := jm.retryPendingCompletion(snapshot, true); err != nil {
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
		if err = os.Remove(jm.completionSpoolPath(job.ID)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
