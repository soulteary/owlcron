package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/soulteary/owlcron/types"
	"os"
	"path/filepath"
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
func (jm *JobManager) RetryPendingCompletions() error {
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	paths, err := filepath.Glob(filepath.Join(jm.engine.config.LogDir, "jobs", "*.completed.json"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		var job types.Job
		if err = json.Unmarshal(raw, &job); err != nil {
			return fmt.Errorf("invalid completion spool: %w", err)
		}
		if !job.Complete || job.ID == "" {
			return fmt.Errorf("invalid completion spool job")
		}
		job.StorageError = ""
		if err = jm.persistCompletion(&job); errors.Is(err, ErrJobHistoryDeleted) {
			jm.engine.DeleteDeadJob(job.ID)
			jm.AckRemoteCompletion(job.ID)
			if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		} else if err != nil {
			return err
		}
		jm.engine.AddDeadJob(&job)
		if !jm.engine.IsMaster() {
			if err = jm.enqueueRemoteCompletion(&job); err != nil {
				return err
			}
		}
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
