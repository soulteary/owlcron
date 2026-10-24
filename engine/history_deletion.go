package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

var ErrHistoryCleanupPending = errors.New("job history cleanup pending completion")

// The marker is stored in the same atomic key as the log. Unlike a separate
// existence check, it fences a writer paused before deletion on another node.
var deletedLogMarker = []byte("\x00owlcron-deleted-log-v1\n")

const historyCleanupPrefix = "history_cleanup/"

type historyCleanupIntent struct {
	ID      string `json:"id"`
	History bool   `json:"history"`
}

func (e *Engine) ensureHistoryMigrated(ctx context.Context) error {
	e.historyMigrationMu.Lock()
	defer e.historyMigrationMu.Unlock()
	if e.historyMigrated {
		return nil
	}
	if err := storage.HistoryMigrate(ctx, e.storage); err != nil {
		return err
	}
	e.historyMigrated = true
	return nil
}

func (e *Engine) EnsureHistoryMigrated(ctx context.Context) error {
	return e.ensureHistoryMigrated(ctx)
}

func historyCleanupKey(id string, history bool) string {
	kind := "log-"
	if history {
		kind = "job-"
	}
	return historyCleanupPrefix + kind + base64.RawURLEncoding.EncodeToString([]byte(id))
}

func (e *Engine) IsJobLogDeleted(id string) (bool, error) {
	return e.isJobLogDeleted(context.Background(), id)
}

func (e *Engine) isJobLogDeleted(ctx context.Context, id string) (bool, error) {
	return e.storage.Exists(ctx, "deleted_job_logs:"+id)
}

func (e *Engine) canCleanJob(job *types.Job) error {
	if job == nil || !job.Complete || job.StorageError != "" {
		return ErrHistoryCleanupPending
	}
	if _, active := e.GetActiveJobs()[job.ID]; active {
		return ErrHistoryCleanupPending
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err == nil {
		return ErrHistoryCleanupPending
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, key := range []string{"active_jobs", "remote_completions"} {
		items, err := e.storage.ListGet(context.Background(), key, 0, 0)
		if err != nil {
			return err
		}
		for _, item := range items {
			if storageItemID(item) == job.ID {
				return ErrHistoryCleanupPending
			}
		}
	}
	return nil
}

func (e *Engine) DeleteJobHistory(id string) error {
	return e.startHistoryCleanup(id, true)
}

func (e *Engine) DeleteJobLog(id string) error {
	return e.startHistoryCleanup(id, false)
}

func (e *Engine) startHistoryCleanup(id string, history bool) error {
	if id == "" {
		return fmt.Errorf("missing job id")
	}
	e.jobManager.finishMu.Lock()
	defer e.jobManager.finishMu.Unlock()
	ctx := context.Background()
	key := historyCleanupKey(id, history)
	if raw, err := e.storage.Get(ctx, key); err == nil {
		var intent historyCleanupIntent
		if err := json.Unmarshal(raw, &intent); err != nil {
			return err
		}
		if intent.ID != id || intent.History != history {
			return fmt.Errorf("invalid cleanup intent")
		}
		return e.finishHistoryCleanup(ctx, key, intent)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	job, exists := e.LookupJob(id)
	if !exists {
		deleted, err := e.IsJobHistoryDeleted(id)
		if err != nil {
			return err
		}
		if !deleted {
			return nil
		}
	} else if err := e.canCleanJob(job); err != nil {
		return err
	}
	intent := historyCleanupIntent{ID: id, History: history}
	raw, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if err := e.storage.Put(ctx, key, raw); err != nil {
		return err
	}
	return e.finishHistoryCleanup(ctx, key, intent)
}

// finishHistoryCleanup is replayable after every write. Never remove the small
// deletion markers: delayed completions can arrive after arbitrary outages.
func (e *Engine) finishHistoryCleanup(ctx context.Context, key string, intent historyCleanupIntent) error {
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	id := intent.ID
	if intent.History {
		if err := e.storage.Put(ctx, "deleted_jobs:"+id, []byte(`{"deleted":true}`)); err != nil {
			return err
		}
		marker, err := json.Marshal(struct {
			ID       string `json:"id"`
			Complete bool   `json:"complete"`
			Deleted  bool   `json:"deleted"`
		}{id, true, true})
		if err != nil {
			return err
		}
		if err := atomic.Update(ctx, "jobs:"+id, func([]byte) ([]byte, error) { return marker, nil }); err != nil {
			return err
		}
	}
	if err := e.storage.Put(ctx, "deleted_job_logs:"+id, []byte(`{"deleted":true}`)); err != nil {
		return err
	}
	if err := storage.JobLogDelete(ctx, e.storage, id); err != nil {
		return err
	}
	if err := atomic.Update(ctx, "job_logs:"+id, func([]byte) ([]byte, error) { return deletedLogMarker, nil }); err != nil {
		return err
	}
	if intent.History {
		if err := storage.HistoryRemove(ctx, e.storage, id); err != nil {
			return err
		}
		e.DeleteDeadJob(id)
	}
	if err := e.removeLocalJobLog(id); err != nil {
		return err
	}
	err := e.storage.Delete(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	return err
}

func (e *Engine) removeLocalJobLog(id string) error {
	if id == "" || filepath.Base(id) != id || id == "." || id == ".." {
		return fmt.Errorf("unsafe job log id")
	}
	e.jobManager.logMu.Lock()
	defer e.jobManager.logMu.Unlock()
	path := filepath.Join(e.config.LogDir, "jobs", id+".log")
	// Refuse directory symlinks so retention cannot traverse outside the job log root.
	for _, dir := range []string{e.config.LogDir, filepath.Dir(path)} {
		info, err := os.Lstat(dir)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in job log directory")
		}
	}
	logRoot, err := os.OpenRoot(e.config.LogDir)
	if err != nil {
		return err
	}
	defer logRoot.Close()
	jobsRoot, err := logRoot.OpenRoot("jobs")
	if err != nil {
		return err
	}
	defer jobsRoot.Close()
	err = jobsRoot.Remove(id + ".log")
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (e *Engine) RetryHistoryDeletions(ctx context.Context) error {
	lister, ok := e.storage.(storage.PrefixKeyLister)
	if !ok {
		return storage.ErrUnsupported
	}
	keys, err := lister.ListKeysPrefix(ctx, historyCleanupPrefix)
	if err != nil {
		return err
	}
	e.jobManager.finishMu.Lock()
	defer e.jobManager.finishMu.Unlock()
	for _, key := range keys {
		raw, err := e.storage.Get(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var intent historyCleanupIntent
		if err := json.Unmarshal(raw, &intent); err != nil {
			return err
		}
		if intent.ID == "" || key != historyCleanupKey(intent.ID, intent.History) {
			return fmt.Errorf("invalid history cleanup intent")
		}
		if err := e.finishHistoryCleanup(ctx, key, intent); err != nil {
			return err
		}
	}
	return nil
}
