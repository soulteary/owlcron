package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/soulteary/owlcron/types"
)

const historyPrefix = "history/completed/"

func historyKey(id string) string {
	return historyPrefix + base64.RawURLEncoding.EncodeToString([]byte(id))
}

// HistoryUpsert writes one bounded index entry. The canonical jobs:<id> record
// must already be durable. Retrying completion never rewrites other jobs.
func HistoryUpsert(ctx context.Context, store Storage, job *types.Job) error {
	if job == nil || job.ID == "" {
		return fmt.Errorf("invalid history job")
	}
	deleted, err := historyDeleted(ctx, store, job.ID)
	if err != nil {
		return err
	}
	if deleted {
		return HistoryRemove(ctx, store, job.ID)
	}
	data, err := json.Marshal(struct {
		ID string `json:"id"`
	}{job.ID})
	if err != nil {
		return err
	}
	if err := store.Put(ctx, historyKey(job.ID), data); err != nil {
		return err
	}
	// Deletion may have committed between the initial check and this write.
	deleted, err = historyDeleted(ctx, store, job.ID)
	if err != nil {
		return err
	}
	if deleted {
		return HistoryRemove(ctx, store, job.ID)
	}
	return nil
}

func historyDeleted(ctx context.Context, store Storage, id string) (bool, error) {
	deleted, err := store.Exists(ctx, "deleted_jobs:"+id)
	if err != nil || deleted {
		return deleted, err
	}
	data, err := store.Get(ctx, "jobs:"+id)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var marker struct {
		Deleted bool `json:"deleted"`
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		return false, err
	}
	return marker.Deleted, nil
}

// HistoryRemove is idempotent. A canonical deletion tombstone must precede this
// operation: a concurrent completion or migration may recreate an index entry,
// but readers always check canonical state and cannot resurrect the job.
func HistoryRemove(ctx context.Context, store Storage, id string) error {
	if id == "" {
		return fmt.Errorf("invalid history job id")
	}
	err := store.Delete(ctx, historyKey(id))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// HistoryMigrate copies the legacy index before deleting it. Each copy and each
// deletion is retryable after a crash. All writers must use HistoryUpsert before
// migration starts; rolling upgrades with legacy writers are unsupported.
func HistoryMigrate(ctx context.Context, store Storage) error {
	lister, ok := store.(PrefixKeyLister)
	if !ok {
		return ErrUnsupported
	}
	keys, err := lister.ListKeysPrefix(ctx, "logs/events/")
	if err != nil {
		return err
	}
	keys = append([]string{"logs/completed"}, keys...)
	for _, key := range keys {
		items, err := store.ListGet(ctx, key, 0, 0)
		if err != nil {
			return err
		}
		for _, item := range items {
			data, err := json.Marshal(item)
			if err != nil {
				return err
			}
			var job types.Job
			if err := json.Unmarshal(data, &job); err != nil {
				return err
			}
			if err := HistoryUpsert(ctx, store, &job); err != nil {
				return err
			}
		}
		if err := store.ListDelete(ctx, key, false); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	return nil
}

// HistoryJobs enumerates the small index namespace, fetching each canonical job
// separately. It does not read logs or materialize legacy whole-list arrays.
func HistoryJobs(ctx context.Context, store Storage) ([]*types.Job, error) {
	lister, ok := store.(PrefixKeyLister)
	if !ok {
		return nil, ErrUnsupported
	}
	keys, err := lister.ListKeysPrefix(ctx, historyPrefix)
	if err != nil {
		return nil, err
	}
	jobs, err := historyReadKeys(ctx, store, keys)
	if err != nil {
		return nil, err
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].TimeStart == jobs[j].TimeStart {
			return jobs[i].ID > jobs[j].ID
		}
		return jobs[i].TimeStart > jobs[j].TimeStart
	})
	return jobs, nil
}

// HistoryPage scans at most limit raw index entries after a lexicographic key
// cursor. Missing and deleted canonical records still advance the cursor. The
// current listing API enumerates keys, but only this page's job bodies are read.
func HistoryPage(ctx context.Context, store Storage, after string, limit int) ([]*types.Job, string, error) {
	if limit <= 0 {
		return nil, "", fmt.Errorf("history page limit must be positive")
	}
	lister, ok := store.(PrefixKeyLister)
	if !ok {
		return nil, "", ErrUnsupported
	}
	keys, err := lister.ListKeysPrefix(ctx, historyPrefix)
	if err != nil {
		return nil, "", err
	}
	sort.Strings(keys)
	start := sort.Search(len(keys), func(i int) bool { return keys[i] > after })
	end := len(keys)
	if limit < len(keys)-start {
		end = start + limit
	}
	jobs, err := historyReadKeys(ctx, store, keys[start:end])
	if err != nil {
		return nil, "", err
	}
	next := ""
	if end < len(keys) {
		next = keys[end-1]
	}
	return jobs, next, nil
}

func historyReadKeys(ctx context.Context, store Storage, keys []string) ([]*types.Job, error) {
	jobs := make([]*types.Job, 0, len(keys))
	for _, key := range keys {
		id, err := base64.RawURLEncoding.DecodeString(key[len(historyPrefix):])
		if err != nil || len(id) == 0 {
			return nil, fmt.Errorf("invalid history key %q", key)
		}
		data, err := store.Get(ctx, "jobs:"+string(id))
		if errors.Is(err, ErrNotFound) {
			if err := HistoryRemove(ctx, store, string(id)); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		var record struct {
			types.Job
			Deleted bool `json:"deleted"`
		}
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, err
		}
		if record.Deleted {
			if err := HistoryRemove(ctx, store, string(id)); err != nil {
				return nil, err
			}
			continue
		}
		deleted, err := store.Exists(ctx, "deleted_jobs:"+string(id))
		if err != nil {
			return nil, err
		}
		if deleted {
			if err := HistoryRemove(ctx, store, string(id)); err != nil {
				return nil, err
			}
			continue
		}
		if record.ID != string(id) {
			return nil, fmt.Errorf("history canonical job id mismatch")
		}
		if !record.Complete {
			continue
		}
		jobs = append(jobs, &record.Job)
	}
	return jobs, nil
}
