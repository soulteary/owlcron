package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func readStoredHistory(ctx context.Context, store storage.Storage, event string) ([]*types.Job, error) {
	jobs, err := storage.HistoryJobs(ctx, store)
	if err != nil {
		return nil, err
	}
	filtered := make([]*types.Job, 0, len(jobs))
	for _, job := range jobs {
		if event == "" || job.Event == event {
			filtered = append(filtered, job)
		}
	}
	return filtered, nil
}

func (s *injectedStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}
func (s *blockedLogStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}
func (s *lifecycleStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}
func (s *queueFailureStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}
func (s *pausedStateStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}

// readAllStoredLog is deliberately test-only: production callers use bounded
// pages, while fixtures compare their complete expected logs.
func readAllStoredLog(ctx context.Context, store storage.Storage, id string) ([]byte, error) {
	var all []byte
	var offset int64
	for {
		page, manifest, err := storage.JobLogRead(ctx, store, id, offset, storage.JobLogChunkSize)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		offset += int64(len(page))
		if offset >= manifest.Offset {
			return all, nil
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("empty page before stored log end")
		}
	}
}

func (s *injectedStorage) GetRange(ctx context.Context, key string, offset int64, limit int) ([]byte, int64, error) {
	return s.Storage.(storage.RangeStorage).GetRange(ctx, key, offset, limit)
}
func (s *blockedLogStorage) GetRange(ctx context.Context, key string, offset int64, limit int) ([]byte, int64, error) {
	return s.Storage.(storage.RangeStorage).GetRange(ctx, key, offset, limit)
}
func (s *cleanupTestStorage) GetRange(ctx context.Context, key string, offset int64, limit int) ([]byte, int64, error) {
	return s.Storage.(storage.RangeStorage).GetRange(ctx, key, offset, limit)
}

func (s *injectedStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	if s.failWrites {
		return errors.New("injected storage outage")
	}
	return s.Storage.(storage.LegacyLogRemover).RemoveLegacyJobLog(ctx, id)
}
func (s *blockedLogStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	return s.Storage.(storage.LegacyLogRemover).RemoveLegacyJobLog(ctx, id)
}
func (s *cleanupTestStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	if err := s.intercept("delete", "job_logs:"+id); err != nil {
		return err
	}
	return s.Storage.(storage.LegacyLogRemover).RemoveLegacyJobLog(ctx, id)
}

func (s *lifecycleStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	return s.Storage.(storage.LegacyLogRemover).RemoveLegacyJobLog(ctx, id)
}
func (s *pausedStateStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	return s.Storage.(storage.LegacyLogRemover).RemoveLegacyJobLog(ctx, id)
}
func (s *queueFailureStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	return s.Storage.(storage.LegacyLogRemover).RemoveLegacyJobLog(ctx, id)
}
