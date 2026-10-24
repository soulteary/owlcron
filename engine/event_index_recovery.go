package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

// repairEventIndex recovers canonical records left by the old canonical-first
// create path. Built-in backends support PrefixKeyLister; adapters without enumeration
// can still load their existing index. The caller holds s.mu, as for loading.
func (s *Scheduler) repairEventIndex(ctx context.Context) error {
	lister, ok := s.engine.storage.(storage.PrefixKeyLister)
	if !ok {
		return nil
	}
	keys, err := lister.ListKeysPrefix(ctx, "events:")
	if err != nil {
		return fmt.Errorf("enumerate event records: %w", err)
	}
	items, err := s.engine.storage.ListGet(ctx, "events", 0, 0)
	if err != nil {
		return fmt.Errorf("read event index: %w", err)
	}
	indexed := make(map[string]bool, len(items))
	for _, item := range items {
		indexed[storageItemID(item)] = true
	}
	missing := make([]string, 0)
	for _, key := range keys {
		if !strings.HasPrefix(key, "events:") {
			continue
		}
		id := strings.TrimPrefix(key, "events:")
		if indexed[id] {
			continue
		}
		raw, err := s.engine.storage.Get(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			continue // Deleted after enumeration; never recreate its canonical data.
		}
		if err != nil {
			return fmt.Errorf("read orphan event %q: %w", id, err)
		}
		var event types.Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return fmt.Errorf("decode orphan event %q: %w", id, err)
		}
		if id == "" || event.ID != id {
			return fmt.Errorf("orphan event %q canonical ID mismatch: %q", id, event.ID)
		}
		missing = append(missing, id)
		indexed[id] = true
	}
	if len(missing) == 0 {
		return nil
	}
	// Append all missing IDs in one atomic update, retaining concurrent additions.
	// Do not call Get inside the callback: Filesystem Update holds the storage lock.
	return storage.UpdateList(ctx, s.engine.storage, "events", func(items []interface{}) ([]interface{}, error) {
		present := make(map[string]bool, len(items))
		for _, item := range items {
			present[storageItemID(item)] = true
		}
		for _, id := range missing {
			if !present[id] {
				items = append(items, map[string]string{"id": id})
				present[id] = true
			}
		}
		return items, nil
	})
}
