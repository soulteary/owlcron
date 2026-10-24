package engine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

// CreateEvent separates creation from update so create-only principals cannot
// replace an existing event by supplying its ID.
func (s *Scheduler) CreateEvent(event *types.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.events[event.ID]; exists {
		return fmt.Errorf("event already exists")
	}
	atomic, ok := s.engine.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// Publish the index first. A failed index write must not reserve the ID:
	// otherwise the durable event cannot be loaded or retried after a restart.
	// A dangling index is safe because loaders require the canonical record.
	if _, err := s.engine.storage.Get(s.engine.ctx, "events:"+event.ID); err == nil {
		return fmt.Errorf("event already exists")
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if err := upsertStorageList(s.engine.ctx, s.engine.storage, "events", event.ID, map[string]string{"id": event.ID}, false); err != nil {
		return err
	}
	// The initial read is only an optimization; CAS remains the authority when
	// another scheduler creates this ID concurrently.
	if err := atomic.Update(s.engine.ctx, "events:"+event.ID, func(existing []byte) ([]byte, error) {
		if len(existing) > 0 {
			return nil, fmt.Errorf("event already exists")
		}
		return raw, nil
	}); err != nil {
		return err
	}
	s.events[event.ID] = types.CloneEvent(event)
	return nil
}
