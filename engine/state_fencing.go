package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

var errStaleStateOwner = errors.New("global state is owned by another leadership generation")

func encodeOwnedState(state *types.State, owner string) ([]byte, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	var fields map[string]interface{}
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	fields["_lease_owner"] = owner
	return json.Marshal(fields)
}

// A new lease can replace only its own state generation or the predecessor
// observed by the lease CAS. A delayed, older promotion cannot steal it back.
func (e *Engine) claimStateOwnership() error {
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return fmt.Errorf("global state requires atomic storage")
	}
	initial, err := encodeOwnedState(e.GetState(), e.leaseToken)
	if err != nil {
		return err
	}
	e.mu.RLock()
	predecessors := append([]string(nil), e.statePredecessors...)
	e.mu.RUnlock()
	err = atomic.Update(e.ctx, "global/state", func(previous []byte) ([]byte, error) {
		if len(previous) == 0 {
			return initial, nil
		}
		var fields map[string]interface{}
		if err := json.Unmarshal(previous, &fields); err != nil {
			return nil, err
		}
		owner, _ := fields["_lease_owner"].(string)
		allowed := owner == "" || owner == e.leaseToken
		for _, predecessor := range predecessors {
			if owner == predecessor {
				allowed = true
			}
		}
		if !allowed {
			return nil, errStaleStateOwner
		}
		fields["_lease_owner"] = e.leaseToken
		return json.Marshal(fields)
	})
	if err != nil {
		return err
	}
	// Preserve the predecessor chain through interrupted promotions. Once state
	// ownership commits, the current owner is sufficient for the next takeover.
	return atomic.Update(e.ctx, "global/master_lease", func(raw []byte) ([]byte, error) {
		var lease leadershipLease
		if err := json.Unmarshal(raw, &lease); err != nil {
			return nil, err
		}
		if lease.Owner != e.leaseToken || lease.Expires <= time.Now().UnixNano() {
			return nil, errLeadershipHeld
		}
		lease.PreviousOwners = nil
		return json.Marshal(lease)
	})
}
