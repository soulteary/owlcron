package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/soulteary/owlcron/storage"
)

var errLeadershipHeld = errors.New("leadership is held by another node")

type leadershipLease struct {
	Hostname       string   `json:"hostname"`
	Owner          string   `json:"owner"`
	Expires        int64    `json:"expires"`
	PreviousOwners []string `json:"previous_owners,omitempty"`
}

func newLeadershipToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
func (e *Engine) leaseDuration() time.Duration {
	duration := time.Duration(e.config.MasterPingTimeout) * time.Second
	if duration < time.Second {
		duration = time.Second
	}
	return duration
}
func (e *Engine) claimLeadership() error {
	e.leaseMu.Lock()
	defer e.leaseMu.Unlock()
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return fmt.Errorf("leadership requires atomic storage")
	}
	var expiry int64
	var predecessors []string
	var acquired bool
	err := atomic.Update(e.ctx, "global/master_lease", func(data []byte) ([]byte, error) {
		now := time.Now().UnixNano()
		var lease leadershipLease
		if len(data) > 0 {
			if err := json.Unmarshal(data, &lease); err != nil {
				return nil, err
			}
		}
		if lease.Owner != "" && lease.Owner != e.leaseToken && lease.Expires > now {
			return nil, errLeadershipHeld
		}
		acquired = lease.Owner != e.leaseToken
		predecessors = append([]string(nil), lease.PreviousOwners...)
		if acquired && lease.Owner != "" {
			predecessors = append(predecessors, lease.Owner)
		}
		expiry = time.Now().Add(e.leaseDuration()).UnixNano()
		return json.Marshal(leadershipLease{Hostname: e.Hostname(), Owner: e.leaseToken, Expires: expiry, PreviousOwners: predecessors})
	})
	if err == nil {
		e.mu.Lock()
		e.leaseUntil = expiry
		if acquired {
			e.statePredecessors = append([]string(nil), predecessors...)
		}
		e.mu.Unlock()
	}
	return err
}

// EnsureLeadership fences each scheduling/manual dispatch against the shared
// atomic lease. A process with an expired or replaced token cannot dispatch.
func (e *Engine) EnsureLeadership() bool {
	e.mu.RLock()
	master := e.multi.Master
	e.mu.RUnlock()
	if !master || e.ctx.Err() != nil {
		return false
	}
	if err := e.claimLeadership(); err == nil {
		return true
	}
	e.mu.Lock()
	e.multi.Master = false
	e.mu.Unlock()
	// Called by the scheduler too: retire its loop asynchronously to avoid
	// waiting for the calling tick itself from Scheduler.Shutdown.
	go e.retireLeadership()
	return false
}
func (e *Engine) retireLeadership() {
	e.roleMu.Lock()
	defer e.roleMu.Unlock()
	e.mu.RLock()
	master := e.multi.Master
	e.mu.RUnlock()
	if master {
		return
	}
	e.scheduler.Shutdown()
	e.mu.Lock()
	e.stateOwner = false
	e.mu.Unlock()
	e.comm.ResetCluster()
}
func (e *Engine) releaseLeadership() error {
	e.leaseMu.Lock()
	defer e.leaseMu.Unlock()
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	err := atomic.Update(context.Background(), "global/master_lease", func(data []byte) ([]byte, error) {
		var lease leadershipLease
		if len(data) > 0 {
			if err := json.Unmarshal(data, &lease); err != nil {
				return nil, err
			}
		}
		if lease.Owner != e.leaseToken {
			return data, nil
		}
		lease.Expires = 0
		return json.Marshal(lease)
	})
	e.mu.Lock()
	e.leaseUntil = 0
	e.mu.Unlock()
	return err
}
func (e *Engine) ownsStoredLeadership() (bool, error) {
	raw, err := e.storage.Get(context.Background(), "global/master_lease")
	if errors.Is(err, storage.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var lease leadershipLease
	if err := json.Unmarshal(raw, &lease); err != nil {
		return false, err
	}
	return lease.Owner == e.leaseToken && lease.Expires > time.Now().UnixNano(), nil
}

func (e *Engine) LeadershipToken() string { return e.leaseToken }
func (e *Engine) ValidateNodeLeadership(hostname, owner string) bool {
	if hostname == "" || owner == "" {
		return false
	}
	raw, err := e.storage.Get(e.ctx, "global/master_lease")
	if err != nil {
		return false
	}
	var lease leadershipLease
	return json.Unmarshal(raw, &lease) == nil && lease.Hostname == hostname && lease.Owner == owner && lease.Expires > time.Now().UnixNano()
}
