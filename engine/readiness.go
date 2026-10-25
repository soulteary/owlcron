package engine

import (
	"context"
	"errors"
	"time"
)

// ReadinessTimeout bounds a storage health check independently of HTTP timeouts.
const ReadinessTimeout = 2 * time.Second

// readinessProbeKey stores a tiny startup marker for read-only reachability
// checks. Probing this key never downloads application state.
const readinessProbeKey = "global/readiness_probe"

// initializeReadiness bounds startup even if a backend ignores cancellation.
// A delayed write is harmless: every node writes the same constant marker.
func (e *Engine) initializeReadiness() error {
	ctx, cancel := context.WithTimeout(e.ctx, ReadinessTimeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- e.storage.Put(ctx, readinessProbeKey, []byte("1")) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
}

var errNotReady = errors.New("engine is not ready")

type readinessProbe struct {
	done chan struct{}
	err  error
}

// Readiness checks local initialization and shared storage access. A healthy
// worker does not need to be the leader. Concurrent callers share one probe so
// a backend that ignores cancellation cannot accumulate blocked goroutines.
func (e *Engine) Readiness(ctx context.Context) error {
	if !e.ready.Load() || e.draining.Load() || e.ctx.Err() != nil {
		return errNotReady
	}
	err := e.storageReadiness(ctx)
	if !e.ready.Load() || e.draining.Load() || e.ctx.Err() != nil {
		return errNotReady
	}
	return err
}

// StartupReadiness deliberately makes no remote calls. An initialized process
// remains alive to renew leases and retry uploads during a blob outage.
func (e *Engine) StartupReadiness() error {
	if !e.ready.Load() || e.draining.Load() || e.ctx.Err() != nil {
		return errNotReady
	}
	return nil
}

func (e *Engine) storageReadiness(ctx context.Context) error {
	e.readinessMu.Lock()
	probe := e.readinessProbe
	if probe == nil {
		probe = &readinessProbe{done: make(chan struct{})}
		e.readinessProbe = probe
		go func() {
			probeCtx, cancel := context.WithTimeout(e.ctx, ReadinessTimeout)
			defer cancel()
			// Backends may implement Exists using Get. Startup creates this
			// tiny marker, so even that implementation has bounded payload.
			var exists bool
			exists, probe.err = e.storage.Exists(probeCtx, readinessProbeKey)
			if probe.err == nil && !exists {
				probe.err = errNotReady
			}
			if probe.err == nil && e.blobStores != nil {
				probe.err = e.blobStores.Readiness(probeCtx, e.storageLayout.NamespaceID)
			}
			if probe.err == nil && e.spoolBudget != nil {
				probe.err = e.spoolBudget.admit()
			}
			// A backend may ignore cancellation and return success after the
			// shared deadline. Never publish that late result as readiness.
			if err := probeCtx.Err(); err != nil {
				probe.err = err
			}
			e.readinessMu.Lock()
			e.readinessProbe = nil
			close(probe.done)
			e.readinessMu.Unlock()
		}()
	}
	e.readinessMu.Unlock()
	timer := time.NewTimer(ReadinessTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return errNotReady
	case <-timer.C:
		return context.DeadlineExceeded
	case <-probe.done:
		return probe.err
	}
}
