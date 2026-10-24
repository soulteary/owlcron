package engine

import (
	"context"
	"errors"
	"time"
)

// ReadinessTimeout bounds a storage health check independently of HTTP timeouts.
const ReadinessTimeout = 2 * time.Second

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
	e.readinessMu.Lock()
	probe := e.readinessProbe
	if probe == nil {
		probe = &readinessProbe{done: make(chan struct{})}
		e.readinessProbe = probe
		go func() {
			probeCtx, cancel := context.WithTimeout(e.ctx, ReadinessTimeout)
			defer cancel()
			// Existence is immaterial: an empty namespace is reachable too. Exists
			// avoids downloading potentially large state records on every probe.
			_, probe.err = e.storage.Exists(probeCtx, "global/state")
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
		if !e.ready.Load() || e.draining.Load() || e.ctx.Err() != nil {
			return errNotReady
		}
		return probe.err
	}
}
