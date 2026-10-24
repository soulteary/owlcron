package engine

// Lifecycle requests signal the executable to close HTTP and engine resources
// before exiting or starting its replacement process.
func (e *Engine) RequestShutdown()                   { e.cancel() }
func (e *Engine) ShutdownRequested() <-chan struct{} { return e.ctx.Done() }
func (e *Engine) RequestRestart()                    { e.mu.Lock(); e.restartRequested = true; e.mu.Unlock(); e.cancel() }
func (e *Engine) RestartRequested() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.restartRequested
}
