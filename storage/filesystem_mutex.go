package storage

import (
	"context"
	"sync"
)

// filesystemMutex is shared by all storage handles for one canonical directory.
// A channel gate lets blocked callers honor cancellation without polling or
// creating a helper goroutine for every wait. The zero value is usable.
type filesystemMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *filesystemMutex) initialize() {
	m.once.Do(func() { m.gate = make(chan struct{}, 1); m.gate <- struct{}{} })
}
func (m *filesystemMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.initialize()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.gate:
	}
	// Cancellation and the token can become ready together. A canceled waiter
	// must return the token rather than executing a storage operation.
	if err := ctx.Err(); err != nil {
		m.Unlock()
		return err
	}
	return nil
}
func (m *filesystemMutex) Lock() { _ = m.LockContext(context.Background()) }
func (m *filesystemMutex) Unlock() {
	m.initialize()
	select {
	case m.gate <- struct{}{}:
	default:
		panic("unlock of unlocked filesystem mutex")
	}
}
