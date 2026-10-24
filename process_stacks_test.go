package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Capture only the owned test child. Go's Process handle prevents signalling an
// already-waited PID; stack files remain private and have a fixed 1 MiB bound.
func (f *processFixture) captureStacks(t *testing.T) {
	t.Helper()
	f.processMu.Lock()
	p := f.currentProcess
	f.processMu.Unlock()
	if p == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	path := f.ConfigPath + ".goroutines"
	_ = os.Remove(path)
	if err := p.cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Logf("stack capture unavailable for %s", f.Config.Hostname)
		return
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			if dir := os.Getenv("OWLCRON_ACCEPTANCE_RESULTS"); dir != "" {
				name := filepath.Join(dir, fmt.Sprintf("stacks-%s-%d.txt", f.Config.Hostname, time.Now().UnixNano()))
				if err := os.WriteFile(name, data, 0600); err == nil {
					t.Logf("goroutine snapshot node=%s file=%s bytes=%d", f.Config.Hostname, name, len(data))
					return
				}
			}
			t.Logf("goroutine snapshot node=%s bytes=%d (set OWLCRON_ACCEPTANCE_RESULTS to retain)", f.Config.Hostname, len(data))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("stack capture timed out for %s", f.Config.Hostname)
}
