package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProcessRemoteControlFileProtocolConcurrent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("OWLCRON_TEST_CONTROL_DIR", dir)
	done, stopped := make(chan struct{}), make(chan struct{})
	errors := make(chan error, 1)
	var seenMu sync.Mutex
	seen := map[string]bool{}
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.request"))
			if err != nil {
				errors <- err
				return
			}
			for _, path := range files {
				seenMu.Lock()
				already := seen[path]
				seen[path] = true
				seenMu.Unlock()
				if already {
					continue
				}
				if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0044 != 0044 {
					errors <- fmt.Errorf("control metadata is not readable across container/host UIDs: %v", err)
					return
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					errors <- err
					return
				}
				var request struct{ Container, Action string }
				if err := json.Unmarshal(raw, &request); err != nil {
					errors <- err
					return
				}
				ok := request.Container == "isolated-acceptance-container" && (request.Action == "inspect" || request.Action == "pause")
				response, _ := json.Marshal(map[string]any{"ok": ok, "labels": map[string]string{"io.owlcron.acceptance": "true"}})
				base := strings.TrimSuffix(path, ".request")
				if err := os.WriteFile(base+".writing", response, 0600); err != nil {
					errors <- err
					return
				}
				if err := os.Rename(base+".writing", base+".response"); err != nil {
					errors <- err
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	t.Cleanup(func() { close(done); <-stopped })
	control := processRemoteContainerControl(t, "isolated-acceptance-container")
	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() { defer callers.Done(); control("pause") }()
	}
	callers.Wait()
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
	seenMu.Lock()
	count := len(seen)
	seenMu.Unlock()
	if count != 9 {
		t.Fatalf("controller handled %d requests, want 9 unique requests", count)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("control protocol leaked %d temporary files", len(files))
	}
}
