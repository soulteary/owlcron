package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func remoteReplayJob(t *testing.T, e *Engine) (*types.Job, string) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "effects")
	event := installTestPlugin(t, e, "read input\nprintf 'effect\\n' >> '"+marker+"'\nprintf '%s\\n' '{\"complete\":true,\"code\":0}'\n")
	job := e.jobManager.createJob(event, e.Hostname(), 123, 0)
	plugin, err := e.jobManager.GetPluginFromStorage(event.Plugin)
	if err != nil {
		t.Fatal(err)
	}
	job.Command = plugin.Command
	return job, marker
}

func TestRemoteReplayDoesNotRepeatCompletedWorkAfterRestart(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		name := "retained history"
		if deleted {
			name = "deleted history"
		}
		t.Run(name, func(t *testing.T) {
			e := jobTestEngine(t)
			job, marker := remoteReplayJob(t, e)
			if err := e.jobManager.LaunchRemoteJob(job); err != nil {
				t.Fatal(err)
			}
			if completed := waitFinished(t, e, job.ID); completed.Code != 0 {
				t.Fatalf("initial execution failed: %+v", completed)
			}
			e.jobManager.Wait()
			if deleted {
				if err := e.DeleteJobHistory(job.ID); err != nil {
					t.Fatal(err)
				}
			}
			fresh, err := NewEngine(e.config, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { fresh.cancel(); fresh.jobManager.Wait() })
			if err := fresh.jobManager.LaunchRemoteJob(job); err != nil {
				t.Fatal(err)
			}
			fresh.jobManager.Wait()
			data, err := os.ReadFile(marker)
			if err != nil || string(data) != "effect\n" {
				t.Fatalf("replayed command repeated its side effect: %q, %v", data, err)
			}
			if len(fresh.GetActiveJobs()) != 0 {
				t.Fatal("duplicate replay reserved an active job")
			}
		})
	}
}

type replayReadFailure struct {
	storage.Storage
	failMarker bool
}

func (s replayReadFailure) Get(ctx context.Context, key string) ([]byte, error) {
	if !s.failMarker && strings.HasPrefix(key, "jobs:") {
		return nil, errors.New("injected history read failure")
	}
	return s.Storage.Get(ctx, key)
}

func (s replayReadFailure) Exists(ctx context.Context, key string) (bool, error) {
	if s.failMarker && strings.HasPrefix(key, "deleted_jobs:") {
		return false, errors.New("injected deletion marker read failure")
	}
	return s.Storage.Exists(ctx, key)
}

func TestRemoteReplayFailsClosedWhenHistoryCannotBeRead(t *testing.T) {
	for _, failMarker := range []bool{false, true} {
		name := "history"
		if failMarker {
			name = "deletion marker"
		}
		t.Run(name, func(t *testing.T) {
			e := jobTestEngine(t)
			job, marker := remoteReplayJob(t, e)
			e.storage = replayReadFailure{Storage: e.storage, failMarker: failMarker}
			if err := e.jobManager.LaunchRemoteJob(job); err == nil {
				t.Fatal("launch accepted without checking durable replay state")
			}
			e.jobManager.Wait()
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("command ran during history outage: %v", err)
			}
			if len(e.GetActiveJobs()) != 0 {
				t.Fatal("failed admission reserved an active job")
			}
		})
	}
}

func TestRemoteReplayConcurrentFramesExecuteOnce(t *testing.T) {
	e := jobTestEngine(t)
	job, marker := remoteReplayJob(t, e)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.jobManager.LaunchRemoteJob(types.CloneJob(job)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if completed := waitFinished(t, e, job.ID); completed.Code != 0 {
		t.Fatalf("execution failed: %+v", completed)
	}
	e.jobManager.Wait()
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "effect\n" {
		t.Fatalf("duplicate frame repeated side effect: %q, %v", data, err)
	}
}
