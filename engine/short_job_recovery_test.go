package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestConcurrentShortJobsPreserveCompletionWhenProcessAlreadyExited(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1,\"code\":0,\"description\":\"short-job-complete\"}'\nexit 0\n")
	event.MaxChildren = 32
	const workers, rounds = 8, 4
	ids := make(chan string, workers*rounds)
	failures := make(chan error, workers*rounds)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				launched, err := e.jobManager.LaunchJobWithIDs(types.CloneEvent(event), 123)
				if err != nil {
					failures <- err
					continue
				}
				if len(launched) != 1 {
					failures <- fmt.Errorf("short job returned %d IDs", len(launched))
					continue
				}
				ids <- launched[0]
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	seen := make(map[string]bool)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate job ID %s", id)
		}
		seen[id] = true
		job := waitFinished(t, e, id)
		if job.Code != 0 || job.Description != "short-job-complete" || job.AbortReason != "" || job.StorageError != "" {
			t.Errorf("rapid completion was cancelled: id=%s code=%d description=%q abort=%q storage=%q", id, job.Code, job.Description, job.AbortReason, job.StorageError)
		}
		var persisted types.Job
		raw, err := e.storage.Get(context.Background(), "jobs:"+id)
		if err != nil {
			t.Errorf("completed job not durable: %s %v", id, err)
			continue
		}
		if err = json.Unmarshal(raw, &persisted); err != nil {
			t.Errorf("decode completion: %v", err)
		} else if !persisted.Complete || persisted.Code != 0 {
			t.Errorf("durable short-job completion lost: %s", id)
		}
	}
	if len(seen) != workers*rounds {
		t.Fatalf("completed %d jobs want %d", len(seen), workers*rounds)
	}
}
