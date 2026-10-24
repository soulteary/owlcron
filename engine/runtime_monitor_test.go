package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartupLoopSamplesShortJobsAndEnforcesMemoryLimit(t *testing.T) {
	for _, limited := range []bool{false, true} {
		name := "samples-short-job"
		if limited {
			name = "memory-limit-aborts"
		}
		t.Run(name, func(t *testing.T) {
			e := newSchedulingTestEngine(t, "monitor-loop")
			event := saveSchedulingFixture(t, e, name)
			event.Timing = nil
			duration := "3"
			if limited {
				duration = "10"
				event.MemoryLimit = 1
			}
			script := filepath.Join(e.config.LogDir, "fixture.sh")
			if err := os.WriteFile(script, []byte("sleep "+duration+"\nprintf '%s\\n' '{\"complete\":true,\"code\":0}'\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := e.scheduler.AddEvent(event); err != nil {
				t.Fatal(err)
			}
			if err := e.Startup(); err != nil {
				t.Fatal(err)
			}
			if err := e.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
				t.Fatal(err)
			}
			waitForCondition(t, 6*time.Second, func() bool { return len(e.GetDeadJobs()) == 1 })
			for _, job := range e.GetDeadJobs() {
				if limited {
					if job.Code == 0 || !strings.Contains(job.AbortReason, "memory") || job.Elapsed >= 5 {
						t.Fatalf("memory limit did not stop short task promptly: %+v", job)
					}
				} else if job.Mem == nil || job.Mem.Max == 0 || job.CPU == nil || job.CPU.Count == 0 {
					t.Fatalf("sub-minute job lacked retained resource samples: %+v", job)
				}
			}
		})
	}
}
