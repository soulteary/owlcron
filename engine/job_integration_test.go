package engine

import (
	"context"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func jobTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	eng, err := NewEngine(&types.Config{LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), StorageBaseDir: filepath.Join(dir, "data"), SecretKey: "test-secret-with-at-least-32-characters"}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	eng.multi.Master = true
	t.Cleanup(func() { eng.cancel(); eng.jobManager.Wait() })
	return eng
}
func installTestPlugin(t *testing.T, e *Engine, script string) *types.Event {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plugin.sh")
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.SavePluginToStorage(&types.Plugin{ID: "ptest", Enabled: true, Command: "/bin/sh " + path, Params: []*types.PluginParam{{ID: "answer", Type: "text", Value: 0}}}); err != nil {
		t.Fatal(err)
	}
	event := &types.Event{ID: "etest", Title: "Contract Event", Enabled: true, Plugin: "ptest", Target: "local", Params: map[string]interface{}{"answer": 42}, MaxChildren: 1}
	e.scheduler.AddEvent(event)
	return event
}
func waitFinished(t *testing.T, e *Engine, id string) *types.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if job, ok := e.LookupJob(id); ok && job.Complete {
			if _, active := e.GetActiveJobs()[id]; !active {
				return job
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job did not finish: %s", id)
	return nil
}
func TestJobSingleNodeProtocolHistoryAndRestart(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf 'input:%s\\n' \"$input\"\nprintf '%s\\n' '{\"progress\":0.5}'\nsleep 0.15\nprintf '%s\\n' '{\"complete\":1,\"code\":0,\"description\":\"ok\"}'\nprintf 'final log\\n'\n")
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatal(ids)
	}
	var progress bool
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if job := e.GetActiveJobs()[ids[0]]; job != nil && job.Progress == 0.5 {
			progress = true
			job.Params["answer"] = "mutated"
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !progress {
		t.Fatal("progress was not visible")
	}
	job := waitFinished(t, e, ids[0])
	if job.Code != 0 || job.Description != "ok" || job.Params["answer"] != 42 {
		t.Fatalf("bad completion: %+v", job)
	}
	log, err := readAllStoredLog(context.Background(), e.storage, job.ID)
	if err != nil || !strings.Contains(string(log), "final log") || !strings.Contains(string(log), `"answer":42`) {
		t.Fatalf("log/input lost: %s %v", log, err)
	}
	// A fresh engine with empty memory reads details from the same persistent store.
	fresh, err := NewEngine(e.config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.cancel()
	persisted, ok := fresh.LookupJob(job.ID)
	if !ok || !persisted.Complete || persisted.Code != 0 {
		t.Fatalf("restart history missing: %+v", persisted)
	}
}
func TestConcurrencyReservationAndAbortNoRetry(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nsleep 30\nprintf '%s\\n' '{\"complete\":1}'\n")
	event.Retries = 3
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ids []string
	successful := 0
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			launched, err := e.jobManager.LaunchJobWithIDs(event, 123)
			if err == nil {
				mu.Lock()
				successful++
				ids = launched
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if successful != 1 {
		t.Fatalf("concurrency limit allowed %d launches", successful)
	}
	if err := e.jobManager.AbortJob(ids[0], "test abort"); err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	if job.Code == 0 || job.RetriesUsed != 0 || job.AbortReason == "" {
		t.Fatalf("abort/retry broken: %+v", job)
	}
	list, err := readStoredHistory(context.Background(), e.storage, "")
	if err != nil || len(list) != 1 {
		t.Fatalf("duplicate completion: %d %v", len(list), err)
	}
}
func TestInterruptedJobRecoveryAndUpsert(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1}'\n")
	event.Title = "Changed"
	if err := e.scheduler.SaveEventToStorage(event); err != nil {
		t.Fatal(err)
	}
	items, err := e.storage.ListGet(context.Background(), "events", 0, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("event upsert: %d %v", len(items), err)
	}
	job := e.jobManager.createJob(event, "local", 123, 0)
	e.jobManager.persistActiveJob(job)
	if err := e.jobManager.RecoverJobs(); err != nil {
		t.Fatal(err)
	}
	finished := waitFinished(t, e, job.ID)
	if finished.Code == 0 || !strings.Contains(finished.Description, "restarted") {
		t.Fatalf("interrupted task lost: %+v", finished)
	}
	if err := e.DeleteJobHistory(job.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.LookupJob(job.ID); ok {
		t.Fatal("deleted history returned after restart")
	}
}

func TestJobRuntimeUpdateSurvivesPluginProgress(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"progress\":0.25}'\nsleep 0.15\nprintf '%s\\n' '{\"progress\":0.75}'\nsleep 0.15\nprintf '%s\\n' '{\"complete\":1}'\n")
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if job := e.GetActiveJobs()[ids[0]]; job != nil && job.Progress > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := e.UpdateActiveJob(ids[0], map[string]interface{}{"notify_success": "", "notify_fail": "failure@example.test", "retries": float64(2), "title": "Updated live job"}); err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	if job.EventTitle != "Updated live job" || job.NotifyFail != "failure@example.test" || job.Retries != 2 {
		t.Fatalf("runtime edit lost after progress/completion: %+v", job)
	}
}
func TestCompletionSpoolRecoveryIsIdempotent(t *testing.T) {
	e := jobTestEngine(t)
	job := &types.Job{ID: "jspool", Event: "event", Hostname: e.Hostname(), Complete: true, Code: 0, TimeStart: 1, TimeEnd: 2, LogFile: filepath.Join(e.config.LogDir, "jobs", "jspool.log")}
	if err := os.MkdirAll(filepath.Dir(job.LogFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(job.LogFile, []byte("recovered log\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.writeCompletionSpool(job); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := e.jobManager.RetryPendingCompletions(); err != nil {
			t.Fatal(err)
		}
	}
	if stored, ok := e.LookupJob(job.ID); !ok || stored.Code != 0 {
		t.Fatal("completion spool was lost")
	}
	items, err := readStoredHistory(context.Background(), e.storage, "")
	if err != nil || len(items) != 1 {
		t.Fatalf("completion replay duplicate: %d %v", len(items), err)
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatal("durable completed spool not cleared")
	}
}

func TestRuntimeTimeoutCanBeExtended(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"progress\":0.25}'\nsleep 1.2\nprintf '%s\\n' '{\"complete\":1}'\n")
	event.Timeout = 1
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if job := e.GetActiveJobs()[ids[0]]; job != nil && job.Progress > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := e.UpdateActiveJob(ids[0], map[string]interface{}{"timeout": float64(3)}); err != nil {
		t.Fatal(err)
	}
	if job := waitFinished(t, e, ids[0]); job.Code != 0 {
		t.Fatalf("extended timeout still used original deadline: %+v", job)
	}
}
func TestCompletedPluginDoesNotLeaveBackgroundChildren(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nsleep 30 &\necho child:$!\nprintf '%s\\n' '{\"complete\":1}'\n")
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	raw, err := os.ReadFile(job.LogFile)
	if err != nil {
		t.Fatal(err)
	}
	pid := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "child:") {
			pid, _ = strconv.Atoi(strings.TrimPrefix(line, "child:"))
		}
	}
	if pid == 0 {
		t.Fatal("child fixture did not start")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		output, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
		if err != nil || len(strings.TrimSpace(string(output))) == 0 || strings.HasPrefix(strings.TrimSpace(string(output)), "Z") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("plugin child %d still running after completion", pid)
}

func TestCronicle135PluginOwnsLaunchContextAndParameterSchema(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf 'input:%s\\n' \"$input\"\nprintf '%s\\n' '{\"complete\":1}'\n")
	plug, err := e.jobManager.GetPluginFromStorage(event.Plugin)
	if err != nil {
		t.Fatal(err)
	}
	plug.Params = append(plug.Params, &types.PluginParam{ID: "hidden", Type: "hidden", Value: "trusted"})
	if err = e.jobManager.SavePluginToStorage(plug); err != nil {
		t.Fatal(err)
	}
	event.CWD = "/path/that/does/not/exist"
	event.UID = "invalid-event-user"
	event.Params["hidden"] = "attacker"
	event.Params["LD_PRELOAD"] = "/tmp/attacker"
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	if job.Code != 0 || job.CWD != "" || job.UID != "" || job.Params["hidden"] != "trusted" {
		t.Fatalf("untrusted context/schema override survived: %+v", job)
	}
	if _, exists := job.Params["LD_PRELOAD"]; exists {
		t.Fatal("undeclared parameter became process environment")
	}
}
