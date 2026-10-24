package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type injectedStorage struct {
	storage.Storage
	failWrites, failLists, failIndexes bool
	failOutbox                         bool
}

type blockedLogStorage struct {
	storage.Storage
	entered, release chan struct{}
	once             sync.Once
}

func (s *blockedLogStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if strings.HasPrefix(key, "job_log_chunks/") {
		s.once.Do(func() { close(s.entered); <-s.release })
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}
func (s *blockedLogStorage) Put(ctx context.Context, key string, data []byte) error {
	return s.Storage.Put(ctx, key, data)
}

func TestLiveLogUploadCannotOverwriteFinalLog(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1}'\n")
	job := e.jobManager.createJob(event, "local", 123, 0)
	if err := e.jobManager.createLogFile(job); err != nil {
		t.Fatal(err)
	}
	store := &blockedLogStorage{Storage: e.storage, entered: make(chan struct{}), release: make(chan struct{})}
	e.storage = store
	liveDone := make(chan error, 1)
	go func() { liveDone <- e.jobManager.uploadJobLogToStorage(job) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("live upload never reached backend")
	}
	file, err := os.OpenFile(job.LogFile, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("final log tail\n")
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	finalDone := make(chan error, 1)
	finalJob := types.CloneJob(job)
	finalJob.Complete = true
	go func() { finalDone <- e.jobManager.uploadJobLogToStorage(finalJob) }()
	close(store.release)
	if err := <-liveDone; err != nil {
		t.Fatal(err)
	}
	if err := <-finalDone; err != nil {
		t.Fatal(err)
	}
	// A late monitor snapshot reads the final file again instead of stale bytes.
	if err := e.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	info, err := storage.JobLogInfo(context.Background(), store, job.ID)
	if err != nil || !info.Final {
		t.Fatalf("late monitor cleared final marker: %+v %v", info, err)
	}
	raw, err := readAllStoredLog(context.Background(), store, job.ID)
	if err != nil || !strings.Contains(string(raw), "final log tail") {
		t.Fatalf("stale live upload overwrote final log: %s %v", raw, err)
	}
}

func TestWorkerMonitorPublishesLiveLogToSharedStorage(t *testing.T) {
	e := jobTestEngine(t)
	t.Cleanup(func() { e.cancel(); e.loopWG.Wait() })
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1}'\n")
	job := e.jobManager.createJob(event, "local", 123, 0)
	if err := e.jobManager.createLogFile(job); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.multi.Master = false
	e.multi.Slave = true
	e.activeJobs[job.ID] = job
	e.mu.Unlock()
	e.jobManager.MonitorServerResources()
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := storage.JobLogInfo(context.Background(), e.storage, job.ID)
		if err == nil && info.Offset > 0 {
			break
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("live log was not synchronized: %+v %v", info, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, err := readAllStoredLog(context.Background(), e.storage, job.ID)
	if err != nil || !strings.Contains(string(data), job.ID) {
		t.Fatalf("worker live log was unavailable to master: %s %v", data, err)
	}
}

func (s *injectedStorage) Put(ctx context.Context, key string, data []byte) error {
	if s.failWrites && (strings.HasPrefix(key, "jobs:") || (strings.HasPrefix(key, "job_logs:") || strings.HasPrefix(key, "job_log_manifest/") || strings.HasPrefix(key, "job_log_chunks/"))) {
		return errors.New("injected storage outage")
	}
	return s.Storage.Put(ctx, key, data)
}
func (s *injectedStorage) ListGet(ctx context.Context, key string, offset, limit int) ([]interface{}, error) {
	if s.failLists {
		return nil, errors.New("injected list read failure")
	}
	return s.Storage.ListGet(ctx, key, offset, limit)
}
func (s *injectedStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if s.failWrites && (strings.HasPrefix(key, "job_logs:") || strings.HasPrefix(key, "job_log_manifest/") || strings.HasPrefix(key, "job_log_chunks/")) {
		return errors.New("injected storage outage")
	}
	if s.failOutbox && key == "remote_completions" {
		return errors.New("injected completion outbox failure")
	}
	if s.failIndexes && (key == "plugins" || key == "events") {
		return errors.New("injected index failure")
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

func TestCompletionOutboxFailureRetainsSpoolAndLargeLogsStayInStorage(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1}'\n")
	job := e.jobManager.createJob(event, "local", 123, 0)
	if err := os.MkdirAll(filepath.Dir(job.LogFile), 0700); err != nil {
		t.Fatal(err)
	}
	largeLog := strings.Repeat("large persisted log line\n", 150000)
	if err := os.WriteFile(job.LogFile, []byte(largeLog), 0600); err != nil {
		t.Fatal(err)
	}
	base := e.storage
	e.storage = &injectedStorage{Storage: base, failOutbox: true}
	e.mu.Lock()
	e.multi.Master = false
	e.multi.Slave = true
	e.mu.Unlock()
	e.jobManager.finishLocalJob(job)
	finished, _ := e.GetDeadJob(job.ID)
	if finished == nil || finished.StorageError == "" {
		t.Fatal("failed outbox write was hidden")
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
		t.Fatal("failed outbox write discarded recovery spool")
	}
	e.storage = base
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	items, err := base.ListGet(context.Background(), "remote_completions", 0, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("outbox recovery: %d %v", len(items), err)
	}
	raw, _ := json.Marshal(items[0])
	if len(raw) > 64*1024 || strings.Contains(string(raw), "large persisted log line") {
		t.Fatal("large log was embedded in websocket completion")
	}
	var envelope map[string]interface{}
	_ = json.Unmarshal(raw, &envelope)
	if envelope["log_stored"] != true || envelope["job"].(map[string]interface{})["log_file"] != "" {
		t.Fatalf("invalid shared storage completion: %#v", envelope)
	}
	stored, err := readAllStoredLog(context.Background(), base, job.ID)
	if err != nil || string(stored) != largeLog {
		t.Fatal("large job log did not survive completion delivery")
	}
}

func TestCompletionSurvivesStorageOutageWithSpool(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf 'durable log\\n'\nprintf '%s\\n' '{\"complete\":1}'\n")
	base := e.storage
	e.storage = &injectedStorage{Storage: base, failWrites: true}
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	e.jobManager.Wait()
	if job.Code != 0 || job.StorageError == "" {
		t.Fatalf("outage was hidden or plugin result lost: %+v", job)
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
		t.Fatal("no durable completion recovery spool")
	}
	if err := e.DeleteJobHistory(job.ID); err == nil {
		t.Fatal("pending completion was deleted before durable recovery")
	}
	e.storage = base
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	recovered, ok := e.LookupJob(job.ID)
	if !ok || recovered.StorageError != "" {
		t.Fatalf("pending completion failed recovery: %+v", recovered)
	}
	log, err := readAllStoredLog(context.Background(), e.storage, job.ID)
	if err != nil || !strings.Contains(string(log), "durable log") {
		t.Fatalf("outage lost log: %s %v", log, err)
	}
	items, err := readStoredHistory(context.Background(), e.storage, "")
	if err != nil || len(items) != 1 {
		t.Fatalf("recovery duplicated history: %d %v", len(items), err)
	}
}

func TestDeletedHistoryCannotReappearFromCompletionReplay(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1}'\n")
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	e.jobManager.Wait()
	if err := e.DeleteJobHistory(job.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.finishJobStorage(job); !errors.Is(err, ErrJobHistoryDeleted) {
		t.Fatalf("deleted canonical record accepted stale completion: %v", err)
	}
	if err := e.jobManager.writeCompletionSpool(job); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.enqueueRemoteCompletion(job); err != nil {
		t.Fatal(err)
	}
	if _, exists := e.LookupJob(job.ID); exists {
		t.Fatal("deleted completion replay resurrected history")
	}
	if _, err := os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatal("deleted recovery spool remained replayable")
	}
	items, err := e.storage.ListGet(context.Background(), "remote_completions", 0, 0)
	if err != nil || len(items) != 0 {
		t.Fatal("deleted completion was re-enqueued")
	}
}

func TestJobThatReportedCompleteStillCannotBeDeletedWhileActive(t *testing.T) {
	e := jobTestEngine(t)
	job := &types.Job{ID: "jstillrunning", Complete: true}
	e.mu.Lock()
	e.activeJobs[job.ID] = job
	e.mu.Unlock()
	if err := e.DeleteJobHistory(job.ID); err == nil {
		t.Fatal("plugin complete message allowed active process deletion")
	}
}

func TestRuntimeUpdateCannotRecreateDurableActiveReservationAfterCompletion(t *testing.T) {
	e := jobTestEngine(t)
	for idx := 0; idx < 20; idx++ {
		job := &types.Job{ID: fmt.Sprintf("jfinishrace%d", idx), Event: "race", Hostname: e.Hostname(), TimeStart: float64(time.Now().UnixNano()) / 1e9}
		e.mu.Lock()
		e.activeJobs[job.ID] = types.CloneJob(job)
		e.mu.Unlock()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = e.UpdateActiveJob(job.ID, map[string]interface{}{"notify_fail": "updated@example.test"})
		}()
		go func() { defer wg.Done(); e.jobManager.finishLocalJob(job) }()
		wg.Wait()
	}
	items, err := e.storage.ListGet(context.Background(), "active_jobs", 0, 0)
	if err != nil || len(items) != 0 {
		t.Fatalf("concurrent update resurrected active reservation: %d %v", len(items), err)
	}
}
func TestStorageLoadAndIndexErrorsAreNotReportedAsSuccess(t *testing.T) {
	e := jobTestEngine(t)
	event := installTestPlugin(t, e, "read input\nprintf '%s\\n' '{\"complete\":1}'\n")
	base := e.storage
	e.storage = &injectedStorage{Storage: base, failLists: true}
	if err := e.scheduler.LoadEventsFromStorage(); err == nil {
		t.Fatal("list failure replaced schedule with empty success")
	}
	if _, ok := e.scheduler.GetEvent(event.ID); !ok {
		t.Fatal("failed read discarded old schedule")
	}
	if err := e.jobManager.RecoverJobs(); err == nil {
		t.Fatal("recovery list failure ignored")
	}
	e.storage = &injectedStorage{Storage: base, failIndexes: true}
	event.Title = "Changed"
	if err := e.scheduler.SaveEventToStorage(event); err == nil {
		t.Fatal("index write failure reported as saved")
	}
	e.storage = base
	if err := e.scheduler.LoadEventsFromStorage(); err != nil {
		t.Fatal(err)
	}
	loaded, _ := e.scheduler.GetEvent(event.ID)
	if loaded.Title != "Changed" {
		t.Fatal("canonical record did not override stale index")
	}
}
