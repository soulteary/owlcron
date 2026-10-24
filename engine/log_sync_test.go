package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

type logSyncStorage struct {
	storage.Storage
	mu          sync.Mutex
	chunkWrites int
	maxChunk    int
	failCommit  bool
}

func (s *logSyncStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, func(raw []byte) ([]byte, error) {
		next, err := fn(raw)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if strings.HasPrefix(key, "job_log_chunks/") {
			s.chunkWrites++
			if len(next) > s.maxChunk {
				s.maxChunk = len(next)
			}
		}
		if strings.HasPrefix(key, "job_log_manifest/") && s.failCommit {
			var m storage.JobLogManifest
			if json.Unmarshal(next, &m) == nil && m.Offset > 0 && m.PendingEnd == 0 {
				s.failCommit = false
				return nil, errors.New("injected manifest publication failure")
			}
		}
		return next, nil
	})
}

func syncFixture(t *testing.T, e *Engine, id, contents string) *types.Job {
	t.Helper()
	job := &types.Job{ID: id, Complete: false, Hostname: e.Hostname(), LogFile: filepath.Join(e.config.LogDir, "jobs", id+".log")}
	if err := os.MkdirAll(filepath.Dir(job.LogFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(job.LogFile, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestIncrementalLogSyncUploadsOnlyNewBytesAndFinalizesWithoutReupload(t *testing.T) {
	e := jobTestEngine(t)
	store := &logSyncStorage{Storage: e.storage}
	e.storage = store
	text := strings.Repeat("a", storage.JobLogChunkSize*2+17)
	job := syncFixture(t, e, "incremental", text)
	if err := e.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	initial := store.chunkWrites
	file, err := os.OpenFile(job.LogFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("new tail")
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	if store.chunkWrites != initial+1 {
		t.Fatalf("old chunks reuploaded: %d -> %d", initial, store.chunkWrites)
	}
	if store.maxChunk > storage.JobLogChunkSize*2 {
		t.Fatalf("unbounded chunk: %d", store.maxChunk)
	}
	beforeFinal := store.chunkWrites
	job.Complete = true
	if err := e.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	if store.chunkWrites != beforeFinal {
		t.Fatal("finalization rewrote log payload")
	}
	m, err := storage.JobLogInfo(context.Background(), store, job.ID)
	if err != nil || !m.Final || m.Offset != int64(len(text)+8) {
		t.Fatalf("final manifest: %+v %v", m, err)
	}
	got, err := readAllStoredLog(context.Background(), store, job.ID)
	if err != nil || string(got) != text+"new tail" {
		t.Fatalf("incremental content mismatch: %v", err)
	}
}

func TestLogSyncResumesExactPendingBatchAfterRestartAndFileGrowth(t *testing.T) {
	a, b := jobTestEngine(t), jobTestEngine(t)
	store := &logSyncStorage{Storage: a.storage, failCommit: true}
	a.storage = store
	text := strings.Repeat("before-", 12000)
	job := syncFixture(t, a, "restart-upload", text)
	if err := a.jobManager.uploadJobLogToStorage(job); err == nil {
		t.Fatal("publication failure hidden")
	}
	m, err := storage.JobLogInfo(context.Background(), store, job.ID)
	if err != nil || m.Offset != 0 || m.PendingEnd != storage.JobLogChunkSize {
		t.Fatalf("missing durable reservation: %+v %v", m, err)
	}
	file, err := os.OpenFile(job.LogFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("after restart")
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	b.storage = store
	job.Complete = true
	if err := b.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	got, err := readAllStoredLog(context.Background(), store, job.ID)
	if err != nil || string(got) != text+"after restart" {
		t.Fatalf("restart upload lost data: %v", err)
	}
}

func TestJobLogCapacityIncludesHeaderAndFailsWithoutSamplingDelay(t *testing.T) {
	for _, scope := range []string{"job", "hard"} {
		t.Run(scope, func(t *testing.T) {
			e := jobTestEngine(t)
			event := installTestPlugin(t, e, "printf 'normal output\\n'\nhead -c 20000 /dev/zero\nprintf '%s\\n' '{\"complete\":1}'\n")
			if scope == "job" {
				event.LogMaxSize = 512
			} else {
				e.config.JobLogHardMaxSize = 512
			}
			ids, err := e.jobManager.LaunchJobWithIDs(event, 1)
			if err != nil {
				t.Fatal(err)
			}
			job := waitFinished(t, e, ids[0])
			if job.Code == 0 || !strings.Contains(job.Description, "capacity") {
				t.Fatalf("capacity failure hidden: %+v", job)
			}
			info, err := os.Stat(job.LogFile)
			if err != nil || info.Size() != 512 {
				t.Fatalf("hard quota exceeded or not exercised: %+v %v", info, err)
			}
			if info.ModTime().After(time.Now().Add(time.Second)) {
				t.Fatal("unexpected log timestamp")
			}
		})
	}
}

func TestInlineLogSnapshotCannotChangeCommittedPrefix(t *testing.T) {
	e := jobTestEngine(t)
	ctx := context.Background()
	// Prepare a live committed prefix so an inline completion can only append.
	if _, err := storage.JobLogAppend(ctx, e.storage, "inline", 0, []byte("prefix"), false, e.logHardLimit()); err != nil {
		t.Fatal(err)
	}
	if err := e.writeJobLog(ctx, "inline", []byte("WRONG-suffix")); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("conflicting prefix accepted: %v", err)
	}
	if err := e.writeJobLog(ctx, "inline", []byte("prefix-suffix")); err != nil {
		t.Fatal(err)
	}
	got, err := readAllStoredLog(ctx, e.storage, "inline")
	if err != nil || string(got) != "prefix-suffix" {
		t.Fatalf("inline completion lost prefix: %q %v", got, err)
	}
}

type slowLogSyncStorage struct {
	storage.Storage
	key     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *slowLogSyncStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if strings.HasPrefix(key, s.key) {
		s.once.Do(func() {
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

func TestSlowLogSyncDoesNotBlockOtherJobsOrHeartbeatLaunch(t *testing.T) {
	e := jobTestEngine(t)
	first := syncFixture(t, e, "slow-upload", "first")
	second := syncFixture(t, e, "fast-upload", "second")
	gate := &slowLogSyncStorage{Storage: e.storage, key: "job_log_chunks/" + base64.RawURLEncoding.EncodeToString([]byte(first.ID)) + "/", entered: make(chan struct{}), release: make(chan struct{})}
	e.storage = gate
	defer func() { e.cancel(); e.loopWG.Wait() }()
	e.jobManager.scheduleLiveLogSync(first)
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start")
	}
	for i := 0; i < 20; i++ {
		e.jobManager.scheduleLiveLogSync(first)
	}
	e.jobManager.scheduleLiveLogSync(second)
	waitForCondition(t, 2*time.Second, func() bool {
		m, err := storage.JobLogInfo(context.Background(), gate, second.ID)
		return err == nil && m.Offset == 6
	})
	e.jobManager.liveLogMu.Lock()
	active := len(e.jobManager.liveLogJobs)
	e.jobManager.liveLogMu.Unlock()
	if active > cap(e.jobManager.logUploadSlots) {
		t.Fatal("unbounded upload queue")
	}
	close(gate.release)
	e.loopWG.Wait()
	got, err := readAllStoredLog(context.Background(), gate, first.ID)
	if err != nil || string(got) != "first" {
		t.Fatalf("slow upload failed: %q %v", got, err)
	}
}

func TestLiveSyncCannotSealEarlyPluginCompletion(t *testing.T) {
	e := jobTestEngine(t)
	job := syncFixture(t, e, "early-complete", "before completion report\n")
	job.Complete = true // Protocol complete is not proof that the process exited.
	e.jobManager.scheduleLiveLogSync(job)
	e.loopWG.Wait()
	m, err := storage.JobLogInfo(context.Background(), e.storage, job.ID)
	if err != nil || m.Final {
		t.Fatalf("live upload prematurely sealed log: %+v %v", m, err)
	}
	f, err := os.OpenFile(job.LogFile, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("output after completion report\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	data, err := readAllStoredLog(context.Background(), e.storage, job.ID)
	if err != nil || !strings.Contains(string(data), "output after completion report") {
		t.Fatalf("final trailing log lost: %q %v", data, err)
	}
}

type canceledLogCheckStorage struct {
	storage.Storage
	entered chan struct{}
	once    sync.Once
}

func (s *canceledLogCheckStorage) Exists(ctx context.Context, key string) (bool, error) {
	if strings.HasPrefix(key, "deleted_job_logs:") {
		s.once.Do(func() { close(s.entered) })
		<-ctx.Done()
		return false, ctx.Err()
	}
	return s.Storage.Exists(ctx, key)
}

func TestLogSyncCancellationInterruptsDeletedMarkerCheck(t *testing.T) {
	e := jobTestEngine(t)
	job := syncFixture(t, e, "cancel-check", "log")
	store := &canceledLogCheckStorage{Storage: e.storage, entered: make(chan struct{})}
	e.storage = store
	done := make(chan error, 1)
	go func() { done <- e.jobManager.uploadJobLogToStorage(job) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("marker check did not start")
	}
	e.cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("marker check ignored upload cancellation")
	}
}
