package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/blob"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func splitTestConfig(t *testing.T) *types.Config {
	t.Helper()
	dir := t.TempDir()
	return &types.Config{
		StorageEngine: "Filesystem", StorageBaseDir: filepath.Join(dir, "state"),
		LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), SecretKey: strings.Repeat("split-test-key-", 3),
		BlobStorage:   &types.BlobStorageConfig{WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": {Engine: "Filesystem", BaseDir: filepath.Join(dir, "objects")}}},
		JobLogStorage: &types.JobLogStorageConfig{WriteFormat: 2, SpoolMaxBytes: 4 << 20},
	}
}
func initializeSplitTestConfig(t *testing.T, cfg *types.Config) {
	t.Helper()
	meta, err := storage.OpenConfiguredMetadata(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stores, err := storage.NewBlobRegistry(cfg.BlobStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer stores.Close()
	if _, err = storage.InitializeStorageLayout(context.Background(), meta, stores, "ready"); err != nil {
		t.Fatal(err)
	}
}
func splitTestEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := splitTestConfig(t)
	initializeSplitTestConfig(t, cfg)
	e, err := NewEngine(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err = e.createDirectories(); err != nil {
		t.Fatal(err)
	}
	if err = e.initializeReadiness(); err != nil {
		t.Fatal(err)
	}
	e.ready.Store(true)
	e.multi.Master = true
	t.Cleanup(func() { e.cancel(); e.jobManager.Wait(); _ = e.blobStores.Close() })
	return e
}

func TestSplitStorageRejectsUninitializedOrUnconfiguredLayoutBeforeMutation(t *testing.T) {
	cfg := splitTestConfig(t)
	if _, err := NewEngine(cfg, zap.NewNop()); err == nil {
		t.Fatal("uninitialized split mode opened")
	}
	initializeSplitTestConfig(t, cfg)
	cfg.BlobStorage = nil
	cfg.JobLogStorage = nil
	if _, err := NewEngine(cfg, zap.NewNop()); err == nil {
		t.Fatal("legacy writer opened split layout")
	}
	meta, err := storage.OpenConfiguredMetadata(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := meta.Exists(context.Background(), "global/users"); err != nil || exists {
		t.Fatalf("startup mutated account state: %v %v", exists, err)
	}
}

func TestSplitJobCompletionReadsAndDeletesImmutableLog(t *testing.T) {
	e := splitTestEngine(t)
	event := installTestPlugin(t, e, "printf 'immutable-log-output\\n'\nprintf '%s\\n' '{\"complete\":1,\"code\":0}'\n")
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil {
		t.Fatal(err)
	}
	job := waitFinished(t, e, ids[0])
	m, err := e.jobLogInfo(context.Background(), job.ID)
	if err != nil || m.Format != 2 || !m.Final || m.Offset == 0 {
		t.Fatalf("not durably sealed: %+v %v", m, err)
	}
	data, total, err := e.ReadJobLogRange(context.Background(), job.ID, 0, 1<<20)
	if err != nil || total != m.Offset || !bytes.Contains(data, []byte("immutable-log-output")) {
		t.Fatalf("read: %q %d %v", data, total, err)
	}
	if _, err := storage.JobLogInfo(context.Background(), e.storage, job.ID); err == nil {
		t.Fatal("legacy parser accepted new format")
	}
	if err = e.DeleteJobLog(job.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.ReadJobLogRange(context.Background(), job.ID, 0, 128); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted log visible: %v", err)
	}
	if _, err = e.jobLogAppend(context.Background(), job.ID, m.Offset, []byte("late"), true, e.logHardLimit()); !errors.Is(err, storage.ErrJobLogDeleted) {
		t.Fatalf("late completion resurrected log: %v", err)
	}
}

type outageBlob struct {
	blob.Store
	fail atomic.Bool
}

func (s *outageBlob) Put(ctx context.Context, w blob.Write, r io.Reader) error {
	if s.fail.Load() {
		return errors.New("injected object outage")
	}
	return s.Store.Put(ctx, w, r)
}

func TestSplitLogOutageRetainsCompletionAndResumesLiveReservation(t *testing.T) {
	e := splitTestEngine(t)
	underlying, _ := e.blobStores.Store("logs")
	failing := &outageBlob{Store: underlying}
	failing.fail.Store(true)
	repo, err := storage.NewJobLogRepository(e.storage, map[string]blob.Store{"logs": failing}, storage.JobLogRepositoryOptions{Namespace: e.storageLayout.NamespaceID, WriteStore: "logs", WriteFormat: 2, PendingTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	e.jobLogs = repo
	job := syncFixture(t, e, "split-recovery", strings.Repeat("r", 7000))
	if err = e.jobManager.uploadJobLogToStorage(job); err == nil {
		t.Fatal("outage reported durable success")
	}
	m, err := repo.Info(context.Background(), job.ID)
	if err != nil || m.Offset != 0 || m.PendingEnd == 0 {
		t.Fatalf("missing pending: %+v %v", m, err)
	}
	job.Complete = true
	job.LogFileSize = 7000
	job.TimeEnd = float64(time.Now().Unix())
	if err = e.jobManager.writeCompletionSpool(job); err != nil {
		t.Fatal(err)
	}
	if err = e.jobManager.RetryPendingCompletions(); err == nil {
		t.Fatal("failed upload acknowledged completion")
	}
	if _, err = os.Stat(e.jobManager.completionSpoolPath(job.ID)); err != nil {
		t.Fatalf("recovery record removed: %v", err)
	}
	failing.fail.Store(false)
	if err = e.jobManager.RetryPendingCompletions(); err != nil {
		t.Fatal(err)
	}
	m, err = repo.Info(context.Background(), job.ID)
	if err != nil || !m.Final || m.Offset != 7000 {
		t.Fatalf("recovery did not seal: %+v %v", m, err)
	}
	if _, err = os.Stat(e.jobManager.completionSpoolPath(job.ID)); !os.IsNotExist(err) {
		t.Fatalf("confirmed completion not cleared: %v", err)
	}
	data, _, err := e.ReadJobLogRange(context.Background(), job.ID, 0, 1<<20)
	if err != nil || len(data) != 7000 {
		t.Fatalf("recovered log: %d %v", len(data), err)
	}
}

func TestSplitBlobOutageBlocksAdmissionButNotStartupOrLeadership(t *testing.T) {
	e := splitTestEngine(t)
	if !e.EnsureLeadership() {
		t.Fatal("cannot acquire lease")
	}
	store, _ := e.blobStores.Store("logs")
	key := e.storageLayout.NamespaceID + "/binding"
	r, err := store.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err = e.Readiness(context.Background()); err == nil {
		t.Fatal("missing blob binding reported ready")
	}
	if err = e.checkLogAdmission(context.Background()); err == nil {
		t.Fatal("blob outage admitted job")
	}
	if err = e.StartupReadiness(); err != nil {
		t.Fatalf("transient blob outage failed startup check: %v", err)
	}
	if !e.EnsureLeadership() {
		t.Fatal("blob outage stopped state lease renewal")
	}
	if err = store.Put(context.Background(), blob.Write{Key: key, Size: int64(len(marker)), SHA256: sha256.Sum256(marker)}, bytes.NewReader(marker)); err != nil {
		t.Fatal(err)
	}
	if err = e.checkLogAdmission(context.Background()); err != nil {
		t.Fatalf("recovered store still blocks: %v", err)
	}
}

func TestSplitMissingCommittedObjectNeverFallsBackToLocalCopy(t *testing.T) {
	e := splitTestEngine(t)
	job := syncFixture(t, e, "missing-object", "preserved-local-copy")
	job.Complete = true
	e.AddDeadJob(job)
	if err := e.jobManager.uploadJobLogToStorage(job); err != nil {
		t.Fatal(err)
	}
	var object storage.JobLogObject
	if err := e.jobLogs.VisitCommittedObjects(context.Background(), job.ID, func(o storage.JobLogObject, _ []byte) error { object = o; return nil }); err != nil {
		t.Fatal(err)
	}
	store, _ := e.blobStores.Store(object.StoreID)
	if err := store.Delete(context.Background(), object.Key); err != nil {
		t.Fatal(err)
	}
	if data, _, err := e.ReadJobLogRange(context.Background(), job.ID, 0, 100); err == nil || errors.Is(err, storage.ErrNotFound) || len(data) > 0 {
		t.Fatalf("missing committed content hidden: %q %v", data, err)
	}
}

func TestSplitGCExpiredInventoryCursorRestartsScan(t *testing.T) {
	e := splitTestEngine(t)
	if _, err := e.jobLogAppend(context.Background(), "gc-cursor", 0, []byte("payload"), true, e.logHardLimit()); err != nil {
		t.Fatal(err)
	}
	e.blobGCCursors["logs"] = "expired-cursor"
	if err := e.collectLogObjects(context.Background()); !errors.Is(err, blob.ErrInvalidCursor) {
		t.Fatalf("cursor error: %v", err)
	}
	if e.blobGCCursors["logs"] != "" {
		t.Fatal("expired cursor persisted forever")
	}
	if err := e.collectLogObjects(context.Background()); err != nil {
		t.Fatalf("fresh scan: %v", err)
	}
}

func TestSplitMaintenanceEvictsOnlyDurableCompletedLocalCaches(t *testing.T) {
	e := splitTestEngine(t)
	e.multi.Master = false
	e.config.HistoryCleanupInterval = 86400
	ctx := context.Background()
	ids := []string{"cached", "active", "spooled", "outbox", "unsealed", "unacknowledged", "different-size", "storage-error", "unknown"}
	for _, id := range ids {
		content := "protected-local-log"
		if id == "cached" {
			content = strings.Repeat("durable-content-", 1000)
		}
		job := syncFixture(t, e, id, content)
		job.Complete = true
		job.TimeEnd = float64(time.Now().Unix())
		if id == "storage-error" {
			job.StorageError = "awaiting recovery"
		}
		if id != "unknown" {
			raw, err := json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.storage.Put(ctx, "jobs:"+id, raw); err != nil {
				t.Fatal(err)
			}
		}
		if id != "unacknowledged" {
			if _, err := e.jobLogAppend(ctx, id, 0, []byte(content), id != "unsealed", e.logHardLimit()); err != nil {
				t.Fatal(err)
			}
		}
		if id == "different-size" {
			if err := os.WriteFile(job.LogFile, []byte(content+"unpublished tail"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if id == "active" {
			e.mu.Lock()
			e.activeJobs[id] = job
			e.mu.Unlock()
		}
		if id == "spooled" {
			if err := e.jobManager.writeCompletionSpool(job); err != nil {
				t.Fatal(err)
			}
		}
		if id == "outbox" {
			if err := e.storage.ListPush(ctx, "remote_completions", map[string]interface{}{"id": id}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.spoolBudget.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	e.spoolBudget.mu.Lock()
	e.spoolBudget.maximum = e.spoolBudget.used
	e.spoolBudget.mu.Unlock()
	if err := e.spoolBudget.admit(); !errors.Is(err, ErrSpoolCapacityExceeded) {
		t.Fatalf("full cache did not block admission: %v", err)
	}
	e.runSplitMaintenanceIfDue(time.Now())
	e.loopWG.Wait()
	for _, id := range ids {
		_, err := os.Stat(filepath.Join(e.config.LogDir, "jobs", id+".log"))
		if id == "cached" {
			if !os.IsNotExist(err) {
				t.Fatalf("durable cache retained: %v", err)
			}
		} else if err != nil {
			t.Fatalf("protected log %s removed: %v", id, err)
		}
	}
	if err := e.spoolBudget.admit(); err != nil {
		t.Fatalf("cache cleanup did not resume admission: %v", err)
	}
	data, _, err := e.ReadJobLogRange(ctx, "cached", 0, 1<<20)
	if err != nil || string(data) != strings.Repeat("durable-content-", 1000) {
		t.Fatalf("remote retention changed with cache eviction: %d %v", len(data), err)
	}
}
