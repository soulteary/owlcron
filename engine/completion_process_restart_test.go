//go:build linux || darwin

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/soulteary/owlcron/blob"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

var errCompletionProcessOutage = errors.New("completion process test backend unavailable")

type completionProcessMetadata struct {
	storage.Storage
	offline  *atomic.Bool
	rejected atomic.Int64
}

func (s *completionProcessMetadata) Put(ctx context.Context, key string, data []byte) error {
	if s.offline.Load() {
		s.rejected.Add(1)
		return errCompletionProcessOutage
	}
	return s.Storage.Put(ctx, key, data)
}

func (s *completionProcessMetadata) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if s.offline.Load() {
		s.rejected.Add(1)
		return errCompletionProcessOutage
	}
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, fn)
}

type completionProcessObjects struct {
	blob.Store
	offline *atomic.Bool
}

func (s *completionProcessObjects) Put(ctx context.Context, spec blob.Write, body io.Reader) error {
	if s.offline.Load() {
		return errCompletionProcessOutage
	}
	return s.Store.Put(ctx, spec, body)
}

func writeCompletionProcessState(t *testing.T, path string, job *types.Job) {
	t.Helper()
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

// The child runs the production launch and plugin paths in a separate address
// space. Its parent kills it without executing Shutdown or a completion defer.
func TestCompletionProcessRestartChild(t *testing.T) {
	path := os.Getenv("OWLCRON_COMPLETION_PROCESS_CONFIG")
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg types.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(&cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.cancel(); e.jobManager.Wait(); _ = e.blobStores.Close() })
	if err := e.createDirectories(); err != nil {
		t.Fatal(err)
	}
	if err := e.initializeReadiness(); err != nil {
		t.Fatal(err)
	}
	var offline atomic.Bool
	meta := &completionProcessMetadata{Storage: e.storage, offline: &offline}
	// Install the stable fault boundaries before any execution goroutine starts.
	// Only the atomic flag changes during the test; no live engine pointer swaps.
	e.storage = meta
	objects := e.blobStores.Stores()
	for id, store := range objects {
		objects[id] = &completionProcessObjects{Store: store, offline: &offline}
	}
	e.jobLogs, err = storage.NewJobLogRepository(meta, objects, storage.JobLogRepositoryOptions{
		Namespace: e.storageLayout.NamespaceID, WriteStore: e.blobStores.WriteStoreID(), WriteFormat: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	setMasterForTest(e)
	plugin := &types.Plugin{ID: "crash-result", Enabled: true, Command: "/bin/sh " + filepath.Join(filepath.Dir(path), "plugin.sh")}
	if err := e.jobManager.SavePluginToStorage(plugin); err != nil {
		t.Fatal(err)
	}
	event := &types.Event{ID: "crash-result", Title: "Crash result durability", Enabled: true, Plugin: plugin.ID, Target: "local", MaxChildren: 1}
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	ids, err := e.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil || len(ids) != 1 {
		t.Fatalf("launch: %v %v", ids, err)
	}
	id := ids[0]
	waitForCondition(t, 10*time.Second, func() bool {
		if failed, ok := e.GetDeadJob(id); ok {
			t.Fatalf("plugin finished before report gate: %+v", failed)
		}
		job := e.GetActiveJobs()[id]
		if job == nil || job.PID <= 1 || job.ProcessIdentity == "" || job.ProcessToken == "" {
			return false
		}
		log, err := os.ReadFile(job.LogFile)
		return err == nil && bytes.Count(log, []byte("preserved-before-crash\n")) == 1
	})
	writeCompletionProcessState(t, path+".started", e.GetActiveJobs()[id])
	used, err := spoolTreeBytes(e.ctx, filepath.Join(cfg.LogDir, "jobs"))
	if err != nil || used >= e.spoolBudget.maximum {
		t.Fatalf("initial spool usage: %d %v", used, err)
	}
	filler := filepath.Join(cfg.LogDir, "jobs", "quota-filler")
	if err := os.WriteFile(filler, make([]byte, e.spoolBudget.maximum-used), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.spoolBudget.refresh(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.spoolBudget.admit(); !errors.Is(err, ErrSpoolCapacityExceeded) {
		t.Fatalf("test did not fill logical spool quota: %v", err)
	}
	offline.Store(true)
	if err := meta.Put(e.ctx, "outage-probe", nil); !errors.Is(err, errCompletionProcessOutage) {
		t.Fatalf("metadata outage was not enabled: %v", err)
	}
	for _, store := range objects {
		if err := store.Put(e.ctx, blob.Write{}, bytes.NewReader(nil)); !errors.Is(err, errCompletionProcessOutage) {
			t.Fatalf("object outage was not enabled: %v", err)
		}
	}
	if err := os.WriteFile(path+".report", nil, 0600); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 10*time.Second, func() bool {
		job := e.GetActiveJobs()[id]
		return job != nil && job.Complete && job.Code == 7 && job.Description == "actual result before SIGKILL" && meta.rejected.Load() > 1
	})
	job := e.GetActiveJobs()[id]
	journal, err := readCompletionJournal(e.jobManager.completionJournalPath(id))
	if err != nil || journal.checkpoint.Phase != completionPhaseReported || journal.checkpoint.Job.Code != 7 {
		t.Fatalf("accepted completion has no durable pre-exit report: %+v %v", journal, err)
	}
	if _, err := os.Stat(e.jobManager.legacyCompletionSpoolPath(id)); !os.IsNotExist(err) {
		t.Fatalf("test unexpectedly had an ordinary completion spool: %v", err)
	}
	if _, err := meta.Get(e.ctx, "jobs:"+id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("test unexpectedly persisted completion history: %v", err)
	}
	rows, err := meta.ListGet(e.ctx, "active_jobs", 0, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("interrupted job lost its original durable reservation: %v %v", rows, err)
	}
	raw, _ = json.Marshal(rows[0])
	var reserved types.Job
	if err := json.Unmarshal(raw, &reserved); err != nil || reserved.Complete || reserved.Code != 0 {
		t.Fatalf("completion leaked through failed shared storage: %+v %v", reserved, err)
	}
	writeCompletionProcessState(t, path+".ready", job)
	// Keep the plugin and engine alive at the exact reported-but-unsealed edge.
	// Parent SIGKILL must be the only event that ends this engine process.
	for {
		time.Sleep(time.Second)
	}
}

func TestCompletionReportSurvivesSIGKILLWithFullQuotaAndBackendOutage(t *testing.T) {
	cfg := splitTestConfig(t)
	cfg.Hostname = "completion-crash-node"
	initializeSplitTestConfig(t, cfg)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("read input\nprintf 'effect\\n' >> '%s'\nprintf 'preserved-before-crash\\n'\nwhile [ ! -e '%s.report' ]; do sleep 0.02; done\nprintf '%%s\\n' '{\"complete\":true,\"code\":7,\"description\":\"actual result before SIGKILL\"}'\nwhile [ ! -e '%s.release' ]; do sleep 0.05; done\n", filepath.Join(dir, "effects"), configPath, configPath)
	if err := os.WriteFile(filepath.Join(dir, "plugin.sh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(exe, "-test.run=^TestCompletionProcessRestartChild$", "-test.timeout=45s")
	child.Env = append(os.Environ(), "OWLCRON_COMPLETION_PROCESS_CONFIG="+configPath)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	output, err := os.Create(filepath.Join(dir, "helper.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	child.Stdout, child.Stderr = output, output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	reaped := false
	t.Cleanup(func() {
		_ = child.Process.Kill()
		if !reaped {
			<-done
		}
		if raw, err := os.ReadFile(configPath + ".started"); err == nil {
			var job types.Job
			if json.Unmarshal(raw, &job) == nil {
				_ = stopRecoveredJobProcess(&job)
			}
		}
	})
	var reported types.Job
	deadline := time.Now().Add(20 * time.Second)
	for {
		if raw, err := os.ReadFile(configPath + ".ready"); err == nil {
			if err := json.Unmarshal(raw, &reported); err != nil {
				t.Fatal(err)
			}
			break
		}
		select {
		case err := <-done:
			reaped = true
			log, _ := os.ReadFile(output.Name())
			t.Fatalf("completion helper exited before crash barrier: %v\n%s", err, log)
		default:
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(output.Name())
			t.Fatalf("completion helper never reached crash barrier\n%s", log)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reported.Complete || reported.Code != 7 || reported.PID <= 1 || reported.ProcessIdentity == "" || reported.ProcessToken == "" {
		t.Fatalf("invalid precrash report: %+v", reported)
	}
	precrashLog, err := os.ReadFile(reported.LogFile)
	if err != nil || bytes.Count(precrashLog, []byte("preserved-before-crash\n")) != 1 {
		t.Fatalf("precrash plugin log was not captured: %q %v", precrashLog, err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = <-done
	reaped = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("helper did not die from real SIGKILL: %v", err)
	}
	if identity, err := processIdentity(reported.PID); err != nil || identity != reported.ProcessIdentity {
		t.Fatalf("test did not leave the plugin alive across daemon SIGKILL: %q %v", identity, err)
	}
	// A fresh address space has no finished-job cache or fault wrapper. Recovery
	// must use the report in the preallocated journal before the active snapshot.
	fresh, err := NewEngine(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	fresh.multi.Master = true
	t.Cleanup(func() { fresh.cancel(); fresh.jobManager.Wait(); _ = fresh.blobStores.Close() })
	if err := fresh.jobManager.RecoverJobs(); err != nil {
		t.Fatalf("recover reported completion: %v", err)
	}
	if _, err := processIdentity(reported.PID); !os.IsNotExist(err) {
		t.Fatalf("recovery retained the old executing plugin: %v", err)
	}
	if members, err := processGroupMembers(reported.PID); err != nil || len(members) != 0 {
		t.Fatalf("recovery retained descendants in the old process group: %v %v", members, err)
	}
	for range 2 {
		if err := fresh.jobManager.RecoverJobs(); err != nil {
			t.Fatal(err)
		}
	}
	finished, ok := fresh.LookupJob(reported.ID)
	if !ok || !finished.Complete || finished.Code != 7 || finished.Description != reported.Description || finished.AbortReason != "" || finished.StorageError != "" {
		t.Fatalf("restart replaced the actual plugin result: %+v", finished)
	}
	if finished.TimeEnd <= 0 || finished.Elapsed < 0 {
		t.Fatalf("recovered result lost its completion timestamp: %+v", finished)
	}
	for _, event := range []string{"", reported.Event} {
		history, err := readStoredHistory(fresh.ctx, fresh.storage, event)
		if err != nil || len(history) != 1 || history[0].ID != reported.ID {
			t.Fatalf("recovery lost or duplicated history for %q: %+v %v", event, history, err)
		}
	}
	log, _, err := fresh.ReadJobLogRange(fresh.ctx, reported.ID, 0, 1024)
	if err != nil || !bytes.Equal(log, precrashLog) {
		t.Fatalf("recovery lost or duplicated the log: %q %v", log, err)
	}
	manifest, err := fresh.jobLogInfo(fresh.ctx, reported.ID)
	if err != nil || !manifest.Final || manifest.Offset != int64(len(log)) {
		t.Fatalf("recovered log was not sealed exactly once: %+v %v", manifest, err)
	}
	if effects, err := os.ReadFile(filepath.Join(dir, "effects")); err != nil || string(effects) != "effect\n" {
		t.Fatalf("recovery re-executed completed work: %q %v", effects, err)
	}
	rows, err := fresh.storage.ListGet(fresh.ctx, "active_jobs", 0, 0)
	if err != nil || len(rows) != 0 {
		t.Fatalf("completed recovery retained the durable reservation: %v %v", rows, err)
	}
	for _, path := range []string{fresh.jobManager.completionJournalPath(reported.ID), fresh.jobManager.legacyCompletionSpoolPath(reported.ID)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("successful recovery retained pending completion %s: %v", filepath.Base(path), err)
		}
	}
	if strings.Contains(finished.Description, "Server restarted") {
		t.Fatal("restart fallback replaced the plugin report")
	}
}
