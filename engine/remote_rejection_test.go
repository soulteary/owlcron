package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

type admissionRefusalStore struct{ storage.Storage }

func (s admissionRefusalStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "jobs:") {
		return nil, errors.New("backend password=worker-secret")
	}
	return s.Storage.Get(ctx, key)
}

func TestClusterPrelaunchFailureSendsCorrelatedRedactedRefusal(t *testing.T) {
	_, worker := nodeIdentityFixture(t)
	lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := worker.storage.Put(worker.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	worker.storage = admissionRefusalStore{Storage: worker.storage}
	conn, remote := overloadSocketPair(t)
	client := &ws.Client{ID: "refusal-master", Conn: conn, NodeEndpoint: true}
	client.SetAuthentication("sender", true, true, nil, "node", lease.Owner, 0)
	job := &types.Job{ID: "history-unavailable", Command: "/bin/true", DispatchToken: "exact-frame"}
	raw, _ = json.Marshal(job)
	NewWebSocketHandler(worker, zap.NewNop()).OnMessage(client, &ws.Message{Type: "launch_job", Data: raw})
	remote.SetReadDeadline(time.Now().Add(time.Second))
	var frame struct {
		Type string `json:"type"`
		Data struct {
			ID            string `json:"id"`
			DispatchToken string `json:"dispatch_token"`
			Reason        string `json:"reason"`
		} `json:"data"`
	}
	if err := remote.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "job_rejected" || frame.Data.ID != job.ID || frame.Data.DispatchToken != job.DispatchToken || frame.Data.Reason == "" {
		t.Fatalf("prelaunch error lost dispatch correlation: %+v", frame)
	}
	if strings.Contains(frame.Data.Reason, "worker-secret") {
		t.Fatal("refusal exposed backend credentials")
	}
	if len(worker.GetActiveJobs()) != 0 {
		t.Fatal("failed prelaunch history read accepted work")
	}
}

func TestClusterWorkerSpoolRefusalReleasesConnectedMasterReservation(t *testing.T) {
	cfg := splitTestConfig(t)
	cfg.Hostname = "refusal-master"
	cfg.ClusterAllowInsecure = true
	cfg.SharedStorage = true
	initializeSplitTestConfig(t, cfg)
	newNode := func(config *types.Config) *Engine {
		t.Helper()
		e, err := NewEngine(config, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Shutdown() })
		if err = e.createDirectories(); err != nil {
			t.Fatal(err)
		}
		if err = e.initializeReadiness(); err != nil {
			t.Fatal(err)
		}
		if err = e.comm.Startup(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	master := newNode(cfg)
	workerConfig := *cfg
	workerConfig.Hostname = "refusal-worker"
	workerConfig.LogDir = filepath.Join(t.TempDir(), "logs")
	workerConfig.QueueDir = filepath.Join(t.TempDir(), "queue")
	worker := newNode(&workerConfig)
	setMasterForTest(master)
	mux := http.NewServeMux()
	worker.comm.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	if err := master.comm.ConnectToSlave(worker.Hostname(), strings.TrimPrefix(server.URL, "http://")); err != nil {
		t.Fatal(err)
	}
	master.comm.SendMasterPings()
	waitForCondition(t, time.Second, func() bool { return worker.GetMulti().MasterHostname == master.Hostname() })
	peer, _ := master.comm.GetSlaveConnection(worker.Hostname())
	connection := peer.Socket
	event := saveSchedulingFixture(t, master, "refused-event")
	event.Target = worker.Hostname()
	event.Retries = 1
	if err := master.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	// Quota changes after authentication/status, while the master still has
	// capacity to dispatch and persist the refusal as an ordinary failed job.
	quotaFile := filepath.Join(worker.config.LogDir, "jobs", "filled-spool")
	if err := os.WriteFile(quotaFile, bytes.Repeat([]byte("x"), int(fraction(worker.spoolBudget.maximum, 91))), 0600); err != nil {
		t.Fatal(err)
	}
	if err := worker.spoolBudget.refresh(worker.ctx); err != nil {
		t.Fatal(err)
	}
	ids, err := master.jobManager.LaunchJobWithIDs(event, 123)
	if err != nil || len(ids) != 1 {
		t.Fatalf("socket dispatch failed before worker admission: %v %v", ids, err)
	}
	waitForCondition(t, 3*time.Second, func() bool { _, ok := master.GetDeadJob(ids[0]); return ok })
	job, _ := master.GetDeadJob(ids[0])
	if job.Code != 1 || !strings.Contains(job.Description, ErrSpoolCapacityExceeded.Error()) || job.RetriesUsed != 1 || job.StorageError != "" {
		t.Fatalf("worker refusal was not durably completed after retry: %+v", job)
	}
	if len(master.GetActiveJobs()) != 0 || len(worker.GetActiveJobs()) != 0 || len(worker.GetDeadJobs()) != 0 {
		t.Fatal("refused execution remained reserved or ran on the worker")
	}
	rows, err := master.storage.ListGet(context.Background(), "active_jobs", 0, 0)
	if err != nil || len(rows) != 0 {
		t.Fatalf("durable active reservation remained: %v %v", rows, err)
	}
	if _, err := master.storage.Get(context.Background(), "jobs:"+job.ID); err != nil {
		t.Fatalf("failed admission has no historical outcome: %v", err)
	}
	master.comm.SendMasterPings()
	current, ok := master.comm.GetSlaveConnection(worker.Hostname())
	if !ok || current.Socket != connection {
		t.Fatal("refusal relied on disconnect/recovery to free the reservation")
	}
}

func TestClusterRefusalCannotCompleteAnotherDispatchAttempt(t *testing.T) {
	e := newSchedulingTestEngine(t, "refusal-master")
	setMasterForTest(e)
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "worker"}); err != nil {
		t.Fatal(err)
	}
	conn, remote := overloadSocketPair(t)
	peer := &SlaveConnection{Hostname: "worker", Socket: conn, ActiveJobs: map[string]*types.Job{}}
	e.comm.mu.Lock()
	e.comm.slaves[peer.Hostname] = peer
	e.comm.mu.Unlock()
	live := &types.Job{ID: "refused-retry", Hostname: peer.Hostname, Event: "missing", TimeStart: 1, Retries: 1, DispatchToken: "first-attempt"}
	e.mu.Lock()
	e.activeJobs[live.ID] = types.CloneJob(live)
	e.mu.Unlock()
	if err := e.jobManager.persistActiveJob(live); err != nil {
		t.Fatal(err)
	}
	refusal := func(source *SlaveConnection, token string) {
		e.comm.handleSlaveMessage(source, map[string]interface{}{"type": "job_rejected", "data": map[string]interface{}{"id": live.ID, "dispatch_token": token, "reason": "backend password=do-not-publish"}})
	}
	for _, token := range []string{"", "another-attempt"} {
		refusal(peer, token)
		if job := e.GetActiveJobs()[live.ID]; job == nil || job.DispatchToken != live.DispatchToken {
			t.Fatal("uncorrelated refusal changed the reservation")
		}
	}
	// An authenticated current worker cannot refuse another worker's job.
	if err := e.comm.SaveServerToStorage(&types.Server{Hostname: "other-worker"}); err != nil {
		t.Fatal(err)
	}
	other := &SlaveConnection{Hostname: "other-worker"}
	e.comm.mu.Lock()
	e.comm.slaves[other.Hostname] = other
	e.comm.mu.Unlock()
	refusal(other, live.DispatchToken)
	refusal(&SlaveConnection{Hostname: peer.Hostname}, live.DispatchToken)
	if job := e.GetActiveJobs()[live.ID]; job == nil || job.RetriesUsed != 0 {
		t.Fatal("another worker or retired connection changed the reservation")
	}
	refusal(peer, live.DispatchToken)
	remote.SetReadDeadline(time.Now().Add(time.Second))
	var frame struct {
		Type string    `json:"type"`
		Data types.Job `json:"data"`
	}
	if err := remote.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "launch_job" || frame.Data.DispatchToken == "" || frame.Data.DispatchToken == live.DispatchToken || frame.Data.RetriesUsed != 1 {
		t.Fatalf("retry did not get a new dispatch identity: %+v", frame)
	}
	e.comm.mu.Lock()
	peer.ActiveJobs[live.ID] = types.CloneJob(&frame.Data)
	e.comm.mu.Unlock()
	refusal(peer, live.DispatchToken)
	job := e.GetActiveJobs()[live.ID]
	if job == nil || job.DispatchToken != frame.Data.DispatchToken || len(e.GetDeadJobs()) != 0 {
		t.Fatal("delayed duplicate refusal completed the replacement attempt")
	}
	e.comm.mu.RLock()
	cached := peer.ActiveJobs[live.ID]
	e.comm.mu.RUnlock()
	if cached == nil || cached.DispatchToken != frame.Data.DispatchToken {
		t.Fatal("delayed refusal removed replacement worker status")
	}
	refusal(peer, frame.Data.DispatchToken)
	completed, ok := e.GetDeadJob(live.ID)
	if !ok || completed.Code != 1 || strings.Contains(completed.Description, "do-not-publish") {
		t.Fatalf("matching refusal was not safely completed: %+v", completed)
	}
	e.comm.mu.RLock()
	_, cachedActive := peer.ActiveJobs[live.ID]
	e.comm.mu.RUnlock()
	if cachedActive || len(e.GetActiveJobs()) != 0 {
		t.Fatal("matching refusal retained an active reservation")
	}
	refusal(peer, frame.Data.DispatchToken)
	if e.GetState().Stats["jobs_completed"] != 1 {
		t.Fatal("duplicate refusal completed the job twice")
	}
}

func TestRemoteAcceptedReplayIsNotRejectedWhenSpoolFills(t *testing.T) {
	e := splitTestEngine(t)
	job := &types.Job{ID: "already-accepted", Hostname: e.Hostname(), Command: "/bin/true", DispatchToken: "accepted-attempt"}
	e.mu.Lock()
	e.activeJobs[job.ID] = types.CloneJob(job)
	e.mu.Unlock()
	e.spoolBudget.mu.Lock()
	e.spoolBudget.initialized = true
	e.spoolBudget.used = e.spoolBudget.maximum
	e.spoolBudget.blocked = true
	e.spoolBudget.mu.Unlock()
	if err := e.jobManager.LaunchRemoteJob(types.CloneJob(job)); err != nil {
		t.Fatalf("full spool rejected an already-accepted duplicate: %v", err)
	}
	e.mu.Lock()
	delete(e.activeJobs, job.ID)
	e.mu.Unlock()
	job.Complete = true
	raw, _ := json.Marshal(job)
	if err := e.storage.Put(e.ctx, "jobs:"+job.ID, raw); err != nil {
		t.Fatal(err)
	}
	if err := e.jobManager.LaunchRemoteJob(types.CloneJob(job)); err != nil {
		t.Fatalf("full spool rejected a durably-completed duplicate: %v", err)
	}
}
