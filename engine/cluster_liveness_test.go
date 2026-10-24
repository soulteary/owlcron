package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

func TestClusterQueuedAdmissionRestoresIdleFollowerFromExactLease(t *testing.T) {
	_, worker := nodeIdentityFixture(t)
	t.Cleanup(func() { worker.cancel(); worker.jobManager.Wait() })
	lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := worker.storage.Put(worker.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	worker.mu.Lock()
	worker.multi.MasterHostname = ""
	worker.multi.Master = false
	worker.multi.Slave = false
	worker.multi.LastPingReceived = 123
	worker.mu.Unlock()
	client := &ws.Client{ID: "queued-master", NodeEndpoint: true}
	client.SetAuthentication("sender", true, true, nil, "node", lease.Owner, 0)
	handler := NewWebSocketHandler(worker, zap.NewNop())
	if handler.AllowMaster(client) {
		t.Fatal("idle follower sent an outgoing report")
	}
	if !client.IsAuthenticated() {
		t.Fatal("outgoing filter revoked a current owner solely because the local heartbeat was idle")
	}
	if worker.GetMulti().LastPingReceived != 123 {
		t.Fatal("outgoing idle report refreshed liveness")
	}
	marker := filepath.Join(t.TempDir(), "started")
	script := filepath.Join(t.TempDir(), "job.sh")
	if err := os.WriteFile(script, []byte("echo started > '"+marker+"'\nprintf '%s\\n' '{\"complete\":true,\"code\":0}'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	job := &types.Job{ID: "queued-after-idle", Hostname: worker.Hostname(), Command: "/bin/sh " + script, TimeStart: float64(time.Now().Unix())}
	raw, _ = json.Marshal(job)
	handler.OnMessage(client, &ws.Message{Type: "launch_job", Data: raw})
	waitForCondition(t, time.Second, func() bool { _, err := os.Stat(marker); return err == nil })
	multi := worker.GetMulti()
	if multi.Master || !multi.Slave || multi.MasterHostname != "sender" || multi.LastPingReceived <= 123 {
		t.Fatalf("authenticated admission did not restore follower: %#v", multi)
	}
}

func TestClusterOnlyIncomingMasterFramesRefreshLiveness(t *testing.T) {
	_, worker := nodeIdentityFixture(t)
	lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := worker.storage.Put(worker.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	worker.mu.Lock()
	worker.multi.Master = false
	worker.multi.MasterHostname = "sender"
	worker.multi.LastPingReceived = 123
	worker.mu.Unlock()
	client := &ws.Client{ID: "live-master", NodeEndpoint: true}
	client.SetAuthentication("sender", true, true, nil, "node", lease.Owner, 0)
	handler := NewWebSocketHandler(worker, zap.NewNop())
	if !handler.AllowMaster(client) {
		t.Fatal("outgoing master filter rejected current owner")
	}
	if worker.GetMulti().LastPingReceived != 123 {
		t.Fatal("outgoing traffic manufactured master liveness")
	}
	handler.OnMessage(client, &ws.Message{Type: "finish_job_ack", Data: json.RawMessage(`{}`)})
	if worker.GetMulti().LastPingReceived <= 123 {
		t.Fatal("authenticated inbound frame did not refresh liveness")
	}
}

func TestClusterIdleRestoreRejectsRevokedMaster(t *testing.T) {
	for _, mode := range []string{"owner_changed", "expired", "sender_disabled", "receiver_disabled", "local_master"} {
		t.Run(mode, func(t *testing.T) {
			_, worker := nodeIdentityFixture(t)
			lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
			client := &ws.Client{ID: "revoked-master", NodeEndpoint: true}
			client.SetAuthentication("sender", true, true, nil, "node", lease.Owner, 0)
			worker.mu.Lock()
			worker.multi.MasterHostname = ""
			worker.multi.Master = false
			worker.multi.LastPingReceived = 123
			worker.mu.Unlock()
			switch mode {
			case "owner_changed":
				lease.Owner = "replacement-owner"
			case "expired":
				lease.Expires = time.Now().Add(-time.Second).UnixNano()
			case "sender_disabled", "receiver_disabled":
				hostname := "sender"
				if mode == "receiver_disabled" {
					hostname = "receiver"
				}
				raw, _ := json.Marshal(&types.Server{Hostname: hostname, Disabled: true})
				if err := worker.storage.Put(worker.ctx, "servers:"+hostname, raw); err != nil {
					t.Fatal(err)
				}
			case "local_master":
				worker.mu.Lock()
				worker.multi.Master = true
				worker.mu.Unlock()
			}
			raw, _ := json.Marshal(lease)
			if err := worker.storage.Put(worker.ctx, "global/master_lease", raw); err != nil {
				t.Fatal(err)
			}
			handler := NewWebSocketHandler(worker, zap.NewNop())
			handler.OnMessage(client, &ws.Message{Type: "finish_job_ack", Data: json.RawMessage(`{}`)})
			if worker.GetMulti().LastPingReceived != 123 {
				t.Fatal("revoked frame refreshed master liveness")
			}
			if client.IsAuthenticated() {
				t.Fatal("revoked connection remained authenticated")
			}
		})
	}
}

func TestClusterStaleTickCannotIdleRefreshedMaster(t *testing.T) {
	_, worker := nodeIdentityFixture(t)
	worker.config.MasterPingTimeout = 1
	lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := worker.storage.Put(worker.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	worker.mu.Lock()
	worker.multi.Master = false
	worker.multi.Slave = true
	worker.multi.MasterHostname = "sender"
	worker.multi.LastPingReceived = time.Now().Add(-time.Hour).Unix()
	worker.mu.Unlock()
	// tick has already observed this expired timestamp and selected failover.
	// The read loop authenticates a queued frame before that decision executes.
	client := &ws.Client{ID: "refreshed-master", NodeEndpoint: true}
	client.SetAuthentication("sender", true, true, nil, "node", lease.Owner, 0)
	handler := NewWebSocketHandler(worker, zap.NewNop())
	handler.OnMessage(client, &ws.Message{Type: "finish_job_ack", Data: json.RawMessage(`{}`)})
	worker.slaveFailover()
	multi := worker.GetMulti()
	if !multi.Slave || multi.MasterHostname != "sender" {
		t.Fatalf("stale tick discarded newly authenticated master: %#v", multi)
	}
	// A real partition still goes idle once no authenticated incoming traffic
	// has refreshed the deadline. The valid shared lease alone is insufficient.
	worker.mu.Lock()
	worker.multi.LastPingReceived = time.Now().Add(-time.Hour).Unix()
	worker.mu.Unlock()
	worker.slaveFailover()
	multi = worker.GetMulti()
	if multi.Slave || multi.MasterHostname != "" {
		t.Fatalf("expired transport did not go idle: %#v", multi)
	}
}

func TestClusterQueuedRoleRestoreRechecksRevocation(t *testing.T) {
	for _, mode := range []string{"owner_changed", "sender_disabled", "receiver_disabled", "authentication_cleared"} {
		t.Run(mode, func(t *testing.T) {
			_, worker := nodeIdentityFixture(t)
			lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
			raw, _ := json.Marshal(lease)
			base := worker.storage
			if err := base.Put(worker.ctx, "global/master_lease", raw); err != nil {
				t.Fatal(err)
			}
			worker.mu.Lock()
			worker.multi.Master = false
			worker.multi.MasterHostname = ""
			worker.multi.LastPingReceived = 123
			worker.mu.Unlock()
			client := &ws.Client{ID: "queued-revoked-master", NodeEndpoint: true}
			client.SetAuthentication("sender", true, true, nil, "node", lease.Owner, 0)
			release := make(chan struct{})
			close(release)
			observer := &slowPeerValidationStore{Storage: base, entered: make(chan struct{}), release: release}
			worker.storage = observer
			handler := NewWebSocketHandler(worker, zap.NewNop())
			worker.roleMu.Lock()
			locked := true
			defer func() {
				if locked {
					worker.roleMu.Unlock()
				}
			}()
			started := make(chan struct{})
			result := make(chan bool, 1)
			go func() { close(started); result <- handler.allowIncomingMaster(client) }()
			<-started
			// The caller is queued behind a role transition. It must not cache a
			// successful canonical check and then revive it after this revocation.
			select {
			case <-result:
				t.Fatal("incoming frame bypassed role serialization")
			case <-time.After(50 * time.Millisecond):
			}
			switch mode {
			case "owner_changed":
				lease.Owner = "replacement-owner"
				raw, _ = json.Marshal(lease)
				if err := base.Put(context.Background(), "global/master_lease", raw); err != nil {
					t.Fatal(err)
				}
			case "sender_disabled", "receiver_disabled":
				hostname := "sender"
				if mode == "receiver_disabled" {
					hostname = "receiver"
				}
				raw, _ := json.Marshal(&types.Server{Hostname: hostname, Disabled: true})
				if err := base.Put(context.Background(), "servers:"+hostname, raw); err != nil {
					t.Fatal(err)
				}
			case "authentication_cleared":
				client.ClearAuthentication()
			}
			worker.roleMu.Unlock()
			locked = false
			select {
			case allowed := <-result:
				if allowed {
					t.Fatal("queued frame revived revoked master authority")
				}
			case <-time.After(time.Second):
				t.Fatal("revoked queued frame did not exit")
			}
			if worker.GetMulti().LastPingReceived != 123 {
				t.Fatal("revoked queued frame refreshed liveness")
			}
			if client.IsAuthenticated() {
				t.Fatal("revoked queued connection retained authentication")
			}
		})
	}
}
