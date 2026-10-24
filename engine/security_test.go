package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	gorillaWS "github.com/gorilla/websocket"
	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

func TestBrowserAuthenticationStatusMatchesPeriodicContract(t *testing.T) {
	eng := securityEngine(t)
	eng.config.Hostname = "visible"
	if err := eng.comm.Startup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eng.comm.Shutdown)
	if err := eng.SaveServerGroupToStorage(&types.ServerGroup{ID: "allowed", Title: "Allowed", Regexp: "^visible(-worker)?$"}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []*types.Event{{ID: "visible-event", Category: "allowed", Target: "visible"}, {ID: "private-event", Category: "private", Target: "private"}} {
		if err := eng.scheduler.AddEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	eng.mu.Lock()
	eng.multi.Master = true
	eng.multi.MasterHostname = "visible"
	eng.multi.Data = map[string]interface{}{"cpu": 1.5, "mem": 100.0}
	eng.state.Stats["started"] = float64(time.Now().Unix() - 15)
	eng.activeJobs["visible-job"] = &types.Job{ID: "visible-job", Category: "allowed", Target: "visible"}
	eng.activeJobs["private-job"] = &types.Job{ID: "private-job", Category: "private", Target: "private"}
	eng.eventQueue["visible-event"] = 1
	eng.eventQueue["private-event"] = 2
	eng.mu.Unlock()
	eng.comm.mu.Lock()
	for _, name := range []string{"visible-worker", "private"} {
		eng.comm.slaves[name] = &SlaveConnection{Hostname: name, Data: map[string]interface{}{
			"uptime": 25.0, "data": map[string]interface{}{"cpu": 2.5, "mem": 200.0},
			"active_jobs": map[string]interface{}{"private-job": "must not be embedded"},
		}}
	}
	eng.comm.mu.Unlock()
	mux := http.NewServeMux()
	eng.comm.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	for _, tc := range []struct {
		name       string
		privileges types.Privileges
		jobs       int
		servers    int
	}{
		{"admin", types.Privileges{"admin": true}, 2, 3},
		{"limited", types.Privileges{"cat_limit": true, "cat_allowed": true, "grp_limit": true, "grp_allowed": true}, 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := eng.SaveUserToStorage(&types.User{ID: tc.name, Username: tc.name, Enabled: true, Privileges: tc.privileges}); err != nil {
				t.Fatal(err)
			}
			if err := eng.SaveSessionToStorage(&types.Session{ID: tc.name + "-session", UserID: tc.name, Expires: time.Now().Unix() + 300}); err != nil {
				t.Fatal(err)
			}
			conn, _, err := gorillaWS.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.WriteJSON(map[string]interface{}{"type": "auth", "data": map[string]string{"session_id": tc.name + "-session"}}); err != nil {
				t.Fatal(err)
			}
			readMessage := func(wantType string) map[string]interface{} {
				t.Helper()
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				var message struct {
					Type string                 `json:"type"`
					Data map[string]interface{} `json:"data"`
				}
				if err := conn.ReadJSON(&message); err != nil {
					t.Fatal(err)
				}
				if message.Type != wantType {
					t.Fatalf("expected %s, got %s", wantType, message.Type)
				}
				return message.Data
			}
			readMessage("auth_success")
			initial := readMessage("status")
			readMessage("update")
			eng.broadcastStatus()
			periodic := readMessage("status")
			for _, data := range []map[string]interface{}{initial, periodic} {
				if data["master"] != true || data["epoch"].(float64) <= 0 || data["uptime"].(float64) < 15 {
					t.Fatalf("missing initial master clock/uptime: %#v", data)
				}
				jobs, ok := data["active_jobs"].(map[string]interface{})
				if !ok || len(jobs) != tc.jobs {
					t.Fatalf("jobs must be a scoped object: %#v", data["active_jobs"])
				}
				queue, ok := data["queue"].(map[string]interface{})
				if !ok || len(queue) != tc.jobs {
					t.Fatalf("queue must be scoped: %#v", data["queue"])
				}
				servers, ok := data["servers"].(map[string]interface{})
				if !ok || len(servers) != tc.servers || servers["visible"] == nil {
					t.Fatalf("missing/scopeless server objects: %#v", data["servers"])
				}
				worker := servers["visible-worker"].(map[string]interface{})
				metrics := worker["data"].(map[string]interface{})
				if worker["uptime"] != 25.0 || metrics["cpu"] != 2.5 || metrics["active_jobs"] != nil {
					t.Fatalf("worker resource metrics contain status envelope: %#v", worker)
				}
			}
			keys := func(data map[string]interface{}) map[string]bool {
				out := map[string]bool{}
				for key := range data {
					out[key] = true
				}
				return out
			}
			if !reflect.DeepEqual(keys(initial), keys(periodic)) {
				t.Fatalf("initial and periodic schema differ: %#v %#v", keys(initial), keys(periodic))
			}
		})
	}
}

func securityEngine(t *testing.T) *Engine {
	t.Helper()
	eng, err := NewEngine(&types.Config{SecretKey: "0123456789abcdef0123456789abcdef", StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), MasterPingTimeout: 60}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

func TestBootstrapCreatesPrivateRandomCredential(t *testing.T) {
	t.Setenv("OWLCRON_ADMIN_PASSWORD", "")
	t.Setenv("OWLCRON_ADMIN_USERNAME", "")
	eng := securityEngine(t)
	if err := eng.ensureDefaultAdmin(); err != nil {
		t.Fatal(err)
	}
	user, err := eng.LoadUserByUsernameFromStorage("admin")
	if err != nil {
		t.Fatal(err)
	}
	if eng.VerifyUserPassword(user, "admin") {
		t.Fatal("shared default password retained")
	}
	path := filepath.Join(eng.config.StorageBaseDir, "bootstrap-admin.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("credential permissions: %o", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var credential struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(raw, &credential); err != nil {
		t.Fatal(err)
	}
	if len(credential.Password) < 32 || !eng.VerifyUserPassword(user, credential.Password) {
		t.Fatal("generated credential cannot authenticate")
	}
	if err := eng.ensureDefaultAdmin(); err != nil {
		t.Fatal(err)
	}
	reloaded, _ := eng.LoadUserFromStorage("admin")
	if reloaded.Password != user.Password {
		t.Fatal("bootstrap reset existing admin")
	}
}

func TestSessionCreationCannotAdoptChangedPasswordRevision(t *testing.T) {
	eng := securityEngine(t)
	user := &types.User{ID: "password-race", Username: "password-race", Enabled: true, Password: "old-verified-password-hash"}
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	session := &types.Session{ID: "old-login", UserID: user.ID, Expires: time.Now().Unix() + 300, Data: map[string]interface{}{"password_revision": PasswordRevision(user)}}
	user.Password = "replacement-password-hash"
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSessionToStorage(session); err == nil {
		t.Fatal("old password verification adopted the replacement password revision")
	}
	if _, err := eng.LoadSessionFromStorage(session.ID); err == nil {
		t.Fatal("failed stale login persisted a session")
	}
}

func TestAccountCASRejectsStaleRevocationAndDeletion(t *testing.T) {
	eng := securityEngine(t)
	if err := eng.SaveUserToStorage(&types.User{ID: "stale-account", Username: "stale-account", Enabled: true, Password: "verified-password", Privileges: types.Privileges{"admin": true}}); err != nil {
		t.Fatal(err)
	}
	expected, err := eng.LoadUserFromStorage("stale-account")
	if err != nil {
		t.Fatal(err)
	}
	staleEdit, _ := eng.LoadUserFromStorage("stale-account")
	staleEdit.Email = "profile@example.com"
	revoked, _ := eng.LoadUserFromStorage("stale-account")
	revoked.Enabled = false
	revoked.Privileges = types.Privileges{}
	if err := eng.SaveUserToStorage(revoked); err != nil {
		t.Fatal(err)
	}
	if err := eng.ChangeUserInStorage(expected, staleEdit); err == nil {
		t.Fatal("stale profile update restored revoked privileges")
	}
	if err := eng.ChangeUserInStorage(expected, nil); err == nil {
		t.Fatal("stale credentials deleted a concurrently changed account")
	}
	latest, err := eng.LoadUserFromStorage("stale-account")
	if err != nil || latest.Enabled || latest.Privileges["admin"] {
		t.Fatal("failed stale edit altered revocation")
	}
	if err := eng.ChangeUserInStorage(latest, nil); err != nil {
		t.Fatal(err)
	}
	if err := eng.ChangeUserInStorage(latest, staleEdit); err == nil {
		t.Fatal("stale profile update resurrected a deleted account")
	}
	if _, err := eng.LoadUserFromStorage("stale-account"); err == nil {
		t.Fatal("deleted account persisted")
	}
}

func TestAPIKeyCanonicalLookupDoesNotRecurseThroughStaleIndex(t *testing.T) {
	eng := securityEngine(t)
	key := &types.APIKey{ID: "legacy-secret", Key: "legacy-secret", Enabled: true}
	if err := eng.SaveAPIKeyToStorage(key); err != nil {
		t.Fatal(err)
	}
	if err := eng.storage.Delete(eng.ctx, "api_keys:"+key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.LoadAPIKeyFromStorage(key.Key); err == nil {
		t.Fatal("stale canonical credential index restored a deleted key")
	}
}

func TestConcurrentBootstrapDoesNotOverwriteAdmin(t *testing.T) {
	t.Setenv("OWLCRON_ADMIN_PASSWORD", "")
	t.Setenv("OWLCRON_ADMIN_USERNAME", "")
	first, second := securityEngine(t), securityEngine(t)
	second.storage = first.storage
	start := make(chan struct{})
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	for _, eng := range []*Engine{first, second} {
		workers.Add(1)
		go func(eng *Engine) { defer workers.Done(); <-start; errors <- eng.ensureDefaultAdmin() }(eng)
	}
	close(start)
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	users, err := first.ListUsersFromStorage()
	if err != nil || len(users) != 1 {
		t.Fatalf("bootstrap duplicated admin: %d %v", len(users), err)
	}
	credentials := 0
	for _, eng := range []*Engine{first, second} {
		raw, err := os.ReadFile(filepath.Join(eng.config.StorageBaseDir, "bootstrap-admin.json"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var credential struct {
			Password string `json:"password"`
		}
		if err := json.Unmarshal(raw, &credential); err != nil {
			t.Fatal(err)
		}
		if !eng.VerifyUserPassword(users[0], credential.Password) {
			t.Fatal("node retained an invalid generated bootstrap password")
		}
		credentials++
	}
	if credentials != 1 {
		t.Fatalf("expected one bootstrap owner, found %d", credentials)
	}
}

func TestNodeAuthenticationRejectsSpoofReplayAndExpiredProof(t *testing.T) {
	eng := securityEngine(t)
	eng.config.ClusterAllowInsecure = true
	eng.config.Hostname = "trusted-master"
	if err := eng.claimLeadership(); err != nil {
		t.Fatal(err)
	}
	proof := eng.MakeNodeAuth("trusted-master", true)
	if hostname, master, err := eng.VerifyNodeAuth(proof); err != nil || hostname != "trusted-master" || !master {
		t.Fatalf("valid proof rejected: %v", err)
	}
	if _, _, err := eng.VerifyNodeAuth(proof); err == nil {
		t.Fatal("node proof replay accepted")
	}
	proof = eng.MakeNodeAuth("trusted-master", true)
	proof.Hostname = "attacker"
	if _, _, err := eng.VerifyNodeAuth(proof); err == nil {
		t.Fatal("tampered node identity accepted")
	}
	proof = eng.MakeNodeAuth("trusted-master", true)
	proof.Timestamp = time.Now().Unix() - 61
	proof.Signature = eng.nodeAuthSignature(proof)
	if _, _, err := eng.VerifyNodeAuth(proof); err == nil {
		t.Fatal("expired node proof accepted")
	}
	handler := NewWebSocketHandler(eng, zap.NewNop())
	client := &ws.Client{ID: "untrusted"}
	handler.handleAuth(client, &ws.Message{Type: "auth", Data: json.RawMessage(`{"master":true}`)})
	if client.IsAuthenticated() || client.IsNodeMaster() {
		t.Fatal("master boolean bypass accepted")
	}
}

func TestReplacedLeadershipCannotAcknowledgeOrControlWorker(t *testing.T) {
	eng := securityEngine(t)
	eng.config.ClusterAllowInsecure = true
	eng.config.Hostname = "trusted-master"
	if err := eng.claimLeadership(); err != nil {
		t.Fatal(err)
	}
	oldOwner := eng.LeadershipToken()
	eng.mu.Lock()
	eng.multi.Master = false
	eng.multi.MasterHostname = "trusted-master"
	eng.mu.Unlock()
	client := &ws.Client{ID: "oldmaster"}
	client.SetAuthentication("trusted-master", true, true, nil, "node", oldOwner, 0)
	handler := NewWebSocketHandler(eng, zap.NewNop())
	if !handler.AllowMaster(client) {
		t.Fatal("valid selected master rejected")
	}
	lease := leadershipLease{Hostname: "trusted-master", Owner: "replacement-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := eng.storage.Put(eng.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	if handler.AllowMaster(client) {
		t.Fatal("replaced leader retained worker authority")
	}
	handler.OnMessage(client, &ws.Message{Type: "shutdown_server", Data: json.RawMessage(`{}`)})
	if eng.ctx.Err() != nil {
		t.Fatal("replaced leader stopped worker")
	}
	if _, allowed := handler.currentPrivileges(client); allowed {
		t.Fatal("replaced leader receives worker data")
	}
}

func TestWebSocketHonorsCredentialRevocationAndScope(t *testing.T) {
	eng := securityEngine(t)
	user := &types.User{ID: "limited", Username: "limited", Enabled: true, Privileges: types.Privileges{"cat_limit": true, "cat_allowed": true}}
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSessionToStorage(&types.Session{ID: "session", UserID: user.ID, Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	client := &ws.Client{ID: "browser"}
	handler := NewWebSocketHandler(eng, zap.NewNop())
	handler.handleAuth(client, &ws.Message{Type: "auth", Data: json.RawMessage(`{"session_id":"session"}`)})
	if !client.IsAuthenticated() || client.IsNodeMaster() {
		t.Fatal("browser authentication failed")
	}
	private := &types.Job{ID: "private", Category: "denied", Target: "host"}
	if _, allowed := handler.FilterBroadcast(client, "job_update", private); allowed {
		t.Fatal("restricted job broadcast disclosed")
	}
	for _, event := range []*types.Event{{ID: "allowed-event", Category: "allowed", Target: "host"}, {ID: "private-event", Category: "denied", Target: "host"}} {
		if err := eng.scheduler.AddEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	for _, alias := range []string{"queue", "event_queue", "eventQueue"} {
		filtered, allowed := handler.FilterBroadcast(client, "update", map[string]interface{}{alias: map[string]int{"allowed-event": 1, "private-event": 2}})
		if !allowed {
			t.Fatalf("valid %s update rejected", alias)
		}
		queue, ok := filtered.(map[string]interface{})[alias].(map[string]interface{})
		if !ok || len(queue) != 1 || queue["allowed-event"] != float64(1) {
			t.Fatalf("queue alias %s leaks restricted event IDs: %#v", alias, filtered)
		}
	}
	if err := eng.DeleteSessionFromStorage("session"); err != nil {
		t.Fatal(err)
	}
	if _, allowed := handler.FilterBroadcast(client, "status", map[string]interface{}{}); allowed {
		t.Fatal("revoked session still receives broadcasts")
	}
}

func TestScopedStateDoesNotMutateSchedulerState(t *testing.T) {
	eng := securityEngine(t)
	for _, event := range []*types.Event{{ID: "allowed", Category: "allowed", Target: "host"}, {ID: "private", Category: "private", Target: "host"}} {
		if err := eng.scheduler.AddEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	eng.mu.Lock()
	eng.state.Cursors["allowed"] = 1
	eng.state.Cursors["private"] = 2
	eng.state.JobCodes["private"] = 1
	eng.state.FlaggedJobs["private-job"] = map[string]int64{"time": 123}
	eng.activeJobs["private-job"] = &types.Job{ID: "private-job", Category: "private", Target: "host"}
	eng.mu.Unlock()
	scoped := eng.ScopedState(map[string]bool{"cat_limit": true, "cat_allowed": true})
	if len(scoped.Cursors) != 1 || scoped.Cursors["allowed"] != 1 || len(scoped.JobCodes) != 0 {
		t.Fatal("scoped state leaked event IDs")
	}
	if len(eng.GetState().Cursors) != 2 {
		t.Fatal("scope filtering mutated persistent scheduler state")
	}
	raw, err := json.Marshal(scoped)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]interface{}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"jobCodes", "job_codes", "flaggedJobs", "flagged_jobs"} {
		if entries, ok := wire[alias].(map[string]interface{}); ok && len(entries) != 0 {
			t.Fatalf("state alias %s exposes restricted IDs: %#v", alias, entries)
		}
	}
}
