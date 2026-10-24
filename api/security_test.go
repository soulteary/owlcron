package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func securityServer(t *testing.T) (*engine.Engine, http.Handler) {
	t.Helper()
	eng, err := engine.NewEngine(&types.Config{SecretKey: "0123456789abcdef0123456789abcdef", StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), MasterPingTimeout: 60, SchedulerStartupGrace: 3600}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveUserToStorage(&types.User{ID: "admin", Username: "admin", Enabled: true, Privileges: types.Privileges{"admin": true}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Startup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Shutdown() })
	if !eng.EnsureLeadership() {
		t.Fatal("fixture failed to become current master")
	}
	mux := http.NewServeMux()
	NewServer(eng, zap.NewNop()).RegisterRoutes(mux)
	return eng, mux
}

func securityRequest(t *testing.T, handler http.Handler, path, body, session string) map[string]interface{} {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	if session != "" {
		r.Header.Set("X-Session-ID", session)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("%s: %s", err, w.Body.String())
	}
	return response
}

func addSecurityUser(t *testing.T, eng *engine.Engine, id string, privileges types.Privileges) string {
	t.Helper()
	if err := eng.SaveUserToStorage(&types.User{ID: id, Username: id, Enabled: true, Privileges: privileges}); err != nil {
		t.Fatal(err)
	}
	session := id + "-session"
	if err := eng.SaveSessionToStorage(&types.Session{ID: session, UserID: id, Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	return session
}

func TestRegistrationAndManagementRequireAdmin(t *testing.T) {
	eng, handler := securityServer(t)
	response := securityRequest(t, handler, "/api/user/create", `{"username":"attacker","password":"123456789012","privileges":{"admin":1}}`, "")
	if response["code"] == float64(0) {
		t.Fatal("unauthenticated administrator registration succeeded")
	}
	regular := addSecurityUser(t, eng, "regular", types.Privileges{})
	for _, path := range []string{"/api/user/create", "/api/app/create_plugin", "/api/app/get_api_keys", "/api/app/add_server"} {
		if securityRequest(t, handler, path, `{}`, regular)["code"] == float64(0) {
			t.Fatalf("regular user accessed %s", path)
		}
	}
	admin := addSecurityUser(t, eng, "administrator", types.Privileges{"admin": true})
	if securityRequest(t, handler, "/api/user/create", `{"username":"newuser","password":"123456789012","privileges":{"run_events":1,"admin":0}}`, admin)["code"] != float64(0) {
		t.Fatal("authorized account creation failed")
	}
	user, err := eng.LoadUserByUsernameFromStorage("newuser")
	if err != nil {
		t.Fatal(err)
	}
	if user.Privileges["admin"] || !user.Privileges["run_events"] {
		t.Fatal("numeric privilege mapping failed")
	}
}

func TestScopedEventsAndJobs(t *testing.T) {
	eng, handler := securityServer(t)
	if err := eng.GetJobManager().SavePluginToStorage(&types.Plugin{ID: "shell", Title: "Shell", Enabled: true, Command: "/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []*types.Event{{ID: "visible", Title: "Visible", Category: "allowed", Target: "host", Plugin: "shell"}, {ID: "private", Title: "Private", Category: "denied", Target: "host", Plugin: "shell"}} {
		if err := eng.GetScheduler().AddEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	session := addSecurityUser(t, eng, "limited", types.Privileges{"cat_limit": true, "cat_allowed": true, "edit_events": true})
	if securityRequest(t, handler, "/api/app/delete_event", `{"id":"visible"}`, session)["code"] == float64(0) {
		t.Fatal("operation privilege not enforced")
	}
	if securityRequest(t, handler, "/api/app/update_event", `{"id":"private","title":"Private","category":"allowed","target":"host","plugin":"shell"}`, session)["code"] == float64(0) {
		t.Fatal("existing event restriction bypassed by category change")
	}
	response := securityRequest(t, handler, "/api/app/get_schedule", `{}`, session)
	rows, ok := response["schedule"].([]interface{})
	if !ok || len(rows) != 1 {
		t.Fatalf("scope filter failed: %#v", response)
	}
	if rows[0].(map[string]interface{})["id"] != "visible" {
		t.Fatal("wrong event exposed")
	}
	if securityRequest(t, handler, "/api/app/update_event", `{"id":"visible","enabled":1}`, session)["code"] != float64(0) {
		t.Fatal("authorized sparse event update rejected")
	}
	event, _ := eng.GetScheduler().GetEvent("visible")
	if !event.Enabled.Bool() || event.Category != "allowed" || event.Plugin != "shell" {
		t.Fatal("sparse update discarded persistent event fields")
	}
}

func TestAPIKeyAuthenticationUsesSecretAndCanonicalRecord(t *testing.T) {
	eng, handler := securityServer(t)
	key := &types.APIKey{ID: "known-id", Key: "random-secret", Enabled: true, Privileges: types.Privileges{"admin": true}}
	if err := eng.SaveAPIKeyToStorage(key); err != nil {
		t.Fatal(err)
	}
	request := func(secret string) float64 {
		r := httptest.NewRequest(http.MethodPost, "/api/app/get_api_keys", bytes.NewBufferString(`{}`))
		r.Header.Set("X-API-Key", secret)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var response map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &response)
		return response["code"].(float64)
	}
	if request(key.ID) == 0 {
		t.Fatal("public API key identifier authenticated")
	}
	if request(key.Key) != 0 {
		t.Fatal("API key secret rejected")
	}
	key.Enabled = false
	if err := eng.SaveAPIKeyToStorage(key); err != nil {
		t.Fatal(err)
	}
	if request(key.Key) == 0 {
		t.Fatal("stale API key list enabled revoked key")
	}
	keys, err := eng.ListAPIKeysFromStorage()
	if err != nil || len(keys) != 1 {
		t.Fatalf("credential index duplicated: %d %v", len(keys), err)
	}
	if err := eng.DeleteAPIKeyFromStorage(key.ID); err != nil {
		t.Fatal(err)
	}
	if request(key.Key) == 0 {
		t.Fatal("deleted API key authenticated")
	}
}

func TestLogTokensAreBoundToReadOnlyJob(t *testing.T) {
	eng, _ := securityServer(t)
	token := makeJobLogToken(eng, "job-one")
	if token == "" {
		t.Fatal("no log token")
	}
	for _, tc := range []struct {
		path, method string
		allowed      bool
	}{
		{"/api/app/get_job_log?id=job-one&t=" + token, "GET", true},
		{"/api/app/get_job_log?id=job-two&t=" + token, "GET", false},
		{"/api/app/fetch_delete_job_log?id=job-one&t=" + token, "GET", false},
		{"/api/app/get_job_log?id=job-one&t=" + token, "POST", false},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		_, err := authenticateLogToken(eng, r)
		if (err == nil) != tc.allowed {
			t.Fatalf("unexpected token authorization %s %s: %v", tc.method, tc.path, err)
		}
	}
}

func TestSessionRevocationAfterPasswordChange(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "user", types.Privileges{})
	if securityRequest(t, handler, "/api/app/get_schedule", `{}`, session)["code"] != float64(0) {
		t.Fatal("fresh session rejected")
	}
	user, err := eng.LoadUserFromStorage("user")
	if err != nil {
		t.Fatal(err)
	}
	user.Password = "changed-password-hash"
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	if securityRequest(t, handler, "/api/app/get_schedule", `{}`, session)["code"] == float64(0) {
		t.Fatal("password-stale session accepted")
	}
	if securityRequest(t, handler, "/api/user/resume_session", `{"session_id":"`+session+`"}`, "")["code"] == float64(0) {
		t.Fatal("resume restored revoked session")
	}
}

func TestLogWatchTokensRequireJobScope(t *testing.T) {
	eng, handler := securityServer(t)
	eng.AddDeadJob(&types.Job{ID: "private-log", Complete: true, Category: "private", Target: "host"})
	eng.AddDeadJob(&types.Job{ID: "visible-log", Complete: true, Category: "allowed", Target: "host"})
	session := addSecurityUser(t, eng, "loguser", types.Privileges{"cat_limit": true, "cat_allowed": true})
	for _, version := range []string{"", "/v1"} {
		if response := securityRequest(t, handler, "/api/app/get_log_watch_auth"+version, `{"id":"private-log"}`, session); response["code"] == float64(0) || response["token"] != nil {
			t.Fatalf("restricted job received a bearer log credential: %#v", response)
		}
		if response := securityRequest(t, handler, "/api/app/get_log_watch_auth"+version, `{"id":"visible-log"}`, session); response["code"] != float64(0) || response["token"] == nil {
			t.Fatalf("authorized log token unavailable: %#v", response)
		}
	}
}

func TestCrossOriginWritesAreRejected(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "administrator", types.Privileges{"admin": true})
	r := httptest.NewRequest(http.MethodPost, "/api/user/create", bytes.NewBufferString(`{"username":"attacker","password":"123456789012"}`))
	r.Header.Set("X-Session-ID", session)
	r.Header.Set("Origin", "https://attacker.example")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var response map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &response)
	if response["code"] == float64(0) {
		t.Fatal("cross-origin privileged write accepted")
	}
}

func TestDestructiveGetIsRejected(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "administrator", types.Privileges{"admin": true})
	r := httptest.NewRequest(http.MethodGet, "/api/app/fetch_delete_job_log?id=one", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: session})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["code"] == float64(0) {
		t.Fatal("destructive GET accepted")
	}
}

func TestCategoryAndGroupListsRespectScopes(t *testing.T) {
	eng, handler := securityServer(t)
	for _, id := range []string{"allowed", "private"} {
		if err := eng.SaveCategoryToStorage(&types.Category{ID: id, Title: id, Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := eng.SaveServerGroupToStorage(&types.ServerGroup{ID: id, Title: id, Regexp: "^" + id + "-host$"}); err != nil {
			t.Fatal(err)
		}
	}
	session := addSecurityUser(t, eng, "scoped", types.Privileges{"cat_limit": true, "cat_allowed": true, "grp_limit": true, "grp_allowed": true})
	for _, path := range []string{"/api/app/get_categories", "/api/app/list_server_groups"} {
		response := securityRequest(t, handler, path, `{}`, session)
		rows, ok := response["rows"].([]interface{})
		if !ok {
			rows, ok = response["data"].([]interface{})
		}
		if !ok || len(rows) != 1 || rows[0].(map[string]interface{})["id"] != "allowed" {
			t.Fatalf("restricted list exposed %s: %#v", path, response)
		}
	}
	if !eng.CanAccess(map[string]bool{"grp_limit": true, "grp_allowed": true}, "", "allowed-host") {
		t.Fatal("allowed hostname group rejected")
	}
	if eng.CanAccess(map[string]bool{"grp_limit": true, "grp_allowed": true}, "", "private-host") {
		t.Fatal("disallowed hostname group accepted")
	}
}

func TestCreatePrivilegeCannotReplaceExistingEvent(t *testing.T) {
	eng, handler := securityServer(t)
	existing := &types.Event{ID: "existing", Title: "Private", Category: "private", Plugin: "shell", Target: "host"}
	if err := eng.GetScheduler().AddEvent(existing); err != nil {
		t.Fatal(err)
	}
	session := addSecurityUser(t, eng, "creator", types.Privileges{"create_events": true, "cat_limit": true, "cat_allowed": true})
	response := securityRequest(t, handler, "/api/app/create_event", `{"id":"existing","title":"Replacement","category":"allowed","plugin":"shell","target":"host"}`, session)
	if response["code"] == float64(0) {
		t.Fatal("create-only account overwrote restricted existing event")
	}
	stored, _ := eng.GetScheduler().GetEvent(existing.ID)
	if stored.Category != "private" || stored.Title != "Private" {
		t.Fatal("event creation modified existing record")
	}
}

func TestManagementMutationsRequireCurrentLeadership(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "operator", types.Privileges{"admin": true})
	lease := map[string]interface{}{"hostname": "replacement-master", "owner": "replacement-token", "expires": time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	if err := eng.GetStorage().Put(context.Background(), "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		"create_event", "update_event", "delete_event", "run_event", "abort_event", "flush_event_queue",
		"create_plugin", "update_plugin", "delete_plugin", "create_category", "update_category", "delete_category",
		"create_server_group", "update_server_group", "delete_server_group", "create_api_key", "update_api_key", "delete_api_key",
		"update_job", "update_jobs", "abort_job", "abort_jobs", "delete_job", "fetch_delete_job_log", "update_master_state",
		"add_server", "remove_server", "restart_server", "shutdown_server",
	}
	for _, name := range paths {
		for _, version := range []string{"", "/v1"} {
			response := securityRequest(t, handler, "/api/app/"+name+version, `{}`, session)
			if response["code"] == float64(0) || response["description"] != "Current master server required" {
				t.Fatalf("stale node entered %s: %#v", name+version, response)
			}
		}
	}
	for _, path := range []string{"/api/user/create", "/api/user/admin_create", "/api/user/admin_update", "/api/user/admin_delete"} {
		response := securityRequest(t, handler, path, `{}`, session)
		if response["description"] != "Current master server required" {
			t.Fatalf("stale node entered %s: %#v", path, response)
		}
	}
	if securityRequest(t, handler, "/api/app/get_schedule", `{}`, session)["code"] != float64(0) {
		t.Fatal("leadership retirement blocked reads")
	}
	if securityRequest(t, handler, "/api/user/resume_session", `{"session_id":"`+session+`"}`, "")["code"] != float64(0) {
		t.Fatal("follower cannot resume session")
	}
}

func TestFollowerSelfServiceDoesNotRequireLeadership(t *testing.T) {
	eng, handler := securityServer(t)
	hash, err := eng.HashPassword("current-password")
	if err != nil {
		t.Fatal(err)
	}
	user := &types.User{ID: "selfuser", Username: "selfuser", Password: hash, Enabled: true, Privileges: types.Privileges{}}
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSessionToStorage(&types.Session{ID: "self-session", UserID: user.ID, Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]interface{}{"hostname": "replacement-master", "owner": "replacement-token", "expires": time.Now().Add(time.Minute).UnixNano()})
	if err := eng.GetStorage().Put(context.Background(), "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	if eng.EnsureLeadership() {
		t.Fatal("fixture failed to retire stale master")
	}
	if response := securityRequest(t, handler, "/api/user/update", `{"username":"selfuser","old_password":"current-password","full_name":"Updated Name"}`, "self-session"); response["code"] != float64(0) {
		t.Fatalf("follower rejected self-service profile: %#v", response)
	}
	login := securityRequest(t, handler, "/api/user/login", `{"username":"selfuser","password":"current-password"}`, "")
	if login["code"] != float64(0) {
		t.Fatalf("follower rejected login: %#v", login)
	}
	session := login["session_id"].(string)
	if securityRequest(t, handler, "/api/user/logout", `{"session_id":"`+session+`"}`, "")["code"] != float64(0) {
		t.Fatal("follower rejected logout")
	}
	if response := securityRequest(t, handler, "/api/user/delete", `{"username":"selfuser","password":"current-password"}`, "self-session"); response["code"] != float64(0) {
		t.Fatalf("follower rejected self deletion: %#v", response)
	}
}

func TestFollowerCannotDeleteAnAdministratorThroughSelfService(t *testing.T) {
	eng, handler := securityServer(t)
	hash, err := eng.HashPassword("administrator-password")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveUserToStorage(&types.User{ID: "self-admin", Username: "self-admin", Enabled: true, Password: hash, Privileges: types.Privileges{"admin": true}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSessionToStorage(&types.Session{ID: "self-admin-session", UserID: "self-admin", Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]interface{}{"hostname": "replacement-master", "owner": "replacement-token", "expires": time.Now().Add(time.Minute).UnixNano()})
	if err := eng.GetStorage().Put(context.Background(), "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", "/v1"} {
		response := securityRequest(t, handler, "/api/user/delete"+version, `{"username":"self-admin","password":"administrator-password"}`, "self-admin-session")
		if response["code"] == float64(0) || response["description"] != "Current master server required for administrator deletion" {
			t.Fatalf("administrator self-delete bypassed leader: %#v", response)
		}
		request := httptest.NewRequest(http.MethodGet, "/api/user/delete"+version+"?username=self-admin&password=administrator-password", nil)
		request.Header.Set("X-Session-ID", "self-admin-session")
		responseWriter := httptest.NewRecorder()
		handler.ServeHTTP(responseWriter, request)
		var denied map[string]interface{}
		if err := json.Unmarshal(responseWriter.Body.Bytes(), &denied); err != nil {
			t.Fatal(err)
		}
		if denied["description"] != "POST required for mutations" {
			t.Fatalf("versioned destructive GET entered handler: %#v", denied)
		}
	}
	if user, err := eng.LoadUserFromStorage("self-admin"); err != nil || !user.Enabled {
		t.Fatalf("blocked deletion changed administrator: %#v %v", user, err)
	}
}
