package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestUserAdministrationAndPasswordRevocation(t *testing.T) {
	eng, handler := securityServer(t)
	hash, err := eng.HashPassword("original-password")
	if err != nil {
		t.Fatal(err)
	}
	admin := &types.User{ID: "admin", Username: "admin", Password: hash, Enabled: true, Privileges: types.Privileges{"admin": true}}
	if err = eng.SaveUserToStorage(admin); err != nil {
		t.Fatal(err)
	}
	if err = eng.SaveSessionToStorage(&types.Session{ID: "admin-session", UserID: "admin", Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	session := "admin-session"
	create := securityRequest(t, handler, "/api/user/admin_create", `{"username":"worker.name","password":"worker-password","email":"worker@example.com","full_name":"Worker Name","active":1,"privileges":{"run_events":1}}`, session)
	if create["code"] != float64(0) {
		t.Fatal(create)
	}
	got := securityRequest(t, handler, "/api/user/admin_get_user", `{"username":"worker.name"}`, session)
	if got["code"] != float64(0) || got["user"].(map[string]interface{})["full_name"] != "Worker Name" {
		t.Fatal(got)
	}
	worker, _ := eng.LoadUserByUsernameFromStorage("worker.name")
	if err = eng.SaveSessionToStorage(&types.Session{ID: "worker-session", UserID: worker.ID, Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	own := securityRequest(t, handler, "/api/user/update", `{"username":"worker.name","old_password":"worker-password","privileges":{"admin":1}}`, "worker-session")
	if own["code"] == float64(0) {
		t.Fatal("self escalation succeeded")
	}
	change := securityRequest(t, handler, "/api/user/admin_update", `{"username":"worker.name","new_password":"new-worker-password"}`, session)
	if change["code"] != float64(0) {
		t.Fatal(change)
	}
	if securityRequest(t, handler, "/api/app/get_schedule", `{}`, "worker-session")["code"] == float64(0) {
		t.Fatal("old session survived password reset")
	}
	if securityRequest(t, handler, "/api/user/admin_delete", `{"username":"admin"}`, session)["code"] == float64(0) {
		t.Fatal("last admin deletion accepted")
	}
	if securityRequest(t, handler, "/api/user/admin_update", `{"username":"admin","active":0}`, session)["code"] == float64(0) {
		t.Fatal("last admin disabling accepted")
	}
	if securityRequest(t, handler, "/api/user/admin_delete", `{"username":"worker.name"}`, session)["code"] != float64(0) {
		t.Fatal("authorized delete failed")
	}
	if _, err := eng.LoadUserByUsernameFromStorage("worker.name"); err == nil {
		t.Fatal("deleted user restored from index")
	}
}
func TestAPIKeyFrontendContractAndMasterState(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "administrator", types.Privileges{"admin": true})
	create := securityRequest(t, handler, "/api/app/create_api_key", `{"id":"testkey","title":"Test Key","active":1,"privileges":{"run_events":1}}`, session)
	if create["code"] != float64(0) {
		t.Fatal(create)
	}
	got := securityRequest(t, handler, "/api/app/get_api_key", `{"id":"testkey"}`, session)
	if got["code"] != float64(0) {
		t.Fatal(got)
	}
	key := got["key"].(map[string]interface{})
	if key["active"] != true || len(key["key"].(string)) < 32 {
		t.Fatal(got)
	}
	list := securityRequest(t, handler, "/api/app/get_api_keys", `{}`, session)
	if len(list["rows"].([]interface{})) != 1 {
		t.Fatal(list)
	}
	state := securityRequest(t, handler, "/api/app/update_master_state", `{"enabled":0}`, session)
	if state["code"] != float64(0) || eng.GetState().Enabled {
		t.Fatal(state)
	}
}
func TestCookieCrossOriginWritesDenied(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("accepted")) })
	r := httptest.NewRequest(http.MethodPost, "http://owlcron.local/api/user/login", strings.NewReader(`{}`))
	r.Header.Set("Origin", "http://attacker.local")
	w := httptest.NewRecorder()
	CORS(next).ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatal("cross-origin cookie write accepted")
	}
}
