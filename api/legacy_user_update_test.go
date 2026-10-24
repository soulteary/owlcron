package api

import (
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestExistingShortUsernameCanUpdateWithoutRelaxingCreation(t *testing.T) {
	eng, handler := securityServer(t)
	adminSession := addSecurityUser(t, eng, "administrator", types.Privileges{"admin": true})
	const password = "legacy-user-password"
	hash, err := eng.HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	user := &types.User{ID: "legacy-user", Username: "x", Password: hash, Email: "old@example.test", Enabled: true, Privileges: types.Privileges{"run_events": true}}
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSessionToStorage(&types.Session{ID: "legacy-session", UserID: user.ID, Expires: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ endpoint, body, session string }{
		{"/api/user/admin_update", `{"username":"x","full_name":"Admin edited"}`, adminSession},
		{"/api/user/update", `{"username":"x","full_name":"Self edited","old_password":"legacy-user-password"}`, "legacy-session"},
	} {
		if got := securityRequest(t, handler, tc.endpoint, tc.body, tc.session); got["code"] != float64(0) {
			t.Fatalf("existing user update rejected: %#v", got)
		}
	}
	current, err := eng.LoadUserFromStorage(user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Username != "x" || current.ID != user.ID || !current.Enabled || current.Privileges["admin"] || !current.Privileges["run_events"] {
		t.Fatal("update changed canonical identity or permissions")
	}
	if current.Data["full_name"] != "Self edited" {
		t.Fatal("profile update not persisted")
	}
	if got := securityRequest(t, handler, "/api/user/admin_update", `{"username":"x","email":"invalid"}`, adminSession); got["code"] == float64(0) {
		t.Fatal("legacy username bypassed email validation")
	}
	if got := securityRequest(t, handler, "/api/user/admin_create", `{"username":"y","password":"new-user-password"}`, adminSession); got["code"] == float64(0) {
		t.Fatal("new short username accepted")
	}
}

func TestUserUpdateCannotReplaceCanonicalIdentity(t *testing.T) {
	expected := &types.User{ID: "original", Username: "x"}
	for _, changed := range []*types.User{{ID: "other", Username: "x"}, {ID: "original", Username: "other"}} {
		if validateUserUpdate(expected, changed) == nil {
			t.Fatal("identity change accepted")
		}
	}
}
