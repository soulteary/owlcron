package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestPasswordRecoveryIsOptInAndRegistrationStaysClosed(t *testing.T) {
	eng, handler := securityServer(t)
	for _, path := range []string{"/api/user/forgot_password", "/api/user/reset_password"} {
		if securityRequest(t, handler, path, `{}`, "")["code"] == float64(0) {
			t.Fatalf("disabled endpoint accepted: %s", path)
		}
	}
	eng.GetConfig().AccountEmailEnabled = true
	if securityRequest(t, handler, "/api/user/create", `{"username":"public-user","password":"a-long-password"}`, "")["code"] == float64(0) {
		t.Fatal("account email enabled public registration")
	}
}

func TestPasswordRecoveryUsesTheSamePublicResponseForIneligibleAccounts(t *testing.T) {
	eng, handler := securityServer(t)
	eng.GetConfig().AccountEmailEnabled = true
	for _, user := range []*types.User{
		{ID: "unverified", Username: "unverified", Email: "user@example.test", Enabled: true},
		{ID: "disabled", Username: "disabled", Email: "user@example.test", VerifiedEmail: "user@example.test", Enabled: false},
	} {
		if err := eng.SaveUserToStorage(user); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"missing", "unverified", "disabled"} {
		body, _ := json.Marshal(map[string]string{"username": name, "email": "user@example.test"})
		r := httptest.NewRequest(http.MethodPost, "/api/user/forgot_password", strings.NewReader(string(body)))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var got map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got["code"] != float64(0) || got["description"] != recoveryRequestMessage {
			t.Fatalf("non-generic response: %#v", got)
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Referrer-Policy") != "no-referrer" {
			t.Fatal("missing privacy headers")
		}
	}
}

func TestPasswordRecoveryRejectsAmbiguousAndOversizedRequests(t *testing.T) {
	eng, handler := securityServer(t)
	eng.GetConfig().AccountEmailEnabled = true
	for _, body := range []string{
		`{"username":"user","username":"other","email":"user@example.test"}`,
		`{"username":"user","email":"user@example.test","admin":true}`,
		`{"username":"user","email":"user@example.test"} {}`,
		`null`, strings.Repeat("x", 4097),
	} {
		if securityRequest(t, handler, "/api/user/forgot_password", body, "")["code"] == float64(0) {
			t.Fatal("accepted malformed recovery request")
		}
	}
	for _, passwords := range [][2]string{{"short", "short"}, {"a-long-password", "different-password"}, {strings.Repeat("界", 25), strings.Repeat("界", 25)}} {
		body, _ := json.Marshal(map[string]string{"user_id": "user", "token": strings.Repeat("a", 64), "new_password": passwords[0], "confirm_password": passwords[1]})
		got := securityRequest(t, handler, "/api/user/reset_password", string(body), "")
		if got["description"] != "Passwords must match and contain 12-72 UTF-8 bytes" {
			t.Fatalf("password validation failed: %#v", got)
		}
	}
}

func TestPasswordRecoveryAcceptsExistingShortUsername(t *testing.T) {
	eng, handler := securityServer(t)
	eng.GetConfig().AccountEmailEnabled = true
	user := &types.User{ID: "short-user", Username: "u", Email: "short@example.test", VerifiedEmail: "short@example.test", Enabled: true}
	if err := eng.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	got := securityRequest(t, handler, "/api/user/forgot_password", `{"username":"u","email":"short@example.test"}`, "")
	if got["code"] != float64(0) {
		t.Fatal("existing login name rejected")
	}
	current, err := eng.LoadUserFromStorage(user.ID)
	if err != nil || len(current.AccountActions) != 1 {
		t.Fatal("no recovery action for existing short username")
	}
}
