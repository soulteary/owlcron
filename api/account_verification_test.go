package api

import (
	"github.com/soulteary/owlcron/types"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccountRequestStrictBody(t *testing.T) {
	for _, body := range []string{`{"email":"a@example.com","admin":true}`, `{"email":"a@example.com"} {}`, `null`, strings.Repeat("x", 5000)} {
		var req struct {
			Email string `json:"email"`
		}
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		w := httptest.NewRecorder()
		if decodeAccountRequest(w, r, &req) {
			t.Errorf("accepted %q", body[:min(len(body), 80)])
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store")
		}
	}
}
func TestAccountAdmissionIndependentLimits(t *testing.T) {
	eng, _ := securityServer(t)
	s := &Server{engine: eng, accountLimiter: newAccountLimiter(&types.Config{AccountEmailRateLimit: 1, AccountEmailIPRateLimit: 10})}
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	release, ok := s.accountRequestAdmission(httptest.NewRecorder(), r, "verify:user")
	if !ok {
		t.Fatal("first request denied")
	}
	release()
	w := httptest.NewRecorder()
	_, ok = s.accountRequestAdmission(w, r, "verify:user")
	if ok || w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Fatal("account limit not enforced")
	}
	release, ok = s.accountRequestAdmission(httptest.NewRecorder(), r, "verify:other")
	if !ok {
		t.Fatal("other account locked")
	}
	release()
}
func TestDisabledVerificationDoesNotMutate(t *testing.T) {
	_, handler := securityServer(t)
	got := securityRequest(t, handler, "/api/user/verify_email", `{"user_id":"unknown","token":"invalid"}`, "")
	if got["code"] == float64(0) {
		t.Fatal("disabled verification accepted")
	}
}

func TestVerifiedEmailCannotBeChangedByProfileSave(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "administrator", types.Privileges{"admin": true})
	eng.GetConfig().AccountEmailEnabled = true
	got := securityRequest(t, handler, "/api/user/admin_update", `{"username":"administrator","email":"replacement@example.com"}`, session)
	if got["code"] == float64(0) {
		t.Fatal("profile update bypassed email verification")
	}
}
