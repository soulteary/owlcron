package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
)

func newAccountLimiter(config *types.Config) *loginLimiter {
	copy := *config
	copy.LoginRateLimit = config.AccountEmailIPRateLimit
	if copy.LoginRateLimit == 0 {
		copy.LoginRateLimit = 20
	}
	copy.LoginAccountRateLimit = config.AccountEmailRateLimit
	if copy.LoginAccountRateLimit == 0 {
		copy.LoginAccountRateLimit = 5
	}
	copy.LoginRateWindowSeconds = config.AccountEmailRateWindowSeconds
	if copy.LoginRateWindowSeconds == 0 {
		copy.LoginRateWindowSeconds = 3600
	}
	return newLoginLimiter(&copy)
}

func (s *Server) accountRequestAdmission(w http.ResponseWriter, r *http.Request, identity string) (func(), bool) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	ip := s.clientIP(r)
	if retry := s.accountLimiter.allowIP(ip); retry > 0 {
		sendAccountLimited(w, retry)
		return nil, false
	}
	release, retry := s.accountLimiter.acquire(ip, identity)
	if retry > 0 {
		sendAccountLimited(w, retry)
		return nil, false
	}
	return release, true
}

func decodeAccountRequest(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method != http.MethodPost {
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return false
	}
	if _, err := decodeRequestParameters(raw); err != nil || len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst) == nil && decoder.Decode(new(interface{})) == io.EOF
}

func (s *Server) handleRequestEmailVerification(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email           string `json:"email"`
		CurrentPassword string `json:"current_password"`
	}
	if !decodeAccountRequest(w, r, &req) {
		s.sendError(w, 1, "Invalid request body")
		return
	}
	auth := GetAuthContext(r)
	if auth == nil || auth.User == nil || !s.engine.GetConfig().AccountEmailEnabled {
		s.sendError(w, 1, "Email verification is unavailable")
		return
	}
	release, ok := s.accountRequestAdmission(w, r, "verify-request:"+auth.User.ID)
	if !ok {
		return
	}
	defer release()
	email := strings.TrimSpace(req.Email)
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email || len(email) > 254 || len(req.CurrentPassword) == 0 || len(req.CurrentPassword) > 72 {
		s.sendError(w, 1, "Invalid email address or current password")
		return
	}
	user, err := s.engine.LoadUserByUsernameFromStorage(auth.User.Username)
	if err != nil || !user.Enabled || !s.engine.VerifyUserPassword(user, req.CurrentPassword) {
		s.sendError(w, 1, "Invalid current password")
		return
	}
	if err := s.engine.IssueEmailVerificationForRevision(r.Context(), user.ID, email, engine.PasswordRevision(user)); err != nil {
		s.sendError(w, 1, "Unable to send verification email. Please retry later.")
		return
	}
	s.sendResponse(w, 0, nil, "Verification email requested. Your current email remains active until verification.")
}

func (s *Server) handleVerifyEmail(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
		Token  string `json:"token"`
	}
	if !decodeAccountRequest(w, r, &req) {
		s.sendError(w, 1, "Invalid request body")
		return
	}
	release, ok := s.accountRequestAdmission(w, r, "verify-consume:"+req.UserID)
	if !ok {
		return
	}
	defer release()
	if !s.engine.GetConfig().AccountEmailEnabled || len(req.UserID) == 0 || len(req.UserID) > 100 || len(req.Token) == 0 || len(req.Token) > 256 {
		s.sendError(w, 1, "Invalid or expired verification link")
		return
	}
	if err := s.engine.ConsumeEmailVerification(r.Context(), req.UserID, req.Token); err != nil {
		s.sendError(w, 1, "Invalid or expired verification link")
		return
	}
	s.sendResponse(w, 0, nil, "Email verified. Sign in again to refresh your account details.")
}

func sendAccountLimited(w http.ResponseWriter, retry time.Duration) {
	seconds := int64((retry + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 1, "description": "Too many account requests. Please retry later."})
}
