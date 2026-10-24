package api

import (
	"net/http"
	"net/mail"
	"strings"
	"time"
)

const recoveryRequestMessage = "If an enabled account matches this username and verified email address, password reset instructions will be sent."

func (s *Server) handleForgotPassword(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Email    string `json:"email"`
	}
	if !decodeAccountRequest(w, r, &req) {
		s.sendError(w, 1, "Invalid request body")
		return
	}
	if !s.engine.GetConfig().AccountEmailEnabled {
		s.sendError(w, 1, "Email password recovery is unavailable")
		return
	}
	release, ok := s.accountRequestAdmission(w, r, "recovery-request:"+req.Username)
	if !ok {
		return
	}
	defer release()
	req.Email = strings.TrimSpace(req.Email)
	address, err := mail.ParseAddress(req.Email)
	if req.Username == "" || len(req.Username) > 50 || err != nil || address.Address != req.Email || len(req.Email) > 254 {
		s.sendError(w, 1, "Enter a valid username and email address")
		return
	}
	// Keep the ordinary local-storage response path uniform without creating
	// detached work or exposing whether a matching account exists.
	started := time.Now()
	err = s.engine.RequestPasswordRecovery(r.Context(), req.Username, req.Email)
	if err != nil {
		s.logger.Warn("Password recovery request could not be queued")
	}
	if remaining := 150*time.Millisecond - time.Since(started); remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	s.sendResponse(w, 0, nil, recoveryRequestMessage)
}

func (s *Server) handleResetPassword(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID          string `json:"user_id"`
		Token           string `json:"token"`
		NewPassword     string `json:"new_password"`
		ConfirmPassword string `json:"confirm_password"`
	}
	if !decodeAccountRequest(w, r, &req) {
		s.sendError(w, 1, "Invalid request body")
		return
	}
	release, ok := s.accountRequestAdmission(w, r, "recovery-consume:"+req.UserID)
	if !ok {
		return
	}
	defer release()
	if !s.engine.GetConfig().AccountEmailEnabled || len(req.UserID) == 0 || len(req.UserID) > 100 || len(req.Token) != 64 {
		s.sendError(w, 1, "Invalid or expired password reset link")
		return
	}
	if len(req.NewPassword) < 12 || len(req.NewPassword) > 72 || req.NewPassword != req.ConfirmPassword {
		s.sendError(w, 1, "Passwords must match and contain 12-72 UTF-8 bytes")
		return
	}
	if err := s.engine.ConsumePasswordRecovery(r.Context(), req.UserID, req.Token, req.NewPassword); err != nil {
		s.sendError(w, 1, "Invalid or expired password reset link")
		return
	}
	s.setSessionCookie(w, r, "")
	s.sendResponse(w, 0, nil, "Password changed. Sign in with your new password.")
}
