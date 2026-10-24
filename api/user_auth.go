package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
)

const sessionExpireDays = 30

func generateSessionID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (s *Server) handleUserLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	clientIP := s.clientIP(r)
	if retry := s.loginLimiter.allowIP(clientIP); retry > 0 {
		sendLoginLimited(w, retry)
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	// Keep oversized or malformed requests outside expensive password work.
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body")
		return
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		s.sendError(w, 1, "Invalid request body")
		return
	}
	if req.Username == "" || len(req.Username) > 50 || req.Password == "" || len(req.Password) > 72 {
		s.sendError(w, 1, "Invalid username or password")
		return
	}
	release, retry := s.loginLimiter.acquire(clientIP, req.Username)
	if retry > 0 {
		sendLoginLimited(w, retry)
		return
	}
	defer release()
	if r.Context().Err() != nil {
		return
	}
	user, err := s.engine.LoadUserByUsernameFromStorage(req.Username)
	valid := false
	if err == nil && user != nil && user.Password != "" {
		valid = s.engine.VerifyUserPassword(user, req.Password)
	} else {
		// A valid cost-10 hash keeps unknown accounts on the same bounded
		// password-work path without exposing whether an account exists.
		dummyPasswordCheck(req.Password)
	}
	release()
	if !valid || user == nil || !user.Enabled {
		s.sendError(w, 1, "Invalid username or password")
		return
	}
	if r.Context().Err() != nil {
		return
	}

	sessionID, err := generateSessionID()
	if err != nil {
		s.sendError(w, 1, "Failed to create session")
		return
	}

	session := &types.Session{
		ID:      sessionID,
		UserID:  user.ID,
		Expires: time.Now().Add(time.Duration(sessionExpireDays) * 24 * time.Hour).Unix(),
		Data:    map[string]interface{}{"password_revision": engine.PasswordRevision(user)},
	}

	if err := s.engine.SaveSessionToStorage(session); err != nil {
		s.sendError(w, 1, "Failed to save session")
		return
	}

	s.setSessionCookie(w, r, sessionID)

	s.engine.LogActivity("user_login", map[string]interface{}{
		"username": user.Username,
		"user_id":  user.ID,
	})

	resp := s.loadScheduleData()
	resp["username"] = user.Username
	resp["session_id"] = sessionID
	resp["user"] = user.SafeMap()
	s.sendResponse(w, 0, resp, "")
}

func (s *Server) handleUserLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	sessionID := getSessionID(r)
	if sessionID == "" {
		var req struct {
			SessionID string `json:"session_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		sessionID = req.SessionID
	}

	if sessionID != "" {
		_ = s.engine.DeleteSessionFromStorage(sessionID)
	}

	s.setSessionCookie(w, r, "")

	s.sendResponse(w, 0, nil, "")
}

func (s *Server) handleResumeSession(w http.ResponseWriter, r *http.Request) {
	sessionID := getSessionID(r)
	if sessionID == "" {
		var req struct {
			SessionID string `json:"session_id"`
		}
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&req)
			sessionID = req.SessionID
		}
		if sessionID == "" {
			sessionID = r.URL.Query().Get("session_id")
		}
	}

	if sessionID == "" {
		s.sendError(w, 1, "No session ID provided")
		return
	}

	session, err := s.engine.LoadSessionFromStorage(sessionID)
	if err != nil {
		s.sendError(w, 1, "Invalid or expired session")
		return
	}

	user, err := s.engine.ValidateUserSession(session)
	if err != nil {
		s.sendError(w, 1, "Invalid or expired session")
		return
	}

	resp := s.loadScheduleData()
	resp["username"] = user.Username
	resp["session_id"] = sessionID
	resp["user"] = user.SafeMap()
	s.sendResponse(w, 0, resp, "")
}

func (s *Server) handleUserCreate(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var req struct {
		Username   string           `json:"username"`
		Password   string           `json:"password"`
		Email      string           `json:"email"`
		Privileges types.Privileges `json:"privileges"`
		FullName   string           `json:"full_name"`
		Active     *types.FlexBool  `json:"active"`
		Enabled    *types.FlexBool  `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	if req.Username == "" || req.Password == "" {
		s.sendError(w, 1, "Username and password are required")
		return
	}

	if !isValidUsername(req.Username) {
		s.sendError(w, 1, "Invalid username")
		return
	}
	if len(req.Password) < 12 || len(req.Password) > 72 {
		s.sendError(w, 1, "Password must contain 12-72 bytes")
		return
	}
	if existing, _ := s.engine.LoadUserByUsernameFromStorage(req.Username); existing != nil {
		s.sendError(w, 1, "Username already exists")
		return
	}

	hashedPassword, err := s.engine.HashPassword(req.Password)
	if err != nil {
		s.sendError(w, 1, "Failed to hash password")
		return
	}

	userID, err := generateSessionID()
	if err != nil {
		s.sendError(w, 1, "Failed to generate user ID")
		return
	}
	userID = userID[:16]

	privs := req.Privileges
	if privs == nil {
		privs = map[string]bool{}
	}

	user := &types.User{
		ID:         userID,
		Username:   req.Username,
		Email:      req.Email,
		Password:   hashedPassword,
		Enabled:    true,
		Privileges: privs,
	}

	user.Data = map[string]interface{}{"full_name": req.FullName}
	if req.Enabled != nil {
		user.Enabled = req.Enabled.Bool()
	} else if req.Active != nil {
		user.Enabled = req.Active.Bool()
	}
	if err := ValidateUser(user); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if err := s.engine.SaveUserToStorage(user); err != nil {
		s.sendError(w, 1, "Failed to save user: "+err.Error())
		return
	}

	s.engine.LogActivity("user_create", map[string]interface{}{
		"username": user.Username,
		"user_id":  user.ID,
	})

	s.sendResponse(w, 0, map[string]interface{}{
		"id":       user.ID,
		"username": user.Username,
	}, "")
}
