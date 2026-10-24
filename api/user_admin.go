package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/soulteary/owlcron/types"
)

type userEditRequest struct {
	Username    string           `json:"username"`
	Password    string           `json:"password"`
	OldPassword string           `json:"old_password"`
	NewPassword string           `json:"new_password"`
	Email       *string          `json:"email"`
	FullName    *string          `json:"full_name"`
	Enabled     *types.FlexBool  `json:"enabled"`
	Active      *types.FlexBool  `json:"active"`
	Privileges  types.Privileges `json:"privileges"`
}

func (s *Server) decodeUserEdit(w http.ResponseWriter, r *http.Request) (userEditRequest, bool) {
	var req userEditRequest
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return req, false
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body")
		return req, false
	}
	return req, true
}
func (s *Server) handleAdminGetUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.engine.ListUsersFromStorage()
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	rows := []interface{}{}
	for _, user := range users {
		rows = append(rows, user.SafeMap())
	}
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	offset, limit := intParam(params, "offset", 0), intParam(params, "limit", 50)
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 10000 {
		limit = 50
	}
	if offset > len(rows) {
		offset = len(rows)
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	s.sendResponse(w, 0, map[string]interface{}{"rows": rows[offset:end], "list": map[string]int{"length": len(rows)}}, "")
}
func intParam(params map[string]interface{}, key string, def int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case string:
		var n int
		if _, err := fmt.Sscan(v, &n); err == nil {
			return n
		}
	}
	return def
}
func (s *Server) handleAdminGetUser(w http.ResponseWriter, r *http.Request) {
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	username, _ := params["username"].(string)
	user, err := s.engine.LoadUserByUsernameFromStorage(username)
	if err != nil {
		s.sendError(w, 1, "User not found")
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"user": user.SafeMap()}, "")
}
func (s *Server) handleAdminUpdateUser(w http.ResponseWriter, r *http.Request) {
	s.updateUser(w, r, true)
}
func (s *Server) handleUpdateOwnUser(w http.ResponseWriter, r *http.Request) {
	s.updateUser(w, r, false)
}
func (s *Server) updateUser(w http.ResponseWriter, r *http.Request, admin bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.decodeUserEdit(w, r)
	if !ok {
		return
	}
	auth := GetAuthContext(r)
	if !admin {
		if auth == nil || auth.User == nil || req.Username != auth.User.Username {
			s.sendError(w, 1, "Own user account required")
			return
		}
		if req.Privileges != nil || req.Enabled != nil || req.Active != nil {
			s.sendError(w, 1, "Cannot change account permissions")
			return
		}
	}
	user, err := s.engine.LoadUserByUsernameFromStorage(req.Username)
	if err != nil {
		s.sendError(w, 1, "User not found")
		return
	}
	beforeRaw, err := json.Marshal(user)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	var expected types.User
	if err := json.Unmarshal(beforeRaw, &expected); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if !admin && !s.engine.VerifyUserPassword(user, req.OldPassword) {
		s.sendError(w, 1, "Invalid current password")
		return
	}
	enabled := user.Enabled
	if req.Enabled != nil {
		enabled = req.Enabled.Bool()
	} else if req.Active != nil {
		enabled = req.Active.Bool()
	}
	privs := user.Privileges
	if admin && req.Privileges != nil {
		privs = req.Privileges
	}
	if user.Enabled && user.Privileges["admin"] && (!enabled || !privs["admin"]) {
		if !s.hasOtherAdmin(user.ID) {
			s.sendError(w, 1, "Cannot remove the last enabled administrator")
			return
		}
	}
	if req.NewPassword != "" {
		if len(req.NewPassword) < 12 || len(req.NewPassword) > 72 {
			s.sendError(w, 1, "Password must contain 12-72 bytes")
			return
		}
		user.Password, err = s.engine.HashPassword(req.NewPassword)
		if err != nil {
			s.sendError(w, 1, err.Error())
			return
		}
	}
	if req.Email != nil && s.engine.GetConfig().AccountEmailEnabled && strings.TrimSpace(*req.Email) != user.Email {
		s.sendError(w, 1, "Use email verification to change the email address")
		return
	}
	if req.Email != nil {
		user.Email = strings.TrimSpace(*req.Email)
	}
	if req.FullName != nil {
		if user.Data == nil {
			user.Data = map[string]interface{}{}
		}
		user.Data["full_name"] = strings.TrimSpace(*req.FullName)
	}
	user.Enabled = enabled
	user.Privileges = privs
	if err = ValidateUser(user); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if admin && !s.engine.EnsureLeadership() {
		s.sendError(w, 1, "Current master server required")
		return
	}
	if err = s.engine.ChangeUserInStorage(&expected, user); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	s.engine.LogActivity("user_update", map[string]interface{}{"username": user.Username})
	s.sendResponse(w, 0, map[string]interface{}{"user": user.SafeMap()}, "")
}
func (s *Server) hasOtherAdmin(id string) bool {
	users, err := s.engine.ListUsersFromStorage()
	if err != nil {
		return false
	}
	for _, u := range users {
		if u.ID != id && u.Enabled && u.Privileges["admin"] {
			return true
		}
	}
	return false
}
func (s *Server) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	s.deleteUser(w, r, true)
}
func (s *Server) handleDeleteOwnUser(w http.ResponseWriter, r *http.Request) {
	s.deleteUser(w, r, false)
}
func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request, admin bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.decodeUserEdit(w, r)
	if !ok {
		return
	}
	auth := GetAuthContext(r)
	user, err := s.engine.LoadUserByUsernameFromStorage(req.Username)
	if err != nil {
		s.sendError(w, 1, "User not found")
		return
	}
	if !admin {
		if auth == nil || auth.User == nil || auth.User.ID != user.ID || !s.engine.VerifyUserPassword(user, req.Password) {
			s.sendError(w, 1, "Invalid account credentials")
			return
		}
		if user.Privileges["admin"] && !s.engine.EnsureLeadership() {
			s.sendError(w, 1, "Current master server required for administrator deletion")
			return
		}
	}
	if user.Enabled && user.Privileges["admin"] && !s.hasOtherAdmin(user.ID) {
		s.sendError(w, 1, "Cannot delete the last enabled administrator")
		return
	}
	if (admin || user.Privileges["admin"]) && !s.engine.EnsureLeadership() {
		s.sendError(w, 1, "Current master server required")
		return
	}
	if err = s.engine.ChangeUserInStorage(user, nil); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	s.engine.LogActivity("user_delete", map[string]interface{}{"username": user.Username})
	s.sendResponse(w, 0, nil, "")
}
