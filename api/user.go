package api

import (
	"net/http"
)

func (s *Server) handleCheckUserExists(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		s.sendError(w, 1, "Missing username")
		return
	}
	_, err := s.engine.LoadUserByUsernameFromStorage(username)
	exists := err == nil
	s.sendResponse(w, 0, map[string]interface{}{
		"username": username,
		"exists":   exists,
	}, "")
}
