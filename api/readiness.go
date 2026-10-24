package api

import (
	"encoding/json"
	"net/http"
)

// Readiness deliberately exposes no backend errors, topology or credentials.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := s.engine.Readiness(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 1, "description": "Service unavailable"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 0})
}
