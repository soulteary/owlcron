package api

import (
	"net/http"
	"strconv"
)

func (s *Server) handleGetNotifications(w http.ResponseWriter, r *http.Request) {
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, "Invalid request")
		return
	}
	state, _ := params["state"].(string)
	switch state {
	case "", "pending", "inflight", "sent", "dead":
	default:
		s.sendError(w, 1, "Invalid notification state")
		return
	}
	limit := 100
	if text, ok := params["limit"].(string); ok {
		limit, _ = strconv.Atoi(text)
	} else if n, ok := params["limit"].(float64); ok {
		limit = int(n)
	}
	if limit < 1 || limit > 1000 {
		s.sendError(w, 1, "Limit must be between 1 and 1000")
		return
	}
	rows, err := s.engine.ListNotifications(r.Context(), state, limit)
	if err != nil {
		s.sendError(w, 1, "Failed to load notifications")
		return
	}
	// Operational metadata suffices for diagnosis; keep message bodies, recipient
	// addresses, URL credentials and internal claim tokens out of API responses.
	for i := range rows {
		rows[i].Payload = nil
		rows[i].LeaseToken = ""
	}
	s.sendResponse(w, 0, map[string]interface{}{"rows": rows}, "")
}
func (s *Server) handleRetryNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "POST required")
		return
	}
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, "Invalid request")
		return
	}
	id, _ := params["id"].(string)
	if id == "" {
		s.sendError(w, 1, "Missing notification id")
		return
	}
	if err = s.engine.RetryNotification(r.Context(), id); err != nil {
		s.sendError(w, 1, "Notification cannot be retried")
		return
	}
	s.sendResponse(w, 0, nil, "")
}
