package api

import (
	"encoding/json"
	"net/http"

	"github.com/soulteary/owlcron/types"
)

func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.engine.ListAPIKeysFromStorage()
	if err != nil {
		s.sendError(w, 1, "Failed to list API keys: "+err.Error())
		return
	}
	rows := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, key.SafeMap())
	}
	s.sendResponse(w, 0, map[string]interface{}{"rows": rows, "list": map[string]int{"length": len(rows)}}, "")
}

func (s *Server) handleGetAPIKey(w http.ResponseWriter, r *http.Request) {
	id := readRequestID(r)
	if id == "" {
		s.sendError(w, 1, "Missing api key id")
		return
	}
	key, err := s.engine.LoadAPIKeyFromStorage(id)
	if err != nil {
		s.sendError(w, 1, "API key not found")
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"id": key.ID, "api_key": key.SafeMap(), "key": key.SafeMap()}, "")
}

func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var key types.APIKey
	if err := json.NewDecoder(r.Body).Decode(&key); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if key.ID == "" {
		key.ID = generateID()
	}
	if err := ValidateAPIKey(&key); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if _, err := s.engine.LoadAPIKeyFromStorage(key.ID); err == nil {
		s.sendError(w, 1, "API key already exists")
		return
	}
	if key.Key == "" {
		var err error
		key.Key, err = generateSessionID()
		if err != nil {
			s.sendError(w, 1, "Failed to generate API key")
			return
		}
	}
	if key.Privileges == nil {
		key.Privileges = map[string]bool{}
	}
	if err := s.engine.SaveAPIKeyToStorage(&key); err != nil {
		s.sendError(w, 1, "Failed to save API key: "+err.Error())
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"id": key.ID, "api_key": key.SafeMap(), "key": key.SafeMap()}, "")
}

func (s *Server) handleUpdateAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var key types.APIKey
	if err := json.NewDecoder(r.Body).Decode(&key); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if err := ValidateAPIKey(&key); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	old, err := s.engine.LoadAPIKeyFromStorage(key.ID)
	if err != nil {
		s.sendError(w, 1, "API key not found")
		return
	}
	if key.Key == "" {
		key.Key = old.Key
	}
	if key.Created == 0 {
		key.Created = old.Created
	}
	if err := s.engine.SaveAPIKeyToStorage(&key); err != nil {
		s.sendError(w, 1, "Failed to update API key: "+err.Error())
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"id": key.ID, "api_key": key.SafeMap(), "key": key.SafeMap()}, "")
}

func (s *Server) handleDeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if req.ID == "" {
		s.sendError(w, 1, "Missing api key id")
		return
	}
	if err := s.engine.DeleteAPIKeyFromStorage(req.ID); err != nil {
		s.sendError(w, 1, "Failed to delete API key: "+err.Error())
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"id": req.ID}, "")
}
