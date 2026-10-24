package api

import (
	"encoding/json"
	"github.com/soulteary/owlcron/types"
	"net/http"
	"time"
)

func (s *Server) handleGetMasterState(w http.ResponseWriter, r *http.Request) {
	s.sendResponse(w, 0, map[string]interface{}{
		"state": s.engine.GetState(),
		"multi": s.engine.GetMulti(),
	}, "")
}

func (s *Server) handleUpdateMasterState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		Enabled *types.FlexBool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if req.Enabled != nil {
		s.engine.UpdateMasterState(req.Enabled.Bool())
	}
	s.engine.LogActivity("master_state_update", map[string]interface{}{
		"enabled": s.engine.GetState().Enabled,
		"time":    time.Now().Unix(),
	})
	s.sendResponse(w, 0, map[string]interface{}{"state": s.engine.GetState()}, "")
}

func (s *Server) handleGetActivity(w http.ResponseWriter, r *http.Request) {
	offset, limit := parseOffsetLimit(r, 0, 100)
	activity, _ := s.engine.GetActivity(offset, limit)
	s.sendResponse(w, 0, map[string]interface{}{"rows": activity, "list": map[string]int{"length": len(activity)}}, "")
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	multi := s.engine.GetMulti()
	hostname := s.engine.Hostname()

	if multi.MasterHostname == "" {
		resp := map[string]interface{}{
			"code":        "master",
			"description": "No master server found",
		}
		s.sendJSCallback(w, "app.receiveConfig", resp)
		return
	}

	resp := map[string]interface{}{
		"code":    0,
		"version": "0.9.102",
		"config": map[string]interface{}{
			"name":               "Owlcron",
			"debug":              0,
			"job_memory_max":     s.engine.GetConfig().JobMemoryMax,
			"base_api_uri":       s.engine.GetConfig().BasePath + "/api",
			"base_path":          s.engine.GetConfig().BasePath,
			"default_privileges": map[string]int{"admin": 0},
			"privilege_list": []map[string]string{
				{"id": "admin", "title": "Administrator"},
				{"id": "create_events", "title": "Create Events"},
				{"id": "edit_events", "title": "Edit Events"},
				{"id": "delete_events", "title": "Delete Events"},
				{"id": "run_events", "title": "Run Events"},
				{"id": "abort_events", "title": "Abort Events"},
				{"id": "state_update", "title": "Toggle Scheduler"},
				{"id": "view_metrics", "title": "View Operational Metrics"},
				{"id": "create_categories", "title": "Create Categories"},
				{"id": "update_categories", "title": "Update Categories"},
				{"id": "delete_categories", "title": "Delete Categories"},
			},
			"free_accounts":            0,
			"external_users":           0,
			"external_user_api":        "",
			"web_socket_use_hostnames": 0,
			"web_direct_connect":       0,
			"socket_io_transports":     0,
		},
		"port":            s.engine.GetConfig().HTTPPort,
		"master_hostname": multi.MasterHostname,
	}

	if multi.Master {
		resp["servers"] = map[string]interface{}{
			hostname: map[string]interface{}{
				"hostname": hostname,
				"ip":       multi.MasterIP,
			},
		}
	}

	s.sendJSCallback(w, "app.receiveConfig", resp)
}
