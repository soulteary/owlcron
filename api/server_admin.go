package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func (s *Server) handleCheckAddServer(w http.ResponseWriter, r *http.Request) {
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	hostname, _ := params["hostname"].(string)
	if hostname == "" {
		s.sendError(w, 1, "Missing hostname")
		return
	}
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		s.sendError(w, 1, "Cannot resolve hostname: "+err.Error())
		return
	}
	ip := ""
	if len(addrs) > 0 {
		ip = addrs[0]
	}
	s.sendResponse(w, 0, map[string]interface{}{
		"hostname": hostname,
		"ip":       ip,
	}, "")
}

func (s *Server) handleAddServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		Hostname string `json:"hostname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if req.Hostname == "" {
		s.sendError(w, 1, "Missing hostname")
		return
	}
	config := s.engine.GetConfig()
	if (config.StorageEngine == "" || config.StorageEngine == "Filesystem") && !config.SharedStorage && req.Hostname != s.engine.Hostname() {
		s.sendError(w, 1, "Adding workers requires shared_storage and a common storage namespace")
		return
	}

	addrs, _ := net.LookupHost(req.Hostname)
	ip := ""
	if len(addrs) > 0 {
		ip = addrs[0]
	}

	server := &types.Server{
		Hostname: req.Hostname,
		IP:       ip,
	}
	comm := s.engine.GetCommunication()
	previous, previousErr := s.engine.GetStorage().Get(context.Background(), "servers:"+req.Hostname)
	if previousErr != nil && previousErr != storage.ErrNotFound {
		s.sendError(w, 1, "Cannot read server definition: "+previousErr.Error())
		return
	}
	if err := comm.SaveServerToStorage(server); err != nil {
		s.sendError(w, 1, "Failed to save server: "+err.Error())
		return
	}
	if err := comm.ConnectToSlave(req.Hostname, ip); err != nil {
		comm.DisconnectSlave(req.Hostname)
		if previousErr == nil {
			var prior types.Server
			if json.Unmarshal(previous, &prior) == nil {
				_ = comm.SaveServerToStorage(&prior)
			}
		} else {
			_ = comm.DeleteServerFromStorage(req.Hostname)
		}
		s.sendError(w, 1, "Failed to authenticate/connect worker: "+err.Error())
		return
	}

	s.engine.LogActivity("server_add", map[string]interface{}{
		"hostname": req.Hostname,
		"time":     time.Now().Unix(),
	})
	s.sendResponse(w, 0, server, "")
}

func (s *Server) handleRemoveServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		Hostname string `json:"hostname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if req.Hostname == "" {
		s.sendError(w, 1, "Missing hostname")
		return
	}
	if req.Hostname == s.engine.GetMulti().MasterHostname {
		s.sendError(w, 1, "Cannot remove the current master server")
		return
	}

	comm := s.engine.GetCommunication()
	if err := comm.DeleteServerFromStorage(req.Hostname); err != nil {
		s.sendError(w, 1, "Failed to remove server: "+err.Error())
		return
	}
	comm.DisconnectSlave(req.Hostname)

	s.engine.LogActivity("server_remove", map[string]interface{}{
		"hostname": req.Hostname,
		"time":     time.Now().Unix(),
	})
	s.sendResponse(w, 0, nil, "")
}

func (s *Server) handleRestartServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		Hostname string `json:"hostname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if req.Hostname == "" {
		s.sendError(w, 1, "Missing hostname")
		return
	}
	if req.Hostname != s.engine.Hostname() {
		if err := s.engine.GetCommunication().SendToSlave(req.Hostname, "restart_server", map[string]interface{}{}); err != nil {
			s.sendError(w, 1, err.Error())
			return
		}
	}
	s.engine.LogActivity("server_restart", map[string]interface{}{
		"hostname": req.Hostname,
		"time":     time.Now().Unix(),
	})
	s.sendResponse(w, 0, map[string]interface{}{
		"hostname": req.Hostname,
		"status":   "restart_requested",
	}, "")
	if req.Hostname == s.engine.Hostname() {
		s.engine.RequestRestart()
	}
}

func (s *Server) handleShutdownServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		Hostname string `json:"hostname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if req.Hostname == "" {
		s.sendError(w, 1, "Missing hostname")
		return
	}
	if req.Hostname != s.engine.Hostname() {
		if err := s.engine.GetCommunication().SendToSlave(req.Hostname, "shutdown_server", map[string]interface{}{}); err != nil {
			s.sendError(w, 1, err.Error())
			return
		}
	}
	s.engine.LogActivity("server_shutdown", map[string]interface{}{
		"hostname": req.Hostname,
		"time":     time.Now().Unix(),
	})
	s.sendResponse(w, 0, map[string]interface{}{
		"hostname": req.Hostname,
		"status":   "shutdown_requested",
	}, "")
	if req.Hostname == s.engine.Hostname() {
		s.engine.RequestShutdown()
	}
}
