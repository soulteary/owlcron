package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/soulteary/owlcron/types"
)

// handleCreateServerGroup 处理创建服务器组
func (s *Server) handleCreateServerGroup(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var group types.ServerGroup
	if err := json.NewDecoder(r.Body).Decode(&group); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	if group.ID == "" {
		group.ID = generateID()
	}
	// 验证服务器组数据
	if err := ValidateServerGroup(&group); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	exists, err := s.engine.GetStorage().Exists(r.Context(), "server_groups:"+group.ID)
	if err != nil {
		s.sendError(w, 1, "Failed to check server group: "+err.Error())
		return
	}
	if exists {
		s.sendError(w, 1, "Server group already exists")
		return
	}
	// 保存到存储
	if err := s.engine.SaveServerGroupToStorage(&group); err != nil {
		s.sendError(w, 1, "Failed to save server group: "+err.Error())
		return
	}

	s.configurationChanged("server_groups")
	s.sendResponse(w, 0, map[string]interface{}{"id": group.ID}, "")
}

// handleUpdateServerGroup 处理更新服务器组
func (s *Server) handleUpdateServerGroup(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var group types.ServerGroup
	if _, err := s.decodeStoredPatch(r, "server_groups", &group); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	// 验证服务器组数据
	if err := ValidateServerGroup(&group); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	// 更新服务器组到存储
	if err := s.engine.SaveServerGroupToStorage(&group); err != nil {
		s.sendError(w, 1, "Failed to update server group: "+err.Error())
		return
	}

	s.configurationChanged("server_groups")
	s.sendResponse(w, 0, nil, "")
}

// handleDeleteServerGroup 处理删除服务器组
func (s *Server) handleDeleteServerGroup(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

	if s.configurationInUse("server_groups", req.ID) {
		s.sendError(w, 1, "Server group is still assigned to events")
		return
	}
	if err := s.deleteConfigurationRecord(r, "server_groups", req.ID); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	s.configurationChanged("server_groups")

	s.sendResponse(w, 0, nil, "")
}

// handleGetServerGroup 处理获取服务器组
func (s *Server) handleGetServerGroup(w http.ResponseWriter, r *http.Request) {
	groupID := readRequestID(r)
	if groupID == "" {
		s.sendError(w, 1, "Missing server group id")
		return
	}

	// 从存储获取服务器组
	ctx := context.Background()
	storage := s.engine.GetStorage()
	if storage == nil {
		s.sendError(w, 1, "Storage not initialized")
		return
	}
	key := fmt.Sprintf("server_groups:%s", groupID)
	data, err := storage.Get(ctx, key)
	if err != nil {
		s.sendError(w, 1, "Server group not found")
		return
	}

	var group types.ServerGroup
	if err := json.Unmarshal(data, &group); err != nil {
		s.sendError(w, 1, "Failed to parse server group: "+err.Error())
		return
	}

	s.sendResponse(w, 0, map[string]interface{}{"group": &group}, "")
}

// handleListServerGroups 处理列出服务器组
func (s *Server) handleListServerGroups(w http.ResponseWriter, r *http.Request) {
	// 从存储获取所有服务器组
	groupsMap, err := s.engine.LoadServerGroupsFromStorage()
	if err != nil {
		s.sendError(w, 1, "Failed to load server groups: "+err.Error())
		return
	}

	groups := make([]*types.ServerGroup, 0, len(groupsMap))
	for _, group := range groupsMap {
		auth := GetAuthContext(r)
		if auth == nil || (!auth.Privileges["admin"] && auth.Privileges["grp_limit"] && !auth.Privileges["grp_"+group.ID]) {
			continue
		}
		groups = append(groups, group)
	}

	s.sendResponse(w, 0, recordPage(r, groups, func(record *types.ServerGroup) string { return record.ID }), "")
}

// handleListServers 处理列出服务器
func (s *Server) handleListServers(w http.ResponseWriter, r *http.Request) {
	// 从通信管理器获取所有服务器
	serversMap := s.engine.GetCommunication().GetAllServers()

	s.sendResponse(w, 0, map[string]interface{}{"servers": serversMap}, "")
}
