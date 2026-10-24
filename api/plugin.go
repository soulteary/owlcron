package api

import (
	"encoding/json"
	"net/http"

	"github.com/soulteary/owlcron/types"
)

// handleCreatePlugin 处理创建插件
func (s *Server) handleCreatePlugin(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var plugin types.Plugin
	if err := json.NewDecoder(r.Body).Decode(&plugin); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	if plugin.ID == "" {
		plugin.ID = generateID()
	}

	// 验证插件数据
	if err := ValidatePlugin(&plugin); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	exists, err := s.engine.GetStorage().Exists(r.Context(), "plugins:"+plugin.ID)
	if err != nil {
		s.sendError(w, 1, "Failed to check plugin: "+err.Error())
		return
	}
	if exists {
		s.sendError(w, 1, "Plugin already exists")
		return
	}
	// 保存插件到存储
	if err := s.engine.GetJobManager().SavePluginToStorage(&plugin); err != nil {
		s.sendError(w, 1, "Failed to save plugin: "+err.Error())
		return
	}

	s.configurationChanged("plugins")
	s.sendResponse(w, 0, map[string]interface{}{"id": plugin.ID}, "")
}

// handleUpdatePlugin 处理更新插件
func (s *Server) handleUpdatePlugin(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var plugin types.Plugin
	if _, err := s.decodeStoredPatch(r, "plugins", &plugin); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	// 验证插件数据
	if err := ValidatePlugin(&plugin); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	// 检查插件是否存在
	_, err := s.engine.GetJobManager().GetPluginFromStorage(plugin.ID)
	if err != nil {
		s.sendError(w, 1, "Plugin not found")
		return
	}

	// 更新插件到存储
	if err := s.engine.GetJobManager().SavePluginToStorage(&plugin); err != nil {
		s.sendError(w, 1, "Failed to update plugin: "+err.Error())
		return
	}

	s.configurationChanged("plugins")
	s.sendResponse(w, 0, nil, "")
}

// handleDeletePlugin 处理删除插件
func (s *Server) handleDeletePlugin(w http.ResponseWriter, r *http.Request) {
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

	// 检查插件是否存在
	_, err := s.engine.GetJobManager().GetPluginFromStorage(req.ID)
	if err != nil {
		s.sendError(w, 1, "Plugin not found")
		return
	}

	if s.configurationInUse("plugins", req.ID) {
		s.sendError(w, 1, "Plugin is still assigned to events")
		return
	}
	// 从存储删除插件
	if err := s.engine.GetJobManager().DeletePluginFromStorage(req.ID); err != nil {
		s.sendError(w, 1, "Failed to delete plugin: "+err.Error())
		return
	}

	s.configurationChanged("plugins")
	s.sendResponse(w, 0, nil, "")
}

// handleGetPlugin 处理获取插件
func (s *Server) handleGetPlugin(w http.ResponseWriter, r *http.Request) {
	pluginID := readRequestID(r)
	if pluginID == "" {
		s.sendError(w, 1, "Missing plugin id")
		return
	}

	// 从存储获取插件
	plugin, err := s.engine.GetJobManager().GetPluginFromStorage(pluginID)
	if err != nil {
		s.sendError(w, 1, "Plugin not found")
		return
	}

	s.sendResponse(w, 0, map[string]interface{}{"plugin": plugin}, "")
}

// handleListPlugins 处理列出插件
// 实现已移动到 plugin_list.go
