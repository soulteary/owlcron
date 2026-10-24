package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	storepkg "github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

// handleListPlugins 处理列出插件
func (s *Server) handleListPlugins(w http.ResponseWriter, r *http.Request) {
	// 从存储加载所有插件
	ctx := context.Background()
	storage := s.engine.GetStorage()
	if storage == nil {
		s.sendError(w, 1, "Storage not initialized")
		return
	}

	items, err := storage.ListGet(ctx, "plugins", 0, 10000)
	if err != nil {
		s.sendError(w, 1, "Failed to load plugins: "+err.Error())
		return
	}

	plugins := make([]*types.Plugin, 0, len(items))
	for _, item := range items {
		itemData, err := json.Marshal(item)
		if err != nil {
			s.sendError(w, 1, "Failed to parse plugins: "+err.Error())
			return
		}

		var plugin types.Plugin
		if err := json.Unmarshal(itemData, &plugin); err != nil {
			s.sendError(w, 1, "Failed to parse plugins: "+err.Error())
			return
		}

		canonical, loadErr := s.engine.GetJobManager().GetPluginFromStorage(plugin.ID)
		if errors.Is(loadErr, storepkg.ErrNotFound) {
			continue
		}
		if loadErr != nil {
			s.sendError(w, 1, "Failed to load plugin: "+loadErr.Error())
			return
		}
		plugins = append(plugins, canonical)
	}

	s.sendResponse(w, 0, recordPage(r, plugins), "")
}
