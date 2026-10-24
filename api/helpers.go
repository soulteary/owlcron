package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/soulteary/owlcron/types"
)

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) loadPlugins() ([]*types.Plugin, error) {
	storage := s.engine.GetStorage()
	if storage == nil {
		return []*types.Plugin{}, nil
	}
	items, err := storage.ListGet(context.Background(), "plugins", 0, 10000)
	if err != nil {
		return []*types.Plugin{}, err
	}
	result := make([]*types.Plugin, 0, len(items))
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			continue
		}
		var plugin types.Plugin
		if err := json.Unmarshal(raw, &plugin); err != nil {
			continue
		}
		result = append(result, &plugin)
	}
	return result, nil
}

// loadScheduleData builds the full schedule payload that the frontend
// expects on login / session resume (events, categories, plugins,
// server_groups, state, epoch).
func (s *Server) loadScheduleData() map[string]interface{} {
	eventsMap := s.engine.GetScheduler().GetAllEvents()
	events := make([]interface{}, 0, len(eventsMap))
	for _, event := range eventsMap {
		events = append(events, event)
	}

	plugins, _ := s.loadPlugins()

	categoriesMap, _ := s.engine.LoadCategoriesFromStorage()
	categories := make([]interface{}, 0, len(categoriesMap))
	for _, cat := range categoriesMap {
		categories = append(categories, cat)
	}

	serverGroupsMap, _ := s.engine.LoadServerGroupsFromStorage()
	serverGroups := make([]interface{}, 0, len(serverGroupsMap))
	for _, group := range serverGroupsMap {
		serverGroups = append(serverGroups, group)
	}

	return map[string]interface{}{
		"schedule":      events,
		"categories":    categories,
		"plugins":       plugins,
		"server_groups": serverGroups,
		"state":         s.engine.GetState(),
		"epoch":         time.Now().Unix(),
	}
}
