package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// LoadEventsFromStorage 从存储加载所有事件
func (s *Scheduler) LoadEventsFromStorage() error {
	if s.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.repairEventIndex(ctx); err != nil {
		return fmt.Errorf("repair event index: %w", err)
	}

	// Publish only after every page and canonical record has been read successfully.
	// The index is paged by raw entry count, including stale or duplicate entries.
	const pageSize = 1000
	loadedEvents := make(map[string]*types.Event)
	for offset := 0; ; {
		items, err := s.engine.storage.ListGet(ctx, "events", offset, pageSize)
		if err != nil {
			return fmt.Errorf("load events at offset %d: %w", offset, err)
		}
		for i, item := range items {
			itemData, err := json.Marshal(item)
			if err != nil {
				return fmt.Errorf("marshal event index entry %d: %w", offset+i, err)
			}
			var entry struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(itemData, &entry); err != nil {
				return fmt.Errorf("decode event index entry %d: %w", offset+i, err)
			}
			if entry.ID == "" {
				return fmt.Errorf("event index entry %d has no ID", offset+i)
			}

			canonical, err := s.engine.storage.Get(ctx, "events:"+entry.ID)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("load event %q: %w", entry.ID, err)
			}
			var event types.Event
			if err := json.Unmarshal(canonical, &event); err != nil {
				return fmt.Errorf("decode event %q: %w", entry.ID, err)
			}
			if event.ID != entry.ID {
				return fmt.Errorf("event %q canonical ID mismatch: %q", entry.ID, event.ID)
			}
			loadedEvents[event.ID] = types.CloneEvent(&event)
			s.logger.Debug("Loaded event from storage", zap.String("event_id", event.ID))
		}
		if len(items) < pageSize {
			break
		}
		offset += len(items)
	}

	s.events = loadedEvents
	s.logger.Info("Loaded events from storage", zap.Int("count", len(s.events)))
	return nil
}

// SaveEventToStorage 保存事件到存储
func (s *Scheduler) SaveEventToStorage(event *types.Event) error {
	if s.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 序列化事件
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// 保存到存储
	key := fmt.Sprintf("events:%s", event.ID)
	if err := s.engine.storage.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to save event to storage: %w", err)
	}

	// 添加到事件列表
	eventListKey := "events"
	if err := upsertStorageList(ctx, s.engine.storage, eventListKey, event.ID, event, false); err != nil {
		return fmt.Errorf("save event index: %w", err)
	}

	s.logger.Debug("Saved event to storage", zap.String("event_id", event.ID))
	return nil
}

// DeleteEventFromStorage 从存储删除事件
func (s *Scheduler) DeleteEventFromStorage(eventID string) error {
	if s.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 删除事件数据
	key := fmt.Sprintf("events:%s", eventID)
	if err := s.engine.storage.Delete(ctx, key); err != nil {
		return fmt.Errorf("failed to delete event from storage: %w", err)
	}

	if err := removeStorageList(ctx, s.engine.storage, "events", eventID); err != nil {
		return err
	}

	s.logger.Debug("Deleted event from storage", zap.String("event_id", eventID))
	return nil
}

// LoadPluginsFromStorage 从存储加载所有插件
func (jm *JobManager) LoadPluginsFromStorage() error {
	if jm.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 从存储获取所有插件
	plugins, err := jm.engine.storage.ListGet(ctx, "plugins", 0, 10000)
	if err != nil {
		return fmt.Errorf("load plugins: %w", err)
	}

	// 解析并加载插件
	for _, item := range plugins {
		itemData, err := json.Marshal(item)
		if err != nil {
			jm.logger.Warn("Failed to marshal plugin", zap.Error(err))
			continue
		}

		var plugin types.Plugin
		if err := json.Unmarshal(itemData, &plugin); err != nil {
			jm.logger.Warn("Failed to unmarshal plugin", zap.Error(err))
			continue
		}

		jm.logger.Debug("Loaded plugin from storage", zap.String("plugin_id", plugin.ID))
	}

	jm.logger.Info("Loaded plugins from storage", zap.Int("count", len(plugins)))
	return nil
}

// GetPluginFromStorage 从存储获取插件
func (jm *JobManager) GetPluginFromStorage(pluginID string) (*types.Plugin, error) {
	if jm.engine.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	key := fmt.Sprintf("plugins:%s", pluginID)
	data, err := jm.engine.storage.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var plugin types.Plugin
	if err := json.Unmarshal(data, &plugin); err != nil {
		return nil, fmt.Errorf("failed to unmarshal plugin: %w", err)
	}

	return &plugin, nil
}

// SavePluginToStorage 保存插件到存储
func (jm *JobManager) SavePluginToStorage(plugin *types.Plugin) error {
	if jm.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 序列化插件
	data, err := json.Marshal(plugin)
	if err != nil {
		return fmt.Errorf("failed to marshal plugin: %w", err)
	}

	// 保存到存储
	key := fmt.Sprintf("plugins:%s", plugin.ID)
	if err := jm.engine.storage.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to save plugin to storage: %w", err)
	}

	// 添加到插件列表
	pluginListKey := "plugins"
	if err := upsertStorageList(ctx, jm.engine.storage, pluginListKey, plugin.ID, plugin, false); err != nil {
		return fmt.Errorf("save plugin index: %w", err)
	}

	jm.logger.Debug("Saved plugin to storage", zap.String("plugin_id", plugin.ID))
	return nil
}

// DeletePluginFromStorage 从存储删除插件
func (jm *JobManager) DeletePluginFromStorage(pluginID string) error {
	if jm.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 删除插件数据
	key := fmt.Sprintf("plugins:%s", pluginID)
	if err := jm.engine.storage.Delete(ctx, key); err != nil {
		return fmt.Errorf("failed to delete plugin from storage: %w", err)
	}

	if err := removeStorageList(ctx, jm.engine.storage, "plugins", pluginID); err != nil {
		return err
	}
	jm.logger.Debug("Deleted plugin from storage", zap.String("plugin_id", pluginID))
	return nil
}

// LoadCategoriesFromStorage 从存储加载所有分类
func (e *Engine) LoadCategoriesFromStorage() (map[string]*types.Category, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()
	categories := make(map[string]*types.Category)

	// 从存储获取所有分类
	items, err := e.storage.ListGet(ctx, "categories", 0, 10000)
	if err != nil {
		return nil, fmt.Errorf("load categories: %w", err)
	}

	// 解析并加载分类
	for _, item := range items {
		itemData, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}

		var category types.Category
		if err := json.Unmarshal(itemData, &category); err != nil {
			return nil, err
		}

		canonical, loadErr := e.storage.Get(ctx, "categories:"+category.ID)
		if errors.Is(loadErr, storage.ErrNotFound) {
			continue
		}
		if loadErr != nil {
			return nil, loadErr
		}
		if err := json.Unmarshal(canonical, &category); err != nil {
			return nil, err
		}
		categories[category.ID] = &category
	}

	e.logger.Info("Loaded categories from storage", zap.Int("count", len(categories)))
	return categories, nil
}

// SaveCategoryToStorage 保存分类到存储
func (e *Engine) SaveCategoryToStorage(category *types.Category) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 序列化分类
	data, err := json.Marshal(category)
	if err != nil {
		return fmt.Errorf("failed to marshal category: %w", err)
	}

	// 保存到存储
	key := fmt.Sprintf("categories:%s", category.ID)
	if err := e.storage.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to save category to storage: %w", err)
	}

	// 添加到分类列表
	categoryListKey := "categories"
	if err := upsertStorageList(ctx, e.storage, categoryListKey, category.ID, category, false); err != nil {
		return fmt.Errorf("save category index: %w", err)
	}

	e.logger.Debug("Saved category to storage", zap.String("category_id", category.ID))
	return nil
}

// LoadServerGroupsFromStorage 从存储加载所有服务器组
func (e *Engine) LoadServerGroupsFromStorage() (map[string]*types.ServerGroup, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()
	groups := make(map[string]*types.ServerGroup)

	// 从存储获取所有服务器组
	items, err := e.storage.ListGet(ctx, "server_groups", 0, 10000)
	if err != nil {
		return nil, fmt.Errorf("load server groups: %w", err)
	}

	// 解析并加载服务器组
	for _, item := range items {
		itemData, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}

		var group types.ServerGroup
		if err := json.Unmarshal(itemData, &group); err != nil {
			return nil, err
		}

		canonical, loadErr := e.storage.Get(ctx, "server_groups:"+group.ID)
		if errors.Is(loadErr, storage.ErrNotFound) {
			continue
		}
		if loadErr != nil {
			return nil, loadErr
		}
		if err := json.Unmarshal(canonical, &group); err != nil {
			return nil, err
		}
		groups[group.ID] = &group
	}

	e.logger.Info("Loaded server groups from storage", zap.Int("count", len(groups)))
	return groups, nil
}

// SaveServerGroupToStorage 保存服务器组到存储
func (e *Engine) SaveServerGroupToStorage(group *types.ServerGroup) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 序列化服务器组
	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal server group: %w", err)
	}

	// 保存到存储
	key := fmt.Sprintf("server_groups:%s", group.ID)
	if err := e.storage.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to save server group to storage: %w", err)
	}

	// 添加到服务器组列表
	groupListKey := "server_groups"
	if err := upsertStorageList(ctx, e.storage, groupListKey, group.ID, group, false); err != nil {
		return fmt.Errorf("save group index: %w", err)
	}

	e.logger.Debug("Saved server group to storage", zap.String("group_id", group.ID))
	return nil
}

func storageItemID(item interface{}) string {
	data, _ := json.Marshal(item)
	var obj struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(data, &obj)
	return obj.ID
}
func upsertStorageList(ctx context.Context, store storage.Storage, key, id string, value interface{}, prepend bool) error {
	return storage.UpdateList(ctx, store, key, func(items []interface{}) ([]interface{}, error) {
		out := make([]interface{}, 0, len(items)+1)
		for _, item := range items {
			if storageItemID(item) != id {
				out = append(out, item)
			}
		}
		if prepend {
			out = append([]interface{}{value}, out...)
		} else {
			out = append(out, value)
		}
		return out, nil
	})
}
func removeStorageList(ctx context.Context, store storage.Storage, key, id string) error {
	return storage.UpdateList(ctx, store, key, func(items []interface{}) ([]interface{}, error) {
		out := make([]interface{}, 0, len(items))
		for _, item := range items {
			if storageItemID(item) != id {
				out = append(out, item)
			}
		}
		return out, nil
	})
}
