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

// LoadServersFromStorage 从存储加载服务器列表
func (c *Communication) LoadServersFromStorage() error {
	if c.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 从存储获取所有服务器
	items, err := c.engine.storage.ListGet(ctx, "servers", 0, 10000)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load servers: %w", err)
	}
	// 解析并连接服务器
	for index, item := range items {
		itemData, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("invalid server record %d: %w", index, err)
		}

		var serverData types.Server
		if err := json.Unmarshal(itemData, &serverData); err != nil {
			return fmt.Errorf("invalid server record %d: %w", index, err)
		}

		hostname, ip := serverData.Hostname, serverData.IP

		if hostname == "" {
			return fmt.Errorf("server record %d has no hostname", index)
		}
		if serverData.Disabled {
			continue
		}

		// 如果是本地服务器，跳过
		if hostname == c.engine.Hostname() {
			continue
		}

		// 连接到从服务器
		if err := c.ConnectToSlave(hostname, ip); err != nil {
			c.logger.Warn("Failed to connect to slave", zap.String("hostname", hostname), zap.Error(err))
		}
	}

	c.logger.Info("Loaded servers from storage", zap.Int("count", len(c.GetAllServers())))
	return nil
}

// SaveServerToStorage 保存服务器定义
func (c *Communication) SaveServerToStorage(server *types.Server) error {
	if c.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	ctx := context.Background()
	raw, err := json.Marshal(server)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("servers:%s", server.Hostname)
	if err := c.engine.storage.Put(ctx, key, raw); err != nil {
		return err
	}
	return storage.UpdateList(ctx, c.engine.storage, "servers", func(items []interface{}) ([]interface{}, error) {
		result := make([]interface{}, 0, len(items)+1)
		for _, item := range items {
			raw, _ := json.Marshal(item)
			var existing types.Server
			if json.Unmarshal(raw, &existing) == nil && existing.Hostname == server.Hostname {
				continue
			}
			result = append(result, item)
		}
		return append(result, server), nil
	})
}

// DeleteServerFromStorage 删除服务器定义
func (c *Communication) DeleteServerFromStorage(hostname string) error {
	if c.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	ctx := context.Background()
	if err := c.engine.storage.Delete(ctx, fmt.Sprintf("servers:%s", hostname)); err != nil {
		return err
	}
	if err := storage.UpdateList(ctx, c.engine.storage, "servers", func(items []interface{}) ([]interface{}, error) {
		result := make([]interface{}, 0, len(items))
		for _, item := range items {
			raw, _ := json.Marshal(item)
			var existing types.Server
			if json.Unmarshal(raw, &existing) == nil && existing.Hostname == hostname {
				continue
			}
			result = append(result, item)
		}
		return result, nil
	}); err != nil {
		return err
	}
	c.DisconnectSlave(hostname)
	return nil
}
