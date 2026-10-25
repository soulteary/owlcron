package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Storage 存储接口
type Storage interface {
	// 基础操作
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)

	// 列表操作
	ListGet(ctx context.Context, key string, offset, limit int) ([]interface{}, error)
	ListPush(ctx context.Context, key string, item interface{}) error
	ListUnshift(ctx context.Context, key string, item interface{}) error
	ListShift(ctx context.Context, key string) (interface{}, error)
	ListPop(ctx context.Context, key string) (interface{}, error)
	ListGetInfo(ctx context.Context, key string) (*ListInfo, error)
	ListDelete(ctx context.Context, key string, recursive bool) error

	// 流操作
	PutStream(ctx context.Context, key string, data []byte) error
	GetStream(ctx context.Context, key string) ([]byte, error)

	// 过期时间
	Expire(ctx context.Context, key string, timestamp int64) error

	// 事务
	BeginTransaction(ctx context.Context) (Transaction, error)
}

// ListInfo 列表信息
type ListInfo struct {
	Length int64         `json:"length"`
	Items  []interface{} `json:"items"`
}

// Transaction 事务接口
type Transaction interface {
	Get(key string) ([]byte, error)
	Put(key string, data []byte) error
	Delete(key string) error
	Commit() error
	Rollback() error
}

// StorageConfig 存储配置
type StorageConfig struct {
	Engine  string                 `json:"engine"` // Filesystem, Couchbase, S3
	BaseDir string                 `json:"base_dir,omitempty"`
	Options map[string]interface{} `json:"options,omitempty"`
}

// NewStorage 创建存储实例
func NewStorage(config *StorageConfig) (Storage, error) {
	if config == nil {
		return nil, fmt.Errorf("storage config is required")
	}
	switch config.Engine {
	case "Filesystem":
		return NewFilesystemStorage(config)
	case "Couchbase":
		return NewCouchbaseStorage(config)
	case "S3":
		return NewS3Storage(config)
	default:
		return nil, errors.New("unknown storage engine: " + config.Engine)
	}
}

// AtomicStorage serializes read/modify/write across storage instances. Remote
// stores implement this using server-side compare-and-swap, not a local mutex.
// Missing or expired records are passed to the callback as nil. A callback may
// run again after a conflict and must not perform external side effects. A
// callback error aborts the update; nil bytes with nil error write an empty
// value. Only a nil Update return confirms persistence: an error may follow a
// committed write if its acknowledgment is lost.
type AtomicStorage interface {
	Update(context.Context, string, func([]byte) ([]byte, error)) error
}

func UpdateList(ctx context.Context, store Storage, key string, fn func([]interface{}) ([]interface{}, error)) error {
	atomic, ok := store.(AtomicStorage)
	if !ok {
		return ErrUnsupported
	}
	return atomic.Update(ctx, key, func(data []byte) ([]byte, error) {
		items := []interface{}{}
		if len(data) > 0 {
			if err := json.Unmarshal(data, &items); err != nil {
				return nil, err
			}
		}
		items, err := fn(items)
		if err != nil {
			return nil, err
		}
		return json.Marshal(items)
	})
}

var ErrUnsupported = errors.New("operation is not supported by this storage backend")
var ErrConflict = errors.New("concurrent storage modification")

// EncodeValue 编码值
func EncodeValue(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// DecodeValue 解码值
func DecodeValue(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
