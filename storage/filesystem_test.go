package storage

import (
	"context"
	"os"
	"testing"
)

func TestFilesystemStorage(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "cronicle_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &StorageConfig{
		Engine:  "Filesystem",
		BaseDir: tmpDir,
	}

	storage, err := NewFilesystemStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	ctx := context.Background()

	// 测试 Put 和 Get
	key := "test/key"
	data := []byte("test data")

	if err := storage.Put(ctx, key, data); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	retrieved, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(retrieved) != string(data) {
		t.Errorf("Expected %s, got %s", string(data), string(retrieved))
	}

	// 测试 Exists
	exists, err := storage.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Failed to check exists: %v", err)
	}
	if !exists {
		t.Error("Key should exist")
	}

	// 测试 Delete
	if err := storage.Delete(ctx, key); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	exists, _ = storage.Exists(ctx, key)
	if exists {
		t.Error("Key should not exist after delete")
	}
}

func TestFilesystemStorageList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cronicle_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &StorageConfig{
		Engine:  "Filesystem",
		BaseDir: tmpDir,
	}

	storage, _ := NewFilesystemStorage(config)
	ctx := context.Background()

	// 测试 ListPush
	key := "test/list"
	item1 := map[string]interface{}{"id": "1", "name": "test1"}
	item2 := map[string]interface{}{"id": "2", "name": "test2"}

	if err := storage.ListPush(ctx, key, item1); err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	if err := storage.ListPush(ctx, key, item2); err != nil {
		t.Fatalf("Failed to push: %v", err)
	}

	// 测试 ListGet
	items, err := storage.ListGet(ctx, key, 0, 10)
	if err != nil {
		t.Fatalf("Failed to get list: %v", err)
	}

	if len(items) != 2 {
		t.Errorf("Expected 2 items, got %d", len(items))
	}

	// 测试 ListShift
	shifted, err := storage.ListShift(ctx, key)
	if err != nil {
		t.Fatalf("Failed to shift: %v", err)
	}

	if shifted == nil {
		t.Error("Shifted item should not be nil")
	}

	// 验证列表长度
	items, _ = storage.ListGet(ctx, key, 0, 10)
	if len(items) != 1 {
		t.Errorf("Expected 1 item after shift, got %d", len(items))
	}
}
