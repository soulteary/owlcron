package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func TestEngineStartup(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	config := &types.Config{
		LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"),
		StorageEngine: "Filesystem", StorageBaseDir: filepath.Join(dir, "data"), SecretKey: "test_secret",
	}

	eng, err := NewEngine(config, logger)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	if err := eng.Startup(); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}

	// 等待一下让引擎初始化
	time.Sleep(100 * time.Millisecond)

	// 检查状态
	state := eng.GetState()
	if state == nil {
		t.Error("State should not be nil")
	}

	if !state.Enabled {
		t.Error("State should be enabled by default")
	}

	// 关闭引擎
	if err := eng.Shutdown(); err != nil {
		t.Errorf("Failed to shutdown engine: %v", err)
	}
}

func TestEngineActiveJobs(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	config := &types.Config{
		LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"),
		StorageEngine: "Filesystem", StorageBaseDir: filepath.Join(dir, "data"), SecretKey: "test_secret",
	}

	eng, err := NewEngine(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Startup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := eng.Shutdown(); err != nil {
			t.Error(err)
		}
	})

	// 获取活动任务
	jobs := eng.GetActiveJobs()
	if jobs == nil {
		t.Error("Active jobs should not be nil")
	}

	if len(jobs) != 0 {
		t.Errorf("Expected 0 active jobs, got %d", len(jobs))
	}
}

func TestEngineIsMaster(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	config := &types.Config{
		LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"),
		StorageEngine: "Filesystem", StorageBaseDir: filepath.Join(dir, "data"), SecretKey: "test_secret",
	}

	eng, err := NewEngine(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Startup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := eng.Shutdown(); err != nil {
			t.Error(err)
		}
	})

	// 当没有服务器列表时，eligible 为 true，应自动成为 master
	if !eng.IsMaster() {
		t.Error("Engine should be master when eligible and no server list exists")
	}
}
