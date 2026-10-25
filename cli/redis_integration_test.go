package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Exercise the actual administrative commands, not just storage.Import/Export.
// Both namespaces are offline and unique to this run.
func TestRedisRealCLIBackupRestore(t *testing.T) {
	addr := os.Getenv("OWLCRON_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set OWLCRON_TEST_REDIS_ADDR for real Redis 8.4+ CLI recovery")
	}
	newConfig := func(suffix string) *types.Config {
		dir := t.TempDir()
		return &types.Config{
			StorageEngine: "Redis", StorageBaseDir: filepath.Join(dir, "data"),
			LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"),
			SecretKey: "redis-cli-test-0123456789abcdef0123456789",
			StorageOptions: map[string]interface{}{
				"addr": addr, "username": os.Getenv("OWLCRON_TEST_REDIS_USERNAME"),
				"password": os.Getenv("OWLCRON_TEST_REDIS_PASSWORD"),
				"prefix":   fmt.Sprintf("redis-cli:%d:%s:", time.Now().UnixNano(), suffix),
			},
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logger := zap.NewNop()
	source := newConfig("source")
	if err := cmdSetup(source, logger); err != nil {
		t.Fatal(err)
	}
	if err := cmdAdmin([]string{"admin", "redis-cli-test-password"}, source, logger); err != nil {
		t.Fatal(err)
	}
	store, err := openStorage(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(store)
	log := []byte("Redis CLI log\x00\xff\n")
	if _, err := storage.JobLogAppend(ctx, store, "finished", 0, log, true, storage.JobLogChunkSize); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "global/state", []byte(`{"cursors":{"event":300},"_lease_owner":"obsolete"}`)); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "backup.json")
	if err := cmdExport([]string{file}, source, logger); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(file); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("CLI export did not create a private backup")
	}
	target := newConfig("target")
	if err := cmdImport([]string{file}, target, logger); err != nil {
		t.Fatal(err)
	}
	restored, err := engine.NewEngine(target, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Shutdown() })
	user, err := restored.LoadUserByUsernameFromStorage("admin")
	if err != nil || !restored.VerifyUserPassword(user, "redis-cli-test-password") {
		t.Fatal("restored Redis administrator cannot authenticate")
	}
	data, _, err := storage.JobLogRead(ctx, restored.GetStorage(), "finished", 0, len(log))
	if err != nil || !bytes.Equal(data, log) {
		t.Fatalf("restored chunked log: %q %v", data, err)
	}
	data, err = restored.GetStorage().Get(ctx, "global/state")
	if err != nil || !bytes.Contains(data, []byte(`"event":300`)) || bytes.Contains(data, []byte("obsolete")) {
		t.Fatalf("restored cursor/owner: %q %v", data, err)
	}
	filesystem := t.TempDir()
	if err := cmdStorageMigrate([]string{filesystem}, source, logger); err != nil {
		t.Fatal(err)
	}
	fs, err := storage.NewFilesystemStorage(&storage.StorageConfig{BaseDir: filesystem})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err = storage.JobLogRead(ctx, fs, "finished", 0, len(log))
	if err != nil || !bytes.Equal(data, log) {
		t.Fatal("Redis-to-Filesystem CLI migration lost chunked log")
	}
}
