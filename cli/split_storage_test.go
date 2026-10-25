package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func splitCLIConfig(t *testing.T) *types.Config {
	t.Helper()
	return &types.Config{
		StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), SecretKey: "test-secret-0123456789abcdef0123459",
		BlobStorage:   &types.BlobStorageConfig{WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": {Engine: "Filesystem", BaseDir: t.TempDir()}}},
		JobLogStorage: &types.JobLogStorageConfig{WriteFormat: 2},
	}
}

func TestSplitCLICompleteBackupRestoreAndProtectedMutations(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()
	source := splitCLIConfig(t)
	if _, err := Run([]string{"setup"}, source, logger); err == nil {
		t.Fatal("setup mutated an uninitialized split namespace")
	}
	if _, err := Run([]string{"storage-init"}, source, logger); err == nil {
		t.Fatal("initialization omitted offline acknowledgement")
	}
	if _, err := Run([]string{"storage-init", "--offline"}, source, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := Run([]string{"setup"}, source, logger); err != nil {
		t.Fatal(err)
	}
	meta, err := openStorage(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(meta)
	registry, err := storage.NewBlobRegistry(source.BlobStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	layout, err := storage.LoadStorageLayout(ctx, meta)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := storage.NewJobLogRepository(meta, registry.Stores(), storage.JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("complete content from CLI\n")
	if _, err := repo.Append(ctx, "job", 0, payload, true, storage.JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	if _, err := Run([]string{"storage-preflight", "--offline"}, source, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := Run([]string{"storage-verify"}, source, logger); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "complete.ndjson")
	if _, err := Run([]string{"export", file}, source, logger); err == nil {
		t.Fatal("split export omitted offline acknowledgement")
	}
	if _, err := Run([]string{"export", "--offline", file}, source, logger); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(file); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("archive is not private")
	}
	if err := os.RemoveAll(source.BlobStorage.Stores["logs"].BaseDir); err != nil {
		t.Fatal(err)
	}
	target := splitCLIConfig(t)
	if _, err := Run([]string{"import", "--offline", file}, target, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := Run([]string{"storage-verify"}, target, logger); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{storage.StorageLayoutKey, storage.JobLogManifestKey("job"), "deleted_jobs:job", "split_migration/source/key"} {
		if _, err := Run([]string{"storage-cli", "put", key, file}, target, logger); err == nil {
			t.Fatalf("raw mutation accepted: %s", key)
		}
		if _, err := Run([]string{"storage-cli", "delete", key}, target, logger); err == nil {
			t.Fatalf("raw delete accepted: %s", key)
		}
	}
	if _, err := Run([]string{"import", "--offline", file}, target, logger); err == nil {
		t.Fatal("second restore reused namespace")
	}
}

func TestSplitCLIOfflineMigration(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()
	source := &types.Config{StorageBaseDir: t.TempDir()}
	meta, err := openStorage(source)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(meta)
	payload := bytes.Repeat([]byte("migrate\n"), 10000)
	for at := 0; at < len(payload); {
		end := min(at+storage.JobLogChunkSize, len(payload))
		if _, err := storage.JobLogAppend(ctx, meta, "job", int64(at), payload[at:end], end == len(payload), storage.JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
		at = end
	}
	target := splitCLIConfig(t)
	configPath := filepath.Join(t.TempDir(), "target.json")
	raw, _ := json.Marshal(target)
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Run([]string{"storage-migrate", "--target-config", configPath}, source, logger); err == nil {
		t.Fatal("migration omitted offline acknowledgement")
	}
	if _, err := Run([]string{"storage-migrate", "--offline", "--target-config", configPath}, source, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := Run([]string{"storage-verify"}, target, logger); err != nil {
		t.Fatal(err)
	}
	if got, _, err := storage.JobLogRead(ctx, meta, "job", 0, len(payload)); err != nil || !bytes.Equal(got, payload) {
		t.Fatal("migration changed source")
	}
}

func TestSplitCLIRejectsMutationDuringFailedRestore(t *testing.T) {
	ctx := context.Background()
	config := splitCLIConfig(t)
	meta, err := openStorage(config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(meta)
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if _, err := storage.InitializeStorageLayout(ctx, meta, registry, "restoring"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "value.txt")
	if err := os.WriteFile(file, []byte("value"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"setup"}, {"maint"}, {"storage-repair"}, {"storage-cli", "put", "ordinary", file}, {"storage-cli", "delete", "ordinary"}} {
		if _, err := Run(args, config, zap.NewNop()); err == nil {
			t.Fatalf("mutated disabled namespace: %v", args)
		}
	}
}

func TestSplitCLICannotOverwriteUnboundV2Root(t *testing.T) {
	ctx := context.Background()
	config := splitCLIConfig(t)
	if _, err := Run([]string{"storage-init", "--offline"}, config, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	meta, err := openStorage(config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(meta)
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	layout, _ := storage.LoadStorageLayout(ctx, meta)
	repo, _ := storage.NewJobLogRepository(meta, registry.Stores(), storage.JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
	if _, err := repo.Append(ctx, "job", 0, []byte("safe"), true, storage.JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	raw, _ := meta.Get(ctx, storage.JobLogManifestKey("job"))
	file := filepath.Join(t.TempDir(), "root.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := meta.Delete(ctx, storage.StorageLayoutKey); err != nil {
		t.Fatal(err)
	}
	legacy := &types.Config{StorageBaseDir: config.StorageBaseDir}
	for _, args := range [][]string{{"storage-cli", "delete", storage.JobLogManifestKey("job")}, {"storage-cli", "put", storage.JobLogManifestKey("new"), file}, {"storage-cli", "delete", "deleted_jobs:job"}} {
		if _, err := Run(args, legacy, zap.NewNop()); err == nil {
			t.Fatalf("unbound root bypassed CLI guard: %v", args)
		}
	}
}
