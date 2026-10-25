package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func splitFileArgs(args []string, requireFile bool) (string, error) {
	var file string
	offline := false
	for _, arg := range args {
		if arg == "--offline" && !offline {
			offline = true
			continue
		}
		if file != "" || strings.HasPrefix(arg, "--") || arg == "" {
			return "", fmt.Errorf("expected --offline and one optional archive path")
		}
		file = arg
	}
	if !offline {
		return "", fmt.Errorf("--offline is required; stop all daemons, workers, uploaders and GC before this operation")
	}
	if requireFile && (file == "" || file == "-") {
		return "", fmt.Errorf("a backup file path is required")
	}
	return file, nil
}

func finishBlobRegistry(registry *storage.BlobRegistry, commandErr *error) {
	if err := registry.Close(); err != nil && *commandErr == nil {
		*commandErr = fmt.Errorf("close blob storage: %w", err)
	}
}

func cmdSplitExport(args []string, config *types.Config) (err error) {
	path, err := splitFileArgs(args, false)
	if err != nil {
		return err
	}
	meta, err := openStorage(config)
	if err != nil {
		return err
	}
	defer finishStorage(meta, &err)
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		return err
	}
	defer finishBlobRegistry(registry, &err)
	var result storage.SplitBackupResult
	write := func(out io.Writer) error {
		var exportErr error
		result, exportErr = storage.ExportSplitTo(context.Background(), meta, registry, out, storage.SplitBackupOptions{Offline: true})
		return exportErr
	}
	if path == "" || path == "-" {
		return write(os.Stdout)
	}
	if err := writeBackupFile(path, write); err != nil {
		return fmt.Errorf("write complete split backup: %w", err)
	}
	fmt.Printf("Exported %d records, %d objects and %d legacy fragments to %s\n", result.Records, result.Objects, result.LegacyChunks, path)
	return nil
}

func cmdSplitImport(args []string, config *types.Config) (err error) {
	path, err := splitFileArgs(args, true)
	if err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	meta, err := openStorage(config)
	if err != nil {
		return err
	}
	defer finishStorage(meta, &err)
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		return err
	}
	defer finishBlobRegistry(registry, &err)
	result, err := storage.ImportSplitFrom(context.Background(), meta, registry, file, storage.SplitBackupOptions{Offline: true})
	if err != nil {
		return fmt.Errorf("restore complete split backup: %w", err)
	}
	fmt.Printf("Restored %d records and %d objects into a new ready namespace.\n", result.Records, result.Objects)
	return nil
}

func cmdSplitStorageInit(args []string, config *types.Config) (err error) {
	if len(args) != 1 || args[0] != "--offline" {
		return fmt.Errorf("usage: storage-init --offline; the metadata namespace must be empty")
	}
	meta, err := openStorage(config)
	if err != nil {
		return err
	}
	defer finishStorage(meta, &err)
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		return err
	}
	defer finishBlobRegistry(registry, &err)
	layout, err := storage.InitializeStorageLayout(context.Background(), meta, registry, "ready")
	if err != nil {
		return err
	}
	fmt.Printf("Initialized split namespace %s. Old writers must remain isolated from this metadata namespace.\n", layout.NamespaceID)
	return nil
}

func cmdSplitStorageVerify(args []string, config *types.Config) (err error) {
	if len(args) != 0 {
		return fmt.Errorf("usage: storage-verify")
	}
	meta, err := openStorage(config)
	if err != nil {
		return err
	}
	defer finishStorage(meta, &err)
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		return err
	}
	defer finishBlobRegistry(registry, &err)
	result, err := storage.VerifySplitStorage(context.Background(), meta, registry)
	if err != nil {
		return err
	}
	fmt.Printf("Verified %d logs, %d immutable objects and %d bytes.\n", result.Logs, result.Objects, result.Bytes)
	return nil
}

func cmdStoragePreflight(args []string, config *types.Config) (err error) {
	if len(args) != 1 || args[0] != "--offline" {
		return fmt.Errorf("usage: storage-preflight --offline")
	}
	meta, err := openStorage(config)
	if err != nil {
		return err
	}
	defer finishStorage(meta, &err)
	var registry *storage.BlobRegistry
	if config.BlobStorage != nil {
		registry, err = storage.NewBlobRegistry(config.BlobStorage)
		if err != nil {
			return err
		}
		defer finishBlobRegistry(registry, &err)
	}
	if err := storage.PreflightStorageCopy(context.Background(), meta, registry, true); err != nil {
		return err
	}
	fmt.Println("Offline source preflight passed. Keep every writer and GC stopped until copying completes.")
	return nil
}

func cmdSplitStorageMigrate(args []string, sourceConfig *types.Config) (err error) {
	var path string
	var offline, resume bool
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--offline":
			if offline {
				return fmt.Errorf("duplicate --offline")
			}
			offline = true
		case "--resume":
			if resume {
				return fmt.Errorf("duplicate --resume")
			}
			resume = true
		case "--target-config":
			if path != "" || i+1 == len(args) {
				return fmt.Errorf("--target-config requires one path")
			}
			i++
			path = args[i]
		default:
			return fmt.Errorf("unknown storage-migrate argument %q", args[i])
		}
	}
	if !offline || path == "" {
		return fmt.Errorf("usage: storage-migrate --offline --target-config <file> [--resume]")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var targetConfig types.Config
	if err := json.Unmarshal(data, &targetConfig); err != nil {
		return fmt.Errorf("decode target configuration: %w", err)
	}
	if targetConfig.BlobStorage == nil || targetConfig.JobLogStorage == nil || targetConfig.JobLogStorage.WriteFormat != 2 {
		return fmt.Errorf("target must configure blob_storage and job_log_storage.write_format=2")
	}
	source, err := openStorage(sourceConfig)
	if err != nil {
		return err
	}
	defer finishStorage(source, &err)
	target, err := openStorage(&targetConfig)
	if err != nil {
		return err
	}
	defer finishStorage(target, &err)
	registry, err := storage.NewBlobRegistry(targetConfig.BlobStorage)
	if err != nil {
		return err
	}
	defer finishBlobRegistry(registry, &err)
	result, err := storage.MigrateLegacyToSplit(context.Background(), source, target, registry, storage.SplitMigrationOptions{Offline: true, Resume: resume})
	if err != nil {
		return fmt.Errorf("offline migration: %w", err)
	}
	fmt.Printf("Migration %s completed: %d records, %d logs, %d bytes; namespace %s is ready.\n", result.MigrationID, result.Records, result.Logs, result.Bytes, result.Namespace)
	return nil
}

func validateStorageCommandMutation(ctx context.Context, meta storage.Storage, config *types.Config) error {
	_, err := storage.LoadStorageLayout(ctx, meta)
	if errors.Is(err, storage.ErrNotFound) {
		if config.BlobStorage != nil || config.JobLogStorage != nil && config.JobLogStorage.WriteFormat == 2 {
			return fmt.Errorf("split storage must be initialized with storage-init --offline before mutation")
		}
		return nil
	}
	if err != nil {
		return err
	}
	registry, err := storage.NewBlobRegistry(config.BlobStorage)
	if err != nil {
		return err
	}
	defer registry.Close()
	_, err = storage.ValidateStorageLayout(ctx, meta, registry)
	return err
}

func guardSplitStorageKey(ctx context.Context, meta storage.Storage, config *types.Config, key string) error {
	if key == storage.StorageLayoutKey || key == "global/split_migration" || strings.HasPrefix(key, "split_migration/") {
		return fmt.Errorf("storage-cli cannot mutate split storage control records")
	}
	if err := validateStorageCommandMutation(ctx, meta, config); err != nil {
		return err
	}
	if _, err := storage.LoadStorageLayout(ctx, meta); errors.Is(err, storage.ErrNotFound) {
		manifestKey := key
		for _, prefix := range []string{"deleted_jobs:", "deleted_job_logs:"} {
			if strings.HasPrefix(key, prefix) {
				manifestKey = storage.JobLogManifestKey(strings.TrimPrefix(key, prefix))
			}
		}
		if strings.HasPrefix(manifestKey, "job_log_manifest/") {
			if raw, err := meta.Get(ctx, manifestKey); err == nil {
				return guardSplitStorageValue(manifestKey, raw)
			} else if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
		return nil
	} else if err != nil {
		return err
	}
	for _, prefix := range []string{"job_log_manifest/", "job_log_chunks/", "job_logs:", "deleted_jobs:", "deleted_job_logs:"} {
		if strings.HasPrefix(key, prefix) {
			return fmt.Errorf("storage-cli cannot mutate protected split log state; use the application deletion or offline migration path")
		}
	}
	return nil
}

func guardSplitStorageValue(key string, data []byte) error {
	if !strings.HasPrefix(key, "job_log_manifest/") {
		return nil
	}
	var header struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return fmt.Errorf("invalid log manifest: %w", err)
	}
	if header.Format != 0 && header.Format != 1 {
		return fmt.Errorf("storage-cli cannot write or delete a split log root")
	}
	return nil
}
