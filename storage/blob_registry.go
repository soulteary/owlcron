package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/soulteary/owlcron/blob"
	"github.com/soulteary/owlcron/types"
)

var storeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

type BlobRegistry struct {
	stores     map[string]blob.Store
	bindings   map[string]string
	writeStore string
}

// OpenConfiguredMetadata applies the same defaults for the daemon and offline
// CLI. It never changes the meaning of legacy S3 or Redis metadata adapters.
func OpenConfiguredMetadata(config *types.Config) (Storage, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	engine := config.StorageEngine
	if engine == "" {
		engine = "Filesystem"
	}
	baseDir := config.StorageBaseDir
	if baseDir == "" {
		baseDir = "data"
	}
	if cfg := config.JobLogStorage; cfg != nil {
		if cfg.WriteFormat < 0 || cfg.WriteFormat > 2 {
			return nil, fmt.Errorf("unsupported job log write_format")
		}
		if cfg.SpoolMaxBytes < 0 || cfg.SpoolResumePercent < 0 || cfg.SpoolResumePercent > 89 || cfg.GCGraceSeconds < 0 || cfg.GCBatchSize < 0 || cfg.GCBatchSize > blob.MaxScanLimit {
			return nil, fmt.Errorf("invalid job_log_storage limits")
		}
		if cfg.WriteFormat == 2 {
			if config.BlobStorage == nil {
				return nil, fmt.Errorf("log format 2 requires blob_storage")
			}
		}
	}
	// Existing v2 roots may still be appended, deleted, or collected when new
	// logs use v1. Their sequence history needs the same durable metadata rules.
	if config.BlobStorage != nil {
		if engine != "Filesystem" && engine != "Couchbase" {
			return nil, fmt.Errorf("split log storage currently requires Filesystem or Couchbase metadata")
		}
		if engine == "Couchbase" && optionString(config.StorageOptions, "durability", "") != "persist_to_majority" {
			return nil, fmt.Errorf("split log storage requires Couchbase durability persist_to_majority")
		}
		if engine == "Filesystem" {
			metadataPath, err := canonicalDirectory(baseDir)
			if err != nil {
				return nil, err
			}
			for id, cfg := range config.BlobStorage.Stores {
				if cfg.Engine != "Filesystem" {
					continue
				}
				blobPath, err := canonicalDirectory(cfg.BaseDir)
				if err != nil {
					return nil, err
				}
				if directoriesOverlap(metadataPath, blobPath) {
					return nil, fmt.Errorf("metadata and blob store %s require separate, non-overlapping directories", id)
				}
			}
		}
	}
	return NewStorage(&StorageConfig{Engine: engine, BaseDir: baseDir, Options: config.StorageOptions})
}

// Resolve existing ancestors too: configuration commonly names a target whose
// final directories have not been created yet. Filesystem metadata enumeration
// must never traverse raw blob objects in an overlapping directory tree.
func canonicalDirectory(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("filesystem directory is required")
	}
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	suffix := ""
	for current := abs; ; current = filepath.Dir(current) {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			return filepath.Join(resolved, suffix), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if parent := filepath.Dir(current); parent == current {
			return "", err
		}
		suffix = filepath.Join(filepath.Base(current), suffix)
	}
}

func directoriesOverlap(a, b string) bool {
	inside := func(parent, child string) bool {
		rel, err := filepath.Rel(parent, child)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
	}
	return inside(a, b) || inside(b, a)
}

func NewBlobRegistry(config *types.BlobStorageConfig) (*BlobRegistry, error) {
	if config == nil || len(config.Stores) == 0 || len(config.Stores) > 16 || !storeIDPattern.MatchString(config.WriteStore) {
		return nil, fmt.Errorf("%w: 1..16 stores and a write_store are required", blob.ErrInvalidConfig)
	}
	if _, ok := config.Stores[config.WriteStore]; !ok {
		return nil, fmt.Errorf("%w: write_store is not configured", blob.ErrInvalidConfig)
	}
	r := &BlobRegistry{stores: make(map[string]blob.Store), bindings: make(map[string]string), writeStore: config.WriteStore}
	success := false
	defer func() {
		if !success {
			_ = r.Close()
		}
	}()
	for id, cfg := range config.Stores {
		if !storeIDPattern.MatchString(id) {
			return nil, fmt.Errorf("%w: invalid store ID", blob.ErrInvalidConfig)
		}
		var store blob.Store
		var err error
		var identity any
		switch cfg.Engine {
		case "Filesystem":
			if cfg.BaseDir == "" {
				return nil, fmt.Errorf("%w: filesystem base_dir is required", blob.ErrInvalidConfig)
			}
			abs, e := filepath.Abs(cfg.BaseDir)
			if e != nil {
				return nil, e
			}
			store, err = blob.NewFilesystem(abs)
			if err == nil {
				abs, err = filepath.EvalSymlinks(abs)
			}
			identity = struct{ Engine, BaseDir string }{"Filesystem", abs}
		case "S3":
			access, e := blobCredential(cfg.AccessKey, cfg.AccessKeyFile, "OWLCRON_BLOB_ACCESS_KEY_ID")
			if e != nil {
				return nil, e
			}
			secret, e := blobCredential(cfg.SecretKey, cfg.SecretKeyFile, "OWLCRON_BLOB_SECRET_ACCESS_KEY")
			if e != nil {
				return nil, e
			}
			token := cfg.SessionToken
			if token == "" {
				token = os.Getenv("OWLCRON_BLOB_SESSION_TOKEN")
			}
			store, err = blob.NewS3(blob.S3Config{Endpoint: cfg.Endpoint, Bucket: cfg.Bucket, Prefix: cfg.Prefix, Region: cfg.Region, PathStyle: cfg.PathStyle, AccessKey: access, SecretKey: secret, SessionToken: token})
			identity = struct{ Engine, Bucket, Prefix string }{"S3", cfg.Bucket, strings.TrimSuffix(cfg.Prefix, "/")}
		default:
			return nil, fmt.Errorf("%w: unsupported blob engine %q", blob.ErrInvalidConfig, cfg.Engine)
		}
		if err != nil {
			if closer, ok := store.(io.Closer); ok {
				_ = closer.Close()
			}
			return nil, fmt.Errorf("open blob store %s: %w", id, err)
		}
		data, _ := json.Marshal(identity)
		sum := sha256.Sum256(data)
		binding := hex.EncodeToString(sum[:])
		for otherID, other := range r.bindings {
			if other == binding {
				if closer, ok := store.(io.Closer); ok {
					_ = closer.Close()
				}
				return nil, fmt.Errorf("%w: stores %s and %s share a physical location", blob.ErrInvalidConfig, otherID, id)
			}
		}
		r.stores[id] = store
		r.bindings[id] = binding
	}
	success = true
	return r, nil
}

func blobCredential(value, file, environment string) (string, error) {
	if value != "" && file != "" {
		return "", fmt.Errorf("%w: configure credential value or file, not both", blob.ErrInvalidConfig)
	}
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return "", fmt.Errorf("read blob credential file: %w", err)
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, 16*1024+1))
		if err != nil {
			return "", err
		}
		if len(data) > 16*1024 {
			return "", fmt.Errorf("%w: credential file is too large", blob.ErrInvalidConfig)
		}
		value = strings.TrimSpace(string(data))
	}
	if value == "" && file == "" {
		value = os.Getenv(environment)
	}
	return value, nil
}

func (r *BlobRegistry) Store(id string) (blob.Store, error) {
	store, ok := r.stores[id]
	if !ok {
		return nil, fmt.Errorf("%w: blob store %q is unavailable", blob.ErrInvalidConfig, id)
	}
	return store, nil
}
func (r *BlobRegistry) Stores() map[string]blob.Store {
	out := make(map[string]blob.Store, len(r.stores))
	for id, store := range r.stores {
		out[id] = store
	}
	return out
}
func (r *BlobRegistry) WriteStoreID() string { return r.writeStore }
func (r *BlobRegistry) Close() error {
	var errs []error
	for _, store := range r.stores {
		if c, ok := store.(io.Closer); ok {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (r *BlobRegistry) marker(namespace, id string) (string, []byte) {
	data, _ := json.Marshal(struct {
		Version   int    `json:"version"`
		Namespace string `json:"namespace_id"`
		StoreID   string `json:"store_id"`
		Binding   string `json:"binding"`
	}{1, namespace, id, r.bindings[id]})
	return namespace + "/binding", data
}

// Readiness reads only small, immutable binding markers. It never creates a
// missing bucket or marker and cannot silently bind an existing ID elsewhere.
func (r *BlobRegistry) Readiness(ctx context.Context, namespace string) error {
	if !namespacePattern.MatchString(namespace) {
		return fmt.Errorf("invalid storage namespace")
	}
	for id, store := range r.stores {
		key, want := r.marker(namespace, id)
		body, err := store.Open(ctx, key)
		if err != nil {
			return fmt.Errorf("blob binding %s: %w", id, err)
		}
		got, err := io.ReadAll(io.LimitReader(body, int64(len(want))+1))
		closeErr := body.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%w: blob namespace marker mismatch for %s", blob.ErrIntegrity, id)
		}
	}
	return ctx.Err()
}
