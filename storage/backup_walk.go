package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	minio "github.com/soulteary/otterio-sdk/v7"
)

// Expiring legacy payloads cannot be made permanent merely by changing storage
// format. The v2 root intentionally has no TTL, so conversion rejects this case
// until the operator applies an explicit retention policy. S3 reads only the
// bounded envelope trailer, never the potentially large base64 data member.
func requirePermanentLegacyLog(ctx context.Context, store Storage, id string) error {
	key := "job_logs:" + id
	var expires int64
	switch s := store.(type) {
	case *FilesystemStorage:
		raw, err := s.Get(ctx, "_expire/"+key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &expires); err != nil {
			return err
		}
	case *S3Storage:
		head, err := s.client.StatObject(ctx, s.bucket, s.objectKey(key), minio.StatObjectOptions{})
		if err != nil {
			return mapS3Error(err)
		}
		if head.Size < 11 || head.ETag == "" {
			return fmt.Errorf("invalid legacy log envelope")
		}
		start := max(int64(0), head.Size-4096)
		tail, err := s.getLegacyObjectRange(ctx, key, head.ETag, start, head.Size-1)
		if err != nil {
			return err
		}
		var doc remoteDocument
		if start == 0 && !bytes.HasPrefix(tail, []byte(`{"data":"`)) {
			if !bytes.HasPrefix(tail, []byte(`{"data":null,`)) || json.Unmarshal(tail, &doc) != nil {
				return ErrUnsupported
			}
		} else {
			quote := bytes.IndexByte(tail, '"')
			if start == 0 {
				quote = bytes.IndexByte(tail[9:], '"')
				if quote >= 0 {
					quote += 9
				}
			} else {
				prefix, err := s.getLegacyObjectRange(ctx, key, head.ETag, 0, 8)
				if err != nil {
					return err
				}
				if !bytes.Equal(prefix, []byte(`{"data":"`)) {
					return ErrUnsupported
				}
			}
			if quote < 0 {
				return ErrUnsupported
			}
			if err := json.Unmarshal(append([]byte(`{"data":"`), tail[quote:]...), &doc); err != nil {
				return err
			}
		}
		if doc.Deleted {
			return ErrNotFound
		}
		expires = doc.Expires
	default:
		return fmt.Errorf("bounded legacy log expiry inspection: %w", ErrUnsupported)
	}
	if expires > 0 && expires <= time.Now().Unix() {
		return ErrNotFound
	}
	if expires != 0 {
		return fmt.Errorf("legacy log %s has an expiration; conversion must not make it permanent", id)
	}
	return nil
}

// walkBackupKeys streams backend inventories. Filesystem traversal holds no
// storage lock while invoking visit, which may read records through Storage.
// Administrative callers must stop all writers; this is not an online snapshot.
// The fallback preserves compatibility with third-party KeyLister adapters.
func walkBackupKeys(ctx context.Context, store Storage, visit func(string) error) error {
	switch s := store.(type) {
	case *FilesystemStorage:
		root, err := os.OpenRoot(s.baseDir)
		if err != nil {
			return err
		}
		defer root.Close()
		var walk func(string, int) error
		walk = func(relative string, depth int) error {
			if depth > 64 {
				return fmt.Errorf("metadata directory nesting exceeds limit")
			}
			dir, err := root.Open(relative)
			if err != nil {
				return err
			}
			defer dir.Close()
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				entries, err := dir.ReadDir(128)
				if err != nil && err != io.EOF {
					return err
				}
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), ".") || entry.Name() == "bootstrap-admin.json" {
						continue
					}
					if entry.Type()&os.ModeSymlink != 0 {
						return fmt.Errorf("symlink in storage")
					}
					key := entry.Name()
					if relative != "." {
						key = relative + "/" + key
					}
					if entry.IsDir() {
						if err := walk(key, depth+1); err != nil {
							return err
						}
						continue
					}
					switch {
					case strings.HasSuffix(key, ".json"):
						key = strings.TrimSuffix(key, ".json")
					case strings.HasSuffix(key, ".list"):
						key = strings.TrimSuffix(key, ".list")
					default:
						continue
					}
					if err := validateKey(key); err != nil {
						return err
					}
					if err := visit(key); err != nil {
						return err
					}
				}
				if err == io.EOF {
					return nil
				}
			}
		}
		return walk(".", 0)
	case *CouchbaseStorage:
		scan, err := s.collection.Scan(gocb.NewRangeScanForPrefix(s.prefix), s.scanOptions(ctx))
		if err != nil {
			return err
		}
		defer scan.Close()
		for item := scan.Next(); item != nil; item = scan.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !strings.HasPrefix(item.ID(), s.prefix) {
				continue
			}
			if err := visit(strings.TrimPrefix(item.ID(), s.prefix)); err != nil {
				return err
			}
		}
		return scan.Err()
	case *S3Storage:
		fetchOwner := false
		opts := minio.ListObjectsOptions{Prefix: s.objectKey(""), Recursive: true, FetchOwner: &fetchOwner}
		namespace := strings.TrimSuffix(s.prefix, "/")
		if namespace != "" {
			namespace += "/"
		}
		for item := range s.client.ListObjectsIter(ctx, s.bucket, opts) {
			if item.Err != nil {
				return item.Err
			}
			if !strings.HasPrefix(item.Key, namespace) {
				continue
			}
			key := strings.TrimPrefix(item.Key, namespace)
			if strings.HasPrefix(key, s3CapabilityPrefix) {
				continue
			}
			if err := validateKey(key); err != nil {
				return err
			}
			// Older versions left raw probes in the application namespace.
			// Inspect only that prefix: fetching every listed body here would
			// defeat bounded range reads for large legacy job logs.
			if strings.HasPrefix(key, "capability/") {
				data, _, err := s.read(ctx, key)
				if errors.Is(err, ErrNotFound) {
					continue
				} else if err != nil {
					return err
				}
				if s3LegacyCapabilityProbe(key, data) {
					continue
				}
			}
			if err := visit(key); err != nil {
				return err
			}
		}
		return ctx.Err()
	default:
		lister, ok := store.(KeyLister)
		if !ok {
			return ErrUnsupported
		}
		keys, err := lister.ListKeys(ctx)
		if err != nil {
			return err
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(key); err != nil {
				return err
			}
		}
		return nil
	}
}

func readBackupEntry(ctx context.Context, store Storage, key string) (BackupEntry, error) {
	if err := validateKey(key); err != nil {
		return BackupEntry{}, err
	}
	data, err := store.Get(ctx, key)
	if err != nil {
		return BackupEntry{}, err
	}
	data, err = backupRecordData(key, data)
	if err != nil {
		return BackupEntry{}, err
	}
	entry := BackupEntry{Data: data}
	if remote, ok := store.(interface {
		readDocument(context.Context, string) (remoteDocument, string, error)
	}); ok {
		doc, _, err := remote.readDocument(ctx, key)
		if err != nil {
			return BackupEntry{}, err
		}
		entry.Expires = doc.Expires
	} else if raw, err := store.Get(ctx, "_expire/"+key); err == nil {
		if err := json.Unmarshal(raw, &entry.Expires); err != nil {
			return BackupEntry{}, fmt.Errorf("invalid expiration for %s: %w", key, err)
		}
	} else if !errors.Is(err, ErrNotFound) {
		return BackupEntry{}, err
	}
	return entry, nil
}

var ErrSplitBackupRequired = errors.New("split storage requires a complete split backup")

func rejectSplitBackupRecord(key string, data []byte) error {
	if key == "global/storage_layout" {
		return ErrSplitBackupRequired
	}
	if strings.HasPrefix(key, "job_log_manifest/") {
		var header struct {
			Format int `json:"format"`
		}
		if err := json.Unmarshal(data, &header); err != nil {
			return fmt.Errorf("invalid job log manifest: %w", err)
		}
		if header.Format != 0 && header.Format != 1 {
			return ErrSplitBackupRequired
		}
	}
	return nil
}

func rejectSplitBackup(ctx context.Context, store Storage) error {
	if _, err := store.Get(ctx, "global/storage_layout"); err == nil {
		return ErrSplitBackupRequired
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	if _, ok := store.(KeyLister); !ok {
		// Import historically accepts write-only wrappers. The layout guard
		// and validation of every incoming manifest still apply to them.
		return nil
	}
	return walkBackupKeys(ctx, store, func(key string) error {
		if !strings.HasPrefix(key, "job_log_manifest/") {
			return nil
		}
		data, err := store.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return rejectSplitBackupRecord(key, data)
	})
}
