package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/couchbase/gocb/v2"
)

// KeyLister returns only live storage keys, never lock files or bootstrap secrets.
type KeyLister interface {
	ListKeys(context.Context) ([]string, error)
}
type BackupEntry struct {
	Data    []byte `json:"data"`
	Expires int64  `json:"expires,omitempty"`
}
type Backup struct {
	Format  string                 `json:"format"`
	Created int64                  `json:"created"`
	Records map[string]BackupEntry `json:"records"`
}

func backupRecordData(key string, data []byte) ([]byte, error) {
	if key != "global/state" {
		return data, nil
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode backup state: %w", err)
	}
	if _, exists := state["_lease_owner"]; !exists {
		return data, nil
	}
	delete(state, "_lease_owner")
	return json.Marshal(state)
}

func Export(ctx context.Context, store Storage) (*Backup, error) {
	backup := &Backup{Format: "owlcron-storage-v1", Created: time.Now().Unix(), Records: map[string]BackupEntry{}}
	_, err := exportRecords(ctx, store, func(key string, entry BackupEntry) error {
		backup.Records[key] = entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	return backup, nil
}

// ExportTo writes the existing backup format one record at a time. Callers
// should use a temporary file: failed exports may have written a partial JSON
// document. Consistency still requires an offline snapshot of the source.
func ExportTo(ctx context.Context, store Storage, writer io.Writer) (int, error) {
	if _, err := fmt.Fprintf(writer, `{"format":"owlcron-storage-v1","created":%d,"records":{`, time.Now().Unix()); err != nil {
		return 0, err
	}
	first := true
	encoder := json.NewEncoder(writer)
	count, err := exportRecords(ctx, store, func(key string, entry BackupEntry) error {
		if !first {
			if _, err := io.WriteString(writer, ","); err != nil {
				return err
			}
		}
		first = false
		name, err := json.Marshal(key)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "%s:", name); err != nil {
			return err
		}
		return encoder.Encode(entry)
	})
	if err != nil {
		return count, err
	}
	_, err = io.WriteString(writer, "}}\n")
	return count, err
}

func exportRecords(ctx context.Context, store Storage, visit func(string, BackupEntry) error) (int, error) {
	lister, ok := store.(KeyLister)
	if !ok {
		return 0, ErrUnsupported
	}
	keys, err := lister.ListKeys(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	sort.Strings(keys)
	for _, key := range keys {
		if strings.HasPrefix(key, "_expire/") || key == "global/master_lease" {
			continue
		}
		data, err := store.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		data, err = backupRecordData(key, data)
		if err != nil {
			return 0, err
		}
		entry := BackupEntry{Data: data}
		if remote, ok := store.(interface {
			readDocument(context.Context, string) (remoteDocument, string, error)
		}); ok {
			doc, _, err := remote.readDocument(ctx, key)
			if err != nil {
				return 0, err
			}
			entry.Expires = doc.Expires
		} else if expiry, err := store.Get(ctx, "_expire/"+key); err == nil {
			_ = json.Unmarshal(expiry, &entry.Expires)
		}
		if err := visit(key, entry); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
func Import(ctx context.Context, store Storage, backup *Backup) error {
	if backup == nil || backup.Format != "owlcron-storage-v1" {
		return fmt.Errorf("unsupported backup format")
	}
	records := make(map[string]BackupEntry, len(backup.Records))
	for key, entry := range backup.Records {
		if err := validateKey(key); err != nil {
			return err
		}
		if key == "global/master_lease" {
			continue
		}
		data, err := backupRecordData(key, entry.Data)
		if err != nil {
			return err
		}
		entry.Data = data
		records[key] = entry
	}
	// Filesystem restores use the durable transaction journal. Remote backends
	// are offline administrative restores; indexes are written last to avoid
	// advertising an entity whose canonical record is absent.
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := strings.Contains(keys[i], ":"), strings.Contains(keys[j], ":")
		if a != b {
			return a
		}
		return keys[i] < keys[j]
	})
	tx, err := store.BeginTransaction(ctx)
	if err != nil && !errors.Is(err, ErrUnsupported) {
		return err
	}
	if tx != nil {
		for _, key := range keys {
			if err = tx.Put(key, records[key].Data); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	} else {
		for _, key := range keys {
			if err = store.Put(ctx, key, records[key].Data); err != nil {
				return err
			}
		}
	}
	for _, key := range keys {
		if expires := records[key].Expires; expires > 0 {
			if err = store.Expire(ctx, key, expires); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *FilesystemStorage) ListKeys(ctx context.Context) (keys []string, err error) {
	err = s.withLock(ctx, func() error {
		return filepath.WalkDir(s.baseDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if path == s.baseDir {
				return nil
			}
			if d.Name() == "bootstrap-admin.json" {
				return nil
			}
			if strings.HasPrefix(d.Name(), ".") {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Type()&fs.ModeSymlink != 0 {
				return fmt.Errorf("symlink in storage")
			}
			if d.IsDir() {
				return nil
			}
			relative, _ := filepath.Rel(s.baseDir, path)
			switch {
			case strings.HasSuffix(relative, ".json"):
				relative = strings.TrimSuffix(relative, ".json")
			case strings.HasSuffix(relative, ".list"):
				relative = strings.TrimSuffix(relative, ".list")
			default:
				return nil
			}
			key := filepath.ToSlash(relative)
			if err := validateKey(key); err != nil {
				return err
			}
			keys = append(keys, key)
			return nil
		})
	})
	sort.Strings(keys)
	return
}
func (s *S3Storage) ListKeys(ctx context.Context) ([]string, error) {
	prefix := s.prefix
	if prefix != "" {
		prefix += "/"
	}
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)})
	keys := []string{}
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			key := strings.TrimPrefix(aws.ToString(obj.Key), prefix)
			if strings.HasPrefix(key, "capability/") {
				continue
			}
			if _, err := s.Get(ctx, key); errors.Is(err, ErrNotFound) {
				continue
			} else if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
func (c *CouchbaseStorage) ListKeys(ctx context.Context) ([]string, error) {
	scan, err := c.collection.Scan(gocb.NewRangeScanForPrefix(c.prefix), &gocb.ScanOptions{Context: ctx, IDsOnly: true})
	if err != nil {
		return nil, err
	}
	defer scan.Close()
	keys := []string{}
	for item := scan.Next(); item != nil; item = scan.Next() {
		key := strings.TrimPrefix(item.ID(), c.prefix)
		if _, err := c.Get(ctx, key); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}
