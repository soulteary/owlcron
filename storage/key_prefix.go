package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/couchbase/gocb/v2"
)

// PrefixKeyLister enumerates live keys in one namespace without scanning other
// namespaces or fetching their record bodies.
type PrefixKeyLister interface {
	ListKeysPrefix(context.Context, string) ([]string, error)
}

func validateKeyPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return validateKey(strings.TrimSuffix(prefix, "/"))
}

func (s *FilesystemStorage) ListKeysPrefix(ctx context.Context, prefix string) (keys []string, err error) {
	if err = validateKeyPrefix(prefix); err != nil {
		return nil, err
	}
	err = s.withLock(ctx, func() error {
		return filepath.WalkDir(s.baseDir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if path == s.baseDir {
				return nil
			}
			relative, err := filepath.Rel(s.baseDir, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			if strings.HasPrefix(d.Name(), ".") {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if d.IsDir() {
				// A directory may contain matches only if its path intersects the prefix.
				if !strings.HasPrefix(relative+"/", prefix) && !strings.HasPrefix(prefix, relative+"/") {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Name() == "bootstrap-admin.json" {
				return nil
			}
			key := relative
			switch {
			case strings.HasSuffix(key, ".json"):
				key = strings.TrimSuffix(key, ".json")
			case strings.HasSuffix(key, ".list"):
				key = strings.TrimSuffix(key, ".list")
			default:
				return nil
			}
			if !strings.HasPrefix(key, prefix) {
				return nil
			}
			if d.Type()&fs.ModeSymlink != 0 {
				return fmt.Errorf("symlink in storage")
			}
			// read uses the existing lock and enforces expiry and path validation.
			if _, err := s.read(key); errors.Is(err, ErrNotFound) {
				return nil
			} else if err != nil {
				return err
			}
			keys = append(keys, key)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *S3Storage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	if err := validateKeyPrefix(prefix); err != nil {
		return nil, err
	}
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(s.objectKey(prefix))})
	keys := []string{}
	namespace := ""
	if s.prefix != "" {
		namespace = s.prefix + "/"
	}
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			objectKey := aws.ToString(obj.Key)
			if !strings.HasPrefix(objectKey, namespace+prefix) {
				continue
			}
			key := strings.TrimPrefix(objectKey, namespace)
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

func (c *CouchbaseStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	if err := validateKeyPrefix(prefix); err != nil {
		return nil, err
	}
	scan, err := c.collection.Scan(gocb.NewRangeScanForPrefix(c.prefix+prefix), &gocb.ScanOptions{Context: ctx, IDsOnly: true})
	if err != nil {
		return nil, err
	}
	defer scan.Close()
	keys := []string{}
	for item := scan.Next(); item != nil; item = scan.Next() {
		if !strings.HasPrefix(item.ID(), c.prefix+prefix) {
			continue
		}
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
