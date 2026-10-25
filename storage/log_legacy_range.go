package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	minio "github.com/soulteary/otterio-sdk/v7"
)

func validateLogRange(key string, offset int64, limit int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if offset < 0 || limit < 0 || limit > 1024*1024 {
		return fmt.Errorf("invalid log range")
	}
	return nil
}

func (s *FilesystemStorage) GetRange(ctx context.Context, key string, offset int64, limit int) (data []byte, total int64, err error) {
	if err = validateLogRange(key, offset, limit); err != nil {
		return nil, 0, err
	}
	err = s.withLock(ctx, func() error {
		p, e := s.path(key)
		if e != nil {
			return e
		}
		expiry, e := s.path("_expire/" + key)
		if e != nil {
			return e
		}
		ef, e := os.Open(expiry)
		if e == nil {
			var ts int64
			metadata, readErr := io.ReadAll(io.LimitReader(ef, 129))
			ef.Close()
			if readErr != nil {
				return readErr
			}
			if len(metadata) > 128 {
				return fmt.Errorf("expiry metadata too large")
			}
			e = json.Unmarshal(metadata, &ts)
			if e != nil {
				return fmt.Errorf("invalid expiry metadata: %w", e)
			}
			if ts <= time.Now().Unix() {
				return ErrNotFound
			}
		} else if !os.IsNotExist(e) {
			return e
		}
		file, e := os.Open(p)
		if os.IsNotExist(e) {
			return ErrNotFound
		}
		if e != nil {
			return e
		}
		defer file.Close()
		info, e := file.Stat()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("invalid log file")
		}
		total = info.Size()
		if offset >= info.Size() || limit == 0 {
			data = []byte{}
			return nil
		}
		if _, e = file.Seek(offset, io.SeekStart); e != nil {
			return e
		}
		data, e = io.ReadAll(io.LimitReader(file, int64(limit)))
		return e
	})
	return
}

func (c *CouchbaseStorage) GetRange(ctx context.Context, key string, offset int64, limit int) ([]byte, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if err := validateLogRange(key, offset, limit); err != nil {
		return nil, 0, err
	}
	return nil, 0, ErrUnsupported
}

// getLegacyObjectRange rejects servers that ignore Range and caps every body
// read. IfMatch prevents metadata and payload from different revisions mixing.
func (s *S3Storage) getLegacyObjectRange(ctx context.Context, key, etag string, start, end int64) ([]byte, error) {
	options := minio.GetObjectOptions{}
	if err := options.SetRange(start, end); err != nil {
		return nil, err
	}
	if err := options.SetMatchETag(etag); err != nil {
		return nil, err
	}
	body, _, headers, err := (minio.Core{Client: s.client}).GetObject(ctx, s.bucket, s.objectKey(key), options)
	if err != nil {
		return nil, mapS3Error(err)
	}
	defer body.Close()
	contentRange := headers.Get("Content-Range")
	if contentRange == "" {
		return nil, fmt.Errorf("S3 server ignored bounded Range: %w", ErrUnsupported)
	}
	expectedPrefix := fmt.Sprintf("bytes %d-%d/", start, end)
	if !strings.HasPrefix(contentRange, expectedPrefix) {
		return nil, fmt.Errorf("invalid S3 Content-Range")
	}
	data, err := io.ReadAll(io.LimitReader(body, end-start+2))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != end-start+1 {
		return nil, fmt.Errorf("invalid S3 range length")
	}
	return data, nil
}

func (s *S3Storage) GetRange(ctx context.Context, key string, offset int64, limit int) ([]byte, int64, error) {
	if err := validateLogRange(key, offset, limit); err != nil {
		return nil, 0, err
	}
	head, err := s.client.StatObject(ctx, s.bucket, s.objectKey(key), minio.StatObjectOptions{})
	if err != nil {
		return nil, 0, mapS3Error(err)
	}
	size := head.Size
	etag := head.ETag
	if size < 11 || etag == "" {
		return nil, 0, fmt.Errorf("invalid legacy log envelope")
	}
	// Data is the first member of the canonical remoteDocument envelope. Its
	// closing quote is in the bounded trailer because base64 contains no quotes.
	tailStart := size - 4096
	if tailStart < 0 {
		tailStart = 0
	}
	tail, err := s.getLegacyObjectRange(ctx, key, etag, tailStart, size-1)
	if err != nil {
		return nil, 0, err
	}
	if tailStart > 0 {
		prefix, err := s.getLegacyObjectRange(ctx, key, etag, 0, 8)
		if err != nil {
			return nil, 0, err
		}
		if !bytes.Equal(prefix, []byte(`{"data":"`)) {
			return nil, 0, ErrUnsupported
		}
	}
	quote := bytes.IndexByte(tail, '"')
	if tailStart == 0 {
		if !bytes.HasPrefix(tail, []byte(`{"data":"`)) {
			var doc remoteDocument
			if json.Unmarshal(tail, &doc) == nil {
				if doc.Deleted || doc.Expires > 0 && doc.Expires <= time.Now().Unix() {
					return nil, 0, ErrNotFound
				}
				if bytes.HasPrefix(tail, []byte(`{"data":null,`)) {
					return []byte{}, 0, nil
				}
			}
			return nil, 0, ErrUnsupported
		}
		quote = bytes.IndexByte(tail[9:], '"')
		if quote < 0 {
			return nil, 0, ErrUnsupported
		}
		quote += 9
	} else if quote < 0 {
		return nil, 0, ErrUnsupported
	}
	dataEnd := tailStart + int64(quote)
	var metadata remoteDocument
	if err = json.Unmarshal(append([]byte(`{"data":"`), tail[quote:]...), &metadata); err != nil {
		return nil, 0, fmt.Errorf("legacy log metadata: %w", err)
	}
	if metadata.Deleted || metadata.Expires > 0 && metadata.Expires <= time.Now().Unix() {
		return nil, 0, ErrNotFound
	}
	encodedLen := dataEnd - 9
	if encodedLen < 0 || encodedLen%4 != 0 {
		return nil, 0, fmt.Errorf("invalid legacy base64 length")
	}
	total := encodedLen / 4 * 3
	for i := quote - 1; i >= 0 && tail[i] == '='; i-- {
		total--
	}
	if offset >= total || limit == 0 {
		return []byte{}, total, nil
	}
	end := offset + int64(limit)
	if end > total {
		end = total
	}
	blockStart := offset / 3 * 4
	blockEnd := (end + 2) / 3 * 4
	encoded, err := s.getLegacyObjectRange(ctx, key, etag, 9+blockStart, 9+blockEnd-1)
	if err != nil {
		return nil, 0, err
	}
	decoded, err := base64.StdEncoding.DecodeString(string(encoded))
	if err != nil {
		return nil, 0, err
	}
	skip := offset - blockStart/4*3
	if skip+end-offset > int64(len(decoded)) {
		return nil, 0, fmt.Errorf("invalid legacy log payload")
	}
	return decoded[skip : skip+end-offset], total, nil
}

// LegacyLogRemover removes only the legacy log payload without reading it. The
// caller must first install the durable manifest deletion fence, since physical
// removal alone cannot prevent a delayed writer from recreating an object.
type LegacyLogRemover interface {
	RemoveLegacyJobLog(context.Context, string) error
}

func legacyLogKey(id string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("empty job log id")
	}
	key := "job_logs:" + id
	if err := validateKey(key); err != nil {
		return "", err
	}
	return key, nil
}

func (s *FilesystemStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	key, err := legacyLogKey(id)
	if err != nil {
		return err
	}
	return s.Delete(ctx, key)
}

func (s *S3Storage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	key, err := legacyLogKey(id)
	if err != nil {
		return err
	}
	err = mapS3Error(s.client.RemoveObject(ctx, s.bucket, s.objectKey(key), minio.RemoveObjectOptions{}))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

type legacyCouchbaseRemover interface {
	Remove(string, *gocb.RemoveOptions) (*gocb.MutationResult, error)
}

func removeLegacyCouchbaseLog(ctx context.Context, collection legacyCouchbaseRemover, prefix, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := legacyLogKey(id)
	if err != nil {
		return err
	}
	_, err = collection.Remove(prefix+key, &gocb.RemoveOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil
	}
	return err
}

func (c *CouchbaseStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	return removeLegacyCouchbaseLog(ctx, c.collection, c.prefix, id)
}
