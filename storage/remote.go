package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// versionedKV must reject writes whose version no longer matches the server.
// An empty version denotes insert-if-absent. Implementations never silently
// replace a failed CAS with an unconditional write.
type versionedKV interface {
	read(context.Context, string) ([]byte, string, error)
	compareAndSwap(context.Context, string, []byte, string) error
}
type remoteStorage struct{ kv versionedKV }
type remoteDocument struct {
	Data     []byte `json:"data"`
	Expires  int64  `json:"expires,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
	Revision string `json:"revision"`
}

func (r *remoteStorage) readDocument(ctx context.Context, key string) (remoteDocument, string, error) {
	if err := validateKey(key); err != nil {
		return remoteDocument{}, "", err
	}
	data, version, err := r.kv.read(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return remoteDocument{}, "", nil
	}
	if err != nil {
		return remoteDocument{}, "", err
	}
	var doc remoteDocument
	if err = json.Unmarshal(data, &doc); err != nil {
		return doc, "", fmt.Errorf("decode storage document: %w", err)
	}
	return doc, version, nil
}
func (r *remoteStorage) mutate(ctx context.Context, key string, fn func(*remoteDocument) error) error {
	for attempt := 0; attempt < 256; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		doc, version, err := r.readDocument(ctx, key)
		if err != nil {
			return err
		}
		if err = fn(&doc); err != nil {
			return err
		}
		revision := make([]byte, 16)
		if _, err = rand.Read(revision); err != nil {
			return err
		}
		doc.Revision = hex.EncodeToString(revision)
		data, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		err = r.kv.compareAndSwap(ctx, key, data, version)
		if !errors.Is(err, ErrConflict) {
			return err
		}
		timer := time.NewTimer(time.Duration(1+attempt%10) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("CAS retry limit: %w", ErrConflict)
}
func (r *remoteStorage) Get(ctx context.Context, key string) ([]byte, error) {
	doc, version, err := r.readDocument(ctx, key)
	if err != nil {
		return nil, err
	}
	if version == "" || doc.Deleted || (doc.Expires > 0 && doc.Expires <= time.Now().Unix()) {
		return nil, ErrNotFound
	}
	return append([]byte(nil), doc.Data...), nil
}
func (r *remoteStorage) Put(ctx context.Context, key string, data []byte) error {
	return r.mutate(ctx, key, func(doc *remoteDocument) error {
		doc.Data = append([]byte(nil), data...)
		doc.Deleted = false
		doc.Expires = 0
		return nil
	})
}
func (r *remoteStorage) Delete(ctx context.Context, key string) error {
	return r.mutate(ctx, key, func(doc *remoteDocument) error { doc.Data = nil; doc.Deleted = true; doc.Expires = 0; return nil })
}
func (r *remoteStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := r.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
func (r *remoteStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return r.mutate(ctx, key, func(doc *remoteDocument) error {
		data := doc.Data
		if doc.Deleted || (doc.Expires > 0 && doc.Expires <= time.Now().Unix()) {
			data = nil
		}
		next, err := fn(append([]byte(nil), data...))
		if err != nil {
			return err
		}
		doc.Data = next
		doc.Deleted = false
		doc.Expires = 0
		return nil
	})
}
func (r *remoteStorage) ListGet(ctx context.Context, key string, offset, limit int) ([]interface{}, error) {
	data, err := r.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return []interface{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	items := []interface{}{}
	if err = json.Unmarshal(data, &items); err != nil {
		return nil, err
	}
	return listSlice(items, offset, limit), nil
}
func (r *remoteStorage) ListPush(ctx context.Context, key string, item interface{}) error {
	return UpdateList(ctx, r, key, func(items []interface{}) ([]interface{}, error) { return append(items, item), nil })
}
func (r *remoteStorage) ListUnshift(ctx context.Context, key string, item interface{}) error {
	return UpdateList(ctx, r, key, func(items []interface{}) ([]interface{}, error) { return append([]interface{}{item}, items...), nil })
}
func (r *remoteStorage) pop(ctx context.Context, key string, last bool) (out interface{}, err error) {
	err = UpdateList(ctx, r, key, func(items []interface{}) ([]interface{}, error) {
		out = nil
		if len(items) == 0 {
			return items, nil
		}
		if last {
			out = items[len(items)-1]
			return items[:len(items)-1], nil
		}
		out = items[0]
		return items[1:], nil
	})
	return
}
func (r *remoteStorage) ListShift(ctx context.Context, key string) (interface{}, error) {
	return r.pop(ctx, key, false)
}
func (r *remoteStorage) ListPop(ctx context.Context, key string) (interface{}, error) {
	return r.pop(ctx, key, true)
}
func (r *remoteStorage) ListGetInfo(ctx context.Context, key string) (*ListInfo, error) {
	items, err := r.ListGet(ctx, key, 0, 0)
	if err != nil {
		return nil, err
	}
	return &ListInfo{Length: int64(len(items)), Items: items}, nil
}
func (r *remoteStorage) ListDelete(ctx context.Context, key string, recursive bool) error {
	return r.Delete(ctx, key)
}
func (r *remoteStorage) PutStream(ctx context.Context, key string, data []byte) error {
	return r.Put(ctx, key, data)
}
func (r *remoteStorage) GetStream(ctx context.Context, key string) ([]byte, error) {
	return r.Get(ctx, key)
}
func (r *remoteStorage) Expire(ctx context.Context, key string, timestamp int64) error {
	return r.mutate(ctx, key, func(doc *remoteDocument) error {
		if doc.Revision == "" || doc.Deleted || (doc.Expires > 0 && doc.Expires <= time.Now().Unix()) {
			return ErrNotFound
		}
		doc.Expires = timestamp
		if timestamp <= 0 {
			// Zero is reserved for no TTL in the envelope. Absolute Unix
			// timestamps at or before zero are expired, just as on Filesystem.
			doc.Expires = 1
		}
		return nil
	})
}
func (r *remoteStorage) BeginTransaction(ctx context.Context) (Transaction, error) {
	return nil, ErrUnsupported
}
func optionString(options map[string]interface{}, key, def string) string {
	v, ok := options[key].(string)
	if ok && v != "" {
		return v
	}
	return def
}
