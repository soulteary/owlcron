package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrNotFound = errors.New("key not found")
var filesystemLocks sync.Map

type FilesystemStorage struct {
	baseDir string
	mu      *filesystemMutex
}

func NewFilesystemStorage(config *StorageConfig) (*FilesystemStorage, error) {
	base := config.BaseDir
	if base == "" {
		base = "data"
	}
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(base, 0700); err != nil {
		return nil, err
	}
	// Resolve the root once; individual keys may never traverse a symlink.
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		return nil, err
	}
	lock, _ := filesystemLocks.LoadOrStore(base, &filesystemMutex{})
	fs := &FilesystemStorage{baseDir: base, mu: lock.(*filesystemMutex)}
	err = fs.withLock(context.Background(), func() error { return fs.recoverTransaction() })
	return fs, err
}

func validateKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || strings.ContainsRune(key, 0) {
		return fmt.Errorf("invalid storage key")
	}
	for _, p := range strings.Split(key, "/") {
		if p == "" || p == "." || p == ".." || strings.HasPrefix(p, ".") {
			return fmt.Errorf("invalid storage key")
		}
	}
	return nil
}

func (fs *FilesystemStorage) path(key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	path := filepath.Join(fs.baseDir, key)
	for p := path; p != fs.baseDir; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlink in storage key")
		}
		if err == nil && p != path && !info.IsDir() {
			return "", fmt.Errorf("non-directory in storage key")
		}
	}
	if _, err := os.Lstat(path + ".list"); err == nil {
		path += ".list"
	} else {
		path += ".json"
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("symlink in storage key")
	} else if err == nil && info.IsDir() {
		return "", fmt.Errorf("storage record path is a directory")
	}
	return path, nil
}

func (fs *FilesystemStorage) withLock(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fs.mu.LockContext(ctx); err != nil {
		return err
	}
	defer fs.mu.Unlock()
	lock, err := os.OpenFile(filepath.Join(fs.baseDir, ".storage.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = fs.recoverTransaction(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func (fs *FilesystemStorage) read(key string) ([]byte, error) {
	p, err := fs.path(key)
	if err != nil {
		return nil, err
	}
	// Expiry is checked on every read, including list mutations.
	ep, err := fs.path("_expire/" + key)
	if err != nil {
		return nil, err
	}
	if data, e := os.ReadFile(ep); e == nil {
		var ts int64
		if err := json.Unmarshal(data, &ts); err != nil {
			return nil, fmt.Errorf("invalid expiry metadata: %w", err)
		}
		if ts <= time.Now().Unix() {
			return nil, ErrNotFound
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return data, err
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".owlcron-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (fs *FilesystemStorage) put(key string, data []byte) error {
	p, err := fs.path(key)
	if err != nil {
		return err
	}
	if _, err := fs.path("_expire/" + key); err != nil {
		return err
	}
	if err = writeAtomic(p, data); err != nil {
		return err
	}
	return fs.remove("_expire/" + key)
}
func (fs *FilesystemStorage) remove(key string) error {
	p, err := fs.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(p))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (fs *FilesystemStorage) Get(ctx context.Context, key string) (out []byte, err error) {
	err = fs.withLock(ctx, func() error { var e error; out, e = fs.read(key); return e })
	return
}
func (fs *FilesystemStorage) Put(ctx context.Context, key string, data []byte) error {
	return fs.withLock(ctx, func() error { return fs.put(key, data) })
}
func (fs *FilesystemStorage) Delete(ctx context.Context, key string) error {
	return fs.withLock(ctx, func() error {
		if err := fs.remove(key); err != nil {
			return err
		}
		return fs.remove("_expire/" + key)
	})
}
func (fs *FilesystemStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := fs.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
func (fs *FilesystemStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return fs.withLock(ctx, func() error {
		data, err := fs.read(key)
		if errors.Is(err, ErrNotFound) {
			data = nil
		} else if err != nil {
			return err
		}
		next, err := fn(data)
		if err != nil {
			return err
		}
		return fs.put(key, next)
	})
}
func listSlice(items []interface{}, offset, limit int) []interface{} {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []interface{}{}
	}
	end := len(items)
	if limit > 0 && limit < len(items)-offset {
		end = offset + limit
	}
	return items[offset:end]
}
func (fs *FilesystemStorage) ListGet(ctx context.Context, key string, offset, limit int) ([]interface{}, error) {
	data, err := fs.Get(ctx, key)
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
func (fs *FilesystemStorage) ListPush(ctx context.Context, key string, item interface{}) error {
	return UpdateList(ctx, fs, key, func(items []interface{}) ([]interface{}, error) { return append(items, item), nil })
}
func (fs *FilesystemStorage) ListUnshift(ctx context.Context, key string, item interface{}) error {
	return UpdateList(ctx, fs, key, func(items []interface{}) ([]interface{}, error) { return append([]interface{}{item}, items...), nil })
}
func (fs *FilesystemStorage) pop(ctx context.Context, key string, last bool) (out interface{}, err error) {
	err = UpdateList(ctx, fs, key, func(items []interface{}) ([]interface{}, error) {
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
func (fs *FilesystemStorage) ListShift(ctx context.Context, key string) (interface{}, error) {
	return fs.pop(ctx, key, false)
}
func (fs *FilesystemStorage) ListPop(ctx context.Context, key string) (interface{}, error) {
	return fs.pop(ctx, key, true)
}
func (fs *FilesystemStorage) ListGetInfo(ctx context.Context, key string) (*ListInfo, error) {
	items, err := fs.ListGet(ctx, key, 0, 0)
	if err != nil {
		return nil, err
	}
	return &ListInfo{Length: int64(len(items)), Items: items}, nil
}
func (fs *FilesystemStorage) ListDelete(ctx context.Context, key string, recursive bool) error {
	return fs.Delete(ctx, key)
}
func (fs *FilesystemStorage) PutStream(ctx context.Context, key string, data []byte) error {
	return fs.Put(ctx, key, data)
}
func (fs *FilesystemStorage) GetStream(ctx context.Context, key string) ([]byte, error) {
	return fs.Get(ctx, key)
}
func (fs *FilesystemStorage) Expire(ctx context.Context, key string, timestamp int64) error {
	return fs.withLock(ctx, func() error {
		if _, err := fs.read(key); err != nil {
			return err
		}
		p, err := fs.path("_expire/" + key)
		if err != nil {
			return err
		}
		data, _ := json.Marshal(timestamp)
		return writeAtomic(p, data)
	})
}
func (fs *FilesystemStorage) BeginTransaction(ctx context.Context) (Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return NewFilesystemTransaction(fs), nil
}
