package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FilesystemTransaction struct {
	storage *FilesystemStorage
	ops     []operation
	mu      sync.Mutex
	closed  bool
}
type operation struct {
	Op   string `json:"op"`
	Key  string `json:"key"`
	Data []byte `json:"data,omitempty"`
}

func NewFilesystemTransaction(fs *FilesystemStorage) *FilesystemTransaction {
	return &FilesystemTransaction{storage: fs}
}
func (t *FilesystemTransaction) Get(key string) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fmt.Errorf("transaction closed")
	}
	for i := len(t.ops) - 1; i >= 0; i-- {
		op := t.ops[i]
		if op.Key == key {
			if op.Op == "delete" {
				return nil, ErrNotFound
			}
			return append([]byte(nil), op.Data...), nil
		}
	}
	return t.storage.Get(context.Background(), key)
}
func (t *FilesystemTransaction) Put(key string, data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("transaction closed")
	}
	if err := validateKey(key); err != nil {
		return err
	}
	t.ops = append(t.ops, operation{Op: "put", Key: key, Data: append([]byte(nil), data...)})
	return nil
}
func (t *FilesystemTransaction) Delete(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("transaction closed")
	}
	if err := validateKey(key); err != nil {
		return err
	}
	t.ops = append(t.ops, operation{Op: "delete", Key: key})
	return nil
}
func (t *FilesystemTransaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("transaction closed")
	}
	err := t.storage.withLock(context.Background(), func() error {
		// Validate the whole batch before recording its commit decision.
		paths := make(map[string]bool, len(t.ops)*2)
		for _, op := range t.ops {
			path, err := t.storage.path(op.Key)
			if err != nil {
				return err
			}
			paths[path] = true
			expiryPath, err := t.storage.path("_expire/" + op.Key)
			if err != nil {
				return err
			}
			paths[expiryPath] = true
		}
		for path := range paths {
			for parent := filepath.Dir(path); parent != t.storage.baseDir; parent = filepath.Dir(parent) {
				if paths[parent] {
					return fmt.Errorf("transaction record overlaps a directory")
				}
			}
		}
		data, err := json.Marshal(t.ops)
		if err != nil {
			return err
		}
		if err = writeAtomic(filepath.Join(t.storage.baseDir, ".transaction.json"), data); err != nil {
			return err
		}
		// Once the durable journal is present the transaction is committed. If a
		// process dies during application, every subsequent operation replays it
		// under the process lock before it can observe data.
		t.closed = true
		return t.storage.recoverTransaction()
	})
	if err == nil {
		t.closed = true
		t.ops = nil
	}
	return err
}
func (t *FilesystemTransaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("transaction closed")
	}
	t.ops = nil
	t.closed = true
	return nil
}
func (fs *FilesystemStorage) recoverTransaction() error {
	path := filepath.Join(fs.baseDir, ".transaction.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var ops []operation
	if err = json.Unmarshal(data, &ops); err != nil {
		return fmt.Errorf("invalid transaction journal: %w", err)
	}
	for _, op := range ops {
		switch op.Op {
		case "put":
			err = fs.put(op.Key, op.Data)
		case "delete":
			err = fs.remove(op.Key)
			if err == nil {
				err = fs.remove("_expire/" + op.Key)
			}
		default:
			return fmt.Errorf("invalid transaction operation")
		}
		if err != nil {
			return err
		}
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	dir, err := os.Open(fs.baseDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
