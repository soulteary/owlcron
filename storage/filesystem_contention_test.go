package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

type filesystemContentionContext struct {
	context.Context
	once    sync.Once
	checked chan struct{}
}

func (c *filesystemContentionContext) Err() error {
	first := false
	c.once.Do(func() { first = true; close(c.checked) })
	if first {
		return nil
	}
	return c.Context.Err()
}

// The holder/release barrier makes this about cancellation of local lock waits,
// not an assertion that a slow filesystem happens to finish within a deadline.
func TestFilesystemContentionLocalWaitHonorsCancellation(t *testing.T) {
	for _, write := range []bool{false, true} {
		name := "Get"
		if write {
			name = "Put"
		}
		t.Run(name, func(t *testing.T) {
			store, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.Put(context.Background(), "record", []byte(`"before"`)); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			released := false
			release := func() {
				if !released {
					released = true
					store.mu.Unlock()
				}
			}
			defer release()
			baseContext, cancel := context.WithCancel(context.Background())
			ctx := &filesystemContentionContext{Context: baseContext, checked: make(chan struct{})}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if write {
					result <- store.Put(ctx, "record", []byte(`"after"`))
				} else {
					_, e := store.Get(ctx, "record")
					result <- e
				}
			}()
			// The first successful ctx check is a barrier immediately before
			// withLock must contend on the mutex held by this goroutine.
			<-ctx.checked
			cancel()
			select {
			case err = <-result:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("waiting operation returned %v, want context.Canceled", err)
				}
			case <-time.After(500 * time.Millisecond):
				release()
				err = <-result
				t.Errorf("local lock wait ignored cancellation until holder released; eventual result %v", err)
			}
			release()
			data, err := store.Get(context.Background(), "record")
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != `"before"` {
				t.Fatalf("canceled waiter mutated record: %s", data)
			}
		})
	}
}

func TestFilesystemContentionFileLockHonorsDeadline(t *testing.T) {
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(store.baseDir, ".storage.lock"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- store.Put(ctx, "record", []byte(`true`)) }()
	select {
	case err = <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("file lock waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("file lock waiter did not observe deadline")
	}
}

// A future shared-reader optimization must still replay the committed redo
// journal under an exclusive lock before exposing any record to a reader.
func TestFilesystemContentionReadersRecoverCommittedJournal(t *testing.T) {
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Put(context.Background(), "record", []byte(`"before"`)); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal([]operation{{Op: "put", Key: "record", Data: []byte(`"committed"`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err = writeAtomic(filepath.Join(store.baseDir, ".transaction.json"), raw); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := store.Get(context.Background(), "record")
			if err != nil {
				failures <- err
			} else if string(data) != `"committed"` {
				failures <- fmt.Errorf("observed unrecovered record %s", data)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if _, err = os.Stat(filepath.Join(store.baseDir, ".transaction.json")); !os.IsNotExist(err) {
		t.Errorf("recovered journal remains: %v", err)
	}
}

func TestFilesystemContentionCanceledWaitersDoNotLeakGate(t *testing.T) {
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	const waiters = 64
	results := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() { results <- store.Put(ctx, "must-not-exist", []byte(`true`)) }()
	}
	cancel()
	for i := 0; i < waiters; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("canceled waiter: %v", err)
			}
		case <-time.After(time.Second):
			store.mu.Unlock()
			t.Fatal("canceled waiter remained blocked")
		}
	}
	store.mu.Unlock()
	live, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = store.Put(live, "live", []byte(`true`)); err != nil {
		t.Fatal(err)
	}
	if exists, err := store.Exists(live, "must-not-exist"); err != nil || exists {
		t.Fatalf("canceled writes committed: exists=%v err=%v", exists, err)
	}
}
