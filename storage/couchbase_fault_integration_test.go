package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// This suite controls only the explicitly named, disposable Couchbase service.
// It never substitutes an in-memory KV implementation for the remote server.
func TestCouchbaseRemoteFaultRecovery(t *testing.T) {
	container := os.Getenv("OWLCRON_TEST_COUCHBASE_CONTAINER")
	if container == "" {
		t.Skip("set OWLCRON_TEST_COUCHBASE_CONTAINER to enable destructive isolated-service fault acceptance")
	}
	conn := os.Getenv("OWLCRON_TEST_COUCHBASE_CONNECTION")
	if conn == "" {
		t.Fatal("Couchbase fault acceptance requires OWLCRON_TEST_COUCHBASE_CONNECTION")
	}
	prefix := fmt.Sprintf("fault-%d:", time.Now().UnixNano())
	cfg := &StorageConfig{Engine: "Couchbase", Options: map[string]interface{}{"connection_string": conn, "bucket": "owlcron-integration", "username": "Administrator", "password": "owlcron-test-secret", "prefix": prefix}}
	control := newRemoteContainerControl(t, container)

	open := func(t *testing.T) *CouchbaseStorage {
		t.Helper()
		s, err := NewCouchbaseStorage(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a, b := open(t), open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	if err := a.Put(ctx, "durable", []byte("before-fault")); err != nil {
		t.Fatal(err)
	}
	ready := func(t *testing.T, store *CouchbaseStorage) {
		t.Helper()
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			readCtx, cancel := context.WithTimeout(ctx, time.Second)
			data, err := store.Get(readCtx, "durable")
			cancel()
			if err == nil && string(data) == "before-fault" {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("Couchbase did not recover the committed pre-fault record within 45 seconds")
	}
	assertUnavailable := func(t *testing.T) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, 12)
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				opCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
				defer cancel()
				if i%2 == 0 {
					_, err := a.Get(opCtx, "durable")
					errs <- err
				} else {
					errs <- b.Put(opCtx, "uncertain/"+strconv.Itoa(i), []byte("attempt"))
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err == nil {
				t.Error("unavailable Couchbase acknowledged an operation")
			}
		}
		// A canceled operation must never be converted into ErrNotFound or a CAS conflict.
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := a.Get(canceled, "durable"); err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
			t.Errorf("canceled remote read lost failure: %v", err)
		}
	}
	t.Run("PausedServerHonorsConcurrentDeadlines", func(t *testing.T) {
		control(t, "pause")
		t.Cleanup(func() { control(t, "unpause") })
		began := time.Now()
		assertUnavailable(t)
		if time.Since(began) > 5*time.Second {
			t.Fatal("concurrent storage deadlines did not bound paused-server operations")
		}
	})
	ready(t, a)
	ready(t, b)
	t.Run("DisconnectedServerReconnects", func(t *testing.T) {
		control(t, "stop")
		t.Cleanup(func() { control(t, "start") })
		assertUnavailable(t)
	})
	ready(t, a)
	ready(t, b)
	// The stale CAS is captured before a real server restart, not synthesized.
	if err := a.Put(ctx, "lease", []byte("old-owner")); err != nil {
		t.Fatal(err)
	}
	_, staleVersion, err := a.read(ctx, "lease")
	if err != nil {
		t.Fatal(err)
	}
	control(t, "restart")
	ready(t, a)
	ready(t, b)
	c := open(t)
	ready(t, c)
	if err := b.Update(ctx, "lease", func([]byte) ([]byte, error) { return []byte("new-owner"), nil }); err != nil {
		t.Fatal(err)
	}
	raw, _, err := b.read(ctx, "lease")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.compareAndSwap(ctx, "lease", raw, staleVersion); !errors.Is(err, ErrConflict) {
		t.Fatalf("restarted server accepted stale owner CAS: %v", err)
	}
	if data, err := c.Get(ctx, "lease"); err != nil || string(data) != "new-owner" {
		t.Fatalf("lease takeover lost: %q %v", data, err)
	}
	// Cross-client CAS updates must remain complete after both timeout and reconnect.
	exerciseStorage(t, a, c)
}
