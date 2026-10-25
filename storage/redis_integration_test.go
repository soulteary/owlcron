package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func redisRealTestConfig(t *testing.T) *StorageConfig {
	t.Helper()
	addr := os.Getenv("OWLCRON_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set OWLCRON_TEST_REDIS_ADDR to run real Redis 8.4+ tests")
	}
	return &StorageConfig{Engine: "Redis", Options: map[string]interface{}{
		"addr":     addr,
		"username": os.Getenv("OWLCRON_TEST_REDIS_USERNAME"),
		"password": os.Getenv("OWLCRON_TEST_REDIS_PASSWORD"),
		"prefix":   fmt.Sprintf("owlcron-test:%d:", time.Now().UnixNano()),
	}}
}

func openRedisRealTestStore(t *testing.T, config *StorageConfig) *RedisStorage {
	t.Helper()
	store, err := NewRedisStorage(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, err := store.client.Scan(ctx, cursor, redisGlobLiteral(store.prefix)+"*", 128).Result()
			if err != nil {
				t.Errorf("cleanup Redis test keys: %v", err)
				break
			}
			if len(keys) > 0 {
				if err := store.client.Unlink(ctx, keys...).Err(); err != nil {
					t.Errorf("cleanup Redis test keys: %v", err)
					break
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = store.Close()
	})
	return store
}

func TestRedisRealStorage(t *testing.T) {
	config := redisRealTestConfig(t)
	a := openRedisRealTestStore(t, config)
	b := openRedisRealTestStore(t, config)
	exerciseStorage(t, a, b)
	if opts := a.client.Options(); opts.MaxRetries != 0 || !opts.ContextTimeoutEnabled || opts.Username != config.Options["username"] {
		t.Fatalf("unsafe client options: retries=%d, deadlines=%v, user=%q", opts.MaxRetries, opts.ContextTimeoutEnabled, opts.Username)
	}
	if _, err := a.BeginTransaction(context.Background()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Redis must explicitly reject multi-key transactions: %v", err)
	}
}

func TestRedisRealExistingEnvelopeAndNativeCAS(t *testing.T) {
	store := openRedisRealTestStore(t, redisRealTestConfig(t))
	ctx := context.Background()
	// This is precisely the shared remoteDocument format, including arbitrary
	// binary data; no Redis-specific encoding, revision key or native TTL.
	raw := []byte(`{"data":"AP9hCg==","expires":0,"revision":"preexisting-revision"}`)
	if err := store.client.Set(ctx, store.prefix+"existing", raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	data, err := store.Get(ctx, "existing")
	if err != nil || !bytes.Equal(data, []byte{0, 255, 'a', '\n'}) {
		t.Fatalf("existing envelope unreadable: %q %v", data, err)
	}
	_, original, err := store.read(ctx, "existing")
	if err != nil || original != string(raw) {
		t.Fatalf("CAS token changed raw format: %q %v", original, err)
	}
	if err := store.Put(ctx, "existing", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := store.compareAndSwap(ctx, "existing", raw, original); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale IFEQ replaced current value: %v", err)
	}
	if err := store.compareAndSwap(ctx, "existing", raw, ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("NX replaced existing record: %v", err)
	}
	if err := store.compareAndSwap(ctx, "missing", raw, original); !errors.Is(err, ErrConflict) {
		t.Fatalf("IFEQ created missing record: %v", err)
	}
	if err := store.Delete(ctx, "existing"); err != nil {
		t.Fatal(err)
	}
	physical, err := store.client.Get(ctx, store.prefix+"existing").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var doc remoteDocument
	if err := json.Unmarshal(physical, &doc); err != nil || !doc.Deleted || len(doc.Revision) != 32 || doc.Data != nil {
		t.Fatalf("existing tombstone format lost: %s %v", physical, err)
	}
	if ttl, err := store.client.TTL(ctx, store.prefix+"existing").Result(); err != nil || ttl != -1 {
		t.Fatalf("native TTL can remove a CAS fence: %v %v", ttl, err)
	}
	var cursor uint64
	for {
		keys, next, err := store.client.Scan(ctx, cursor, redisGlobLiteral(store.prefix+redisCapabilityPrefix)+"*", 128).Result()
		if err != nil || len(keys) != 0 {
			t.Fatalf("startup leaked capability keys: %v %v", keys, err)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
}

func TestRedisRealCapabilityRecordsBackupRoundTrip(t *testing.T) {
	store := openRedisRealTestStore(t, redisRealTestConfig(t))
	source, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	records := map[string][]byte{
		"capability/foo": []byte("second"),
		"capability/redis-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {0, 1, 255},
	}
	for key, data := range records {
		if err := source.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	backup, err := Export(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := Import(ctx, store, backup); err != nil {
		t.Fatal(err)
	}
	for key, payload := range map[string]string{
		"capability/redis-00000000000000000000000000000000":              "first",
		"capability/redis-11111111111111111111111111111111":              "second",
		"capability/redis-22222222222222222222222222222222":              "must-not-write",
		redisCapabilityPrefix + "redis-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": "second",
	} {
		if err := store.client.Set(ctx, store.prefix+key, payload, 30*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"capability/foo", "capability/redis-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	for _, prefix := range []string{"", "capability/"} {
		keys, err := store.ListKeysPrefix(ctx, prefix)
		if err != nil || !reflect.DeepEqual(keys, want) {
			t.Fatalf("prefix %q lost imported application records: %v %v", prefix, keys, err)
		}
	}
	reexported, err := Export(ctx, store)
	if err != nil || len(reexported.Records) != len(records) {
		t.Fatalf("Redis re-export lost imported application records: %#v %v", reexported, err)
	}
	for key, data := range records {
		if !bytes.Equal(reexported.Records[key].Data, data) {
			t.Fatalf("Redis re-export changed %q", key)
		}
	}
}

func TestRedisRealPrefixIsolation(t *testing.T) {
	config := redisRealTestConfig(t)
	config.Options["prefix"] = config.Options["prefix"].(string) + "[*?]:"
	store := openRedisRealTestStore(t, config)
	ctx := context.Background()
	for _, key := range []string{"jobs:z", "jobs:a", "jobs:expired", "jobs:deleted", "deleted_jobs:retained"} {
		if err := store.Put(ctx, key, []byte(`{"deleted":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Expire(ctx, "jobs:expired", time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "jobs:deleted"); err != nil {
		t.Fatal(err)
	}
	// An unrelated logical namespace contains invalid data. Enumerating jobs
	// must never read or decode it, even when its physical prefix has glob chars.
	if err := store.client.Set(ctx, store.prefix+"other:invalid", "not-json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	keys, err := store.ListKeysPrefix(ctx, "jobs:")
	if err != nil || !reflect.DeepEqual(keys, []string{"jobs:a", "jobs:z"}) {
		t.Fatalf("prefix isolation/live filtering: %v %v", keys, err)
	}
	keys, err = store.ListKeysPrefix(ctx, "deleted_jobs:")
	if err != nil || !reflect.DeepEqual(keys, []string{"deleted_jobs:retained"}) {
		t.Fatalf("application deletion fence hidden: %v %v", keys, err)
	}
	if err := store.client.Set(ctx, store.prefix+"job_logs:legacy", strings.Repeat("invalid payload", 10000), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveLegacyJobLog(ctx, "legacy"); err != nil {
		t.Fatalf("legacy cleanup decoded a large payload: %v", err)
	}
	if exists, err := store.client.Exists(ctx, store.prefix+"job_logs:legacy").Result(); err != nil || exists != 0 {
		t.Fatalf("legacy UNLINK failed: %v %v", exists, err)
	}
}

func TestRedisRealConstructorDeadlineBudget(t *testing.T) {
	for _, cancelDuringProbe := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelDuringProbe), func(t *testing.T) {
			config := redisRealTestConfig(t)
			gate := newRedisFaultGate(t, config.Options["addr"].(string))
			config.Options["addr"] = gate.addr
			config.Options["read_timeout_ms"] = 2000
			config.Options["write_timeout_ms"] = 2000
			config.Options["startup_timeout_ms"] = 150
			if cancelDuringProbe {
				config.Options["startup_timeout_ms"] = 2000
			}
			// Handshake, PING and INFO come from real Redis. Its first probe SET
			// really commits, but the response stalls on the connection. Cleanup
			// must use the remaining startup budget, not a fresh background one.
			gate.mode.Store(4)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				store *RedisStorage
				err   error
			}
			done := make(chan result, 1)
			started := time.Now()
			go func() {
				store, err := NewRedisStorageContext(ctx, config)
				done <- result{store, err}
			}()
			if cancelDuringProbe {
				until := time.Now().Add(time.Second)
				for gate.hits.Load() == 0 {
					if time.Now().After(until) {
						t.Fatal("constructor did not reach its real capability SET")
					}
					time.Sleep(time.Millisecond)
				}
				cancel()
			}
			select {
			case outcome := <-done:
				if outcome.store != nil {
					_ = outcome.store.Close()
					t.Fatal("constructor returned storage after probe response loss")
				}
				want := context.DeadlineExceeded
				if cancelDuringProbe {
					want = context.Canceled
				}
				if !errors.Is(outcome.err, want) {
					t.Fatalf("constructor outcome=%v, want %v", outcome.err, want)
				}
				if time.Since(started) > 700*time.Millisecond {
					t.Fatalf("probe cleanup exceeded startup budget: %v", time.Since(started))
				}
			case <-time.After(time.Second):
				t.Fatal("constructor ignored startup budget while cleaning probe")
			}
			if gate.hits.Load() != 1 || gate.casWrites.Load() != 1 {
				t.Fatalf("probe replayed or failed before real CAS: stalls=%d writes=%d", gate.hits.Load(), gate.casWrites.Load())
			}
			gate.mode.Store(0)
			// Reopening succeeds and test cleanup removes the TTL-protected
			// abandoned private probe without exposing it as application data.
			_ = openRedisRealTestStore(t, config)
		})
	}
}
