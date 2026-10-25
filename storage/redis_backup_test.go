package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	minio "github.com/soulteary/otterio-sdk/v7"
)

func TestRedisRealBackupRoundTrip(t *testing.T) {
	ctx := context.Background()
	source := openRedisRealTestStore(t, redisRealTestConfig(t))
	expires := time.Now().Unix() + 3600
	records := map[string][]byte{
		"users:admin":          []byte(`{"id":"admin","username":"admin","password":"hash"}`),
		"users":                []byte(`[{"id":"admin","username":"admin"}]`),
		"global/state":         []byte(`{"cursors":{"event":123},"enabled":true,"_lease_owner":"retired-generation"}`),
		"jobs:done":            []byte(`{"id":"done","complete":true}`),
		"deleted_jobs:removed": []byte(`{"deleted":true}`),
		"binary":               {0, 1, 255, '\n', '\r'},
		"job_logs:legacy":      []byte("plain legacy log\n"),
		"global/event_queue/a": []byte(`[123,456]`),
	}
	for key, data := range records {
		if err := source.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Expire(ctx, "users:admin", expires); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"expired", "storage-tombstone", "global/master_lease"} {
		if err := source.Put(ctx, key, []byte(`{"owner":"retired"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Expire(ctx, "expired", time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	if err := source.Delete(ctx, "storage-tombstone"); err != nil {
		t.Fatal(err)
	}
	logData := bytes.Repeat([]byte{0, 255, 'x', '\n'}, JobLogChunkSize/4+12)
	if _, err := JobLogAppend(ctx, source, "binary-log", 0, logData[:JobLogChunkSize], false, JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	if _, err := JobLogAppend(ctx, source, "binary-log", JobLogChunkSize, logData[JobLogChunkSize:], true, JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	if _, err := JobLogAppend(ctx, source, "removed-log", 0, []byte("removed"), true, JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	if err := JobLogDelete(ctx, source, "removed-log"); err != nil {
		t.Fatal(err)
	}
	backup, err := Export(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Format != "owlcron-storage-v1" || backup.Records["users:admin"].Expires != expires {
		t.Fatalf("backup format/expiry changed: %#v", backup)
	}
	for _, key := range []string{"expired", "storage-tombstone", "global/master_lease"} {
		if _, exists := backup.Records[key]; exists {
			t.Fatalf("backup exported transient/expired/deleted record %s", key)
		}
	}
	if !bytes.Equal(backup.Records["binary"].Data, records["binary"]) {
		t.Fatal("backup changed binary record")
	}
	var state map[string]interface{}
	if err := json.Unmarshal(backup.Records["global/state"].Data, &state); err != nil || state["_lease_owner"] != nil {
		t.Fatalf("backup retained retired lease generation: %s %v", backup.Records["global/state"].Data, err)
	}
	// Exercise the exact on-disk format, not a Redis-specific backup variant.
	var streamed bytes.Buffer
	count, err := ExportTo(ctx, source, &streamed)
	if err != nil || count != len(backup.Records) {
		t.Fatalf("streaming backup: %d %v", count, err)
	}
	var decoded Backup
	if err := json.Unmarshal(streamed.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	filesystem, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Import(ctx, filesystem, &decoded); err != nil {
		t.Fatalf("Redis -> Filesystem restore: %v", err)
	}
	fromFilesystem, err := Export(ctx, filesystem)
	if err != nil {
		t.Fatal(err)
	}
	destination := openRedisRealTestStore(t, redisRealTestConfig(t))
	if err := Import(ctx, destination, fromFilesystem); err != nil {
		t.Fatalf("Filesystem -> Redis restore: %v", err)
	}
	for _, target := range []Storage{filesystem, destination} {
		for key, entry := range decoded.Records {
			got, err := target.Get(ctx, key)
			if err != nil || !bytes.Equal(got, entry.Data) {
				t.Fatalf("round trip changed %s: %q %v", key, got, err)
			}
		}
		got, manifest, err := JobLogRead(ctx, target, "binary-log", 0, len(logData)+1)
		if err != nil || !manifest.Final || !bytes.Equal(got, logData) {
			t.Fatalf("round trip lost log chunks/final marker: %d %#v %v", len(got), manifest, err)
		}
		manifest, err = JobLogInfo(ctx, target, "removed-log")
		if err != nil || !manifest.Deleted {
			t.Fatalf("round trip lost application log deletion fence: %#v %v", manifest, err)
		}
		if _, err := JobLogAppend(ctx, target, "removed-log", 0, []byte("late writer"), false, JobLogAbsoluteMax); !errors.Is(err, ErrJobLogDeleted) {
			t.Fatalf("restored deleted log was resurrected: %v", err)
		}
	}
	doc, _, err := destination.readDocument(ctx, "users:admin")
	if err != nil || doc.Expires != expires {
		t.Fatalf("restore omitted logical expiration: %d %v", doc.Expires, err)
	}
	if _, err := destination.Get(ctx, "global/master_lease"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("restore retained stale master lease: %v", err)
	}
}

type redisInterruptedImport struct {
	Storage
	writes int
}

func (s *redisInterruptedImport) Put(ctx context.Context, key string, value []byte) error {
	s.writes++
	if s.writes == 2 {
		return fmt.Errorf("interrupted administrative restore")
	}
	return s.Storage.Put(ctx, key, value)
}

func TestRedisRealBackupInterruptedImportCanResume(t *testing.T) {
	ctx := context.Background()
	destination := openRedisRealTestStore(t, redisRealTestConfig(t))
	backup := &Backup{Format: "owlcron-storage-v1", Records: map[string]BackupEntry{
		"users:admin": {Data: []byte(`{"id":"admin"}`)},
		"users:other": {Data: []byte(`{"id":"other"}`)},
		"users":       {Data: []byte(`[{"id":"admin"},{"id":"other"}]`)},
	}}
	if err := Import(ctx, &redisInterruptedImport{Storage: destination}, backup); err == nil {
		t.Fatal("interrupted import reported success")
	}
	if exists, err := destination.Exists(ctx, "users"); err != nil || exists {
		t.Fatalf("interrupted import advertised incomplete index: %v %v", exists, err)
	}
	if err := Import(ctx, destination, backup); err != nil {
		t.Fatalf("retry restore: %v", err)
	}
	for key, entry := range backup.Records {
		got, err := destination.Get(ctx, key)
		if err != nil || !bytes.Equal(got, entry.Data) {
			t.Fatalf("resumed restore lost %s: %q %v", key, got, err)
		}
	}
}

type redisBackupFaultHook struct{ after func(redis.Cmder, error) }

func (h redisBackupFaultHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h redisBackupFaultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.after(cmd, err)
		return err
	}
}
func (h redisBackupFaultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRedisRealBackupTransportRecovery(t *testing.T) {
	config := redisRealTestConfig(t)
	gate := newRedisFaultGate(t, config.Options["addr"].(string))
	defer gate.mode.Store(0)
	config.Options["addr"] = gate.addr
	source := openRedisRealTestStore(t, config)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	backup := &Backup{Format: "owlcron-storage-v1", Records: map[string]BackupEntry{
		"users:admin": {Data: []byte(`{"id":"admin"}`)},
		"users:other": {Data: []byte(`{"id":"other"}`)},
		"users":       {Data: []byte(`[{"id":"admin"},{"id":"other"}]`)},
	}}
	if err := Import(ctx, source, backup); err != nil {
		t.Fatal(err)
	}
	// The SCAN reply really comes from Redis; disconnect before the following
	// record read. Export must return the transport failure, never success with
	// records silently omitted from an apparently complete backup.
	var failExport atomic.Bool
	source.client.AddHook(redisBackupFaultHook{after: func(cmd redis.Cmder, err error) {
		if err == nil && cmd.Name() == "scan" && failExport.CompareAndSwap(true, false) {
			gate.mode.Store(2)
		}
	}})
	failExport.Store(true)
	var partial bytes.Buffer
	if _, err := ExportTo(ctx, source, &partial); err == nil {
		t.Fatal("export reported success after record-read transport loss")
	} else {
		assertRedisFault(t, err)
	}
	if failExport.Load() || gate.hits.Load() == 0 {
		t.Fatal("backup failure did not cross the real SCAN/GET transport")
	}
	gate.mode.Store(0)
	var completed bytes.Buffer
	if count, err := ExportTo(ctx, source, &completed); err != nil || count != len(backup.Records) {
		t.Fatalf("export did not recover after reconnect: %d %v", count, err)
	}
	var restored Backup
	if err := json.Unmarshal(completed.Bytes(), &restored); err != nil {
		t.Fatal(err)
	}
	destinationConfig := redisRealTestConfig(t)
	destinationConfig.Options["addr"] = gate.addr
	destination := openRedisRealTestStore(t, destinationConfig)
	// Interrupt after one canonical record has actually committed, before the
	// second write can reach Redis. The index must remain unpublished. This
	// emulates an interrupted offline restore, not a transaction rollback.
	var failImport atomic.Bool
	destination.client.AddHook(redisBackupFaultHook{after: func(cmd redis.Cmder, err error) {
		if err == nil && cmd.Name() == "set" && failImport.CompareAndSwap(true, false) {
			gate.mode.Store(2)
		}
	}})
	failImport.Store(true)
	if err := Import(ctx, destination, &restored); err == nil {
		t.Fatal("restore reported success after mid-import disconnect")
	} else {
		assertRedisFault(t, err)
	}
	if failImport.Load() {
		t.Fatal("restore did not commit its first record before disconnection")
	}
	gate.mode.Store(0)
	if raw, err := destination.Get(ctx, "users:admin"); err != nil || !bytes.Equal(raw, backup.Records["users:admin"].Data) {
		t.Fatalf("first confirmed restore record missing: %q %v", raw, err)
	}
	if exists, err := destination.Exists(ctx, "users"); err != nil || exists {
		t.Fatalf("interrupted network restore advertised incomplete index: %v %v", exists, err)
	}
	if err := Import(ctx, destination, &restored); err != nil {
		t.Fatalf("restore did not recover after reconnect: %v", err)
	}
	for key, entry := range backup.Records {
		got, err := destination.Get(ctx, key)
		if err != nil || !bytes.Equal(got, entry.Data) {
			t.Fatalf("recovered network restore lost %s: %q %v", key, got, err)
		}
	}
}

func TestRedisRealBackupExpiryTransportRecovery(t *testing.T) {
	config := redisRealTestConfig(t)
	direct := openRedisRealTestStore(t, config)
	gate := newRedisFaultGate(t, config.Options["addr"].(string))
	defer gate.mode.Store(0)
	config.Options["addr"] = gate.addr
	destination := openRedisRealTestStore(t, config)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	expired, future := time.Now().Unix()-60, time.Now().Unix()+3600
	backup := &Backup{Format: "owlcron-storage-v1", Records: map[string]BackupEntry{
		"records:expired": {Data: []byte("must stay expired"), Expires: expired},
		"records:future":  {Data: []byte("future record"), Expires: future},
		"records":         {Data: []byte(`[{"id":"expired"},{"id":"future"}]`)},
	}}
	var reads atomic.Int32
	destination.client.AddHook(redisBackupFaultHook{after: func(cmd redis.Cmder, err error) {
		if cmd.Name() != "get" || cmd.Args()[1] != destination.prefix+"records:expired" {
			return
		}
		// Count Put's initial missing-key read too: its real Redis reply is
		// redis.Nil, while Expire's following read must succeed.
		if reads.Add(1) == 2 && err == nil {
			// Put already committed every record and index. The second read is
			// Expire's CAS read: lose its following write on the real transport.
			gate.mode.Store(2)
		}
	}})
	if err := Import(ctx, destination, backup); err == nil {
		t.Fatal("restore reported success after expiry-write transport loss")
	} else {
		assertRedisFault(t, err)
	}
	if reads.Load() != 2 || gate.hits.Load() == 0 {
		t.Fatal("restore did not fail during the real expiry CAS")
	}
	for key, entry := range backup.Records {
		data, err := direct.Get(ctx, key)
		if err != nil || !bytes.Equal(data, entry.Data) {
			t.Fatalf("expiry failure happened before all record writes: %s %q %v", key, data, err)
		}
	}
	doc, _, err := direct.readDocument(ctx, "records:expired")
	if err != nil || doc.Expires != 0 {
		t.Fatalf("failed expiry CAS unexpectedly committed: %d %v", doc.Expires, err)
	}
	gate.mode.Store(0)
	if err := Import(ctx, destination, backup); err != nil {
		t.Fatalf("retry after expiry-write failure: %v", err)
	}
	for key, timestamp := range map[string]int64{"records:expired": expired, "records:future": future} {
		doc, _, err := direct.readDocument(ctx, key)
		if err != nil || doc.Expires != timestamp {
			t.Fatalf("retry changed absolute expiry for %s: %d, want %d: %v", key, doc.Expires, timestamp, err)
		}
	}
	if _, err := direct.Get(ctx, "records:expired"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry revived an already expired record: %v", err)
	}
	if err := direct.Expire(ctx, "records:expired", future); !errors.Is(err, ErrNotFound) {
		t.Fatalf("extending the recovered expiry revived its record: %v", err)
	}
	if data, err := direct.Get(ctx, "records:future"); err != nil || !bytes.Equal(data, backup.Records["records:future"].Data) {
		t.Fatalf("retry lost the future record: %q %v", data, err)
	}
}

func TestRedisRealBackupCrossBackend(t *testing.T) {
	// These are the same disposable services and credentials as the existing
	// real storage suite. Each direction uses its own empty namespace.
	_ = redisRealTestConfig(t)
	for _, backend := range []struct {
		name, variable string
	}{
		{"S3", "OWLCRON_TEST_S3_ENDPOINT"},
		{"Couchbase", "OWLCRON_TEST_COUCHBASE_CONNECTION"},
	} {
		t.Run(backend.name, func(t *testing.T) {
			endpoint := os.Getenv(backend.variable)
			if endpoint == "" {
				t.Skipf("set %s for real Redis/%s backup migration", backend.variable, backend.name)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			prefix := fmt.Sprintf("redis-backup-%d:", time.Now().UnixNano())
			var target Storage
			if backend.name == "S3" {
				store, err := NewS3Storage(&StorageConfig{Engine: "S3", Options: map[string]interface{}{
					"endpoint": endpoint, "bucket": "owlcron-integration", "prefix": prefix,
					"access_key": "owlcron-test", "secret_key": "owlcron-test-secret",
				}})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.client.MakeBucket(ctx, store.bucket, minio.MakeBucketOptions{}); err != nil {
					if exists, checkErr := store.client.BucketExists(ctx, store.bucket); checkErr != nil || !exists {
						t.Fatal(err)
					}
				}
				target = store
			} else {
				store, err := NewCouchbaseStorage(&StorageConfig{Engine: "Couchbase", Options: map[string]interface{}{
					"connection_string": endpoint, "bucket": "owlcron-integration", "prefix": prefix,
					"username": "Administrator", "password": "owlcron-test-secret",
				}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				target = store
			}
			source := openRedisRealTestStore(t, redisRealTestConfig(t))
			expires := time.Now().Unix() + 3600
			for key, data := range map[string][]byte{
				"users:admin":          []byte(`{"id":"admin","username":"admin"}`),
				"users":                []byte(`[{"id":"admin","username":"admin"}]`),
				"global/state":         []byte(`{"cursors":{"event":123},"_lease_owner":"retired"}`),
				"global/master_lease":  []byte(`{"owner":"retired"}`),
				"deleted_jobs:removed": []byte(`{"deleted":true}`),
				"binary":               {0, 255, '\n', '\r'},
				"expired":              []byte("expired"),
				"storage-tombstone":    []byte("deleted"),
			} {
				if err := source.Put(ctx, key, data); err != nil {
					t.Fatal(err)
				}
			}
			if err := source.Expire(ctx, "users:admin", expires); err != nil {
				t.Fatal(err)
			}
			if err := source.Expire(ctx, "expired", time.Now().Unix()-1); err != nil {
				t.Fatal(err)
			}
			if err := source.Delete(ctx, "storage-tombstone"); err != nil {
				t.Fatal(err)
			}
			logData := bytes.Repeat([]byte{0, 255, 'x', '\n'}, JobLogChunkSize/4+12)
			for at := 0; at < len(logData); at += JobLogChunkSize {
				end := min(at+JobLogChunkSize, len(logData))
				if _, err := JobLogAppend(ctx, source, "binary-log", int64(at), logData[at:end], end == len(logData), JobLogAbsoluteMax); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := JobLogAppend(ctx, source, "removed-log", 0, []byte("removed"), true, JobLogAbsoluteMax); err != nil {
				t.Fatal(err)
			}
			if err := JobLogDelete(ctx, source, "removed-log"); err != nil {
				t.Fatal(err)
			}
			backup, err := Export(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"expired", "storage-tombstone", "global/master_lease"} {
				if _, exists := backup.Records[key]; exists {
					t.Fatalf("backup retained expired/deleted/transient key %s", key)
				}
			}
			if bytes.Contains(backup.Records["global/state"].Data, []byte("_lease_owner")) {
				t.Fatal("backup retained the retired leadership owner")
			}
			if err := Import(ctx, target, backup); err != nil {
				t.Fatalf("Redis -> %s: %v", backend.name, err)
			}
			assertRedisCrossBackendBackup(t, ctx, target, backup, logData)
			// This record originates on the other backend, proving the reverse
			// export includes new backend data as well as the Redis-origin fixture.
			backendData := []byte{255, 0, 'b', '\n'}
			if err := target.Put(ctx, "backend-binary", backendData); err != nil {
				t.Fatal(err)
			}
			if err := target.Expire(ctx, "backend-binary", expires); err != nil {
				t.Fatal(err)
			}
			backup.Records["backend-binary"] = BackupEntry{Data: backendData, Expires: expires}
			reverse, err := Export(ctx, target)
			if err != nil {
				t.Fatalf("export %s: %v", backend.name, err)
			}
			destination := openRedisRealTestStore(t, redisRealTestConfig(t))
			if err := Import(ctx, destination, reverse); err != nil {
				t.Fatalf("%s -> Redis: %v", backend.name, err)
			}
			assertRedisCrossBackendBackup(t, ctx, destination, backup, logData)
		})
	}
}

func assertRedisCrossBackendBackup(t *testing.T, ctx context.Context, store Storage, expected *Backup, logData []byte) {
	t.Helper()
	actual, err := Export(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Format != expected.Format || len(actual.Records) != len(expected.Records) {
		t.Fatalf("cross-backend backup changed format or record count: %s, %d want %d", actual.Format, len(actual.Records), len(expected.Records))
	}
	for key, want := range expected.Records {
		got, exists := actual.Records[key]
		if !exists || !bytes.Equal(got.Data, want.Data) || got.Expires != want.Expires {
			t.Fatalf("cross-backend restore changed %s: data=%q expires=%d want data=%q expires=%d", key, got.Data, got.Expires, want.Data, want.Expires)
		}
	}
	if data, manifest, err := JobLogRead(ctx, store, "binary-log", 0, len(logData)+1); err != nil || !manifest.Final || !bytes.Equal(data, logData) {
		t.Fatalf("cross-backend restore lost chunked log: %d %#v %v", len(data), manifest, err)
	}
	if _, err := JobLogAppend(ctx, store, "removed-log", 0, []byte("late writer"), false, JobLogAbsoluteMax); !errors.Is(err, ErrJobLogDeleted) {
		t.Fatalf("cross-backend restore lost log deletion fence: %v", err)
	}
	for _, key := range []string{"expired", "storage-tombstone", "global/master_lease"} {
		if _, err := store.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-backend restore revived %s: %v", key, err)
		}
	}
}
