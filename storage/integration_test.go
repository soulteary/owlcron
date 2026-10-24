package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func exerciseStorage(t *testing.T, a, b Storage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("integration/%d", time.Now().UnixNano())
	if err := a.Put(ctx, prefix+"/blob", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	data, err := b.Get(ctx, prefix+"/blob")
	if err != nil || string(data) != "payload" {
		t.Fatalf("cross-client get: %q %v", data, err)
	}
	key := prefix + "/list"
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 1 {
				store = b
			}
			errs <- store.ListPush(ctx, key, i)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	info, err := b.ListGetInfo(ctx, key)
	if err != nil || info.Length != 40 {
		t.Fatalf("lost concurrent updates: %#v %v", info, err)
	}
	seen := map[float64]bool{}
	for i := 0; i < 40; i++ {
		v, err := a.ListShift(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		n, ok := v.(float64)
		if !ok || seen[n] {
			t.Fatalf("duplicate or invalid pop: %#v", v)
		}
		seen[n] = true
	}
	if err := a.Expire(ctx, prefix+"/blob", time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, prefix+"/blob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired read: %v", err)
	}
	if err := a.Expire(ctx, prefix+"/blob", time.Now().Unix()+300); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired record was revived by extending TTL: %v", err)
	}
	if err := b.Put(ctx, prefix+"/blob", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if data, err := a.Get(ctx, prefix+"/blob"); err != nil || string(data) != "new" {
		t.Fatalf("put should reset expiry: %q %v", data, err)
	}
	for _, timestamp := range []int64{0, -1} {
		if err := a.Expire(ctx, prefix+"/blob", timestamp); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Get(ctx, prefix+"/blob"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("past absolute timestamp %d did not expire: %v", timestamp, err)
		}
		if err := b.Put(ctx, prefix+"/blob", []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range []Storage{a, b} {
		if err := store.Delete(ctx, prefix+"/blob"); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := a.Exists(ctx, prefix+"/blob"); ok || err != nil {
		t.Fatalf("delete: %v %v", ok, err)
	}
	for _, invalid := range []string{"../outside", "/tmp/escape", "a/../escape", "a\\b", "a//b"} {
		if err := a.Put(ctx, invalid, []byte("bad")); err == nil {
			t.Fatalf("accepted unsafe key %q", invalid)
		}
	}
}

func TestFilesystemCrossInstanceAndRecovery(t *testing.T) {
	dir := t.TempDir()
	a, err := NewFilesystemStorage(&StorageConfig{BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewFilesystemStorage(&StorageConfig{BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	exerciseStorage(t, a, b)
	ctx := context.Background()
	if err := a.Put(ctx, "broken", []byte("invalid json")); err != nil {
		t.Fatal(err)
	}
	if err := b.ListPush(ctx, "broken", 1); err == nil {
		t.Fatal("corrupt list silently overwritten")
	}
	tx, _ := a.BeginTransaction(ctx)
	_ = tx.Put("tx/a", []byte("a"))
	_ = tx.Put("tx/b", []byte("b"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("tx/c", []byte("c")); err == nil {
		t.Fatal("closed transaction reusable")
	}
	// Emulate interruption after a committed journal was synced but before
	// applying its second operation. Reopening must replay the entire batch.
	journal, _ := json.Marshal([]operation{{Op: "put", Key: "recovery/a", Data: []byte("a")}, {Op: "put", Key: "recovery/b", Data: []byte("b")}})
	if err := writeAtomic(filepath.Join(dir, ".transaction.json"), journal); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFilesystemStorage(&StorageConfig{BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"recovery/a", "recovery/b"} {
		if _, err := reopened.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, "link/secret", []byte("bad")); err == nil {
		t.Fatal("followed symlink")
	}
}

func TestS3RealStorage(t *testing.T) {
	endpoint := os.Getenv("OWLCRON_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OWLCRON_TEST_S3_ENDPOINT to run real S3 test")
	}
	cfg := &StorageConfig{Engine: "S3", Options: map[string]interface{}{"endpoint": endpoint, "bucket": "owlcron-integration", "access_key": "owlcron-test", "secret_key": "owlcron-test-secret"}}
	a, err := NewS3Storage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = a.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(a.bucket)})
	if err != nil {
		// Tests may be repeated against the same isolated bucket.
		if _, e := a.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(a.bucket)}); e != nil {
			t.Fatal(err)
		}
	}
	b, err := NewS3Storage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	exerciseStorage(t, a, b)
	if _, err := a.BeginTransaction(ctx); !errors.Is(err, ErrUnsupported) {
		t.Fatal("S3 must explicitly reject multi-key transactions")
	}
	if err := a.Put(ctx, "global/state", []byte(`{"cursors":{"restored":987},"_lease_owner":"stale-generation"}`)); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, "global/master_lease", []byte(`{"owner":"stale-generation"}`)); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, "deleted_jobs:removed", []byte(`{"deleted":true}`)); err != nil {
		t.Fatal(err)
	}
	backup, err := Export(ctx, a)
	if err != nil {
		t.Fatalf("S3 backup export: %v", err)
	}
	if _, exists := backup.Records["global/master_lease"]; exists {
		t.Fatal("S3 backup exported a transient lease")
	}
	var state map[string]interface{}
	if err := json.Unmarshal(backup.Records["global/state"].Data, &state); err != nil || state["_lease_owner"] != nil {
		t.Fatal("S3 backup retained lease generation")
	}
	restored, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Import(ctx, restored, backup); err != nil {
		t.Fatalf("S3 backup restore: %v", err)
	}
	if deleted, err := restored.Exists(ctx, "deleted_jobs:removed"); err != nil || !deleted {
		t.Fatal("S3 backup dropped history deletion fence")
	}
	raw, err := restored.Get(ctx, "global/state")
	if err != nil || json.Unmarshal(raw, &state) != nil || state["cursors"].(map[string]interface{})["restored"] != float64(987) {
		t.Fatal("S3 restore lost scheduler cursor")
	}
}
func TestCouchbaseRealStorage(t *testing.T) {
	conn := os.Getenv("OWLCRON_TEST_COUCHBASE_CONNECTION")
	if conn == "" {
		t.Skip("set OWLCRON_TEST_COUCHBASE_CONNECTION to run real Couchbase test")
	}
	cfg := &StorageConfig{Engine: "Couchbase", Options: map[string]interface{}{"connection_string": conn, "bucket": "owlcron-integration", "username": "Administrator", "password": "owlcron-test-secret"}}
	a, err := NewCouchbaseStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewCouchbaseStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	exerciseStorage(t, a, b)
	backup, err := Export(context.Background(), a)
	if err != nil {
		t.Fatalf("remote backup scan: %v", err)
	}
	if len(backup.Records) == 0 {
		t.Fatal("remote backup empty")
	}
	restored, _ := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err = Import(context.Background(), restored, backup); err != nil {
		t.Fatal(err)
	}
}
func TestRemoteStorageRequiresRealConfiguration(t *testing.T) {
	for _, engine := range []string{"S3", "Couchbase"} {
		if _, err := NewStorage(&StorageConfig{Engine: engine, BaseDir: t.TempDir()}); err == nil {
			t.Fatalf("%s silently accepted fake storage", engine)
		}
	}
}
