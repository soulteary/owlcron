package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompleteBackupRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src, _ := NewFilesystemStorage(&StorageConfig{BaseDir: dir})
	records := map[string][]byte{"users:admin": []byte(`{"id":"admin","username":"admin","password":"hash"}`), "users": []byte(`[{"id":"admin","username":"admin"}]`), "global/state": []byte(`{"cursors":{"event":123}}`), "jobs:job": []byte(`{"id":"job","complete":true}`), "job_logs:job": []byte("plain log\n"), "global/event_queue/event": []byte(`[123,456]`)}
	for key, data := range records {
		if err := src.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Expire(ctx, "users:admin", time.Now().Unix()+300); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bootstrap-admin.json"), []byte(`{"password":"plaintext"}`), 0600); err != nil {
		t.Fatal(err)
	}
	_ = src.Put(ctx, "global/master_lease", []byte(`{"owner":"old"}`))
	backup, err := Export(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := backup.Records["bootstrap-admin"]; ok {
		t.Fatal("plaintext bootstrap exported")
	}
	if _, ok := backup.Records["global/master_lease"]; ok {
		t.Fatal("transient lease exported")
	}
	if len(backup.Records) != len(records) {
		t.Fatalf("backup incomplete: %d", len(backup.Records))
	}
	serialized, _ := json.Marshal(backup)
	var decoded Backup
	if err = json.Unmarshal(serialized, &decoded); err != nil {
		t.Fatal(err)
	}
	dst, _ := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err = Import(ctx, dst, &decoded); err != nil {
		t.Fatal(err)
	}
	for key, data := range records {
		got, err := dst.Get(ctx, key)
		if err != nil || string(got) != string(data) {
			t.Fatalf("lost %s: %s %v", key, got, err)
		}
	}
	expiry, err := dst.Get(ctx, "_expire/users:admin")
	if err != nil || len(expiry) == 0 {
		t.Fatal("expiry omitted")
	}
	decoded.Records["../escape"] = BackupEntry{Data: []byte("bad")}
	if err = Import(ctx, dst, &decoded); err == nil {
		t.Fatal("unsafe restore key accepted")
	}
}

func TestBackupStripsLeadershipGenerationAndKeepsRecoveryData(t *testing.T) {
	ctx := context.Background()
	src, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Put(ctx, "global/state", []byte(`{"cursors":{"event":123},"_lease_owner":"retired-generation","enabled":true}`)); err != nil {
		t.Fatal(err)
	}
	for key, data := range map[string][]byte{
		"users:admin":          []byte(`{"username":"admin","password":"hash","enabled":true}`),
		"jobs:done":            []byte(`{"id":"done","complete":true}`),
		"deleted_jobs:removed": []byte(`{"deleted":true}`),
	} {
		if err := src.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	backup, err := Export(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]interface{}
	if err := json.Unmarshal(backup.Records["global/state"].Data, &state); err != nil || state["_lease_owner"] != nil {
		t.Fatal("export retained retired leader generation")
	}
	// Older and manually prepared backups also pass through import sanitation.
	backup.Records["global/state"] = BackupEntry{Data: []byte(`{"cursors":{"event":123},"_lease_owner":"stale-owner","enabled":true}`)}
	backup.Records["global/master_lease"] = BackupEntry{Data: []byte(`{"owner":"stale-owner","expires":999999999999999999}`)}
	dst, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Import(ctx, dst, backup); err != nil {
		t.Fatal(err)
	}
	if exists, err := dst.Exists(ctx, "global/master_lease"); err != nil || exists {
		t.Fatal("import restored a stale lease")
	}
	raw, err := dst.Get(ctx, "global/state")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if state["_lease_owner"] != nil || state["cursors"].(map[string]interface{})["event"] != float64(123) {
		t.Fatal("import retained lease owner or lost cursor")
	}
	for _, key := range []string{"users:admin", "jobs:done", "deleted_jobs:removed"} {
		if _, err := dst.Get(ctx, key); err != nil {
			t.Fatalf("lost recovery key %s: %v", key, err)
		}
	}
}
