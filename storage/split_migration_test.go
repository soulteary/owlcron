package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/blob"
)

func makeLegacyMigrationSource(t *testing.T) (*FilesystemStorage, []byte) {
	t.Helper()
	ctx := context.Background()
	source, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("v1 log\n"), 20000)
	for at := 0; at < len(payload); {
		end := min(at+JobLogChunkSize, len(payload))
		if _, err := JobLogAppend(ctx, source, "v1", int64(at), payload[at:end], end == len(payload), JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
		at = end
	}
	if _, err := JobLogAppend(ctx, source, "deleted", 0, []byte("removed"), true, JobLogAbsoluteMax); err != nil {
		t.Fatal(err)
	}
	if err := JobLogDelete(ctx, source, "deleted"); err != nil {
		t.Fatal(err)
	}
	for key, data := range map[string][]byte{
		"jobs:v1":              []byte(`{"id":"v1","complete":true}`),
		"jobs:legacy":          []byte(`{"id":"legacy","complete":true}`),
		"job_logs:legacy":      []byte("old complete log\n"),
		"users:user":           []byte(`{"id":"user"}`),
		"deleted_jobs:deleted": []byte(`{"deleted":true}`),
		"global/state":         []byte(`{"cursors":{"event":456},"_lease_owner":"retired"}`),
	} {
		if err := source.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	return source, payload
}

func TestSplitMigrationConvertsLegacyAndV1WithoutChangingSource(t *testing.T) {
	ctx := context.Background()
	source, payload := makeLegacyMigrationSource(t)
	before, err := snapshotLegacySource(ctx, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	target := newSplitBackupFixture(t, false)
	result, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Logs != 3 || result.MigrationID == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
	after, err := snapshotLegacySource(ctx, source, nil)
	if err != nil || before != after {
		t.Fatal("migration changed source")
	}
	layout, err := ValidateStorageLayout(ctx, target.meta, target.registry)
	if err != nil {
		t.Fatal(err)
	}
	repo, _ := NewJobLogRepository(target.meta, target.registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
	got, m, err := repo.Read(ctx, "v1", 0, len(payload))
	if err != nil || !bytes.Equal(got, payload) || !m.Final {
		t.Fatal("v1 log did not migrate")
	}
	if got, _, err := repo.Read(ctx, "legacy", 0, 100); err != nil || string(got) != "old complete log\n" {
		t.Fatalf("legacy log: %q %v", got, err)
	}
	if m, err := repo.Info(ctx, "deleted"); err != nil || !m.Deleted {
		t.Fatal("deleted log revived")
	}
	keys, err := target.meta.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, "job_log_chunks/") || strings.HasPrefix(key, "job_logs:") {
			t.Fatalf("payload remained in metadata: %s", key)
		}
	}
	if _, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true}); err == nil {
		t.Fatal("non-resume overwrote populated target")
	}
}

type interruptedMigrationTarget struct {
	*FilesystemStorage
	failKey    string
	fail       bool
	failExpiry bool
}

func (s *interruptedMigrationTarget) Expire(ctx context.Context, key string, timestamp int64) error {
	if s.failExpiry && key == s.failKey {
		s.failExpiry = false
		return errors.New("injected crash after Put before Expire")
	}
	return s.FilesystemStorage.Expire(ctx, key, timestamp)
}

func (s *interruptedMigrationTarget) Put(ctx context.Context, key string, data []byte) error {
	if s.fail && key == s.failKey {
		s.fail = false
		return errors.New("injected interrupted copy")
	}
	return s.FilesystemStorage.Put(ctx, key, data)
}

func TestSplitMigrationSkipsExpiredLegacyPayload(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(fmt.Sprintf("resume=%t", resume), func(t *testing.T) {
			ctx := context.Background()
			source, payload := makeLegacyMigrationSource(t)
			const expiredKey = "job_logs:expired"
			expiredPayload := []byte("expired legacy log\n")
			if err := source.Put(ctx, expiredKey, expiredPayload); err != nil {
				t.Fatal(err)
			}
			if err := source.Expire(ctx, expiredKey, time.Now().Unix()-1); err != nil {
				t.Fatal(err)
			}
			// Filesystem inventories still contain expired payload files. There
			// is no manifest or deletion fence to bypass their range read.
			for _, key := range []string{JobLogManifestKey("expired"), "deleted_jobs:expired", "deleted_job_logs:expired"} {
				if _, err := source.Get(ctx, key); !errors.Is(err, ErrNotFound) {
					t.Fatalf("unexpected expired log metadata %s: %v", key, err)
				}
			}
			if _, _, err := source.GetRange(ctx, expiredKey, 0, 0); !errors.Is(err, ErrNotFound) {
				t.Fatalf("legacy payload did not expire: %v", err)
			}
			path := filepath.Join(source.baseDir, expiredKey+".json")
			if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, expiredPayload) {
				t.Fatalf("expired payload file missing: %q %v", raw, err)
			}
			target := newSplitBackupFixture(t, false)
			var namespace string
			if resume {
				fault := &interruptedMigrationTarget{FilesystemStorage: target.meta, failKey: "users:user", fail: true}
				if _, err := MigrateLegacyToSplit(ctx, source, fault, target.registry, SplitMigrationOptions{Offline: true}); err == nil || fault.fail {
					t.Fatalf("migration did not reach injected interruption: %v", err)
				}
				layout, err := LoadStorageLayout(ctx, target.meta)
				if err != nil || layout.Phase != "migrating" {
					t.Fatalf("interrupted target visible: %+v %v", layout, err)
				}
				namespace = layout.NamespaceID
			}
			result, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true, Resume: resume})
			if err != nil {
				t.Fatal(err)
			}
			if result.Logs != 3 || resume && result.Namespace != namespace {
				t.Fatalf("unexpected migration result: %+v", result)
			}
			layout, err := ValidateStorageLayout(ctx, target.meta, target.registry)
			if err != nil {
				t.Fatal(err)
			}
			repo, err := NewJobLogRepository(target.meta, target.registry.Stores(), JobLogRepositoryOptions{Namespace: layout.NamespaceID, WriteStore: "logs", WriteFormat: 2})
			if err != nil {
				t.Fatal(err)
			}
			for id, want := range map[string][]byte{"legacy": []byte("old complete log\n"), "v1": payload} {
				if got, m, err := repo.Read(ctx, id, 0, len(want)); err != nil || !bytes.Equal(got, want) || !m.Final {
					t.Fatalf("valid log %s did not migrate: %+v %v", id, m, err)
				}
			}
			if _, err := repo.Info(ctx, "expired"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("expired legacy log revived: %v", err)
			}
			if _, err := target.meta.Get(ctx, expiredKey); !errors.Is(err, ErrNotFound) {
				t.Fatalf("expired payload copied into metadata: %v", err)
			}
			if got, err := target.meta.Get(ctx, "users:user"); err != nil || string(got) != `{"id":"user"}` {
				t.Fatalf("valid metadata did not migrate: %q %v", got, err)
			}
			if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, expiredPayload) {
				t.Fatalf("migration changed expired source file: %q %v", raw, err)
			}
		})
	}
}

func TestSplitMigrationResumeAcceptsOnlyRecordedExpiry(t *testing.T) {
	ctx := context.Background()
	source, _ := makeLegacyMigrationSource(t)
	expires := time.Now().Unix() + 2
	if err := source.Put(ctx, "aaa_session:expires", []byte("short lived")); err != nil {
		t.Fatal(err)
	}
	if err := source.Expire(ctx, "aaa_session:expires", expires); err != nil {
		t.Fatal(err)
	}
	target := newSplitBackupFixture(t, false)
	fault := &interruptedMigrationTarget{FilesystemStorage: target.meta, failKey: "users:user", fail: true}
	if _, err := MigrateLegacyToSplit(ctx, source, fault, target.registry, SplitMigrationOptions{Offline: true}); err == nil {
		t.Fatal("fault did not interrupt migration")
	}
	layout, err := LoadStorageLayout(ctx, target.meta)
	if err != nil || layout.Phase != "migrating" {
		t.Fatalf("interrupted target visible: %+v %v", layout, err)
	}
	if wait := time.Until(time.Unix(expires, 0)); wait > 0 {
		time.Sleep(wait)
	}
	result, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true, Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Namespace != layout.NamespaceID {
		t.Fatal("resume changed namespace")
	}
	if exists, err := target.meta.Exists(ctx, "aaa_session:expires"); err != nil || exists {
		t.Fatal("expired session revived")
	}
	if _, err := ValidateStorageLayout(ctx, target.meta, target.registry); err != nil {
		t.Fatal(err)
	}
}

func TestSplitMigrationResumeRejectsChangedSource(t *testing.T) {
	for _, mode := range []string{"changed", "deleted", "added"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			source, _ := makeLegacyMigrationSource(t)
			target := newSplitBackupFixture(t, false)
			fault := &interruptedMigrationTarget{FilesystemStorage: target.meta, failKey: "users:user", fail: true}
			if _, err := MigrateLegacyToSplit(ctx, source, fault, target.registry, SplitMigrationOptions{Offline: true}); err == nil {
				t.Fatal("fault did not interrupt")
			}
			switch mode {
			case "changed":
				_ = source.Put(ctx, "users:user", []byte(`{"id":"changed"}`))
			case "deleted":
				_ = source.Delete(ctx, "users:user")
			case "added":
				_ = source.Put(ctx, "users:new", []byte(`{"id":"new"}`))
			}
			if _, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true, Resume: true}); err == nil {
				t.Fatal("changed source accepted")
			}
			layout, _ := LoadStorageLayout(ctx, target.meta)
			if layout.Phase != "migrating" {
				t.Fatal("failed resume activated target")
			}
		})
	}
}

func TestSplitMigrationResumeRejectsForeignBindings(t *testing.T) {
	ctx := context.Background()
	source, _ := makeLegacyMigrationSource(t)
	target := newSplitBackupFixture(t, false)
	fault := &interruptedMigrationTarget{FilesystemStorage: target.meta, failKey: "users:user", fail: true}
	if _, err := MigrateLegacyToSplit(ctx, source, fault, target.registry, SplitMigrationOptions{Offline: true}); err == nil {
		t.Fatal("fault did not interrupt")
	}
	foreign := newSplitBackupFixture(t, false)
	if _, err := MigrateLegacyToSplit(ctx, source, target.meta, foreign.registry, SplitMigrationOptions{Offline: true, Resume: true}); err == nil {
		t.Fatal("foreign blob binding accepted")
	}
}

func TestSplitMigrationResumeFinishesLostExpiration(t *testing.T) {
	ctx := context.Background()
	source, _ := makeLegacyMigrationSource(t)
	expires := time.Now().Unix() + 2
	if err := source.Put(ctx, "aaa_session:expires", []byte("session")); err != nil {
		t.Fatal(err)
	}
	if err := source.Expire(ctx, "aaa_session:expires", expires); err != nil {
		t.Fatal(err)
	}
	target := newSplitBackupFixture(t, false)
	fault := &interruptedMigrationTarget{FilesystemStorage: target.meta, failKey: "aaa_session:expires", failExpiry: true}
	if _, err := MigrateLegacyToSplit(ctx, source, fault, target.registry, SplitMigrationOptions{Offline: true}); err == nil {
		t.Fatal("expiry fault did not interrupt migration")
	}
	if exists, err := target.meta.Exists(ctx, "aaa_session:expires"); err != nil || !exists {
		t.Fatal("fault did not leave unexpired target record")
	}
	if wait := time.Until(time.Unix(expires, 0)); wait > 0 {
		time.Sleep(wait)
	}
	if _, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true, Resume: true}); err != nil {
		t.Fatal(err)
	}
	if exists, err := target.meta.Exists(ctx, "aaa_session:expires"); err != nil || exists {
		t.Fatal("lost expiration resurrected source record")
	}
}

func TestSplitMigrationResumesAfterLayoutBeforeCheckpoint(t *testing.T) {
	ctx := context.Background()
	source, _ := makeLegacyMigrationSource(t)
	target := newSplitBackupFixture(t, false)
	layout, err := InitializeStorageLayout(ctx, target.meta, target.registry, "migrating")
	if err != nil {
		t.Fatal(err)
	}
	result, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true, Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Namespace != layout.NamespaceID {
		t.Fatal("initialization resume abandoned reserved namespace")
	}
	if _, err := ValidateStorageLayout(ctx, target.meta, target.registry); err != nil {
		t.Fatal(err)
	}
}

type initializingDelayBlob struct {
	blob.Store
	delay time.Duration
}

func (s *initializingDelayBlob) Put(ctx context.Context, spec blob.Write, reader io.Reader) error {
	if s.delay > 0 {
		delay := s.delay
		s.delay = 0
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, spec, reader)
}

func TestSplitMigrationAcceptsExpiryDuringNamespaceInitialization(t *testing.T) {
	ctx := context.Background()
	source, _ := makeLegacyMigrationSource(t)
	expires := time.Now().Unix() + 1
	if err := source.Put(ctx, "session:short", []byte("short")); err != nil {
		t.Fatal(err)
	}
	if err := source.Expire(ctx, "session:short", expires); err != nil {
		t.Fatal(err)
	}
	target := newSplitBackupFixture(t, false)
	target.registry.stores["logs"] = &initializingDelayBlob{Store: target.registry.stores["logs"], delay: time.Until(time.Unix(expires, 0)) + 10*time.Millisecond}
	if _, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: true}); err != nil {
		t.Fatal(err)
	}
	if exists, err := target.meta.Exists(ctx, "session:short"); err != nil || exists {
		t.Fatal("initialization revived expired session")
	}
}

func TestSplitMigrationRejectsIncompleteAndActiveSource(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"pending", "active", "lease", "offline"} {
		t.Run(mode, func(t *testing.T) {
			source, _ := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
			switch mode {
			case "pending":
				raw, _ := json.Marshal(JobLogManifest{Offset: 0, PendingEnd: 1, PendingHash: strings.Repeat("a", 64)})
				_ = source.Put(ctx, JobLogManifestKey("pending"), raw)
			case "active":
				_ = source.Put(ctx, "active_jobs", []byte(`[{"id":"running"}]`))
			case "lease":
				_ = source.Put(ctx, "global/master_lease", []byte(fmt.Sprintf(`{"owner":"live","expires":%d}`, time.Now().Add(time.Hour).UnixNano())))
			}
			target := newSplitBackupFixture(t, false)
			if _, err := MigrateLegacyToSplit(ctx, source, target.meta, target.registry, SplitMigrationOptions{Offline: mode != "offline"}); err == nil {
				t.Fatal("unsafe source accepted")
			}
			if _, err := LoadStorageLayout(ctx, target.meta); !errors.Is(err, ErrNotFound) {
				t.Fatal("preflight failure mutated target")
			}
		})
	}
}
