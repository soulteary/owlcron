package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/soulteary/owlcron/blob"
	"github.com/soulteary/owlcron/types"
)

func layoutFixture(t *testing.T) (Storage, *BlobRegistry, string) {
	t.Helper()
	meta, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	registry, err := NewBlobRegistry(&types.BlobStorageConfig{WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": {Engine: "Filesystem", BaseDir: dir}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return meta, registry, dir
}

func TestStorageLayoutInitializationPhasesAndReadOnlyReadiness(t *testing.T) {
	ctx := context.Background()
	meta, registry, _ := layoutFixture(t)
	if _, err := ValidateStorageLayout(ctx, meta, registry); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	layout, err := InitializeStorageLayout(ctx, meta, registry, "restoring")
	if err != nil {
		t.Fatal(err)
	}
	if layout.Phase != "restoring" || len(layout.NamespaceID) != 32 || layout.Version != 2 {
		t.Fatalf("layout: %+v", layout)
	}
	if _, err := ValidateStorageLayout(ctx, meta, registry); !errors.Is(err, ErrStorageLayoutNotReady) {
		t.Fatalf("restoring became ready: %v", err)
	}
	if _, err := ValidateStorageLayoutPhase(ctx, meta, registry, "restoring"); err != nil {
		t.Fatal(err)
	}
	if err := SetStorageLayoutPhase(ctx, meta, layout.NamespaceID, "restoring", "ready"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := ValidateStorageLayout(ctx, meta, registry); err != nil {
			t.Fatal(err)
		}
	}
	copy := registry.Stores()
	delete(copy, "logs")
	if _, err := registry.Store("logs"); err != nil {
		t.Fatal("registry map was mutable")
	}
	if _, err := registry.Store("missing"); !errors.Is(err, blob.ErrInvalidConfig) {
		t.Fatal(err)
	}
	if err := SetStorageLayoutPhase(ctx, meta, strings.Repeat("0", 32), "ready", "restoring"); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong namespace: %v", err)
	}
	again, err := InitializeStorageLayout(ctx, meta, registry, "ready")
	if err != nil || again.NamespaceID != layout.NamespaceID {
		t.Fatalf("idempotent init: %+v %v", again, err)
	}
}

type failBindingStore struct {
	blob.Store
	fail bool
	puts int
}

func (s *failBindingStore) Put(ctx context.Context, spec blob.Write, r io.Reader) error {
	s.puts++
	if strings.HasSuffix(spec.Key, "/binding") && s.fail {
		s.fail = false
		return errors.New("injected binding outage")
	}
	return s.Store.Put(ctx, spec, r)
}
func TestStorageLayoutInitializationResumesSameReservedNamespace(t *testing.T) {
	ctx := context.Background()
	meta, registry, _ := layoutFixture(t)
	wrapped := &failBindingStore{Store: registry.stores["logs"], fail: true}
	registry.stores["logs"] = wrapped
	if _, err := InitializeStorageLayout(ctx, meta, registry, "ready"); err == nil {
		t.Fatal("injected outage ignored")
	}
	pending, err := LoadStorageLayout(ctx, meta)
	if err != nil || pending.Phase != "initializing" {
		t.Fatalf("reservation missing: %+v %v", pending, err)
	}
	if _, err := InitializeStorageLayout(ctx, meta, registry, "restoring"); err == nil {
		t.Fatal("changed target phase accepted")
	}
	ready, err := InitializeStorageLayout(ctx, meta, registry, "ready")
	if err != nil || ready.NamespaceID != pending.NamespaceID {
		t.Fatalf("resume: %+v %v", ready, err)
	}
	before := wrapped.puts
	if err := registry.Readiness(ctx, ready.NamespaceID); err != nil {
		t.Fatal(err)
	}
	if wrapped.puts != before {
		t.Fatal("readiness wrote data")
	}
}

type lostLayoutReply struct {
	Storage
	atomic AtomicStorage
	lose   bool
}

func (s *lostLayoutReply) ListKeys(ctx context.Context) ([]string, error) {
	return s.Storage.(KeyLister).ListKeys(ctx)
}

func (s *lostLayoutReply) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	err := s.atomic.Update(ctx, key, fn)
	if err == nil && key == StorageLayoutKey && s.lose {
		s.lose = false
		return errors.New("injected lost metadata acknowledgment")
	}
	return err
}
func TestStorageLayoutLostReservationReplyDoesNotUploadUnderUnknownIdentity(t *testing.T) {
	ctx := context.Background()
	meta, registry, _ := layoutFixture(t)
	wrapped := &failBindingStore{Store: registry.stores["logs"]}
	registry.stores["logs"] = wrapped
	uncertain := &lostLayoutReply{Storage: meta, atomic: meta.(AtomicStorage), lose: true}
	if _, err := InitializeStorageLayout(ctx, uncertain, registry, "ready"); err == nil {
		t.Fatal("lost reply ignored")
	}
	if wrapped.puts != 0 {
		t.Fatal("uploaded before reservation confirmed")
	}
	pending, err := LoadStorageLayout(ctx, meta)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := InitializeStorageLayout(ctx, uncertain, registry, "ready")
	if err != nil || ready.NamespaceID != pending.NamespaceID {
		t.Fatalf("resume: %+v %v", ready, err)
	}
}

func TestStorageLayoutRejectsNonemptyOrReboundTargets(t *testing.T) {
	ctx := context.Background()
	meta, registry, _ := layoutFixture(t)
	if err := meta.Put(ctx, "users:existing", []byte("state")); err != nil {
		t.Fatal(err)
	}
	if _, err := InitializeStorageLayout(ctx, meta, registry, "ready"); err == nil {
		t.Fatal("nonempty target initialized")
	}
	if _, err := LoadStorageLayout(ctx, meta); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mutated nonempty target: %v", err)
	}
	meta, registry, _ = layoutFixture(t)
	layout, err := InitializeStorageLayout(ctx, meta, registry, "ready")
	if err != nil {
		t.Fatal(err)
	}
	_, other, _ := layoutFixture(t)
	if _, err := ValidateStorageLayout(ctx, meta, other); err == nil {
		t.Fatal("different physical binding accepted")
	}
	if _, err := InitializeStorageLayout(ctx, meta, other, "ready"); err == nil {
		t.Fatal("init rebound existing namespace")
	}
	current, err := LoadStorageLayout(ctx, meta)
	if err != nil || current.NamespaceID != layout.NamespaceID {
		t.Fatal("namespace changed")
	}
}

func TestStorageLayoutCorruptMarkerAndUnknownFormatFailClosed(t *testing.T) {
	ctx := context.Background()
	meta, registry, dir := layoutFixture(t)
	layout, err := InitializeStorageLayout(ctx, meta, registry, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, layout.NamespaceID, "binding"), []byte("another dataset"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Readiness(ctx, layout.NamespaceID); !errors.Is(err, blob.ErrIntegrity) {
		t.Fatalf("wrong marker: %v", err)
	}
	data, err := meta.Get(ctx, StorageLayoutKey)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), `"version":2`, `"version":99`, 1))
	if err := meta.Put(ctx, StorageLayoutKey, data); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadStorageLayout(ctx, meta); err == nil {
		t.Fatal("unknown layout accepted")
	}
}

func TestConfiguredMetadataPreservesLegacyDefaultsAndRequiresDurableSplitBackend(t *testing.T) {
	store, err := OpenConfiguredMetadata(&types.Config{StorageBaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*FilesystemStorage); !ok {
		t.Fatal("legacy default changed")
	}
	for _, cfg := range []*types.Config{
		{StorageEngine: "Redis", BlobStorage: &types.BlobStorageConfig{}, JobLogStorage: &types.JobLogStorageConfig{WriteFormat: 1}},
		{StorageEngine: "S3", BlobStorage: &types.BlobStorageConfig{}},
		{StorageEngine: "Couchbase", BlobStorage: &types.BlobStorageConfig{}, JobLogStorage: &types.JobLogStorageConfig{WriteFormat: 1}},
		{JobLogStorage: &types.JobLogStorageConfig{WriteFormat: 2}},
		{JobLogStorage: &types.JobLogStorageConfig{WriteFormat: 3}},
		{JobLogStorage: &types.JobLogStorageConfig{SpoolResumePercent: 90}},
		{JobLogStorage: &types.JobLogStorageConfig{GCBatchSize: 1001}},
	} {
		if _, err := OpenConfiguredMetadata(cfg); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", cfg)
		}
	}
	for _, test := range []struct {
		name  string
		level gocb.DurabilityLevel
	}{{"", gocb.DurabilityLevelUnknown}, {"none", gocb.DurabilityLevelNone}, {"majority", gocb.DurabilityLevelMajority}, {"majority_and_persist_to_active", gocb.DurabilityLevelMajorityAndPersistOnMaster}, {"persist_to_majority", gocb.DurabilityLevelPersistToMajority}} {
		level, err := couchbaseDurability(test.name)
		if err != nil || level != test.level {
			t.Fatalf("durability %s: %d %v", test.name, level, err)
		}
	}
	if _, err := couchbaseDurability("arbitrary"); err == nil {
		t.Fatal("invalid durability accepted")
	}
}

func TestBlobRegistryCredentialFilesAndBindingIdentity(t *testing.T) {
	dir := t.TempDir()
	access := filepath.Join(dir, "access")
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(access, []byte("access\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte("secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := types.BlobStoreConfig{Engine: "S3", Endpoint: "http://127.0.0.1:1", Bucket: "logs", Prefix: "prefix", AccessKeyFile: access, SecretKeyFile: secret, PathStyle: true}
	newRegistry := func(cfg types.BlobStoreConfig) *BlobRegistry {
		t.Helper()
		r, err := NewBlobRegistry(&types.BlobStorageConfig{WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": cfg}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	first := newRegistry(cfg)
	cfg.Endpoint = "http://127.0.0.1:2"
	cfg.AccessKeyFile = ""
	cfg.SecretKeyFile = ""
	cfg.AccessKey = "rotated"
	cfg.SecretKey = "rotated-secret"
	second := newRegistry(cfg)
	if first.bindings["logs"] != second.bindings["logs"] {
		t.Fatal("credential/endpoint rotation changed physical binding fingerprint")
	}
	cfg.Prefix = "other"
	third := newRegistry(cfg)
	if first.bindings["logs"] == third.bindings["logs"] {
		t.Fatal("prefix change retained binding")
	}
	if _, err := blobCredential("inline", access, ""); !errors.Is(err, blob.ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewBlobRegistry(&types.BlobStorageConfig{WriteStore: "one", Stores: map[string]types.BlobStoreConfig{"one": cfg, "two": cfg}}); !errors.Is(err, blob.ErrInvalidConfig) {
		t.Fatalf("aliased stores accepted: %v", err)
	}
}

func TestConfiguredMetadataRejectsOverlappingBlobDirectories(t *testing.T) {
	root := t.TempDir()
	metadata := filepath.Join(root, "state")
	for _, dir := range []string{metadata, root, filepath.Join(metadata, "objects")} {
		_, err := OpenConfiguredMetadata(&types.Config{StorageBaseDir: metadata, BlobStorage: &types.BlobStorageConfig{
			WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": {Engine: "Filesystem", BaseDir: dir}},
		}})
		if err == nil || !strings.Contains(err.Error(), "non-overlapping") {
			t.Fatalf("overlap %s: %v", dir, err)
		}
	}
	if err := os.Mkdir(metadata, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(metadata, alias); err != nil {
		t.Skip(err)
	}
	_, err := OpenConfiguredMetadata(&types.Config{StorageBaseDir: metadata, BlobStorage: &types.BlobStorageConfig{
		WriteStore: "logs", Stores: map[string]types.BlobStoreConfig{"logs": {Engine: "Filesystem", BaseDir: filepath.Join(alias, "nested")}},
	}})
	if err == nil || !strings.Contains(err.Error(), "non-overlapping") {
		t.Fatalf("symlink overlap: %v", err)
	}
}
