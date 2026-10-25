package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	minio "github.com/soulteary/otterio-sdk/v7"
	"github.com/soulteary/otterio-sdk/v7/pkg/credentials"
)

func TestFilesystemTransactionDeletesExpiryAndRejectsCorruption(t *testing.T) {
	ctx := context.Background()
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "nested/account", []byte("private")); err != nil {
		t.Fatal(err)
	}
	if err := store.Expire(ctx, "nested/account", time.Now().Unix()+300); err != nil {
		t.Fatal(err)
	}
	tx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete("nested/account"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "_expire/nested/account"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("transaction delete retained TTL metadata: %v", err)
	}
	if err := store.Put(ctx, "nested/account", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "_expire/nested/account", []byte("invalid expiry")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "nested/account"); err == nil {
		t.Fatal("corrupt expiry metadata silently made a credential live")
	}
}

func TestFilesystemRejectsExpirySymlinkBeforeWriting(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "_expire"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "_expire", "credentials")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), "credentials/user", []byte("must not persist")); err == nil {
		t.Fatal("expiry path followed a symlink")
	}
	if _, err := os.Stat(filepath.Join(dir, "credentials", "user.json")); !os.IsNotExist(err) {
		t.Fatal("failed expiry validation partially wrote the credential")
	}
}

func TestTransactionRejectsConflictingPathsBeforeDurableCommit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFilesystemStorage(&StorageConfig{BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("account", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("account.json/child", []byte("cannot coexist with account file")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("accepted a batch whose first record blocks its later directory")
	}
	if _, err := os.Stat(filepath.Join(dir, ".transaction.json")); !os.IsNotExist(err) {
		t.Fatal("invalid batch left an unreplayable commit journal")
	}
	if _, err := store.Get(ctx, "account"); !errors.Is(err, ErrNotFound) {
		t.Fatal("invalid transaction partially persisted")
	}
	if err := store.Put(ctx, "normal", []byte("still usable")); err != nil {
		t.Fatalf("failed transaction blocked future storage operations: %v", err)
	}
}

type recoveryS3Transport struct {
	mu          sync.Mutex
	objects     map[string][]byte
	failed      bool
	probeWrites int
}

func (s *recoveryS3Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Method == http.MethodPut && !s.failed {
		s.failed = true
		return nil, errors.New("transient first probe failure")
	}
	response := func(status int, data []byte, headers http.Header) (*http.Response, error) {
		headers.Set("Content-Length", strconv.Itoa(len(data)))
		headers.Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
		return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: req}, nil
	}
	key := req.URL.Path
	data, exists := s.objects[key]
	etag := func(data []byte) string {
		sum := md5.Sum(data)
		return `"` + hex.EncodeToString(sum[:]) + `"`
	}
	switch req.Method {
	case http.MethodDelete:
		delete(s.objects, key)
		return response(http.StatusNoContent, nil, make(http.Header))
	case http.MethodGet:
		if !exists {
			return response(http.StatusNotFound, []byte(`<Error><Code>NoSuchKey</Code><Message>Missing object</Message></Error>`), make(http.Header))
		}
		return response(http.StatusOK, data, http.Header{"Etag": []string{etag(data)}})
	case http.MethodPut:
		if strings.Contains(key, "/capability/") {
			s.probeWrites++
		}
		if (req.Header.Get("If-None-Match") == "*" && exists) || (req.Header.Get("If-Match") != "" && (!exists || req.Header.Get("If-Match") != etag(data))) {
			return response(http.StatusPreconditionFailed, []byte(`<Error><Code>PreconditionFailed</Code><Message>Version mismatch</Message></Error>`), make(http.Header))
		}
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		s.objects[key] = payload
		return response(http.StatusOK, nil, http.Header{"Etag": []string{etag(payload)}})
	}
	return response(http.StatusBadRequest, nil, make(http.Header))
}

func TestS3ConditionalProbeRecoversAfterTransientFailure(t *testing.T) {
	transport := &recoveryS3Transport{objects: make(map[string][]byte)}
	client, err := minio.New("https://storage.invalid", &minio.Options{
		Region: "us-east-1", Creds: credentials.NewStaticV4("test", "test", ""),
		Secure: true, Transport: transport, BucketLookup: minio.BucketLookupPath, MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &S3Storage{client: client, bucket: "test"}
	ctx := context.Background()
	if err := store.compareAndSwap(ctx, "first", []byte("payload"), ""); err == nil {
		t.Fatal("transient probe failure was ignored")
	}
	if err := store.compareAndSwap(ctx, "second", []byte("payload"), ""); err != nil {
		t.Fatalf("healthy backend remained permanently disabled: %v", err)
	}
	transport.mu.Lock()
	probes := transport.probeWrites
	transport.mu.Unlock()
	if err := store.compareAndSwap(ctx, "third", []byte("payload"), ""); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.probeWrites != probes {
		t.Fatal("successful capability check was not cached")
	}
}
