package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeSpec(key string, data []byte) Write {
	return Write{Key: key, Size: int64(len(data)), SHA256: sha256.Sum256(data)}
}
func newTestFilesystem(t *testing.T) *Filesystem {
	t.Helper()
	s, err := NewFilesystem(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestFilesystemImmutableRoundTrip(t *testing.T) {
	s := newTestFilesystem(t)
	ctx := context.Background()
	key := "namespace/logs/job/incarnation/chunk"
	data := bytes.Repeat([]byte{0, 1, 255, 10}, int(MaxObjectSize)/4)
	for i := 0; i < 2; i++ {
		if err := s.Put(ctx, writeSpec(key, data), bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	changed := bytes.Repeat([]byte("x"), len(data))
	if err := s.Put(ctx, writeSpec(key, changed), bytes.NewReader(changed)); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("replacement: %v", err)
	}
	body, err := s.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	body.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read: %v", err)
	}
	info, err := s.Stat(ctx, key)
	if err != nil || info.Size != int64(len(data)) || info.Key != key || info.LastModified.IsZero() {
		t.Fatalf("stat: %+v %v", info, err)
	}
	for i := 0; i < 2; i++ {
		if err := s.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Open(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestFilesystemRejectsInvalidBodiesAndPaths(t *testing.T) {
	s := newTestFilesystem(t)
	ctx := context.Background()
	for _, key := range []string{"", "../escape", "a/../escape", "/absolute", "a//b", "a\\b", "a/.owlcron-upload-other"} {
		if err := s.Put(ctx, writeSpec(key, []byte("a")), bytes.NewReader([]byte("a"))); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("key %q: %v", key, err)
		}
	}
	for _, body := range [][]byte{[]byte("short"), []byte("longer than declared"), []byte("same size!")} {
		if err := s.Put(ctx, writeSpec("safe", []byte("expected!")), bytes.NewReader(body)); !errors.Is(err, ErrIntegrity) {
			t.Errorf("body %q: %v", body, err)
		}
	}
	if _, err := s.Stat(ctx, "safe"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("x"), int(MaxObjectSize)+1)
	if err := s.Put(ctx, writeSpec("large", large), bytes.NewReader(large)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Put(canceled, writeSpec("canceled", nil), bytes.NewReader(nil)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestFilesystemPublicationRaceNeverMixesBytes(t *testing.T) {
	s := newTestFilesystem(t)
	ctx := context.Background()
	key := "log/chunk"
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := bytes.Repeat([]byte{byte(i)}, int(MaxObjectSize))
			errs <- s.Put(ctx, writeSpec(key, data), bytes.NewReader(data))
		}(i)
	}
	wg.Wait()
	close(errs)
	success := 0
	for err := range errs {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrIntegrity) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("winners=%d", success)
	}
	body, err := s.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	body.Close()
	if err != nil || len(data) != int(MaxObjectSize) {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(data, bytes.Repeat(data[:1], len(data))) {
		t.Fatal("mixed published content")
	}
}

func TestFilesystemRejectsSymlinkObjectsAndParents(t *testing.T) {
	base := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "object"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := NewFilesystem(base)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := os.Symlink(outside, filepath.Join(base, "external")); err != nil {
		t.Skip(err)
	}
	if err := os.Mkdir(filepath.Join(base, "real"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(base, "alias")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"external/object", "alias/object"} {
		if _, err := s.Open(context.Background(), key); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("open %s: %v", key, err)
		}
		if err := s.Put(context.Background(), writeSpec(key, []byte("bad")), bytes.NewReader([]byte("bad"))); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("put %s: %v", key, err)
		}
	}
}

func TestFilesystemInventoryHasBoundedPagesAndRestartableSessions(t *testing.T) {
	base := t.TempDir()
	s, err := NewFilesystem(base)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for i := 0; i < 300; i++ {
		if err := os.WriteFile(filepath.Join(base, fmt.Sprintf("other-%04d", i)), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 7; i++ {
		data := []byte("log")
		key := fmt.Sprintf("wanted/%d/object", i)
		if err := s.Put(ctx, writeSpec(key, data), bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := s.Scan(ctx, "wanted", cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Objects) > 2 {
			t.Fatal("unbounded page")
		}
		for _, item := range page.Objects {
			if seen[item.Key] {
				t.Fatalf("duplicate %s", item.Key)
			}
			seen[item.Key] = true
		}
		pages++
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
		if pages > 30 {
			t.Fatal("inventory did not finish")
		}
	}
	if len(seen) != 7 {
		t.Fatalf("objects=%d", len(seen))
	}
	page, err := s.Scan(ctx, "", "", 1)
	if err != nil || page.NextCursor == "" {
		t.Fatalf("cursor: %v", err)
	}
	if _, err := s.Scan(ctx, "other", page.NextCursor, 1); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("prefix mismatch: %v", err)
	}
	s.scans[page.NextCursor].touched = time.Now().Add(-2 * scanLifetime)
	if _, err := s.Scan(ctx, "", page.NextCursor, 1); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired cursor: %v", err)
	}
	if _, err := s.Scan(ctx, "", "", MaxScanLimit+1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
}

func TestFilesystemUnpublishedTempCleanupAndOversizedObject(t *testing.T) {
	base := t.TempDir()
	s, err := NewFilesystem(base)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tmp := filepath.Join(base, ".owlcron-upload-abandoned")
	if err := os.WriteFile(tmp, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(tmp, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scan(context.Background(), "", "", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temporary file was not reclaimed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "oversized"), make([]byte, MaxObjectSize+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(context.Background(), "oversized"); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
}
