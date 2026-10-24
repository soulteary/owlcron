package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestJobLogIncrementalRangeAndFinal(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	data := bytes.Repeat([]byte("x"), JobLogChunkSize+123)
	for at := 0; at < len(data); {
		n := 7777
		if n > len(data)-at {
			n = len(data) - at
		}
		if _, e := JobLogAppend(ctx, s, "a", int64(at), data[at:at+n], false, 1<<20); e != nil {
			t.Fatal(e)
		}
		at += n
	}
	if _, e := JobLogAppend(ctx, s, "a", int64(len(data)), nil, true, 1<<20); e != nil {
		t.Fatal(e)
	}
	got, m, e := JobLogRead(ctx, s, "a", JobLogChunkSize-10, 100)
	if e != nil || len(got) != 100 || !m.Final {
		t.Fatalf("range %d %+v %v", len(got), m, e)
	}
	if _, e := JobLogAppend(ctx, s, "a", 0, data[:123], false, 1<<20); e != nil {
		t.Fatal(e)
	}
	if _, e := JobLogAppend(ctx, s, "a", int64(len(data)), []byte("late"), false, 1<<20); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e := JobLogAppend(ctx, s, "a", 0, []byte("different"), false, 1<<20); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}
func TestJobLogConcurrentAppendAndDeletion(t *testing.T) {
	ctx := context.Background()
	s := historyStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := JobLogAppend(ctx, s, "a", 0, []byte("same"), false, 100); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if e := JobLogDelete(ctx, s, "a"); e != nil {
		t.Fatal(e)
	}
	if _, e := JobLogAppend(ctx, s, "a", 0, []byte("same"), true, 100); !errors.Is(e, ErrJobLogDeleted) {
		t.Fatal(e)
	}
	if _, _, e := JobLogRead(ctx, s, "a", 0, 100); !errors.Is(e, ErrJobLogDeleted) {
		t.Fatal(e)
	}
	if e := JobLogDelete(ctx, s, "a"); e != nil {
		t.Fatal(e)
	}
}

type logFaultStore struct {
	*FilesystemStorage
	failManifest  bool
	manifestCalls int
}

func (s *logFaultStore) Update(ctx context.Context, key string, f func([]byte) ([]byte, error)) error {
	if strings.HasPrefix(key, "job_log_manifest/") {
		s.manifestCalls++
	}
	if s.failManifest && strings.HasPrefix(key, "job_log_manifest/") && s.manifestCalls == 2 {
		return errors.New("manifest failure")
	}
	return s.FilesystemStorage.Update(ctx, key, f)
}
func TestJobLogRecoversUnpublishedChunk(t *testing.T) {
	ctx := context.Background()
	s := &logFaultStore{FilesystemStorage: historyStore(t), failManifest: true}
	if _, e := JobLogAppend(ctx, s, "a", 0, []byte("recover"), false, 100); e == nil {
		t.Fatal("expected failure")
	}
	if m, e := JobLogInfo(ctx, s, "a"); e != nil || m.Offset != 0 || m.PendingEnd != 7 {
		t.Fatalf("%+v %v", m, e)
	}
	s.failManifest = false
	if _, e := JobLogAppend(ctx, s, "a", 0, []byte("recover"), true, 100); e != nil {
		t.Fatal(e)
	}
	b, _, e := JobLogRead(ctx, s, "a", 0, 100)
	if e != nil || string(b) != "recover" {
		t.Fatalf("%q %v", b, e)
	}
	if _, e := JobLogAppend(ctx, s, "b", 0, []byte("oversize"), false, 1); !errors.Is(e, ErrJobLogLimit) {
		t.Fatal(e)
	}
}

type logPausedStore struct {
	*FilesystemStorage
	staged, release chan struct{}
	once            sync.Once
}

func (s *logPausedStore) Update(ctx context.Context, key string, f func([]byte) ([]byte, error)) error {
	if strings.HasPrefix(key, "job_log_chunks/") {
		s.once.Do(func() { close(s.staged); <-s.release })
	}
	return s.FilesystemStorage.Update(ctx, key, f)
}
func TestJobLogDeleteFencesNotYetCreatedReservedChunk(t *testing.T) {
	ctx := context.Background()
	base := historyStore(t)
	s := &logPausedStore{FilesystemStorage: base, staged: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, e := JobLogAppend(ctx, s, "a", 0, []byte("late"), false, 100); done <- e }()
	<-s.staged
	if e := JobLogDelete(ctx, base, "a"); e != nil {
		t.Fatal(e)
	}
	close(s.release)
	if e := <-done; !errors.Is(e, ErrJobLogDeleted) {
		t.Fatal(e)
	}
	raw, e := base.Get(ctx, jobLogPrefix("a")+"00000000000000000000")
	if e != nil || strings.Contains(string(raw), "data") {
		t.Fatalf("unexpected late chunk %q %v", raw, e)
	}
}

type logReadCountingStore struct {
	*FilesystemStorage
	chunkReads int
}

func (s *logReadCountingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "job_log_chunks/") {
		s.chunkReads++
	}
	return s.FilesystemStorage.Get(ctx, key)
}
func TestJobLogRangeFetchesOnlyOverlappingChunks(t *testing.T) {
	ctx := context.Background()
	base := historyStore(t)
	data := bytes.Repeat([]byte("x"), JobLogChunkSize)
	for i := 0; i < 20; i++ {
		if _, e := JobLogAppend(ctx, base, "a", int64(i*JobLogChunkSize), data, false, 2<<20); e != nil {
			t.Fatal(e)
		}
	}
	s := &logReadCountingStore{FilesystemStorage: base}
	got, _, e := JobLogRead(ctx, s, "a", 10*JobLogChunkSize-3, 10)
	if e != nil || len(got) != 10 || s.chunkReads != 2 {
		t.Fatalf("read bytes=%d chunks=%d err=%v", len(got), s.chunkReads, e)
	}
}

func TestJobLogRejectsCorruptManifestBeforeReadAppendOrDelete(t *testing.T) {
	cases := []string{
		`{"offset":-1}`,
		`{"offset":1073741825}`,
		`{"offset":0,"pending_end":1073741825,"pending_hash":"abc"}`,
		`{"offset":10,"pending_end":9,"pending_hash":"abc"}`,
		`{"offset":10,"pending_end":10,"pending_hash":"abc"}`,
		`{"offset":0,"pending_end":65537,"pending_hash":"abc"}`,
		`{"offset":0,"pending_end":10,"pending_hash":"abc"}`,
		`{"offset":0,"pending_hash":"abc"}`,
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			ctx := context.Background()
			s := historyStore(t)
			if e := s.Put(ctx, jobLogManifestKey("a"), []byte(raw)); e != nil {
				t.Fatal(e)
			}
			if _, e := JobLogInfo(ctx, s, "a"); e == nil {
				t.Fatal("Info accepted corrupt manifest")
			}
			if _, _, e := JobLogRead(ctx, s, "a", 0, 10); e == nil {
				t.Fatal("Read accepted corrupt manifest")
			}
			if _, e := JobLogAppend(ctx, s, "a", 0, []byte("x"), false, 100); e == nil {
				t.Fatal("Append accepted corrupt manifest")
			}
			if e := JobLogDelete(ctx, s, "a"); e == nil {
				t.Fatal("Delete accepted corrupt manifest")
			}
			after, e := s.Get(ctx, jobLogManifestKey("a"))
			if e != nil || string(after) != raw {
				t.Fatalf("modified corrupt manifest: %q %v", after, e)
			}
		})
	}
}

// Wrappers must explicitly forward bounded legacy removal rather than silently
// falling back to generic Delete, which can fetch a remote legacy blob.
type logNoLegacyRemovalStore struct {
	Storage
	AtomicStorage
}

func TestJobLogDeleteRequiresBoundedLegacyRemoval(t *testing.T) {
	ctx := context.Background()
	base := historyStore(t)
	if e := base.Put(ctx, "job_logs:a", []byte("legacy")); e != nil {
		t.Fatal(e)
	}
	s := &logNoLegacyRemovalStore{Storage: base, AtomicStorage: base}
	if e := JobLogDelete(ctx, s, "a"); !errors.Is(e, ErrUnsupported) {
		t.Fatal(e)
	}
	b, e := base.Get(ctx, "job_logs:a")
	if e != nil || string(b) != "legacy" {
		t.Fatalf("legacy touched %q %v", b, e)
	}
	m, e := JobLogInfo(ctx, base, "a")
	if e != nil || !m.Deleted {
		t.Fatalf("fence missing %+v %v", m, e)
	}
	if e := JobLogDelete(ctx, base, "a"); e != nil {
		t.Fatal(e)
	}
}
