package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/blob"
)

var errV2Fault = errors.New("injected v2 fault")

type v2MemoryObject struct {
	data     []byte
	modified time.Time
}
type v2MemoryBlob struct {
	mu         sync.Mutex
	objects    map[string]v2MemoryObject
	puts       int
	opens      int
	failPut    int
	losePut    int
	beforePut  func(blob.Write)
	beforeOpen func(string)
	afterScan  func()
}

func newV2MemoryBlob() *v2MemoryBlob { return &v2MemoryBlob{objects: map[string]v2MemoryObject{}} }
func (b *v2MemoryBlob) Put(ctx context.Context, w blob.Write, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(r, w.Size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != w.Size || sha256.Sum256(data) != w.SHA256 {
		return blob.ErrIntegrity
	}
	b.mu.Lock()
	b.puts++
	n := b.puts
	fail, lose, hook := b.failPut, b.losePut, b.beforePut
	b.mu.Unlock()
	if hook != nil {
		hook(w)
	}
	if n == fail {
		return errV2Fault
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if old, ok := b.objects[w.Key]; ok && !bytes.Equal(old.data, data) {
		return blob.ErrIntegrity
	}
	b.objects[w.Key] = v2MemoryObject{append([]byte(nil), data...), time.Now().Add(-time.Hour)}
	if n == lose {
		return errV2Fault
	}
	return nil
}
func (b *v2MemoryBlob) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.opens++
	hook := b.beforeOpen
	b.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	object, ok := b.objects[key]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), object.data...))), nil
}
func (b *v2MemoryBlob) Stat(ctx context.Context, key string) (blob.Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	object, ok := b.objects[key]
	if !ok {
		return blob.Info{}, blob.ErrNotFound
	}
	return blob.Info{Key: key, Size: int64(len(object.data)), LastModified: object.modified}, ctx.Err()
}
func (b *v2MemoryBlob) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
	return nil
}
func (b *v2MemoryBlob) Scan(ctx context.Context, prefix, cursor string, limit int) (blob.Page, error) {
	if err := ctx.Err(); err != nil {
		return blob.Page{}, err
	}
	b.mu.Lock()
	keys := []string{}
	for key := range b.objects {
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	page := blob.Page{}
	for _, key := range keys[:min(len(keys), limit)] {
		object := b.objects[key]
		page.Objects = append(page.Objects, blob.Info{Key: key, Size: int64(len(object.data)), LastModified: object.modified})
	}
	if len(keys) > limit {
		page.NextCursor = keys[limit-1]
	}
	hook := b.afterScan
	b.mu.Unlock()
	if hook != nil {
		hook()
	}
	return page, nil
}

type v2FaultMetadata struct {
	*FilesystemStorage
	mu             sync.Mutex
	updates        int
	failAt         int
	loseAt         int
	blockReads     bool
	blockAfterLoss bool
}

func (s *v2FaultMetadata) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	blocked := s.blockReads
	s.mu.Unlock()
	if blocked {
		return nil, errV2Fault
	}
	return s.FilesystemStorage.Get(ctx, key)
}
func (s *v2FaultMetadata) Update(ctx context.Context, key string, f func([]byte) ([]byte, error)) error {
	s.mu.Lock()
	s.updates++
	n := s.updates
	fail, lose, block := s.failAt, s.loseAt, s.blockAfterLoss
	s.mu.Unlock()
	if n == fail {
		return errV2Fault
	}
	err := s.FilesystemStorage.Update(ctx, key, f)
	if err == nil && n == lose {
		if block {
			s.mu.Lock()
			s.blockReads = true
			s.mu.Unlock()
		}
		return errV2Fault
	}
	return err
}

func v2Repository(t *testing.T, meta Storage, b *v2MemoryBlob) *JobLogRepository {
	t.Helper()
	r, err := NewJobLogRepository(meta, map[string]blob.Store{"logs": b}, JobLogRepositoryOptions{Namespace: "testnamespace", WriteStore: "logs", WriteFormat: 2})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func v2Root(t *testing.T, meta Storage, id string) *JobLogV2Root {
	t.Helper()
	raw, err := meta.Get(context.Background(), jobLogManifestKey(id))
	if err != nil {
		t.Fatal(err)
	}
	r, err := DecodeJobLogV2Root(raw)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func v2Append(t *testing.T, r *JobLogRepository, id string, offset int64, data []byte, final bool) JobLogManifest {
	t.Helper()
	m, err := r.Append(context.Background(), id, offset, data, final, JobLogAbsoluteMax)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func v2CollectAll(t *testing.T, r *JobLogRepository) JobLogGCResult {
	t.Helper()
	total := JobLogGCResult{}
	cursor := ""
	for n := 0; n < 10000; n++ {
		result, err := r.CollectStore(context.Background(), "logs", cursor, 7)
		if err != nil {
			t.Fatal(err)
		}
		total.Scanned += result.Scanned
		total.Deleted += result.Deleted
		total.Bytes += result.Bytes
		cursor = result.NextCursor
		if cursor == "" {
			return total
		}
	}
	t.Fatal("inventory did not terminate")
	return total
}

func TestJobLogV2FixedChunksPagesAndBoundedReads(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	data := bytes.Repeat([]byte("0123456789abcdef"), (JobLogChunkSize*129+777)/16+1)
	data = data[:JobLogChunkSize*129+777]
	for at := 0; at < len(data); {
		end := min(at+JobLogChunkSize-13, len(data))
		v2Append(t, r, "large", int64(at), data[at:end], false)
		at = end
	}
	v2Append(t, r, "large", int64(len(data)), nil, true)
	root := v2Root(t, meta, "large")
	if len(root.Pages) != 2 || !root.Final {
		t.Fatalf("unexpected root %+v", root)
	}
	b.mu.Lock()
	b.opens = 0
	b.mu.Unlock()
	start := int64(JobLogChunkSize*127 - 17)
	got, m, err := r.Read(ctx, "large", start, JobLogChunkSize+90)
	if err != nil || !bytes.Equal(got, data[start:start+int64(len(got))]) || m.Offset != int64(len(data)) {
		t.Fatalf("read len=%d manifest=%+v error=%v", len(got), m, err)
	}
	b.mu.Lock()
	reads := b.opens
	b.mu.Unlock()
	if reads > 5 {
		t.Fatalf("range read fetched %d objects", reads)
	}
	before, _ := meta.ListKeys(ctx)
	if len(before) != 1 {
		t.Fatalf("append created %d metadata records", len(before))
	}
	result := v2CollectAll(t, r)
	if result.Deleted == 0 {
		t.Fatal("old immutable versions were not reclaimed")
	}
	if _, _, err = r.Read(ctx, "large", start, JobLogChunkSize+90); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err = r.VisitCommittedObjects(ctx, "large", func(o JobLogObject, data []byte) error {
		count++
		if int64(len(data)) != o.Size {
			return errors.New("wrong object size")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 132 {
		t.Fatalf("expected 130 chunks and 2 pages, got %d", count)
	}
}

func TestJobLogV2TinyAppendsKeepOneMetadataRoot(t *testing.T) {
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	for i := 0; i < 200; i++ {
		v2Append(t, r, "small", int64(i), []byte{byte(i)}, false)
	}
	root := v2Root(t, meta, "small")
	if len(root.Pages) != 1 || root.Offset != 200 {
		t.Fatal(root)
	}
	page, _, err := r.readPage(context.Background(), "small", root, 0)
	if err != nil || len(page.Chunks) != 1 {
		t.Fatalf("%+v %v", page, err)
	}
	v2CollectAll(t, r)
	b.mu.Lock()
	objects := len(b.objects)
	b.mu.Unlock()
	if objects != 2 {
		t.Fatalf("expected one current chunk and page, got %d", objects)
	}
	keys, err := meta.ListKeys(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatalf("metadata keys=%v err=%v", keys, err)
	}
}

func TestJobLogV2FaultRecoveryAndAcknowledgmentLoss(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		metaFail, metaLose, blobFail, blobLose int
		block                                  bool
	}{
		{"reserve-before", 1, 0, 0, 0, false}, {"reserve-after", 0, 1, 0, 0, false}, {"reserve-unknown", 0, 1, 0, 0, true},
		{"chunk-before", 0, 0, 1, 0, false}, {"chunk-after", 0, 0, 0, 1, false},
		{"page-before", 0, 0, 2, 0, false}, {"page-after", 0, 0, 0, 2, false},
		{"publish-before", 2, 0, 0, 0, false}, {"publish-after", 0, 2, 0, 0, false}, {"publish-unknown", 0, 2, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			meta := &v2FaultMetadata{FilesystemStorage: historyStore(t), failAt: tc.metaFail, loseAt: tc.metaLose, blockAfterLoss: tc.block}
			b := newV2MemoryBlob()
			b.failPut = tc.blobFail
			b.losePut = tc.blobLose
			r := v2Repository(t, meta, b)
			_, firstErr := r.Append(ctx, "recover", 0, []byte("durable bytes"), false, 100)
			if (tc.metaFail > 0 || tc.blobFail > 0 || tc.block) && firstErr == nil {
				t.Fatal("expected injected failure")
			}
			meta.mu.Lock()
			meta.failAt = 0
			meta.loseAt = 0
			meta.blockReads = false
			meta.mu.Unlock()
			b.mu.Lock()
			b.failPut = 0
			b.losePut = 0
			b.mu.Unlock()
			m, err := r.Append(ctx, "recover", 0, []byte("durable bytes"), true, 100)
			if err != nil {
				t.Fatal(err)
			}
			if !m.Final {
				m, err = r.Append(ctx, "recover", m.Offset, nil, true, 100)
				if err != nil {
					t.Fatal(err)
				}
			}
			got, _, err := r.Read(ctx, "recover", 0, 100)
			if err != nil || string(got) != "durable bytes" {
				t.Fatalf("%q %v", got, err)
			}
			root := v2Root(t, meta, "recover")
			if root.IssuedSeq != 1 || root.Pending != nil || !root.Final {
				t.Fatalf("unexpected recovery root %+v", root)
			}
		})
	}
}

func TestJobLogV2CrossBoundaryPlanSurvivesEachObjectFailure(t *testing.T) {
	// A reservation crossing both a chunk and page boundary has four objects.
	for fail := 1; fail <= 4; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			meta := historyStore(t)
			b := newV2MemoryBlob()
			r := v2Repository(t, meta, b)
			chunk := bytes.Repeat([]byte("a"), JobLogChunkSize)
			for i := 0; i < 127; i++ {
				v2Append(t, r, "cross", int64(i*JobLogChunkSize), chunk, false)
			}
			v2Append(t, r, "cross", 127*JobLogChunkSize, chunk[:17], false)
			b.mu.Lock()
			b.failPut = b.puts + fail
			b.mu.Unlock()
			_, err := r.Append(context.Background(), "cross", 127*JobLogChunkSize+17, chunk, true, JobLogAbsoluteMax)
			if err == nil {
				t.Fatal("expected failure")
			}
			root := v2Root(t, meta, "cross")
			if root.Pending == nil || len(root.Pending.Chunks) != 2 || len(root.Pending.Pages) != 2 {
				t.Fatal("reservation not fully registered")
			}
			b.mu.Lock()
			b.failPut = 0
			b.mu.Unlock()
			v2Append(t, r, "cross", 127*JobLogChunkSize+17, chunk, true)
			got, _, err := r.Read(context.Background(), "cross", 127*JobLogChunkSize+17, JobLogChunkSize)
			if err != nil || !bytes.Equal(got, chunk) {
				t.Fatal(err)
			}
		})
	}
}

func TestJobLogV2ConcurrentReplayAndConflictingInput(t *testing.T) {
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Append(ctx, "same", 0, []byte("same"), false, 100); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if _, err := r.Append(ctx, "same", 0, []byte("diff"), false, 100); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
	root := v2Root(t, meta, "same")
	if root.IssuedSeq != 1 || root.Offset != 4 {
		t.Fatal(root)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, data := range []string{"one", "two"} {
		go func(data string) {
			<-start
			_, err := r.Append(ctx, "different", 0, []byte(data), false, 100)
			results <- err
		}(data)
	}
	close(start)
	e1, e2 := <-results, <-results
	if (e1 == nil) == (e2 == nil) {
		t.Fatalf("expected exactly one winner: %v %v", e1, e2)
	}
}

func TestJobLogV2DeleteAndRevocationFenceLateUploads(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprint(remove), func(t *testing.T) {
			ctx := context.Background()
			meta := historyStore(t)
			b := newV2MemoryBlob()
			r := v2Repository(t, meta, b)
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			b.beforePut = func(blob.Write) { once.Do(func() { close(started); <-release }) }
			done := make(chan error, 1)
			go func() { _, err := r.Append(ctx, "late", 0, []byte("original"), false, 100); done <- err }()
			<-started
			root := v2Root(t, meta, "late")
			if root.Pending == nil {
				t.Fatal("PUT started before reservation")
			}
			if remove {
				if err := r.Delete(ctx, "late"); err != nil {
					t.Fatal(err)
				}
			} else {
				r.now = func() time.Time { return time.Unix(0, root.Pending.Deadline+1) }
				if revoked, err := r.RevokeExpiredPending(ctx, "late"); err != nil || !revoked {
					t.Fatalf("%t %v", revoked, err)
				}
			}
			// Sweep while the old PUT is still blocked; it can arrive after DELETE.
			v2CollectAll(t, r)
			close(release)
			err := <-done
			if remove && !errors.Is(err, ErrJobLogDeleted) {
				t.Fatalf("late publish=%v", err)
			}
			if !remove && !errors.Is(err, ErrConflict) {
				t.Fatalf("revoked publish=%v", err)
			}
			if !remove {
				v2Append(t, r, "late", 0, []byte("newvalue"), true)
			}
			v2CollectAll(t, r)
			if remove {
				b.mu.Lock()
				remaining := len(b.objects)
				b.mu.Unlock()
				if remaining != 0 {
					t.Fatalf("late objects remaining: %d", remaining)
				}
			} else {
				got, _, err := r.Read(ctx, "late", 0, 100)
				if err != nil || string(got) != "newvalue" {
					t.Fatalf("%q %v", got, err)
				}
				root = v2Root(t, meta, "late")
				if root.IssuedSeq != 2 {
					t.Fatal("revoked sequence was reused")
				}
			}
		})
	}
}

func TestJobLogV2GCPreservesPendingUploads(t *testing.T) {
	ctx := context.Background()
	meta := &v2FaultMetadata{FilesystemStorage: historyStore(t)}
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "pending", 0, []byte("a"), false)
	meta.mu.Lock()
	meta.failAt = meta.updates + 2
	meta.mu.Unlock()
	if _, err := r.Append(ctx, "pending", 1, []byte("b"), false, 100); err == nil {
		t.Fatal("expected publication failure")
	}
	root := v2Root(t, meta, "pending")
	if root.Pending == nil {
		t.Fatal("missing pending plan")
	}
	v2CollectAll(t, r)
	for _, ref := range []JobLogObjectRef{root.Pending.Chunks[0].Object, root.Pending.Pages[0].Object} {
		if _, err := b.Stat(ctx, logObjectKey(root.Namespace, "pending", root.Incarnation, ref.ID)); err != nil {
			t.Fatal("collected pending object")
		}
	}
	meta.mu.Lock()
	meta.failAt = 0
	meta.mu.Unlock()
	v2Append(t, r, "pending", 1, []byte("b"), false)
	got, _, err := r.Read(ctx, "pending", 0, 100)
	if err != nil || string(got) != "ab" {
		t.Fatalf("%q %v", got, err)
	}
}

type v2SnapshotMetadata struct {
	*FilesystemStorage
	key      string
	snapshot []byte
}

func (s *v2SnapshotMetadata) Get(ctx context.Context, key string) ([]byte, error) {
	if key == s.key {
		return append([]byte(nil), s.snapshot...), ctx.Err()
	}
	return s.FilesystemStorage.Get(ctx, key)
}

func TestJobLogV2SnapshotWatermarkProtectsNewerReservations(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	writer := v2Repository(t, meta, b)
	v2Append(t, writer, "watermark", 0, []byte("a"), false)
	before, err := meta.Get(ctx, jobLogManifestKey("watermark"))
	if err != nil {
		t.Fatal(err)
	}
	v2Append(t, writer, "watermark", 1, []byte("b"), false)
	// Give the collector a complete earlier root snapshot while inventory
	// includes later reservations. Newer objects are not in that snapshot's
	// mark set, and only the issued-sequence watermark protects them.
	stale := &v2SnapshotMetadata{FilesystemStorage: meta, key: jobLogManifestKey("watermark"), snapshot: before}
	collector := v2Repository(t, stale, b)
	result, err := collector.CollectStore(ctx, "logs", "", 100)
	if err != nil || result.Deleted != 0 {
		t.Fatalf("snapshot collector result=%+v error=%v", result, err)
	}
	got, _, err := writer.Read(ctx, "watermark", 0, 100)
	if err != nil || string(got) != "ab" {
		t.Fatalf("new reservation was lost: %q %v", got, err)
	}
	if current := v2CollectAll(t, writer); current.Deleted != 2 {
		t.Fatalf("expected only retired tail and page: %+v", current)
	}
}

func TestJobLogV2MissingObjectsNeverBecomeEOFOrFallback(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "broken", 0, []byte("content"), true)
	root := v2Root(t, meta, "broken")
	page, _, err := r.readPage(ctx, "broken", root, 0)
	if err != nil {
		t.Fatal(err)
	}
	key := logObjectKey(root.Namespace, "broken", root.Incarnation, page.Chunks[0].ID)
	if err = b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Read(ctx, "broken", 0, 100)
	if !errors.Is(err, ErrJobLogIntegrity) || errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong missing-object error: %v", err)
	}
	if err = meta.PutStream(ctx, "job_logs:broken", []byte("legacy fallback")); err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Read(ctx, "broken", 0, 100)
	if !errors.Is(err, ErrJobLogIntegrity) {
		t.Fatal(err)
	}
}

func TestJobLogV2PoisonedOwnerCannotAuthorizeSweep(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "broken", 0, []byte("a"), false)
	v2Append(t, r, "broken", 1, []byte("b"), false)
	v2Append(t, r, "healthy", 0, []byte("a"), false)
	v2Append(t, r, "healthy", 1, []byte("b"), false)
	root := v2Root(t, meta, "broken")
	if err := b.Delete(ctx, logObjectKey(root.Namespace, "broken", root.Incarnation, root.Pages[0].ID)); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	before := len(b.objects)
	b.mu.Unlock()
	result, err := r.CollectStore(ctx, "logs", "", 100)
	if err == nil || result.Errors == 0 || result.Deleted != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	b.mu.Lock()
	after := len(b.objects)
	b.mu.Unlock()
	if before-after != 2 {
		t.Fatal("poisoned owner was swept")
	}
}

func TestJobLogV2ReaderRetriesAfterOldPageCollection(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "reader", 0, []byte("a"), false)
	root := v2Root(t, meta, "reader")
	oldKey := logObjectKey(root.Namespace, "reader", root.Incarnation, root.Pages[0].ID)
	b.beforeOpen = func(key string) {
		if key != oldKey {
			return
		}
		b.mu.Lock()
		b.beforeOpen = nil
		b.mu.Unlock()
		v2Append(t, r, "reader", 1, []byte("b"), false)
		v2CollectAll(t, r)
	}
	got, _, err := r.Read(ctx, "reader", 0, 1)
	if err != nil || string(got) != "a" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestJobLogV2LegacyRoutingAndFormatGuards(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	if _, err := JobLogAppend(ctx, meta, "old", 0, []byte("v1"), true, 100); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutStream(ctx, "job_logs:legacy", []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"old": "v1", "legacy": "legacy"} {
		got, _, err := r.Read(ctx, id, 0, 100)
		if err != nil || string(got) != want {
			t.Fatalf("%s:%q %v", id, got, err)
		}
	}
	v2Append(t, r, "new", 0, []byte("v2"), true)
	if _, err := JobLogInfo(ctx, meta, "new"); !errors.Is(err, ErrJobLogFormat) {
		t.Fatal("legacy reader accepted v2")
	}
	if _, err := JobLogAppend(ctx, meta, "new", 0, []byte("bad"), false, 100); !errors.Is(err, ErrJobLogFormat) {
		t.Fatal("legacy writer accepted v2")
	}
	if err := JobLogDelete(ctx, meta, "new"); !errors.Is(err, ErrJobLogFormat) {
		t.Fatal("legacy deletion accepted v2")
	}
	if err := meta.Put(ctx, jobLogManifestKey("future"), []byte(`{"format":999,"offset":0}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Read(ctx, "future", 0, 100); !errors.Is(err, ErrJobLogFormat) {
		t.Fatal(err)
	}
}

func TestJobLogV2RebindAndDeletedFence(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "copy", 0, []byte("archived"), true)
	targetMeta := historyStore(t)
	targetBlob := newV2MemoryBlob()
	if err := r.VisitCommittedObjects(ctx, "copy", func(o JobLogObject, data []byte) error {
		key, err := RebindJobLogObjectKey(o.Key, "fresh")
		if err != nil {
			return err
		}
		return targetBlob.Put(ctx, blob.Write{Key: key, Size: o.Size, SHA256: sha256.Sum256(data)}, bytes.NewReader(data))
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := meta.Get(ctx, jobLogManifestKey("copy"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err = RebindJobLogV2Root(raw, "fresh", "restored")
	if err != nil {
		t.Fatal(err)
	}
	if err = targetMeta.Put(ctx, jobLogManifestKey("copy"), raw); err != nil {
		t.Fatal(err)
	}
	target, err := NewJobLogRepository(targetMeta, map[string]blob.Store{"restored": targetBlob}, JobLogRepositoryOptions{Namespace: "fresh", WriteStore: "restored", WriteFormat: 2})
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.objects = map[string]v2MemoryObject{}
	b.mu.Unlock()
	got, _, err := target.Read(ctx, "copy", 0, 100)
	if err != nil || string(got) != "archived" {
		t.Fatalf("%q %v", got, err)
	}
	if err = target.Delete(ctx, "absent"); err != nil {
		t.Fatal(err)
	}
	deleted := v2Root(t, targetMeta, "absent")
	if !deleted.Deleted || !deleted.Final || deleted.Incarnation == "" {
		t.Fatal(deleted)
	}
	if _, err = target.Append(ctx, "absent", 0, []byte("resurrect"), true, 100); !errors.Is(err, ErrJobLogDeleted) {
		t.Fatal(err)
	}
}

func TestJobLogV2RootCorruptionAndLimits(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "valid", 0, []byte("test"), true)
	valid := v2Root(t, meta, "valid")
	for _, mutate := range []func(*JobLogV2Root){
		func(r *JobLogV2Root) { r.Offset = -1 }, func(r *JobLogV2Root) { r.Offset = JobLogAbsoluteMax + 1 },
		func(r *JobLogV2Root) { r.Pages = nil }, func(r *JobLogV2Root) { r.IssuedSeq = 0 },
		func(r *JobLogV2Root) { r.Incarnation = "../bad" }, func(r *JobLogV2Root) { r.Pages[0].Size = JobLogChunkSize + 1 },
	} {
		copyRoot := *valid
		copyRoot.Pages = append([]JobLogObjectRef(nil), valid.Pages...)
		mutate(&copyRoot)
		raw, _ := json.Marshal(copyRoot)
		if _, err := DecodeJobLogV2Root(raw); err == nil {
			t.Fatalf("accepted invalid root: %s", raw)
		}
	}
	if _, err := r.Append(ctx, "oversize", 0, make([]byte, JobLogChunkSize+1), false, JobLogAbsoluteMax); err == nil {
		t.Fatal("oversized append accepted")
	}
	if _, err := r.Append(ctx, "beyond", JobLogAbsoluteMax, []byte("x"), false, JobLogAbsoluteMax); !errors.Is(err, ErrJobLogLimit) {
		t.Fatal(err)
	}
	if _, err := r.Append(ctx, strings.Repeat("x", JobLogIDMaxBytes+1), 0, []byte("x"), false, 100); err == nil {
		t.Fatal("oversized owner accepted")
	}
}

type v2FormatSwitchMetadata struct {
	*FilesystemStorage
	updates   int
	replaceAt int
}

func (s *v2FormatSwitchMetadata) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	s.updates++
	if s.updates == s.replaceAt {
		// Simulate a legacy writer winning new-root creation, or an unsupported
		// old binary replacing a v2 root before a write outcome is reconciled.
		if err := s.FilesystemStorage.Put(ctx, key, []byte(`{"offset":0,"final":false,"deleted":false}`)); err != nil {
			return err
		}
		return errV2Fault
	}
	return s.FilesystemStorage.Update(ctx, key, fn)
}

func TestJobLogV2WriteReadbackRejectsChangedFormat(t *testing.T) {
	for _, phase := range []string{"reserve", "publish", "seal", "delete", "revoke"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			meta := &v2FormatSwitchMetadata{FilesystemStorage: historyStore(t)}
			b := newV2MemoryBlob()
			r := v2Repository(t, meta, b)
			var err error
			switch phase {
			case "reserve":
				meta.replaceAt = 1
				_, err = r.Append(ctx, "mixed", 0, []byte("a"), false, 100)
			case "publish":
				meta.replaceAt = 2
				_, err = r.Append(ctx, "mixed", 0, []byte("a"), false, 100)
			case "seal":
				v2Append(t, r, "mixed", 0, []byte("a"), false)
				meta.replaceAt = meta.updates + 1
				_, err = r.Append(ctx, "mixed", 1, nil, true, 100)
			case "delete":
				v2Append(t, r, "mixed", 0, []byte("a"), false)
				meta.replaceAt = meta.updates + 1
				err = r.Delete(ctx, "mixed")
			case "revoke":
				b.failPut = 1
				if _, e := r.Append(ctx, "mixed", 0, []byte("a"), false, 100); e == nil {
					t.Fatal("expected upload failure")
				}
				pending := v2Root(t, meta, "mixed").Pending
				r.now = func() time.Time { return time.Unix(0, pending.Deadline+1) }
				meta.replaceAt = meta.updates + 1
				_, err = r.RevokeExpiredPending(ctx, "mixed")
			}
			if !errors.Is(err, ErrJobLogFormat) {
				t.Fatalf("%s returned %v", phase, err)
			}
		})
	}
}

func TestJobLogV2CollectorKeepsLiveChunkFromRetiredTailAttempt(t *testing.T) {
	ctx := context.Background()
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "mixedrefs", 0, []byte("a"), false)
	data := bytes.Repeat([]byte("b"), JobLogChunkSize)
	v2Append(t, r, "mixedrefs", 1, data, false)
	oldRoot := v2Root(t, meta, "mixedrefs")
	oldPage, _, err := r.readPage(ctx, "mixedrefs", oldRoot, 0)
	if err != nil {
		t.Fatal(err)
	}
	fullRef, tailRef := oldPage.Chunks[0], oldPage.Chunks[1]
	v2Append(t, r, "mixedrefs", JobLogChunkSize+1, []byte("c"), true)
	v2CollectAll(t, r)
	if _, err = b.Stat(ctx, logObjectKey(oldRoot.Namespace, "mixedrefs", oldRoot.Incarnation, fullRef.ID)); err != nil {
		t.Fatal("live full chunk was collected with the old tail")
	}
	if _, err = b.Stat(ctx, logObjectKey(oldRoot.Namespace, "mixedrefs", oldRoot.Incarnation, tailRef.ID)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal("retired tail was not collected")
	}
	got, _, err := r.Read(ctx, "mixedrefs", JobLogChunkSize-1, 3)
	if err != nil || string(got) != "bbc" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestJobLogV2MaximumRootEncodingIsBounded(t *testing.T) {
	r := &JobLogV2Root{Format: 2, Namespace: strings.Repeat("n", 128), StoreID: strings.Repeat("s", 128), Incarnation: strings.Repeat("a", 32), CommitSeq: 16384, IssuedSeq: 16385, Offset: JobLogAbsoluteMax - 1}
	for i := 0; i < jobLogV2MaxPages; i++ {
		r.Pages = append(r.Pages, JobLogObjectRef{ID: fmt.Sprintf("%020d-p-%032x", uint64(i+1), i), Size: JobLogV2PageMaxBytes, SHA256: strings.Repeat("b", 64)})
	}
	r.Pending = &JobLogV2Pending{AttemptID: strings.Repeat("c", 32), UploadSeq: r.IssuedSeq, BaseCommitSeq: r.CommitSeq, Start: r.Offset, End: JobLogAbsoluteMax, InputSHA256: logDigest([]byte("x")), Final: true, Deadline: time.Now().Add(time.Minute).UnixNano(), Chunks: []JobLogV2ChunkUpdate{{Index: 16383, Object: JobLogObjectRef{ID: fmt.Sprintf("%020d-c-%032x", r.IssuedSeq, 1), Size: JobLogChunkSize, SHA256: strings.Repeat("d", 64)}}}, Pages: []JobLogV2PageUpdate{{Index: 127, Object: JobLogObjectRef{ID: fmt.Sprintf("%020d-p-%032x", r.IssuedSeq, 2), Size: JobLogV2PageMaxBytes, SHA256: strings.Repeat("e", 64)}}}}
	raw, err := EncodeJobLogV2Root(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > JobLogV2RootMaxBytes {
		t.Fatalf("maximum root is %d bytes", len(raw))
	}
	if _, err = DecodeJobLogV2Root(raw); err != nil {
		t.Fatal(err)
	}
	r.Pages = append(r.Pages, r.Pages[0])
	if _, err = EncodeJobLogV2Root(r); err == nil {
		t.Fatal("excess page accepted")
	}
}

func TestJobLogV2GCGraceRetainsUnknownModificationTime(t *testing.T) {
	meta := historyStore(t)
	b := newV2MemoryBlob()
	r := v2Repository(t, meta, b)
	v2Append(t, r, "grace", 0, []byte("a"), false)
	v2Append(t, r, "grace", 1, []byte("b"), false)
	r.opts.GCGracePeriod = time.Minute
	b.mu.Lock()
	for key, object := range b.objects {
		object.modified = time.Time{}
		b.objects[key] = object
	}
	before := len(b.objects)
	b.mu.Unlock()
	result := v2CollectAll(t, r)
	if result.Deleted != 0 {
		t.Fatal("unknown timestamp was ignored")
	}
	r.opts.GCGracePeriod = 0
	result = v2CollectAll(t, r)
	if result.Deleted != before-2 {
		t.Fatal(result)
	}
}
