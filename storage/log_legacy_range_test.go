package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
)

func TestFilesystemLegacyLogRange(t *testing.T) {
	ctx := context.Background()
	s, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("abcdef"), 100000)
	if err = s.Put(ctx, "job_logs:one", data); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 1, 99, int64(len(data) - 3), int64(len(data) + 1)} {
		got, total, err := s.GetRange(ctx, "job_logs:one", offset, 10)
		if err != nil {
			t.Fatal(err)
		}
		end := offset + 10
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		start := offset
		if start > end {
			start = end
		}
		if total != int64(len(data)) || !bytes.Equal(got, data[start:end]) {
			t.Fatalf("offset %d total %d data %q", offset, total, got)
		}
	}
	if err = s.Expire(ctx, "job_logs:one", time.Now().Add(-time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.GetRange(ctx, "job_logs:one", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, _, err = s.GetRange(ctx, "job_logs:missing", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "secret")
	if err = os.WriteFile(target, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, filepath.Join(s.baseDir, "job_logs:link.json")); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.GetRange(ctx, "job_logs:link", 0, 10); err == nil {
		t.Fatal("followed symlink")
	}
	if _, _, err = s.GetRange(ctx, "../escape", 0, 10); err == nil {
		t.Fatal("accepted traversal")
	}
	if _, _, err = s.GetRange(ctx, "job_logs:one", 0, 1024*1024+1); err == nil {
		t.Fatal("accepted oversized range")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = s.GetRange(canceled, "job_logs:one", 0, 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func legacyS3Store(t *testing.T, handler http.HandlerFunc) *S3Storage {
	t.Helper()
	return prefixS3Store(t, "tenant", handler)
}

func TestS3LegacyLogBoundedRanges(t *testing.T) {
	for _, length := range []int{0, 1, 2, 3, 37, 300001, 300002, 300003, 1500001} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), length)
			doc, _ := json.Marshal(remoteDocument{Data: data, Revision: "rev"})
			maxRange := 0
			s := legacyS3Store(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/bucket/tenant/job_logs:one" {
					t.Errorf("path %s", r.URL.Path)
				}
				w.Header().Set("ETag", `"rev"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Length", fmt.Sprint(len(doc)))
					return
				}
				if r.Header.Get("If-Match") != `"rev"` {
					t.Error("missing IfMatch")
				}
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
					t.Fatal(err)
				}
				if start < 0 || end >= len(doc) || end < start {
					t.Fatalf("range %d-%d", start, end)
				}
				if end-start+1 > maxRange {
					maxRange = end - start + 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(doc)))
				w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(doc[start : end+1])
			})
			for _, offset := range []int64{0, 1, 2, 3, 33, int64(length - 1), int64(length + 3)} {
				if offset < 0 {
					continue
				}
				got, total, err := s.GetRange(context.Background(), "job_logs:one", offset, 1024*1024)
				if err != nil {
					t.Fatal(err)
				}
				start := offset
				if start > int64(length) {
					start = int64(length)
				}
				end := start + 1024*1024
				if end > int64(length) {
					end = int64(length)
				}
				if total != int64(length) || !bytes.Equal(got, data[start:end]) {
					t.Fatalf("offset %d len=%d total=%d", offset, len(got), total)
				}
			}
			if maxRange > base64RangeCap() {
				t.Fatalf("unbounded read %d", maxRange)
			}
		})
	}
}
func base64RangeCap() int { return (1024*1024+4)/3*4 + 4 }

func TestS3LegacyLogFailuresAndExpiry(t *testing.T) {
	for _, mode := range []string{"ignored", "conflict", "expired", "deleted", "metadata_too_large", "bad_range"} {
		t.Run(mode, func(t *testing.T) {
			doc := remoteDocument{Data: bytes.Repeat([]byte("x"), 10000), Revision: "rev"}
			if mode == "expired" {
				doc.Expires = time.Now().Add(-time.Hour).Unix()
			}
			if mode == "deleted" {
				doc.Deleted = true
				doc.Data = nil
			}
			if mode == "metadata_too_large" {
				doc.Revision = string(bytes.Repeat([]byte("r"), 5000))
			}
			body, _ := json.Marshal(doc)
			s := legacyS3Store(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"rev"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					return
				}
				if mode == "conflict" {
					w.WriteHeader(412)
					fmt.Fprint(w, `<Error><Code>PreconditionFailed</Code></Error>`)
					return
				}
				if mode == "ignored" {
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					w.Write(body)
					return
				}
				var start, end int
				fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
				header := fmt.Sprintf("bytes %d-%d/%d", start, end, len(body))
				if mode == "bad_range" {
					header = "bytes 0-1/2"
				}
				w.Header().Set("Content-Range", header)
				w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
				w.WriteHeader(206)
				w.Write(body[start : end+1])
			})
			_, _, err := s.GetRange(context.Background(), "job_logs:one", 0, 100)
			if err == nil {
				t.Fatal("accepted bad range")
			}
			if (mode == "expired" || mode == "deleted") && !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
			if mode == "conflict" && !errors.Is(err, ErrConflict) {
				t.Fatal(err)
			}
		})
	}
	c := &CouchbaseStorage{}
	if _, _, err := c.GetRange(context.Background(), "job_logs:one", 0, 100); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestFilesystemLegacyLogConcurrentReplaceAndCorruptExpiry(t *testing.T) {
	ctx := context.Background()
	s, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	a, b := bytes.Repeat([]byte("a"), 4096), bytes.Repeat([]byte("b"), 4096)
	if err = s.Put(ctx, "job_logs:race", a); err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			data := a
			if i%2 != 0 {
				data = b
			}
			if err := s.Put(ctx, "job_logs:race", data); err != nil {
				failures <- err
				return
			}
		}
	}()
	for i := 0; i < 50; i++ {
		got, total, err := s.GetRange(ctx, "job_logs:race", 7, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if total != 4096 || (!bytes.Equal(got, a[:1000]) && !bytes.Equal(got, b[:1000])) {
			t.Fatal("mixed revisions")
		}
	}
	<-done
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	expiry, err := s.path("_expire/job_logs:race")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(expiry), 0700); err != nil {
		t.Fatal(err)
	}
	for _, metadata := range [][]byte{[]byte("123 garbage"), bytes.Repeat([]byte("1"), 1000)} {
		if err = os.WriteFile(expiry, metadata, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err = s.GetRange(ctx, "job_logs:race", 0, 10); err == nil {
			t.Fatal("corrupt expiry accepted")
		}
	}
}

func TestS3RemoveLegacyLogNeverReadsPayload(t *testing.T) {
	calls := 0
	s := legacyS3Store(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodDelete {
			t.Errorf("legacy delete fetched body: %s", r.Method)
			w.WriteHeader(500)
			return
		}
		if r.URL.Path != "/bucket/tenant/job_logs:one" {
			t.Errorf("wrong delete key: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if err := s.RemoveLegacyJobLog(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls %d", calls)
	}
}

type legacyCBRemoveSpy struct {
	calls int
	key   string
	err   error
	ctx   context.Context
}

func (s *legacyCBRemoveSpy) Remove(key string, opts *gocb.RemoveOptions) (*gocb.MutationResult, error) {
	s.calls++
	s.key = key
	s.ctx = opts.Context
	return nil, s.err
}
func (s *legacyCBRemoveSpy) Get(string, *gocb.GetOptions) (*gocb.GetResult, error) {
	panic("legacy deletion must never Get")
}

func TestCouchbaseRemoveLegacyLogNeverReadsPayload(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "request")
	for _, failure := range []error{nil, gocb.ErrDocumentNotFound, errors.New("backend outage")} {
		spy := &legacyCBRemoveSpy{err: failure}
		err := removeLegacyCouchbaseLog(ctx, spy, "tenant:", "one")
		if spy.calls != 1 || spy.key != "tenant:job_logs:one" || spy.ctx != ctx {
			t.Fatalf("unexpected remove: %+v", spy)
		}
		if errors.Is(failure, gocb.ErrDocumentNotFound) {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, failure) {
			t.Fatalf("want %v got %v", failure, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	spy := &legacyCBRemoveSpy{}
	if err := removeLegacyCouchbaseLog(canceled, spy, "tenant:", "one"); !errors.Is(err, context.Canceled) || spy.calls != 0 {
		t.Fatalf("canceled remove: %v %+v", err, spy)
	}
}

func TestFilesystemRemoveLegacyLogWithCorruptExpiry(t *testing.T) {
	s, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Put(context.Background(), "job_logs:one", bytes.Repeat([]byte("x"), 100000)); err != nil {
		t.Fatal(err)
	}
	path, err := s.path("_expire/job_logs:one")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("invalid ttl"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.RemoveLegacyJobLog(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expiry retained: %v", err)
	}
	if err = s.RemoveLegacyJobLog(context.Background(), "one"); err != nil {
		t.Fatalf("delete not idempotent: %v", err)
	}
}
