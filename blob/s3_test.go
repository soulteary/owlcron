package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type s3Fixture struct {
	mu       sync.Mutex
	objects  map[string][]byte
	requests int
	lists    int
	t        *testing.T
}

func (f *s3Fixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	for _, header := range []string{"If-Match", "If-None-Match"} {
		if _, present := r.Header[header]; present {
			f.t.Errorf("unexpected conditional header %s", header)
			http.Error(w, "conditional request forbidden", 501)
			return
		}
	}
	if r.URL.Query().Get("list-type") == "2" {
		f.lists++
		prefix, start := r.URL.Query().Get("prefix"), r.URL.Query().Get("start-after")
		limit, _ := strconv.Atoi(r.URL.Query().Get("max-keys"))
		if limit < 1 || limit > MaxScanLimit {
			f.t.Errorf("unbounded listing %d", limit)
			http.Error(w, "bad list", 400)
			return
		}
		keys := []string{}
		for key := range f.objects {
			if strings.HasPrefix(key, prefix) && key > start {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		type item struct {
			Key          string
			Size         int
			ETag         string
			LastModified string
		}
		type result struct {
			XMLName               xml.Name `xml:"ListBucketResult"`
			Name                  string
			Prefix                string
			IsTruncated           bool
			NextContinuationToken string
			Contents              []item
		}
		out := result{Name: "bucket", Prefix: prefix}
		if len(keys) > limit {
			out.IsTruncated = true
			out.NextContinuationToken = "must-not-prefetch"
			keys = keys[:limit]
		}
		for _, key := range keys {
			out.Contents = append(out.Contents, item{key, len(f.objects[key]), `"opaque"`, time.Now().UTC().Format(time.RFC3339)})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(out)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			f.t.Error(err)
		}
		if r.ContentLength != int64(len(data)) || len(r.TransferEncoding) > 0 {
			f.t.Errorf("non-plain upload: contentLength=%d bytes=%d transfer=%v", r.ContentLength, len(data), r.TransferEncoding)
		}
		sum := sha256.Sum256(data)
		if r.Header.Get("X-Amz-Content-Sha256") != hex.EncodeToString(sum[:]) {
			f.t.Error("missing concrete SHA-256 header")
		}
		md := md5.Sum(data)
		if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(md[:]) {
			f.t.Error("missing MD5 transport checksum")
		}
		f.objects[key] = append([]byte(nil), data...)
		w.Header().Set("ETag", `"opaque"`)
		w.WriteHeader(200)
	case http.MethodGet, http.MethodHead:
		data, ok := f.objects[key]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", `"opaque"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(204)
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		w.WriteHeader(400)
	}
}

func newTestS3(t *testing.T, handler http.Handler) (*S3, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{Endpoint: server.URL, Bucket: "bucket", Prefix: "data", Region: "us-east-1", AccessKey: "access", SecretKey: "secret", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, server
}

func TestS3OrdinaryObjectContractNeverUsesConditionalWrites(t *testing.T) {
	fixture := &s3Fixture{objects: make(map[string][]byte), t: t}
	s, _ := newTestS3(t, fixture)
	ctx := context.Background()
	key := "namespace/logs/chunk"
	data := []byte{0, 255, 1, '\n'}
	if fixture.requests != 0 {
		t.Fatal("constructor performed a remote capability probe")
	}
	if err := s.Put(ctx, writeSpec(key, data), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	body, err := s.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	body.Close()
	if err != nil || !bytes.Equal(data, got) {
		t.Fatalf("GET: %x %v", got, err)
	}
	info, err := s.Stat(ctx, key)
	if err != nil || info.Size != int64(len(data)) {
		t.Fatalf("HEAD: %+v %v", info, err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	before := fixture.requests
	if err := s.Put(ctx, writeSpec("bad", data), bytes.NewReader([]byte("corrupt"))); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	if fixture.requests != before {
		t.Fatal("bad upload reached network")
	}
}

func TestS3InventoryUsesBoundedCancelablePages(t *testing.T) {
	fixture := &s3Fixture{objects: map[string][]byte{"data/ns/1": []byte("a"), "data/ns/2": []byte("b"), "data/ns/3": []byte("c"), "data/other": []byte("d")}, t: t}
	s, _ := newTestS3(t, fixture)
	ctx := context.Background()
	page, err := s.Scan(ctx, "ns/", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 2 || page.NextCursor == "" || fixture.lists != 1 {
		t.Fatalf("first page: %+v lists=%d", page, fixture.lists)
	}
	page, err = s.Scan(ctx, "ns/", page.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "ns/3" || page.NextCursor != "" || fixture.lists != 2 {
		t.Fatalf("second page: %+v lists=%d", page, fixture.lists)
	}
	if _, err := s.Scan(ctx, "ns/", "invalid", 2); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Scan(canceled, "ns/", "", 2); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestS3PermissionAndBucketErrorsNeverBecomeMissingObjects(t *testing.T) {
	for _, test := range []struct {
		code       string
		status     int
		permission bool
	}{{"AccessDenied", 403, true}, {"NoSuchBucket", 404, false}} {
		t.Run(test.code, func(t *testing.T) {
			s, _ := newTestS3(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.status)
				_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>unavailable</Message></Error>", test.code)
			}))
			_, err := s.Open(context.Background(), "key")
			if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrPermission) != test.permission {
				t.Fatalf("classification: %v", err)
			}
		})
	}
}

func TestS3ListCancellationReachesRequest(t *testing.T) {
	entered := make(chan struct{})
	s, _ := newTestS3(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Scan(ctx, "", "", 1); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("list ignored cancellation")
	}
}

func TestS3RejectsInvalidConfigurationBeforeRequests(t *testing.T) {
	for _, endpoint := range []string{"ftp://storage", "http://user:secret@storage", "http://storage/base", "http://storage?secret=value", "http://storage#fragment"} {
		if _, err := NewS3(S3Config{Endpoint: endpoint, Bucket: "bucket", AccessKey: "access", SecretKey: "secret"}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("endpoint %q: %v", endpoint, err)
		}
	}
	if _, err := NewS3(S3Config{Endpoint: "http://storage", Bucket: "../bucket", AccessKey: "access", SecretKey: "secret"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bucket: %v", err)
	}
}
