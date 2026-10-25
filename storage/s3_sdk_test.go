package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	minio "github.com/soulteary/otterio-sdk/v7"
	"github.com/soulteary/otterio-sdk/v7/pkg/credentials"
)

type s3SDKRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn s3SDKRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func newS3SDKTestClient(t *testing.T, transport http.RoundTripper) *minio.Client {
	t.Helper()
	client, err := minio.New("https://storage.invalid", &minio.Options{
		Region: "us-east-1", Creds: credentials.NewStaticV4("test", "test", ""),
		Secure: true, Transport: transport, BucketLookup: minio.BucketLookupPath, MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestS3SDKConditionalWritesStaySinglePUT(t *testing.T) {
	backend := &recoveryS3Transport{objects: make(map[string][]byte), failed: true}
	type writeRequest struct {
		match, noneMatch string
		length           int64
	}
	var writes []writeRequest
	transport := s3SDKRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/test/tenant/state" || req.URL.RawQuery != "" {
			return nil, fmt.Errorf("unexpected object or multipart request: %s %s", req.Method, req.URL)
		}
		if req.Method == http.MethodPut {
			if req.Header.Get("Content-Type") != "application/json" {
				return nil, fmt.Errorf("unexpected state content type: %s", req.Header.Get("Content-Type"))
			}
			writes = append(writes, writeRequest{
				match: req.Header.Get("If-Match"), noneMatch: req.Header.Get("If-None-Match"), length: req.ContentLength,
			})
		}
		return backend.RoundTrip(req)
	})
	store := &S3Storage{client: newS3SDKTestClient(t, transport), bucket: "test", prefix: "tenant", capabilityChecked: true}
	ctx := context.Background()
	if err := store.compareAndSwap(ctx, "state", []byte("initial"), ""); err != nil {
		t.Fatal(err)
	}
	initial, version, err := store.read(ctx, "state")
	if err != nil || string(initial) != "initial" || version == "" || strings.Contains(version, `"`) {
		t.Fatalf("read did not return data and normalized ETag: %q %q %v", initial, version, err)
	}
	// This exceeds the SDK's default multipart threshold. State updates must
	// still use one atomic conditional PUT, independent of their size.
	large := bytes.Repeat([]byte("x"), 17<<20)
	if err := store.compareAndSwap(ctx, "state", large, version); err != nil {
		t.Fatal(err)
	}
	if err := store.compareAndSwap(ctx, "state", []byte("stale overwrite"), version); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale ETag did not report a conflict: %v", err)
	}
	current, _, err := store.read(ctx, "state")
	if err != nil || !bytes.Equal(current, large) {
		t.Fatalf("stale write replaced current data: length=%d err=%v", len(current), err)
	}
	if len(writes) != 3 {
		t.Fatalf("conditional writes used %d PUT attempts, want 3", len(writes))
	}
	if writes[0].noneMatch != "*" || writes[0].match != "" || writes[0].length != int64(len(initial)) {
		t.Fatalf("insert conditions changed: %+v", writes[0])
	}
	for _, write := range writes[1:] {
		if write.match != `"`+version+`"` || write.noneMatch != "" {
			t.Fatalf("update did not send the original ETag exactly once quoted: %+v", write)
		}
	}
	if writes[1].length != int64(len(large)) {
		t.Fatalf("large state PUT length = %d, want %d", writes[1].length, len(large))
	}
}

func TestS3SDKErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		code string
		want error
	}{
		{code: "NoSuchKey", want: ErrNotFound},
		{code: "NotFound", want: ErrNotFound},
		{code: "PreconditionFailed", want: ErrConflict},
		{code: "ConditionalRequestConflict", want: ErrConflict},
		{code: "NoSuchBucket"},
		{code: "AccessDenied"},
		{code: "Conflict"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				var original error = minio.ErrorResponse{Code: tc.code, Message: "test response"}
				if wrapped {
					original = fmt.Errorf("request failed: %w", original)
				}
				got := mapS3Error(original)
				if tc.want != nil && !errors.Is(got, tc.want) {
					t.Fatalf("wrapped=%v: got %v, want %v", wrapped, got, tc.want)
				}
				if tc.want == nil && got != original {
					t.Fatalf("wrapped=%v: unrelated S3 error was changed: %v", wrapped, got)
				}
			}
		})
	}
	if mapS3Error(nil) != nil || !errors.Is(mapS3Error(context.Canceled), context.Canceled) {
		t.Fatal("error mapping changed success or cancellation")
	}
}

func TestS3SDKReadRejectsMissingETag(t *testing.T) {
	transport := s3SDKRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK, Request: req,
			Header: http.Header{"Content-Length": {"7"}, "Last-Modified": {time.Unix(1700000000, 0).UTC().Format(http.TimeFormat)}},
			Body:   io.NopCloser(strings.NewReader("payload")), ContentLength: 7,
		}, nil
	})
	store := &S3Storage{client: newS3SDKTestClient(t, transport), bucket: "test"}
	if _, _, err := store.read(context.Background(), "state"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("successful GET without a version was accepted as safe: %v", err)
	}
}

func TestS3SDKReadMissingAndCanceled(t *testing.T) {
	backend := &recoveryS3Transport{objects: make(map[string][]byte), failed: true}
	store := &S3Storage{client: newS3SDKTestClient(t, backend), bucket: "test"}
	if _, _, err := store.read(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object was not classified during read: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := store.read(ctx, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read was classified as missing: %v", err)
	}
}

func TestS3SDKReadPreservesGzipObjectBytes(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte("persisted job log\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/test/tenant/state" {
			http.Error(w, "unexpected object request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
		w.Header().Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"stored-gzip-version"`)
		_, _ = w.Write(compressed.Bytes())
	}))
	defer server.Close()
	store, err := NewS3Storage(&StorageConfig{Options: map[string]interface{}{
		"endpoint": server.URL, "bucket": "test", "prefix": "tenant", "access_key": "test", "secret_key": "test",
	}})
	if err != nil {
		t.Fatal(err)
	}
	data, version, err := store.read(context.Background(), "state")
	if err != nil || !bytes.Equal(data, compressed.Bytes()) || version != "stored-gzip-version" {
		t.Fatalf("object bytes or ETag changed during transport: length=%d version=%q err=%v", len(data), version, err)
	}
}

type s3SDKBlockedBody struct{ ctx context.Context }

func (body s3SDKBlockedBody) Read([]byte) (int, error) {
	<-body.ctx.Done()
	return 0, body.ctx.Err()
}

func (body s3SDKBlockedBody) Close() error { return nil }

func TestS3SDKRequestTimeoutIncludesResponseBody(t *testing.T) {
	transport := &s3TimeoutTransport{
		timeout: 20 * time.Millisecond,
		transport: s3SDKRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Request: req, Body: s3SDKBlockedBody{ctx: req.Context()}}, nil
		}),
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://storage.invalid/test/state", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	completed := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(response.Body)
		completed <- err
	}()
	select {
	case err := <-completed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("body timeout lost its cause: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("response body was not bounded by the request timeout")
	}
}

func TestS3SDKEndpointAndCredentialsCompatibility(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "env-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("AWS_SESSION_TOKEN", "env-session")
	for _, tc := range []struct {
		name, endpoint string
		pathStyle      *bool
		wantHost       string
		wantPath       string
	}{
		{name: "custom HTTP defaults to path style", endpoint: "http://storage.invalid:9000", wantHost: "storage.invalid:9000", wantPath: "/test/tenant/state"},
		{name: "custom HTTPS defaults to path style", endpoint: "https://storage.invalid", wantHost: "storage.invalid", wantPath: "/test/tenant/state"},
		{name: "explicit DNS style", endpoint: "https://storage.invalid", pathStyle: boolPointer(false), wantHost: "test.storage.invalid", wantPath: "/tenant/state"},
		{name: "default AWS endpoint", wantPath: "/tenant/state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := map[string]interface{}{"bucket": "test", "prefix": "/tenant/"}
			if tc.endpoint != "" {
				options["endpoint"] = tc.endpoint
			}
			if tc.pathStyle != nil {
				options["path_style"] = *tc.pathStyle
			}
			store, err := NewS3Storage(&StorageConfig{Options: options})
			if err != nil {
				t.Fatal(err)
			}
			creds, err := store.client.GetCreds()
			if err != nil || creds.AccessKeyID != "env-access" || creds.SecretAccessKey != "env-secret" || creds.SessionToken != "env-session" {
				t.Fatalf("environment credentials were not preserved: %v", err)
			}
			// Presigning is local with explicit region/static credentials and
			// checks addressing/signing without contacting an external service.
			u, err := store.client.PresignedGetObject(context.Background(), store.bucket, store.objectKey("state"), time.Minute, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantHost != "" && u.Host != tc.wantHost {
				t.Fatalf("request host = %q, want %q", u.Host, tc.wantHost)
			}
			if tc.endpoint == "" && (!strings.HasPrefix(u.Host, "test.") || !strings.HasSuffix(u.Host, ".amazonaws.com")) {
				t.Fatalf("default endpoint did not resolve to virtual-hosted AWS S3: %s", u.Host)
			}
			if u.Path != tc.wantPath || u.Query().Get("X-Amz-Security-Token") != "env-session" {
				t.Fatalf("presigned path/token changed: path=%q", u.Path)
			}
		})
	}
}

func TestS3SDKRejectsUnsupportedEndpointComponents(t *testing.T) {
	for _, endpoint := range []string{
		"https://user:password@storage.invalid",
		"https://storage.invalid/s3",
		"https://storage.invalid?region=us-east-1",
		"https://storage.invalid/#fragment",
		"ftp://storage.invalid",
		"http://",
	} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := NewS3Storage(&StorageConfig{Options: map[string]interface{}{
				"endpoint": endpoint, "bucket": "test", "access_key": "test", "secret_key": "test",
			}})
			if err == nil {
				t.Fatal("unsupported endpoint was accepted")
			}
		})
	}
}

func boolPointer(value bool) *bool { return &value }
