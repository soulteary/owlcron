package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	minio "github.com/soulteary/otterio-sdk/v7"
	"github.com/soulteary/otterio-sdk/v7/pkg/credentials"
)

type S3Storage struct {
	*remoteStorage
	client            *minio.Client
	bucket, prefix    string
	capabilityOnce    sync.Once
	capabilityGate    chan struct{}
	capabilityChecked bool
}

// Keep raw capability payloads outside the namespace of valid application keys.
const s3CapabilityPrefix = ".capability/"

func NewS3Storage(config *StorageConfig) (*S3Storage, error) {
	bucket := optionString(config.Options, "bucket", "")
	if bucket == "" {
		return nil, fmt.Errorf("S3 storage requires bucket")
	}
	access := optionString(config.Options, "access_key", os.Getenv("AWS_ACCESS_KEY_ID"))
	secret := optionString(config.Options, "secret_key", os.Getenv("AWS_SECRET_ACCESS_KEY"))
	token := optionString(config.Options, "session_token", os.Getenv("AWS_SESSION_TOKEN"))
	if access == "" || secret == "" {
		return nil, fmt.Errorf("S3 storage requires access_key and secret_key or AWS credential environment")
	}
	endpoint := optionString(config.Options, "endpoint", "")
	lookup := minio.BucketLookupAuto
	if endpoint == "" {
		// The SDK resolves the regional AWS endpoint using Options.Region.
		endpoint = "https://s3.amazonaws.com"
	} else {
		lookup = minio.BucketLookupPath
		if !strings.Contains(endpoint, "://") {
			endpoint = "https://" + endpoint
		}
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 endpoint: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("S3 endpoint must be an HTTP(S) host without credentials, a path, query, or fragment")
	}
	if pathStyle, ok := config.Options["path_style"].(bool); ok {
		lookup = minio.BucketLookupDNS
		if pathStyle {
			lookup = minio.BucketLookupPath
		}
	}
	transport, err := minio.DefaultTransport(parsed.Scheme == "https")
	if err != nil {
		return nil, fmt.Errorf("initialize S3 transport: %w", err)
	}
	client, err := minio.New(endpoint, &minio.Options{
		Region:       optionString(config.Options, "region", "us-east-1"),
		Creds:        credentials.NewStaticV4(access, secret, token),
		Secure:       parsed.Scheme == "https",
		BucketLookup: lookup,
		MaxRetries:   3,
		Transport: s3TimeoutTransport{
			transport: transport,
			timeout:   30 * time.Second,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("initialize S3 client: %w", err)
	}
	store := &S3Storage{client: client, bucket: bucket, prefix: strings.Trim(optionString(config.Options, "prefix", "owlcron"), "/")}
	store.remoteStorage = &remoteStorage{kv: store}
	return store, nil
}
func (s *S3Storage) objectKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}
func (s *S3Storage) read(ctx context.Context, key string) ([]byte, string, error) {
	body, info, _, err := (minio.Core{Client: s.client}).GetObject(ctx, s.bucket, s.objectKey(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, "", mapS3Error(err)
	}
	defer body.Close()
	if info.ETag == "" {
		return nil, "", fmt.Errorf("S3 object has no ETag; refusing unversioned storage")
	}
	data, err := io.ReadAll(body)
	// ObjectInfo.ETag is unquoted; SetMatchETag restores the HTTP quotes.
	// Both the payload and its version must come from this same GET.
	return data, info.ETag, err
}
func (s *S3Storage) compareAndSwap(ctx context.Context, key string, data []byte, version string) error {
	if err := s.ensureConditionalWrites(ctx); err != nil {
		return err
	}
	return mapS3Error(s.putConditional(ctx, s.objectKey(key), data, version))
}

func (s *S3Storage) putConditional(ctx context.Context, key string, data []byte, version string) error {
	// A growing state document must never switch to a multipart operation.
	options := minio.PutObjectOptions{ContentType: "application/json", DisableMultipart: true}
	if version == "" {
		options.SetMatchETagExcept("*")
	} else {
		options.SetMatchETag(version)
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), options)
	return err
}

func mapS3Error(err error) error {
	var response minio.ErrorResponse
	if errors.As(err, &response) {
		switch response.Code {
		case "NoSuchKey", "NotFound", "NoSuchObject":
			return ErrNotFound
		case "PreconditionFailed", "ConditionalRequestConflict":
			return ErrConflict
		}
	}
	return err
}

func s3PreconditionFailed(err error) bool {
	var response minio.ErrorResponse
	return errors.As(err, &response) && response.Code == "PreconditionFailed"
}

// The SDK accepts a RoundTripper rather than an http.Client. Preserve the old
// per-request timeout through response body reads, without limiting an entire
// paginated backup or detaching the caller's cancellation.
type s3TimeoutTransport struct {
	transport http.RoundTripper
	timeout   time.Duration
}

func (t s3TimeoutTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(request.Context(), t.timeout)
	response, err := t.transport.RoundTrip(request.Clone(ctx))
	if err != nil || response == nil || response.Body == nil {
		cancel()
		return response, err
	}
	response.Body = &s3TimeoutBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

type s3TimeoutBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body *s3TimeoutBody) Close() error {
	defer body.cancel()
	return body.ReadCloser.Close()
}

// Only a successful probe is cached. A transient outage or cancellation during
// the first write must not prevent recovery after the backend becomes healthy.
func (s *S3Storage) ensureConditionalWrites(ctx context.Context) error {
	s.capabilityOnce.Do(func() { s.capabilityGate = make(chan struct{}, 1) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.capabilityGate <- struct{}{}:
	}
	defer func() { <-s.capabilityGate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.capabilityChecked {
		return nil
	}
	if err := s.checkConditionalWrites(ctx); err != nil {
		return err
	}
	s.capabilityChecked = true
	return nil
}

// Some S3-compatible servers silently ignore If-Match. Probe before the first
// mutation so accepting such a server can never lose scheduler state.
func (s *S3Storage) checkConditionalWrites(ctx context.Context) error {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	key := s.objectKey(s3CapabilityPrefix + hex.EncodeToString(nonce))
	err := s.putConditional(ctx, key, []byte("probe"), "")
	if err != nil {
		return fmt.Errorf("S3 conditional insert probe: %w", err)
	}
	defer s.client.RemoveObject(context.WithoutCancel(ctx), s.bucket, key, minio.RemoveObjectOptions{})
	err = s.putConditional(ctx, key, []byte("must not overwrite"), "invalid-version")
	if s3PreconditionFailed(err) {
		err = s.putConditional(ctx, key, []byte("must not replace"), "")
		if s3PreconditionFailed(err) {
			return s.checkAtomicCondition(ctx, key)
		}
		if err == nil {
			return fmt.Errorf("S3 server does not enforce If-None-Match; refusing unsafe concurrent storage")
		}
		return fmt.Errorf("S3 conditional collision probe: %w", err)
	}
	if err == nil {
		return fmt.Errorf("S3 server does not enforce If-Match; refusing unsafe concurrent storage")
	}
	return fmt.Errorf("S3 conditional update probe: %w", err)
}

// Recognize only the exact names and raw payloads generated by older probes.
// A valid remoteDocument at the same path is still application data.
func s3LegacyCapabilityProbe(key string, data []byte) bool {
	nonce, ok := strings.CutPrefix(key, "capability/")
	if !ok || len(nonce) != 32 {
		return false
	}
	for _, digit := range nonce {
		if !('0' <= digit && digit <= '9' || 'a' <= digit && digit <= 'f') {
			return false
		}
	}
	switch string(data) {
	case "probe", "must not overwrite", "must not replace",
		"claim-0", "claim-1", "claim-2", "claim-3", "claim-4", "claim-5", "claim-6", "claim-7":
		return true
	default:
		return false
	}
}

func (s *S3Storage) checkAtomicCondition(ctx context.Context, key string) error {
	body, original, _, err := (minio.Core{Client: s.client}).GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	body.Close()
	if original.ETag == "" {
		return fmt.Errorf("S3 probe object has no ETag; refusing unversioned storage")
	}
	gate := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	var winners atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			err := s.putConditional(ctx, key, []byte(fmt.Sprintf("claim-%d", i)), original.ETag)
			if err == nil {
				winners.Add(1)
				return
			}
			if errors.Is(mapS3Error(err), ErrConflict) {
				return
			}
			errs <- err
		}(i)
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		return fmt.Errorf("S3 atomic conditional probe: %w", err)
	}
	if winners.Load() != 1 {
		return fmt.Errorf("S3 server allowed %d concurrent writes for one ETag; refusing non-atomic storage", winners.Load())
	}
	return nil
}
