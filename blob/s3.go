package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	minio "github.com/soulteary/otterio-sdk/v7"
	"github.com/soulteary/otterio-sdk/v7/pkg/credentials"
	"github.com/soulteary/otterio-sdk/v7/pkg/s3utils"
)

type S3Config struct {
	Endpoint, Bucket, Prefix, Region   string
	AccessKey, SecretKey, SessionToken string
	PathStyle                          bool
}

type S3 struct {
	client         *minio.Client
	transport      *http.Transport
	bucket, prefix string
}

func NewS3(config S3Config) (*S3, error) {
	if config.Bucket == "" || config.AccessKey == "" || config.SecretKey == "" {
		return nil, fmt.Errorf("%w: S3 bucket and credentials are required", ErrInvalidConfig)
	}
	if err := s3utils.CheckValidBucketName(config.Bucket); err != nil {
		return nil, fmt.Errorf("%w: invalid S3 bucket name", ErrInvalidConfig)
	}
	if err := validatePrefix(config.Prefix); err != nil {
		return nil, err
	}
	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("%w: S3 endpoint must be an HTTP(S) host without credentials, path, query, or fragment", ErrInvalidConfig)
	}
	region := config.Region
	if region == "" {
		region = "us-east-1"
	}
	transport, err := minio.DefaultTransport(u.Scheme == "https")
	if err != nil {
		return nil, err
	}
	// Object bytes and checksums must not change because of HTTP decompression.
	transport.DisableCompression = true
	lookup := minio.BucketLookupAuto
	if config.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, &minio.Options{
		Region: region, Secure: u.Scheme == "https", BucketLookup: lookup,
		Creds:      credentials.NewStaticV4(config.AccessKey, config.SecretKey, config.SessionToken),
		MaxRetries: 1, TrailingHeaders: false,
		Transport: boundedTransport{transport: transport, timeout: 30 * time.Second},
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return &S3{client: client, transport: transport, bucket: config.Bucket, prefix: strings.TrimSuffix(config.Prefix, "/")}, nil
}

func (s *S3) physical(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *S3) Put(ctx context.Context, spec Write, body io.Reader) error {
	data, err := prepare(ctx, spec, body)
	if err != nil {
		return err
	}
	md5sum := md5.Sum(data) // S3 transport checksum; SHA-256 remains the content identity.
	_, err = (minio.Core{Client: s.client}).PutObject(ctx, s.bucket, s.physical(spec.Key), bytes.NewReader(data), spec.Size,
		base64.StdEncoding.EncodeToString(md5sum[:]), hex.EncodeToString(spec.SHA256[:]),
		minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true, DisableContentSha256: true})
	// No conditional headers and no unconditional retry after an unknown result.
	// The repository reconciles a lost reply with a bounded GET and SHA-256.
	return mapS3Error(err)
}

func (s *S3) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateKey(key); err != nil {
		return nil, err
	}
	body, info, _, err := (minio.Core{Client: s.client}).GetObject(ctx, s.bucket, s.physical(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, mapS3Error(err)
	}
	if info.Size < 0 || info.Size > MaxObjectSize {
		body.Close()
		return nil, fmt.Errorf("%w: invalid object length", ErrIntegrity)
	}
	return body, nil
}

func (s *S3) Stat(ctx context.Context, key string) (Info, error) {
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	if err := ValidateKey(key); err != nil {
		return Info{}, err
	}
	info, err := s.client.StatObject(ctx, s.bucket, s.physical(key), minio.StatObjectOptions{})
	if err != nil {
		return Info{}, mapS3Error(err)
	}
	if info.Size < 0 || info.Size > MaxObjectSize {
		return Info{}, fmt.Errorf("%w: invalid object length", ErrIntegrity)
	}
	return Info{Key: key, Size: info.Size, LastModified: info.LastModified}, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	err := mapS3Error(s.client.RemoveObject(ctx, s.bucket, s.physical(key), minio.RemoveObjectOptions{}))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

type s3Cursor struct {
	Prefix string `json:"p"`
	Key    string `json:"k"`
}

func (s *S3) Scan(ctx context.Context, prefix, cursor string, limit int) (Page, error) {
	if err := validateScan(prefix, limit); err != nil {
		return Page{}, err
	}
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	startAfter := ""
	if cursor != "" {
		if len(cursor) > 4096 {
			return Page{}, ErrInvalidCursor
		}
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		var parsed s3Cursor
		if err != nil || json.Unmarshal(data, &parsed) != nil || parsed.Prefix != prefix || !strings.HasPrefix(parsed.Key, prefix) || ValidateKey(parsed.Key) != nil {
			return Page{}, ErrInvalidCursor
		}
		startAfter = s.physical(parsed.Key)
	}
	page := Page{Objects: make([]Info, 0, limit)}
	// The synchronous iterator stops before requesting another page. The SDK's
	// low-level ListObjectsV2 uses Background, so it cannot honor cancellation.
	// MaxKeys bounds the server page; StartAfter gives a restartable cursor.
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	previous := startAfter
	for item := range s.client.ListObjectsIter(listCtx, s.bucket, minio.ListObjectsOptions{Prefix: s.physical(prefix), Recursive: true, MaxKeys: limit, StartAfter: startAfter}) {
		if item.Err != nil {
			return Page{}, mapS3Error(item.Err)
		}
		if err := ctx.Err(); err != nil {
			return Page{}, err
		}
		if item.Key <= previous || !strings.HasPrefix(item.Key, s.physical(prefix)) {
			return Page{}, fmt.Errorf("%w: unordered or out-of-prefix inventory response", ErrIntegrity)
		}
		previous = item.Key
		key := item.Key
		if s.prefix != "" {
			key = strings.TrimPrefix(key, s.prefix+"/")
		}
		if err := ValidateKey(key); err != nil {
			return Page{}, fmt.Errorf("%w: invalid inventory object", ErrIntegrity)
		}
		if item.Size < 0 {
			return Page{}, fmt.Errorf("%w: invalid inventory size", ErrIntegrity)
		}
		page.Objects = append(page.Objects, Info{Key: key, Size: item.Size, LastModified: item.LastModified})
		if len(page.Objects) == limit {
			data, _ := json.Marshal(s3Cursor{Prefix: prefix, Key: key})
			page.NextCursor = base64.RawURLEncoding.EncodeToString(data)
			break
		}
	}
	return page, ctx.Err()
}

func (s *S3) Close() error { s.transport.CloseIdleConnections(); return nil }

func mapS3Error(err error) error {
	if err == nil {
		return nil
	}
	var response minio.ErrorResponse
	if errors.As(err, &response) {
		switch response.Code {
		case "NoSuchKey", "NoSuchObject", "NotFound":
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken":
			return fmt.Errorf("%w: %w", ErrPermission, err)
		}
		if response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("%w: %w", ErrPermission, err)
		}
	}
	return err
}

type boundedTransport struct {
	transport http.RoundTripper
	timeout   time.Duration
}

func (t boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), t.timeout)
	response, err := t.transport.RoundTrip(req.Clone(ctx))
	if err != nil || response == nil || response.Body == nil {
		cancel()
		return response, err
	}
	// Even an incompatible listing server cannot make XML decoding unbounded.
	response.Body = &boundedBody{ReadCloser: response.Body, cancel: cancel, remaining: 8 << 20}
	return response, nil
}

type boundedBody struct {
	io.ReadCloser
	cancel    context.CancelFunc
	remaining int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, fmt.Errorf("%w: S3 response exceeded limit", ErrIntegrity)
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
func (b *boundedBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }
