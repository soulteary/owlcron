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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3Storage struct {
	*remoteStorage
	client            *s3.Client
	bucket, prefix    string
	capabilityOnce    sync.Once
	capabilityGate    chan struct{}
	capabilityChecked bool
}

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
	cfg := aws.Config{Region: optionString(config.Options, "region", "us-east-1"), Credentials: credentials.NewStaticCredentialsProvider(access, secret, token), HTTPClient: &http.Client{Timeout: 30 * time.Second}}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		endpoint := optionString(config.Options, "endpoint", "")
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
		if b, ok := config.Options["path_style"].(bool); ok {
			o.UsePathStyle = b
		}
	})
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
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.objectKey(key))})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	return data, aws.ToString(out.ETag), err
}
func (s *S3Storage) compareAndSwap(ctx context.Context, key string, data []byte, version string) error {
	if err := s.ensureConditionalWrites(ctx); err != nil {
		return err
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.objectKey(key)), Body: bytes.NewReader(data), ContentType: aws.String("application/json")}
	if version == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(version)
	}
	_, err := s.client.PutObject(ctx, in)
	var api smithy.APIError
	if errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "ConditionalRequestConflict") {
		return ErrConflict
	}
	return err
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
	key := s.objectKey("capability/" + hex.EncodeToString(nonce))
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("probe")), IfNoneMatch: aws.String("*")})
	if err != nil {
		return fmt.Errorf("S3 conditional insert probe: %w", err)
	}
	defer s.client.DeleteObject(context.WithoutCancel(ctx), &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("must not overwrite")), IfMatch: aws.String("\"invalid-version\"")})
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "PreconditionFailed" {
		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("must not replace")), IfNoneMatch: aws.String("*")})
		var collision smithy.APIError
		if errors.As(err, &collision) && collision.ErrorCode() == "PreconditionFailed" {
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

func (s *S3Storage) checkAtomicCondition(ctx context.Context, key string) error {
	original, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	original.Body.Close()
	gate := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	var winners atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(fmt.Sprintf("claim-%d", i))), IfMatch: original.ETag})
			if err == nil {
				winners.Add(1)
				return
			}
			var api smithy.APIError
			if errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "ConditionalRequestConflict") {
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
