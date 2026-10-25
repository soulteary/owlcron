package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	redisclient "github.com/soulteary/redis-kit/client"
)

// RedisStorage stores the existing remoteDocument bytes in a standalone Redis
// 8.4+ server. Revisions and logical expiry remain in that envelope: Redis TTLs
// must not discard deletion fences or reset an in-flight compare-and-swap.
type RedisStorage struct {
	*remoteStorage
	client *redis.Client
	prefix string
}

var (
	_ Storage          = (*RedisStorage)(nil)
	_ AtomicStorage    = (*RedisStorage)(nil)
	_ KeyLister        = (*RedisStorage)(nil)
	_ PrefixKeyLister  = (*RedisStorage)(nil)
	_ LegacyLogRemover = (*RedisStorage)(nil)
)

func NewRedisStorage(config *StorageConfig) (*RedisStorage, error) {
	return NewRedisStorageContext(context.Background(), config)
}

// NewRedisStorageContext bounds the connection and capability checks by both
// the caller's context and startup_timeout_ms (10 seconds by default).
func NewRedisStorageContext(ctx context.Context, config *StorageConfig) (*RedisStorage, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Redis storage context is required")
	}
	if config == nil {
		return nil, fmt.Errorf("Redis storage config is required")
	}
	opts := config.Options
	for key := range opts {
		if key == "addrs" || key == "master_name" || key == "cluster" || key == "sentinel" || strings.HasPrefix(key, "sentinel_") || strings.HasPrefix(key, "cluster_") {
			return nil, fmt.Errorf("Redis storage supports a single instance; option %s is unsupported", key)
		}
	}
	addr, err := redisStringOption(opts, "addr", "")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("Redis storage requires addr")
	}
	prefix, err := redisStringOption(opts, "prefix", "owlcron:")
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, fmt.Errorf("Redis option prefix must not be empty")
	}
	cfg := redisclient.DefaultConfig()
	cfg.Addr = addr
	if cfg.Username, err = redisStringOption(opts, "username", os.Getenv("REDIS_USERNAME")); err != nil {
		return nil, err
	}
	if cfg.Password, err = redisStringOption(opts, "password", os.Getenv("REDIS_PASSWORD")); err != nil {
		return nil, err
	}
	if cfg.DB, err = redisIntOption(opts, "db", 0, 0); err != nil {
		return nil, err
	}
	if cfg.PoolSize, err = redisIntOption(opts, "pool_size", cfg.PoolSize, 1); err != nil {
		return nil, err
	}
	if cfg.MinIdleConns, err = redisIntOption(opts, "min_idle_conns", min(cfg.MinIdleConns, cfg.PoolSize), 0); err != nil {
		return nil, err
	}
	if cfg.MinIdleConns > cfg.PoolSize {
		return nil, fmt.Errorf("Redis option min_idle_conns must not exceed pool_size")
	}
	for _, target := range []struct {
		name  string
		value *time.Duration
	}{
		{"dial_timeout_ms", &cfg.DialTimeout},
		{"read_timeout_ms", &cfg.ReadTimeout},
		{"write_timeout_ms", &cfg.WriteTimeout},
		{"pool_timeout_ms", &cfg.PoolTimeout},
	} {
		ms, err := redisIntOption(opts, target.name, int(*target.value/time.Millisecond), 1)
		if err != nil {
			return nil, err
		}
		if int64(ms) > math.MaxInt64/int64(time.Millisecond) {
			return nil, fmt.Errorf("Redis option %s is too large", target.name)
		}
		*target.value = time.Duration(ms) * time.Millisecond
	}
	startupMS, err := redisIntOption(opts, "startup_timeout_ms", 10000, 1)
	if err != nil || int64(startupMS) > math.MaxInt64/int64(time.Millisecond) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("Redis option startup_timeout_ms is too large")
	}
	if cfg.TLSConfig, err = redisTLSConfig(opts); err != nil {
		return nil, err
	}
	// A successful write followed by a lost response is indeterminate. Never
	// repeat it at the transport layer, or replay its Update callback as a CAS
	// conflict. Callers must recover by re-reading durable state.
	cfg.MaxRetries = -1
	cfg.ContextTimeoutEnabled = true
	startup, cancel := context.WithTimeout(ctx, time.Duration(startupMS)*time.Millisecond)
	defer cancel()
	client, err := redisclient.NewClientContext(startup, cfg)
	if err != nil {
		return nil, err
	}
	store := &RedisStorage{client: client, prefix: prefix}
	store.remoteStorage = &remoteStorage{kv: store}
	if err = store.checkCapabilities(startup); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("Redis storage initialization: %w", err)
	}
	return store, nil
}

func redisStringOption(opts map[string]interface{}, key, def string) (string, error) {
	v, exists := opts[key]
	if !exists {
		return def, nil
	}
	text, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("Redis option %s must be a string", key)
	}
	return text, nil
}

func redisIntOption(opts map[string]interface{}, key string, def, min int) (int, error) {
	v, exists := opts[key]
	if !exists {
		return def, nil
	}
	var n int64
	switch v := v.(type) {
	case int:
		n = int64(v)
	case int64:
		n = v
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) || v >= float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("Redis option %s must be an integer", key)
		}
		n = int64(v)
	default:
		return 0, fmt.Errorf("Redis option %s must be an integer", key)
	}
	if n < int64(min) || int64(int(n)) != n {
		return 0, fmt.Errorf("Redis option %s must be at least %d and fit an integer", key, min)
	}
	return int(n), nil
}

func redisTLSConfig(opts map[string]interface{}) (*tls.Config, error) {
	enabled, exists := opts["tls"]
	tlsEnabled := false
	if exists {
		var ok bool
		tlsEnabled, ok = enabled.(bool)
		if !ok {
			return nil, fmt.Errorf("Redis option tls must be a boolean")
		}
	}
	caFile, err := redisStringOption(opts, "tls_ca_file", "")
	if err != nil {
		return nil, err
	}
	certFile, err := redisStringOption(opts, "tls_cert_file", "")
	if err != nil {
		return nil, err
	}
	keyFile, err := redisStringOption(opts, "tls_key_file", "")
	if err != nil {
		return nil, err
	}
	serverName, err := redisStringOption(opts, "tls_server_name", "")
	if err != nil {
		return nil, err
	}
	if !tlsEnabled {
		if caFile != "" || certFile != "" || keyFile != "" || serverName != "" {
			return nil, fmt.Errorf("Redis TLS options require tls: true")
		}
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read Redis TLS CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("Redis TLS CA contains no certificates")
		}
		config.RootCAs = roots
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("Redis TLS requires both tls_cert_file and tls_key_file")
	}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load Redis TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func redisVersionSupported(info string) bool {
	for _, line := range strings.Split(info, "\n") {
		version, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		parts := strings.Split(version, ".")
		if len(parts) != 3 {
			return false
		}
		major, e1 := strconv.Atoi(parts[0])
		minor, e2 := strconv.Atoi(parts[1])
		patch, e3 := strconv.Atoi(parts[2])
		return e1 == nil && e2 == nil && e3 == nil && minor >= 0 && patch >= 0 && (major > 8 || (major == 8 && minor >= 4))
	}
	return false
}

func redisServerMode(info string) string {
	for _, line := range strings.Split(info, "\n") {
		if mode, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_mode:"); ok {
			return mode
		}
	}
	return ""
}

func (s *RedisStorage) checkCapabilities(ctx context.Context) (outErr error) {
	info, err := redisRequest(ctx, func() (string, error) { return s.client.Info(ctx, "server").Result() })
	if err != nil {
		return fmt.Errorf("INFO server (required to verify Redis 8.4+): %w", err)
	}
	if !redisVersionSupported(info) {
		return fmt.Errorf("Redis storage requires Redis 8.4 or newer")
	}
	if mode := redisServerMode(info); mode != "standalone" {
		return fmt.Errorf("Redis storage requires standalone mode, server reported %q", mode)
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	key := "capability/redis-" + hex.EncodeToString(nonce)
	physical := s.prefix + key
	defer func() {
		// Cleanup shares the startup budget. Every probe write has a 30-second
		// physical TTL, so cancellation or an exhausted budget can leave Redis to
		// reclaim the private key without delaying startup failure.
		if ctx.Err() != nil {
			if outErr == nil {
				outErr = ctx.Err()
			}
			return
		}
		if err := s.unlink(ctx, physical); err != nil {
			outErr = errors.Join(outErr, fmt.Errorf("clean up Redis capability probe: %w", err))
		}
	}()
	for _, probe := range []struct {
		value string
		mode  string
		old   string
		ok    bool
	}{
		{"first", "NX", "", true},
		{"must-not-write", "NX", "", false},
		{"second", "IFEQ", "first", true},
		{"must-not-write", "IFEQ", "first", false},
	} {
		args := []interface{}{"SET", physical, probe.value, probe.mode}
		if probe.mode == "IFEQ" {
			args = append(args, probe.old)
		}
		args = append(args, "PX", 30000)
		result, err := s.commandText(ctx, args...)
		if probe.ok && err != nil {
			return fmt.Errorf("native SET %s capability: %w", probe.mode, err)
		}
		if probe.ok && result != "OK" {
			return fmt.Errorf("native SET %s capability: unexpected result %q", probe.mode, result)
		}
		if !probe.ok && !errors.Is(err, redis.Nil) {
			if err != nil {
				return fmt.Errorf("native SET %s failed-condition capability: %w", probe.mode, err)
			}
			return fmt.Errorf("native SET %s failed-condition capability: unexpected result %q", probe.mode, result)
		}
	}
	data, err := s.getBytes(ctx, physical)
	if err != nil {
		return fmt.Errorf("GET capability verification: %w", err)
	}
	if !bytes.Equal(data, []byte("second")) {
		return fmt.Errorf("GET capability verification returned unexpected bytes")
	}
	var cursor uint64
	found := false
	for {
		keys, next, err := s.scan(ctx, cursor, redisGlobLiteral(physical))
		if err != nil {
			return fmt.Errorf("SCAN capability: %w", err)
		}
		for _, candidate := range keys {
			found = found || candidate == physical
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if !found {
		return fmt.Errorf("SCAN capability did not return its private probe")
	}
	return nil
}

func (s *RedisStorage) read(ctx context.Context, key string) ([]byte, string, error) {
	data, err := s.getBytes(ctx, s.prefix+key)
	if errors.Is(err, redis.Nil) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	// Every remoteDocument is non-empty, making the original raw bytes an
	// opaque CAS token while retaining the existing envelope without extra keys.
	if len(data) == 0 {
		return nil, "", fmt.Errorf("empty Redis storage document")
	}
	return data, string(data), nil
}

func (s *RedisStorage) compareAndSwap(ctx context.Context, key string, data []byte, version string) error {
	args := []interface{}{"SET", s.prefix + key, data, "NX"}
	if version != "" {
		args = []interface{}{"SET", s.prefix + key, data, "IFEQ", version}
	}
	result, err := s.commandText(ctx, args...)
	if errors.Is(err, redis.Nil) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if result != "OK" {
		return fmt.Errorf("unexpected Redis SET result %q", result)
	}
	return nil
}

func (s *RedisStorage) Close() error { return s.client.Close() }

// redisGlobLiteral escapes every Redis MATCH metacharacter, including one
// supplied as part of the deployment namespace rather than an application key.
func redisGlobLiteral(value string) string {
	return strings.NewReplacer("\\", "\\\\", "*", "\\*", "?", "\\?", "[", "\\[", "]", "\\]").Replace(value)
}

func (s *RedisStorage) ListKeys(ctx context.Context) ([]string, error) {
	return s.ListKeysPrefix(ctx, "")
}

func (s *RedisStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	if err := validateKeyPrefix(prefix); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var cursor uint64
	for {
		physicalKeys, next, err := s.scan(ctx, cursor, redisGlobLiteral(s.prefix+prefix)+"*")
		if err != nil {
			return nil, err
		}
		for _, physical := range physicalKeys {
			if !strings.HasPrefix(physical, s.prefix+prefix) {
				continue
			}
			key := strings.TrimPrefix(physical, s.prefix)
			if strings.HasPrefix(key, "capability/") || seen[key] {
				continue
			}
			// Only matching application keys are read. Tombstones and expired
			// envelopes stay in Redis for CAS but never enter exports or indexes.
			if _, err := s.Get(ctx, key); errors.Is(err, ErrNotFound) {
				continue
			} else if err != nil {
				return nil, err
			}
			seen[key] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *RedisStorage) RemoveLegacyJobLog(ctx context.Context, id string) error {
	key, err := legacyLogKey(id)
	if err != nil {
		return err
	}
	// The caller installs the manifest's durable deletion fence first. UNLINK
	// removes a potentially large legacy payload without reading or freeing it
	// synchronously on the Redis command thread.
	return s.unlink(ctx, s.prefix+key)
}

// go-redis applies context deadlines to socket I/O, but a cancellation without
// a deadline does not interrupt an in-flight socket read. Return cancellation
// promptly without closing the shared pool. Every request has finite dial,
// socket and pool timeouts, bounding the lifetime of the underlying work, and
// the buffered result channel allows it to finish after its caller returns.
// Cancellation cannot revoke a write already sent to Redis: its outcome may be
// unknown, and neither this helper nor remoteStorage replays it as a conflict.
func redisRequest[T any](ctx context.Context, request func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if ctx.Done() == nil {
		return request()
	}
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		if err := ctx.Err(); err != nil {
			done <- outcome{err: err}
			return
		}
		value, err := request()
		done <- outcome{value, err}
	}()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case result := <-done:
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		return result.value, result.err
	}
}

func (s *RedisStorage) commandText(ctx context.Context, args ...interface{}) (string, error) {
	return redisRequest(ctx, func() (string, error) { return s.client.Do(ctx, args...).Text() })
}

func (s *RedisStorage) getBytes(ctx context.Context, key string) ([]byte, error) {
	return redisRequest(ctx, func() ([]byte, error) { return s.client.Get(ctx, key).Bytes() })
}

func (s *RedisStorage) unlink(ctx context.Context, key string) error {
	_, err := redisRequest(ctx, func() (int64, error) { return s.client.Unlink(ctx, key).Result() })
	return err
}

func (s *RedisStorage) scan(ctx context.Context, cursor uint64, match string) ([]string, uint64, error) {
	type page struct {
		keys   []string
		cursor uint64
	}
	result, err := redisRequest(ctx, func() (page, error) {
		keys, next, err := s.client.Scan(ctx, cursor, match, 128).Result()
		return page{keys, next}, err
	})
	return result.keys, result.cursor, err
}
