package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

type redisStorageHook struct{ process func(redis.Cmder) error }

func (h redisStorageHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h redisStorageHook) ProcessHook(_ redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error { return h.process(cmd) }
}
func (h redisStorageHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func hookedRedisStorage(t *testing.T, prefix string, process func(redis.Cmder) error) *RedisStorage {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "unused:6379", MaxRetries: -1, Dialer: func(context.Context, string, string) (net.Conn, error) {
		return nil, fmt.Errorf("unit test unexpectedly opened a connection")
	}})
	c.AddHook(redisStorageHook{process})
	t.Cleanup(func() { _ = c.Close() })
	s := &RedisStorage{client: c, prefix: prefix}
	s.remoteStorage = &remoteStorage{kv: s}
	return s
}

func TestRedisCASCommandsAndErrorClassification(t *testing.T) {
	for _, version := range []string{"", "old\x00\xff"} {
		for _, response := range []error{nil, redis.Nil, io.EOF, context.Canceled, context.DeadlineExceeded} {
			t.Run(fmt.Sprintf("version=%q/error=%v", version, response), func(t *testing.T) {
				calls := 0
				s := hookedRedisStorage(t, "namespace:", func(cmd redis.Cmder) error {
					calls++
					want := []interface{}{"SET", "namespace:key", []byte{0, 255}, "NX"}
					if version != "" {
						want = []interface{}{"SET", "namespace:key", []byte{0, 255}, "IFEQ", version}
					}
					if !reflect.DeepEqual(cmd.Args(), want) {
						t.Fatalf("CAS command %#v, want %#v", cmd.Args(), want)
					}
					cmd.SetErr(response)
					if response == nil {
						cmd.(*redis.Cmd).SetVal("OK")
					}
					return response
				})
				err := s.compareAndSwap(context.Background(), "key", []byte{0, 255}, version)
				if response == redis.Nil {
					if !errors.Is(err, ErrConflict) {
						t.Fatalf("failed condition must be Conflict: %v", err)
					}
				} else if !errors.Is(err, response) || errors.Is(err, ErrConflict) {
					t.Fatalf("transport/cancellation error classification: %v, want %v", err, response)
				}
				if calls != 1 {
					t.Fatalf("CAS replayed %d times", calls)
				}
			})
		}
	}
}

func TestRedisReadUsesRawBytesAsOpaqueVersion(t *testing.T) {
	want := "{\"data\":\"AP8=\",\"revision\":\"raw\"}"
	s := hookedRedisStorage(t, "namespace:", func(cmd redis.Cmder) error {
		if !reflect.DeepEqual(cmd.Args(), []interface{}{"get", "namespace:key"}) {
			t.Fatalf("read command %#v", cmd.Args())
		}
		cmd.(*redis.StringCmd).SetVal(want)
		return nil
	})
	data, version, err := s.read(context.Background(), "key")
	if err != nil || string(data) != want || version != want {
		t.Fatalf("read lost raw bytes: %q %q %v", data, version, err)
	}
}

func TestRedisPrefixScanEscapesAndFilters(t *testing.T) {
	namespace := "owl[*?]:"
	page := 0
	reads := []string{}
	s := hookedRedisStorage(t, namespace, func(cmd redis.Cmder) error {
		switch cmd.Name() {
		case "scan":
			if got := cmd.Args()[3]; got != `owl\[\*\?\]:jobs:*` {
				t.Fatalf("unescaped namespace MATCH: %#v", cmd.Args())
			}
			page++
			if page == 1 {
				cmd.(*redis.ScanCmd).SetVal([]string{namespace + "jobs:z", namespace + "jobs:a", namespace + "jobs:deleted", namespace + "jobs:expired", "other:jobs:malformed"}, 42)
			} else {
				cmd.(*redis.ScanCmd).SetVal([]string{namespace + "jobs:z"}, 0)
			}
		case "get":
			key := cmd.Args()[1].(string)
			reads = append(reads, key)
			data := `{"data":"bGl2ZQ==","revision":"1"}`
			switch key {
			case namespace + "jobs:deleted":
				data = `{"data":null,"deleted":true,"revision":"2"}`
			case namespace + "jobs:expired":
				data = `{"data":"b2xk","expires":1,"revision":"3"}`
			case "other:jobs:malformed":
				t.Fatal("read another namespace's payload")
			}
			cmd.(*redis.StringCmd).SetVal(data)
		default:
			t.Fatalf("unexpected command %v", cmd.Args())
		}
		return nil
	})
	keys, err := s.ListKeysPrefix(context.Background(), "jobs:")
	if err != nil || !reflect.DeepEqual(keys, []string{"jobs:a", "jobs:z"}) {
		t.Fatalf("scan live sorted unique keys: %v %v", keys, err)
	}
	if len(reads) != 4 {
		t.Fatalf("duplicate key was fetched twice: %v", reads)
	}
}

func TestRedisLegacyRemovalDoesNotReadPayload(t *testing.T) {
	s := hookedRedisStorage(t, "ns:", func(cmd redis.Cmder) error {
		if !reflect.DeepEqual(cmd.Args(), []interface{}{"unlink", "ns:job_logs:job"}) {
			t.Fatalf("removal must only UNLINK one validated key: %v", cmd.Args())
		}
		cmd.(*redis.IntCmd).SetVal(1)
		return nil
	})
	if err := s.RemoveLegacyJobLog(context.Background(), "job"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveLegacyJobLog(context.Background(), "a/../../escape"); err == nil {
		t.Fatal("accepted unsafe legacy log id")
	}
}

func TestRedisConfigurationValidation(t *testing.T) {
	for _, config := range []*StorageConfig{nil, {Engine: "Redis"}, {Engine: "Redis", Options: map[string]interface{}{"addr": "unused", "db": -1}}, {Engine: "Redis", Options: map[string]interface{}{"addr": "unused", "read_timeout_ms": 0}}, {Engine: "Redis", Options: map[string]interface{}{"addr": "unused", "write_timeout_ms": 0.5}}, {Engine: "Redis", Options: map[string]interface{}{"addr": "unused", "tls": "true"}}} {
		if _, err := NewRedisStorage(config); err == nil {
			t.Fatalf("accepted invalid config %#v", config)
		}
	}
	if _, err := NewStorage(&StorageConfig{Engine: "Redis"}); err == nil {
		t.Fatal("Redis engine accepted absent configuration")
	}
	if _, err := NewRedisStorageContext(nil, &StorageConfig{}); err == nil {
		t.Fatal("accepted nil constructor context")
	}
	for _, value := range []interface{}{nil, "1", 1.5, -1, true} {
		if _, err := redisIntOption(map[string]interface{}{"db": value}, "db", 0, 0); err == nil {
			t.Fatalf("accepted invalid db %#v", value)
		}
	}
	if got, err := redisIntOption(map[string]interface{}{"db": float64(2)}, "db", 0, 0); err != nil || got != 2 {
		t.Fatalf("JSON db option: %d %v", got, err)
	}
	for _, key := range []string{"addr", "username", "password", "prefix", "tls_ca_file", "tls_cert_file", "tls_key_file", "tls_server_name"} {
		opts := map[string]interface{}{"addr": "unused:6379", key: 123}
		if _, err := NewRedisStorage(&StorageConfig{Options: opts}); err == nil || !strings.Contains(err.Error(), key+" must be a string") {
			t.Fatalf("string option %s silently ignored: %v", key, err)
		}
	}
	if _, err := NewRedisStorage(&StorageConfig{Options: map[string]interface{}{"addr": "unused:6379", "prefix": ""}}); err == nil || !strings.Contains(err.Error(), "prefix must not be empty") {
		t.Fatalf("empty prefix accepted or silently defaulted: %v", err)
	}
	for _, key := range []string{"addrs", "master_name", "sentinel", "sentinel_username", "sentinel_password", "sentinel_addrs", "cluster", "cluster_addrs", "cluster_mode"} {
		if _, err := NewRedisStorage(&StorageConfig{Options: map[string]interface{}{"addr": "unused:6379", key: ""}}); err == nil || !strings.Contains(err.Error(), "single instance") {
			t.Fatalf("accepted unsupported topology option %s: %v", key, err)
		}
	}
	if got, err := redisStringOption(nil, "username", "fallback"); err != nil || got != "fallback" {
		t.Fatalf("missing option fallback: %q %v", got, err)
	}
	if got, err := redisStringOption(map[string]interface{}{"username": ""}, "username", "fallback"); err != nil || got != "" {
		t.Fatalf("explicit empty username must select default account: %q %v", got, err)
	}
}

func TestRedisTLSConfiguration(t *testing.T) {
	config, err := redisTLSConfig(map[string]interface{}{"tls": true, "tls_server_name": "redis.internal"})
	if err != nil || config == nil || config.MinVersion != tls.VersionTLS12 || config.ServerName != "redis.internal" || config.InsecureSkipVerify {
		t.Fatalf("secure TLS defaults: %#v %v", config, err)
	}
	badCA := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, opts := range []map[string]interface{}{
		{"tls_server_name": "redis.internal"},
		{"tls": true, "tls_cert_file": "cert.pem"},
		{"tls": true, "tls_key_file": "key.pem"},
		{"tls": true, "tls_ca_file": badCA},
		{"tls": true, "tls_ca_file": filepath.Join(t.TempDir(), "absent.pem")},
	} {
		if _, err := redisTLSConfig(opts); err == nil {
			t.Fatalf("accepted incomplete TLS options %#v", opts)
		}
	}
}

func TestRedisMinimumServerVersion(t *testing.T) {
	for _, version := range []string{"8.4.0", "8.4.7", "8.10.0", "9.0.0"} {
		if !redisVersionSupported("# Server\r\nredis_version:" + version + "\r\n") {
			t.Fatalf("rejected supported Redis %s", version)
		}
	}
	for _, info := range []string{"redis_version:7.4.9", "redis_version:8.2.1", "redis_version:8.3.99", "redis_version:unknown", "redis_version:8.4", "redis_version:garbage.4.0", "# Server"} {
		if redisVersionSupported(info) {
			t.Fatalf("accepted unsupported or unidentified server %q", info)
		}
	}
}

func TestRedisRejectsNonStandaloneServer(t *testing.T) {
	for _, mode := range []string{"cluster", "sentinel", "", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			commands := 0
			store := hookedRedisStorage(t, "ns:", func(cmd redis.Cmder) error {
				commands++
				if cmd.Name() != "info" {
					t.Fatalf("non-standalone constructor issued %v", cmd.Args())
				}
				cmd.(*redis.StringCmd).SetVal("# Server\r\nredis_version:8.4.7\r\nredis_mode:" + mode + "\r\n")
				return nil
			})
			if err := store.checkCapabilities(context.Background()); err == nil || !strings.Contains(err.Error(), "standalone mode") {
				t.Fatalf("accepted non-standalone mode %q: %v", mode, err)
			}
			if commands != 1 {
				t.Fatalf("non-standalone constructor touched application keys: %d", commands)
			}
		})
	}
	if got := redisServerMode("# Server\r\nredis_mode:standalone\r\n"); got != "standalone" {
		t.Fatalf("cannot identify standalone server: %q", got)
	}
}

func TestRedisCancelledProbeKeepsStartupBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands := make(chan string, 4)
	store := hookedRedisStorage(t, "ns:", func(cmd redis.Cmder) error {
		commands <- cmd.Name()
		switch cmd.Name() {
		case "info":
			cmd.(*redis.StringCmd).SetVal("redis_version:8.4.7\r\nredis_mode:standalone\r\n")
			return nil
		case "set":
			args := cmd.Args()
			if !reflect.DeepEqual(args[len(args)-2:], []interface{}{"PX", 30000}) {
				t.Errorf("private probe has no bounded cleanup TTL: %v", args)
			}
			cancel()
			return context.Canceled
		default:
			t.Errorf("canceled startup issued independent cleanup command %v", cmd.Args())
			return nil
		}
	})
	if err := store.checkCapabilities(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup replaced cancellation with cleanup outcome: %v", err)
	}
	if len(commands) != 2 {
		t.Fatalf("canceled startup ran %d commands instead of INFO plus one probe", len(commands))
	}
}
