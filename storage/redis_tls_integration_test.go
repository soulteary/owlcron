package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRedisRealTLSConnection(t *testing.T) {
	addr, ca := os.Getenv("OWLCRON_TEST_REDIS_TLS_ADDR"), os.Getenv("OWLCRON_TEST_REDIS_TLS_CA_FILE")
	if addr == "" {
		t.Skip("set OWLCRON_TEST_REDIS_TLS_ADDR and OWLCRON_TEST_REDIS_TLS_CA_FILE for real Redis TLS")
	}
	if ca == "" {
		t.Fatal("explicit TLS acceptance requires a trusted CA file")
	}
	options := map[string]interface{}{
		"addr": addr, "username": os.Getenv("OWLCRON_TEST_REDIS_USERNAME"),
		"password": os.Getenv("OWLCRON_TEST_REDIS_PASSWORD"),
		"tls":      true, "tls_ca_file": ca, "tls_server_name": "localhost",
		"prefix":          fmt.Sprintf("redis-tls:%d:", time.Now().UnixNano()),
		"dial_timeout_ms": 1000, "startup_timeout_ms": 2000,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewRedisStorageContext(ctx, &StorageConfig{Engine: "Redis", Options: options})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put(ctx, "binary", []byte{0, 1, 255}); err != nil {
		t.Fatal(err)
	}
	if data, err := store.Get(ctx, "binary"); err != nil || string(data) != string([]byte{0, 1, 255}) {
		t.Fatalf("TLS byte round trip: %v %v", data, err)
	}
	if err := store.unlink(ctx, store.prefix+"binary"); err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct {
		name, key string
		value     interface{}
	}{
		{"wrong-server-name", "tls_server_name", "invalid.example"},
		{"untrusted-ca", "tls_ca_file", ""},
		{"wrong-acl-user", "username", "not-owlcron"},
	} {
		t.Run(change.name, func(t *testing.T) {
			bad := make(map[string]interface{}, len(options))
			for key, value := range options {
				bad[key] = value
			}
			bad[change.key] = change.value
			unexpected, err := NewRedisStorageContext(ctx, &StorageConfig{Engine: "Redis", Options: bad})
			if unexpected != nil {
				_ = unexpected.Close()
			}
			if err == nil {
				t.Fatal("invalid TLS certificate or ACL identity accepted")
			}
		})
	}
}
