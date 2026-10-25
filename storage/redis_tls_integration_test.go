package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func redisRealTLSOptions(t *testing.T) map[string]interface{} {
	t.Helper()
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
	// A manually supplied one-way TLS endpoint remains supported. The hermetic
	// acceptance service supplies both files and requires client authentication.
	for option, env := range map[string]string{
		"tls_cert_file": "OWLCRON_TEST_REDIS_TLS_CERT_FILE",
		"tls_key_file":  "OWLCRON_TEST_REDIS_TLS_KEY_FILE",
	} {
		if value := os.Getenv(env); value != "" {
			options[option] = value
		}
	}
	return options
}

func redisTLSOptionsCopy(options map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{}, len(options))
	for key, value := range options {
		copy[key] = value
	}
	return copy
}

func TestRedisRealTLSConnection(t *testing.T) {
	options := redisRealTLSOptions(t)
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
		expected  []string
	}{
		{"wrong-server-name", "tls_server_name", "invalid.example", []string{"certificate is valid for"}},
		{"untrusted-ca", "tls_ca_file", "", []string{"certificate signed by unknown authority"}},
		{"wrong-acl-user", "username", "not-owlcron", []string{"wrongpass", "invalid username-password pair"}},
	} {
		t.Run(change.name, func(t *testing.T) {
			bad := redisTLSOptionsCopy(options)
			bad[change.key] = change.value
			redisTLSRequireRejection(t, bad, change.expected...)
		})
	}
}

// redisTLSRequireRejection rejects timeout/network failures: a negative
// authentication case only passes on evidence of the requested rejection.
func redisTLSRequireRejection(t *testing.T, options map[string]interface{}, expected ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unexpected, err := NewRedisStorageContext(ctx, &StorageConfig{Engine: "Redis", Options: options})
	if unexpected != nil {
		_ = unexpected.Close()
	}
	if err == nil {
		t.Fatal("invalid TLS certificate or ACL identity accepted")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("authentication case failed without an authentication rejection: %v", err)
	}
	message := strings.ToLower(err.Error())
	for _, fragment := range expected {
		if strings.Contains(message, strings.ToLower(fragment)) {
			return
		}
	}
	t.Fatalf("expected certificate or ACL rejection containing one of %q, got: %v", expected, err)
}

func TestRedisRealMTLSClientAuthentication(t *testing.T) {
	options := redisRealTLSOptions(t)
	for _, env := range []string{
		"OWLCRON_TEST_REDIS_TLS_CERT_FILE", "OWLCRON_TEST_REDIS_TLS_KEY_FILE",
		"OWLCRON_TEST_REDIS_TLS_BAD_CERT_FILE", "OWLCRON_TEST_REDIS_TLS_BAD_KEY_FILE",
	} {
		if os.Getenv(env) == "" {
			t.Skipf("set %s to run real Redis mutual TLS acceptance", env)
		}
	}
	config := func(options map[string]interface{}) *StorageConfig {
		return &StorageConfig{Engine: "Redis", Options: options}
	}
	t.Run("valid-client-storage-and-backup-restore", func(t *testing.T) {
		a := openRedisRealTestStore(t, config(options))
		b := openRedisRealTestStore(t, config(options))
		exerciseStorage(t, a, b)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		payload := []byte{0, 1, 255, '\n'}
		if err := a.Put(ctx, "mtls/binary", payload); err != nil {
			t.Fatal(err)
		}
		// Exercise both native CAS success and stale-token conflict explicitly;
		// TLS transport must preserve the existing byte envelope and revision.
		raw, version, err := a.read(ctx, "mtls/binary")
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Update(ctx, "mtls/binary", func(data []byte) ([]byte, error) {
			if !bytes.Equal(data, payload) {
				return nil, fmt.Errorf("mutual TLS changed binary payload")
			}
			return append(data, 42), nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := a.compareAndSwap(ctx, "mtls/binary", raw, version); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale native CAS through mutual TLS: %v", err)
		}
		expires := time.Now().Add(5 * time.Minute).Unix()
		if err := b.Expire(ctx, "mtls/binary", expires); err != nil {
			t.Fatal(err)
		}
		backup, err := Export(ctx, a)
		if err != nil {
			t.Fatalf("mutual TLS backup export: %v", err)
		}
		restoredOptions := redisTLSOptionsCopy(options)
		restoredOptions["prefix"] = options["prefix"].(string) + "restored:"
		restored := openRedisRealTestStore(t, config(restoredOptions))
		if err := Import(ctx, restored, backup); err != nil {
			t.Fatalf("mutual TLS backup restore: %v", err)
		}
		reexported, err := Export(ctx, restored)
		if err != nil || len(reexported.Records) != len(backup.Records) {
			t.Fatalf("mutual TLS re-export record count: %v", err)
		}
		for key, entry := range backup.Records {
			got, exists := reexported.Records[key]
			if !exists || !bytes.Equal(got.Data, entry.Data) || got.Expires != entry.Expires {
				t.Fatalf("mutual TLS backup round trip changed %q", key)
			}
		}
	})
	t.Run("missing-client-certificate", func(t *testing.T) {
		bad := redisTLSOptionsCopy(options)
		delete(bad, "tls_cert_file")
		delete(bad, "tls_key_file")
		redisTLSRequireRejection(t, bad, "tls: certificate required", "tls: bad certificate")
	})
	t.Run("untrusted-client-certificate", func(t *testing.T) {
		bad := redisTLSOptionsCopy(options)
		bad["tls_cert_file"] = os.Getenv("OWLCRON_TEST_REDIS_TLS_BAD_CERT_FILE")
		bad["tls_key_file"] = os.Getenv("OWLCRON_TEST_REDIS_TLS_BAD_KEY_FILE")
		// With identical issuer names but different CA keys, OpenSSL can report
		// the failed certificate signature as decrypt_error instead of unknown_ca.
		// Both are explicit TLS alerts; a plain EOF or timeout still fails here.
		redisTLSRequireRejection(t, bad, "tls: unknown certificate authority", "tls: bad certificate", "tls: error decrypting message")
	})
	t.Run("mismatched-client-private-key", func(t *testing.T) {
		bad := redisTLSOptionsCopy(options)
		bad["tls_key_file"] = os.Getenv("OWLCRON_TEST_REDIS_TLS_BAD_KEY_FILE")
		redisTLSRequireRejection(t, bad, "private key does not match public key", "private key type does not match public key type")
	})
	t.Run("client-certificate-does-not-bypass-acl", func(t *testing.T) {
		bad := redisTLSOptionsCopy(options)
		bad["password"] = "deliberately-invalid-acl-password"
		redisTLSRequireRejection(t, bad, "wrongpass", "invalid username-password pair")
	})
	t.Run("reconnect-requires-client-identity", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		first, err := NewRedisStorageContext(ctx, config(options))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = first.Close() })
		if err := first.Put(ctx, "mtls/reconnect", []byte("persisted")); err != nil {
			t.Fatal(err)
		}
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		// A separate pool must perform a new TLS handshake. A previous successful
		// connection must not let a fresh unauthenticated connection in.
		reopened := openRedisRealTestStore(t, config(options))
		data, err := reopened.Get(ctx, "mtls/reconnect")
		if err != nil || string(data) != "persisted" {
			t.Fatalf("reconnected mutual TLS storage: %q %v", data, err)
		}
		bad := redisTLSOptionsCopy(options)
		delete(bad, "tls_cert_file")
		delete(bad, "tls_key_file")
		redisTLSRequireRejection(t, bad, "tls: certificate required", "tls: bad certificate")
	})
}
