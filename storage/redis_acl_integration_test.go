package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisMinimalACLTestConfig(t *testing.T) *StorageConfig {
	t.Helper()
	username := os.Getenv("OWLCRON_TEST_REDIS_MINIMAL_USERNAME")
	if username == "" {
		t.Skip("set OWLCRON_TEST_REDIS_MINIMAL_USERNAME to run strict Redis ACL acceptance")
	}
	config := redisRealTestConfig(t)
	password := os.Getenv("OWLCRON_TEST_REDIS_MINIMAL_PASSWORD")
	prefix := os.Getenv("OWLCRON_TEST_REDIS_MINIMAL_PREFIX")
	db, err := strconv.Atoi(os.Getenv("OWLCRON_TEST_REDIS_MINIMAL_DB"))
	if password == "" || prefix == "" || err != nil || db <= 0 {
		t.Fatal("strict Redis ACL acceptance requires password, prefix, and a nonzero DB")
	}
	config.Options["username"] = username
	config.Options["password"] = password
	config.Options["prefix"] = fmt.Sprintf("%s%d:", prefix, time.Now().UnixNano())
	config.Options["db"] = db
	return config
}

func requireRedisNoPermission(t *testing.T, err error) {
	t.Helper()
	// Only an actual Redis NOPERM reply proves the ACL rejected a command.
	// A disconnected socket, timeout, missing key, or protocol error cannot pass.
	if !redis.HasErrorPrefix(err, "NOPERM ") {
		t.Fatalf("expected Redis NOPERM, got %T: %v", err, err)
	}
}

// The fixture grants only +ping +info|server +get +set +scan +unlink +select
// and the configured key pattern. CLIENT, Lua, native list/TTL commands, and
// all other command categories remain denied. Redis 8.4 permits the special
// authentication/negotiation commands without explicitly granting +auth/+hello;
// their availability is not evidence that the command allowlist is broader.
func TestRedisRealMinimalACL(t *testing.T) {
	config := redisMinimalACLTestConfig(t)
	a := openRedisRealTestStore(t, config)
	b := openRedisRealTestStore(t, config)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if a.client.Options().DB != config.Options["db"] || a.client.Options().Username != config.Options["username"] {
		t.Fatal("minimal ACL fixture did not select its named user and nonzero DB")
	}

	t.Run("storage_contract", func(t *testing.T) {
		exerciseStorage(t, a, b)
		binary := []byte{0, 255, 'a', '\n'}
		if err := a.PutStream(ctx, "contract/stream", binary); err != nil {
			t.Fatal(err)
		}
		if got, err := b.GetStream(ctx, "contract/stream"); err != nil || !bytes.Equal(got, binary) {
			t.Fatalf("stream contract: %q %v", got, err)
		}
		if err := a.Update(ctx, "contract/update", func(old []byte) ([]byte, error) {
			return append(old, []byte("atomic")...), nil
		}); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Get(ctx, "contract/update"); err != nil || string(got) != "atomic" {
			t.Fatalf("atomic update contract: %q %v", got, err)
		}
		for _, item := range []string{"middle", "last"} {
			if err := a.ListPush(ctx, "contract/list", item); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.ListUnshift(ctx, "contract/list", "first"); err != nil {
			t.Fatal(err)
		}
		if got, err := a.ListGet(ctx, "contract/list", 1, 1); err != nil || !reflect.DeepEqual(got, []interface{}{"middle"}) {
			t.Fatalf("list page: %#v %v", got, err)
		}
		if got, err := b.ListPop(ctx, "contract/list"); err != nil || got != "last" {
			t.Fatalf("list pop: %#v %v", got, err)
		}
		if err := a.ListDelete(ctx, "contract/list", true); err != nil {
			t.Fatal(err)
		}
		if exists, err := b.Exists(ctx, "contract/list"); err != nil || exists {
			t.Fatalf("list deletion: %v %v", exists, err)
		}
		if _, err := a.BeginTransaction(ctx); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("transactions must remain unsupported: %v", err)
		}
	})

	t.Run("native_cas_and_logical_expiry", func(t *testing.T) {
		const key = "native/envelope"
		raw := []byte(`{"data":"AP9hCg==","expires":0,"revision":"existing-revision"}`)
		if err := a.client.Set(ctx, a.prefix+key, raw, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Get(ctx, key); err != nil || !bytes.Equal(got, []byte{0, 255, 'a', '\n'}) {
			t.Fatalf("existing shared envelope: %q %v", got, err)
		}
		_, token, err := a.read(ctx, key)
		if err != nil || token != string(raw) {
			t.Fatalf("raw CAS token changed: %q %v", token, err)
		}
		if err := b.Put(ctx, key, []byte("replacement")); err != nil {
			t.Fatal(err)
		}
		for _, version := range []string{token, ""} {
			if err := a.compareAndSwap(ctx, key, raw, version); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale IFEQ/NX overwrote a record: %v", err)
			}
		}
		if err := a.compareAndSwap(ctx, "native/missing", raw, token); !errors.Is(err, ErrConflict) {
			t.Fatalf("IFEQ created an absent record: %v", err)
		}
		if err := a.Expire(ctx, key, time.Now().Unix()-1); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("logical expiry remained visible: %v", err)
		}
		physical, err := a.client.Get(ctx, a.prefix+key).Bytes()
		var expired remoteDocument
		if err != nil || json.Unmarshal(physical, &expired) != nil || expired.Expires <= 0 || expired.Revision == "" {
			t.Fatalf("logical expiry discarded the physical CAS fence: %s %v", physical, err)
		}
		if err := b.Put(ctx, key, []byte("restored")); err != nil {
			t.Fatal(err)
		}
		if err := a.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		physical, err = b.client.Get(ctx, b.prefix+key).Bytes()
		var tombstone remoteDocument
		if err != nil || json.Unmarshal(physical, &tombstone) != nil || !tombstone.Deleted || tombstone.Data != nil || len(tombstone.Revision) != 32 {
			t.Fatalf("shared tombstone format lost: %s %v", physical, err)
		}
	})

	t.Run("list_keys_and_legacy_log", func(t *testing.T) {
		for _, key := range []string{"listing/live", "listing/expired", "listing/deleted"} {
			if err := a.Put(ctx, key, []byte("payload")); err != nil {
				t.Fatal(err)
			}
		}
		if err := a.Expire(ctx, "listing/expired", time.Now().Unix()-1); err != nil {
			t.Fatal(err)
		}
		if err := a.Delete(ctx, "listing/deleted"); err != nil {
			t.Fatal(err)
		}
		if keys, err := a.ListKeysPrefix(ctx, "listing/"); err != nil || !reflect.DeepEqual(keys, []string{"listing/live"}) {
			t.Fatalf("SCAN live-record enumeration: %v %v", keys, err)
		}
		physical := a.prefix + "job_logs:legacy-acl"
		if err := a.client.Set(ctx, physical, "legacy unencoded payload", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := a.RemoveLegacyJobLog(ctx, "legacy-acl"); err != nil {
			t.Fatalf("UNLINK legacy payload: %v", err)
		}
		if _, err := a.client.Get(ctx, physical).Bytes(); !errors.Is(err, redis.Nil) {
			t.Fatalf("legacy payload survived UNLINK: %v", err)
		}
	})

	t.Run("backup_and_chunked_logs", func(t *testing.T) {
		expires := time.Now().Unix() + 3600
		if err := a.Put(ctx, "users:acl", []byte(`{"id":"acl"}`)); err != nil {
			t.Fatal(err)
		}
		if err := a.Expire(ctx, "users:acl", expires); err != nil {
			t.Fatal(err)
		}
		data := bytes.Repeat([]byte{0, 255, 'x', '\n'}, JobLogChunkSize/4+12)
		if _, err := JobLogAppend(ctx, a, "acl-log", 0, data[:JobLogChunkSize], false, JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
		if _, err := JobLogAppend(ctx, a, "acl-log", JobLogChunkSize, data[JobLogChunkSize:], true, JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
		if _, err := JobLogAppend(ctx, a, "acl-deleted-log", 0, []byte("removed"), true, JobLogAbsoluteMax); err != nil {
			t.Fatal(err)
		}
		if err := JobLogDelete(ctx, a, "acl-deleted-log"); err != nil {
			t.Fatal(err)
		}
		var streamed bytes.Buffer
		if count, err := ExportTo(ctx, a, &streamed); err != nil || count == 0 {
			t.Fatalf("minimal ACL streaming export: %d %v", count, err)
		}
		var backup Backup
		if err := json.Unmarshal(streamed.Bytes(), &backup); err != nil {
			t.Fatal(err)
		}
		if backup.Format != "owlcron-storage-v1" || backup.Records["users:acl"].Expires != expires {
			t.Fatal("export changed the shared backup format or logical expiry")
		}
		for _, key := range []string{"native/envelope", "listing/expired", "listing/deleted"} {
			if _, exists := backup.Records[key]; exists {
				t.Fatalf("export included expired/deleted record %q", key)
			}
		}
		filesystem, err := NewFilesystemStorage(&StorageConfig{BaseDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		if err := Import(ctx, filesystem, &backup); err != nil {
			t.Fatal(err)
		}
		fromFilesystem, err := Export(ctx, filesystem)
		if err != nil {
			t.Fatal(err)
		}
		restoredConfig := redisMinimalACLTestConfig(t)
		restored := openRedisRealTestStore(t, restoredConfig)
		if err := Import(ctx, restored, fromFilesystem); err != nil {
			t.Fatalf("filesystem -> minimal ACL Redis restore: %v", err)
		}
		for key, entry := range backup.Records {
			if got, err := restored.Get(ctx, key); err != nil || !bytes.Equal(got, entry.Data) {
				t.Fatalf("restore changed %q: %q %v", key, got, err)
			}
		}
		got, manifest, err := JobLogRead(ctx, restored, "acl-log", 0, len(data)+1)
		if err != nil || !manifest.Final || !bytes.Equal(got, data) {
			t.Fatalf("restored binary log: %d %#v %v", len(got), manifest, err)
		}
		if _, err := JobLogAppend(ctx, restored, "acl-deleted-log", 0, []byte("late"), false, JobLogAbsoluteMax); !errors.Is(err, ErrJobLogDeleted) {
			t.Fatalf("restore lost log deletion fence: %v", err)
		}
		doc, _, err := restored.readDocument(ctx, "users:acl")
		if err != nil || doc.Expires != expires {
			t.Fatalf("restored logical expiry: %d %v", doc.Expires, err)
		}
	})

	t.Run("mutual_tls", func(t *testing.T) {
		options := redisRealTLSOptions(t)
		if os.Getenv("OWLCRON_TEST_REDIS_TLS_CERT_FILE") == "" || os.Getenv("OWLCRON_TEST_REDIS_TLS_KEY_FILE") == "" {
			t.Skip("set Redis TLS client certificate and key for combined mutual TLS and minimal ACL acceptance")
		}
		for _, key := range []string{"username", "password", "db", "prefix"} {
			options[key] = config.Options[key]
		}
		options["prefix"] = options["prefix"].(string) + "mutual-tls:"
		tlsConfig := &StorageConfig{Engine: "Redis", Options: options}
		secure := openRedisRealTestStore(t, tlsConfig)
		binary := []byte{0, 255, 'a', '\n'}
		if err := secure.Put(ctx, "mtls/binary", binary); err != nil {
			t.Fatal(err)
		}
		if got, err := secure.Get(ctx, "mtls/binary"); err != nil || !bytes.Equal(got, binary) {
			t.Fatalf("mutual TLS plus minimal ACL binary round trip: %q %v", got, err)
		}
		backup, err := Export(ctx, secure)
		if err != nil {
			t.Fatalf("mutual TLS plus minimal ACL export: %v", err)
		}
		restoredOptions := redisTLSOptionsCopy(options)
		restoredOptions["prefix"] = options["prefix"].(string) + "restore:"
		restored := openRedisRealTestStore(t, &StorageConfig{Engine: "Redis", Options: restoredOptions})
		if err := Import(ctx, restored, backup); err != nil {
			t.Fatalf("mutual TLS plus minimal ACL restore: %v", err)
		}
		if got, err := restored.Get(ctx, "mtls/binary"); err != nil || !bytes.Equal(got, binary) {
			t.Fatalf("mutual TLS plus minimal ACL restored bytes: %q %v", got, err)
		}
		requireRedisNoPermission(t, secure.client.Do(ctx, "CONFIG", "GET", "*").Err())
		requireRedisNoPermission(t, secure.client.Do(ctx, "GET", "outside-minimal-acl").Err())
	})

	t.Run("denied_commands", func(t *testing.T) {
		const canary = "security/canary"
		if err := a.Put(ctx, canary, []byte("unchanged")); err != nil {
			t.Fatal(err)
		}
		physical := a.prefix + canary
		for _, command := range []struct {
			name string
			args []interface{}
		}{
			{"client_setinfo", []interface{}{"CLIENT", "SETINFO", "LIB-NAME", "must-be-denied"}},
			{"info_all", []interface{}{"INFO"}},
			{"info_memory", []interface{}{"INFO", "memory"}},
			{"config", []interface{}{"CONFIG", "GET", "*"}},
			{"acl", []interface{}{"ACL", "GETUSER", config.Options["username"]}},
			// Invalid options also make an accidentally overprivileged fixture safe.
			// They must still return NOPERM, not a syntax error, to prove denial.
			{"flushdb", []interface{}{"FLUSHDB", "INVALID-ACCEPTANCE-OPTION"}},
			{"flushall", []interface{}{"FLUSHALL", "INVALID-ACCEPTANCE-OPTION"}},
			{"eval", []interface{}{"EVAL", "return redis.call('SET', KEYS[1], 'changed')", 1, physical}},
			{"evalsha", []interface{}{"EVALSHA", "0000000000000000000000000000000000000000", 1, physical}},
			{"script", []interface{}{"SCRIPT", "LOAD", "return 1"}},
			{"function", []interface{}{"FUNCTION", "LIST"}},
			{"keys", []interface{}{"KEYS", a.prefix + "*"}},
			{"del", []interface{}{"DEL", physical}},
			{"expire", []interface{}{"EXPIRE", physical, 1}},
			{"ttl", []interface{}{"TTL", physical}},
			{"exists", []interface{}{"EXISTS", physical}},
			{"mget", []interface{}{"MGET", physical}},
			{"mset", []interface{}{"MSET", physical, "changed"}},
			{"multi", []interface{}{"MULTI"}},
			{"watch", []interface{}{"WATCH", physical}},
			{"publish", []interface{}{"PUBLISH", a.prefix + "channel", "message"}},
		} {
			t.Run(command.name, func(t *testing.T) {
				requireRedisNoPermission(t, a.client.Do(ctx, command.args...).Err())
				if got, err := a.Get(ctx, canary); err != nil || string(got) != "unchanged" {
					t.Fatalf("denied command changed its data canary: %q %v", got, err)
				}
			})
		}
	})

	t.Run("namespace_boundary", func(t *testing.T) {
		observer := redis.NewClient(&redis.Options{
			Addr: config.Options["addr"].(string), DB: config.Options["db"].(int),
			Username: os.Getenv("OWLCRON_TEST_REDIS_USERNAME"), Password: os.Getenv("OWLCRON_TEST_REDIS_PASSWORD"),
			MaxRetries: -1, ContextTimeoutEnabled: true,
		})
		defer func() { _ = observer.Close() }()
		outside := fmt.Sprintf("outside-minimal-acl:%d", time.Now().UnixNano())
		if err := observer.Set(ctx, outside, "outside-unchanged", 0).Err(); err != nil {
			t.Fatalf("prepare namespace observer: %v", err)
		}
		defer func() {
			if err := observer.Unlink(context.Background(), outside).Err(); err != nil {
				t.Errorf("remove outside namespace canary: %v", err)
			}
		}()
		for _, command := range []struct {
			name string
			args []interface{}
		}{
			{"get", []interface{}{"GET", outside}},
			{"set", []interface{}{"SET", outside, "changed"}},
			{"set_ifeq", []interface{}{"SET", outside, "changed", "IFEQ", "outside-unchanged"}},
			{"set_nx", []interface{}{"SET", outside + ":missing", "changed", "NX"}},
			{"unlink", []interface{}{"UNLINK", outside}},
		} {
			t.Run(command.name, func(t *testing.T) {
				requireRedisNoPermission(t, a.client.Do(ctx, command.args...).Err())
				if got, err := observer.Get(ctx, outside).Result(); err != nil || got != "outside-unchanged" {
					t.Fatalf("namespace rejection changed outside data: %q %v", got, err)
				}
				if _, err := observer.Get(ctx, outside+":missing").Result(); !errors.Is(err, redis.Nil) {
					t.Fatalf("namespace rejection created an outside key: %v", err)
				}
			})
		}
		// Redis key ACL patterns control access, not SCAN's visibility of names.
		// The storage's namespace filter must still exclude unrelated physical keys.
		if keys, err := a.ListKeys(ctx); err != nil {
			t.Fatal(err)
		} else {
			for _, key := range keys {
				if key == outside {
					t.Fatal("ListKeys exposed an unrelated physical namespace")
				}
			}
		}
	})

	t.Run("missing_select", func(t *testing.T) {
		username := os.Getenv("OWLCRON_TEST_REDIS_NOSELECT_USERNAME")
		password := os.Getenv("OWLCRON_TEST_REDIS_NOSELECT_PASSWORD")
		if username == "" || password == "" {
			t.Fatal("strict ACL acceptance requires the companion user without SELECT")
		}
		options := make(map[string]interface{}, len(config.Options))
		for key, value := range config.Options {
			options[key] = value
		}
		options["username"], options["password"] = username, password
		withoutSelect := &StorageConfig{Engine: "Redis", Options: options}
		store, err := NewRedisStorage(withoutSelect)
		if store != nil {
			_ = store.Close()
			t.Fatal("DB1 initialization succeeded without SELECT permission")
		}
		requireRedisNoPermission(t, err)
		// Prove the same credentials, server, and remaining allowlist work in DB0;
		// the DB1 failure must come from SELECT rather than authentication/network.
		options["db"] = 0
		working := openRedisRealTestStore(t, withoutSelect)
		if err := working.Put(ctx, "noselect/positive", []byte("db0")); err != nil {
			t.Fatal(err)
		}
		if got, err := working.Get(ctx, "noselect/positive"); err != nil || string(got) != "db0" {
			t.Fatalf("DB0 companion-user control: %q %v", got, err)
		}
	})
}
