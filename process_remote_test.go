package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
)

// Remote acceptance is deliberately explicit: ordinary unit/process tests need
// no external service, while an enabled backend with missing configuration fails.
// Credentials are supplied through the environment and are never logged.
func TestProcessRemoteClusterRecovery(t *testing.T) {
	raw := os.Getenv("OWLCRON_REMOTE_PROCESS_BACKENDS")
	if raw == "" {
		t.Skip("set OWLCRON_REMOTE_PROCESS_BACKENDS=S3,Couchbase for real remote-storage acceptance")
	}
	if testing.Short() {
		t.Fatal("explicit remote process acceptance cannot run with -short")
	}
	seen := map[string]bool{}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		var engine, variable string
		switch name {
		case "S3":
			engine, variable = name, "OWLCRON_REMOTE_S3_CONFIG"
		case "Couchbase":
			engine, variable = name, "OWLCRON_REMOTE_COUCHBASE_CONFIG"
		default:
			t.Fatalf("unsupported remote process backend %q", name)
		}
		if seen[engine] {
			t.Fatalf("duplicate remote process backend %q", engine)
		}
		seen[engine] = true
		configJSON := os.Getenv(variable)
		if configJSON == "" {
			t.Fatalf("explicit %s acceptance requires %s JSON storage options", engine, variable)
		}
		options := map[string]any{}
		if err := json.Unmarshal([]byte(configJSON), &options); err != nil || options == nil {
			t.Fatalf("%s must contain a JSON object (contents redacted)", variable)
		}
		t.Run(engine, func(t *testing.T) {
			cases := []struct {
				name string
				run  func(*testing.T, func(...*processFixture))
			}{
				{"ActiveJobAndChunkedLog", processSIGKILLRecoversActiveJobAndChunkedLog},
				{"MailAndWebHookQueue", processSIGKILLRecoversMailAndWebHookQueue},
				{"ClusterPartitionAndFailover", processClusterNetworkPartitionAndKilledLeaderRecovery},
				{"StorageOutageCompletionSpool", processRemoteStorageOutageCompletionSpool},
			}
			for _, test := range cases {
				t.Run(test.name, func(t *testing.T) {
					var nonce [16]byte
					if _, err := rand.Read(nonce[:]); err != nil {
						t.Fatal(err)
					}
					isolated := make(map[string]any, len(options)+1)
					for key, value := range options {
						isolated[key] = value
					}
					prefix, _ := options["prefix"].(string)
					isolated["prefix"] = strings.TrimRight(prefix, "/:") + "/process-acceptance/" + hex.EncodeToString(nonce[:]) + "/"
					test.run(t, func(fixtures ...*processFixture) {
						for _, f := range fixtures {
							f.Config.StorageEngine = engine
							f.Config.StorageOptions = isolated
							f.Put(t, "api_keys:"+f.APIKey, map[string]any{"id": f.APIKey, "key": f.APIKey, "enabled": true, "privileges": map[string]bool{"admin": true}})
						}
						store := fixtures[0].Store(t)
						// Registered before daemons start, so process cleanup runs first.
						// ListKeys is scoped by the freshly randomized backend prefix.
						t.Cleanup(func() { cleanupRemoteProcessNamespace(t, store) })
					})
				})
			}
		})
	}
}

func cleanupRemoteProcessNamespace(t *testing.T, store storage.Storage) {
	t.Helper()
	lister, ok := store.(interface {
		ListKeys(context.Context) ([]string, error)
	})
	if !ok {
		t.Error("remote acceptance store cannot enumerate its isolated namespace")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	keys, err := lister.ListKeys(ctx)
	if err != nil {
		t.Error("enumerate isolated remote acceptance namespace: cleanup failed")
		return
	}
	for _, key := range keys {
		if err := store.Delete(ctx, key); err != nil {
			t.Error("delete isolated remote acceptance key: cleanup failed")
			return
		}
	}
}
