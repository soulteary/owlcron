package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func TestSetupAdminAndFullBackupRestore(t *testing.T) {
	config := &types.Config{StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), SecretKey: "test-secret-0123456789abcdef0123456789"}
	logger := zap.NewNop()
	if err := cmdSetup(config, logger); err != nil {
		t.Fatal(err)
	}
	if err := cmdSetup(config, logger); err != nil {
		t.Fatal(err)
	}
	if err := cmdAdmin([]string{"admin", "administrator-password"}, config, logger); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.NewEngine(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	user, err := eng.LoadUserByUsernameFromStorage("admin")
	if err != nil || !eng.VerifyUserPassword(user, "administrator-password") {
		t.Fatalf("CLI admin schema incompatible: %+v %v", user, err)
	}
	if plugin, err := eng.GetJobManager().GetPluginFromStorage("echoplug"); err != nil || plugin.Command != "bin/echo-plugin/echo-plugin" {
		t.Fatalf("seed plugin missing: %+v %v", plugin, err)
	}
	ctx := context.Background()
	store := eng.GetStorage()
	_ = store.Put(ctx, "jobs:finished", []byte(`{"id":"finished","complete":true}`))
	_ = store.Put(ctx, "job_logs:finished", []byte("log payload"))
	_ = store.Put(ctx, "global/state", []byte(`{"cursors":{"event":300}}`))
	file := filepath.Join(t.TempDir(), "backup.json")
	if err := cmdExport([]string{file}, config, logger); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("backup not private")
	}
	restored := &types.Config{StorageBaseDir: t.TempDir(), SecretKey: config.SecretKey, LogDir: t.TempDir(), QueueDir: t.TempDir()}
	if err := cmdImport([]string{file}, restored, logger); err != nil {
		t.Fatal(err)
	}
	fresh, err := engine.NewEngine(restored, logger)
	if err != nil {
		t.Fatal(err)
	}
	u, err := fresh.LoadUserByUsernameFromStorage("admin")
	if err != nil || !fresh.VerifyUserPassword(u, "administrator-password") {
		t.Fatal("restored authentication broken")
	}
	if data, err := fresh.GetStorage().Get(ctx, "job_logs:finished"); err != nil || string(data) != "log payload" {
		t.Fatal("restored log missing")
	}
	target := t.TempDir()
	if err := cmdStorageMigrate([]string{target}, config, logger); err != nil {
		t.Fatal(err)
	}
	migrated, _ := storage.NewFilesystemStorage(&storage.StorageConfig{BaseDir: target})
	if _, err := migrated.Get(ctx, "global/state"); err != nil {
		t.Fatal("migration dropped state")
	}
}

func TestCommandFailuresReturnErrors(t *testing.T) {
	config := &types.Config{StorageBaseDir: t.TempDir(), SecretKey: "test-secret-0123456789abcdef0123456789"}
	logger := zap.NewNop()
	if handled, err := Run(nil, config, logger); handled || err != nil {
		t.Fatal("empty arguments must enter daemon startup")
	}
	badBackup := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(badBackup, []byte("null"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"unknown-command"}, {"admin"}, {"admin", "valid-user", "short"},
		{"import"}, {"import", "missing-backup"}, {"import", badBackup},
		{"export", filepath.Join(t.TempDir(), "absent-dir", "backup.json")},
		{"storage-cli"}, {"storage-cli", "unknown"}, {"storage-cli", "get"},
		{"storage-cli", "get", "missing"}, {"storage-cli", "put", "some-key", "missing-file"},
		{"storage-migrate"},
	} {
		if handled, err := Run(args, config, logger); !handled || err == nil {
			t.Fatalf("failed command reported success: %v handled=%v err=%v", args, handled, err)
		}
	}
	badConfig := &types.Config{StorageEngine: "unsupported", SecretKey: config.SecretKey}
	validBackup := filepath.Join(t.TempDir(), "empty-backup.json")
	if err := os.WriteFile(validBackup, []byte(`{"format":"owlcron-storage-v1","records":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"setup"}, {"admin", "valid-user", "a-secure-password"}, {"export"}, {"import", validBackup}, {"maint"}, {"storage-cli", "get", "key"}, {"storage-migrate", t.TempDir()}, {"storage-repair"}} {
		if handled, err := Run(args, badConfig, logger); !handled || err == nil {
			t.Fatalf("storage open failure ignored: %v handled=%v err=%v", args, handled, err)
		}
	}
	store, err := openStorage(config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStorage(store)
	if err := store.Put(context.Background(), "users", []byte("malformed list")); err != nil {
		t.Fatal(err)
	}
	if handled, err := Run([]string{"storage-repair"}, config, logger); !handled || err == nil {
		t.Fatal("corrupt storage was reported repaired successfully")
	}
}
func TestLegacyImportRebuildsCanonicalRecords(t *testing.T) {
	raw := []byte(`{"users":[{"username":"admin","password":"hash","enabled":true}],"plugins":[{"id":"shell","command":"/bin/sh"}]}`)
	backup, err := readBackup(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := backup.Records["users:admin"]; !ok {
		t.Fatal("missing user canonical record")
	}
	var user types.User
	_ = json.Unmarshal(backup.Records["users:admin"].Data, &user)
	if user.ID != "admin" {
		t.Fatal("legacy user id missing")
	}
	var users []types.User
	if err = json.Unmarshal(backup.Records["users"].Data, &users); err != nil || len(users) != 1 || users[0].ID != "admin" {
		t.Fatal("legacy user index cannot resolve canonical record")
	}
	if _, ok := backup.Records["plugins:shell"]; !ok {
		t.Fatal("missing plugin canonical record")
	}
}

func TestBuildVersionString(t *testing.T) {
	version, commit := Version, Commit
	t.Cleanup(func() { Version, Commit = version, commit })
	Version, Commit = "v0.1.2-test", "0123456789abcdef"
	if got := VersionString(); got != "OwlCron v0.1.2-test (commit 0123456789abcdef)" {
		t.Fatalf("unexpected build version: %s", got)
	}
	if handled, err := Run([]string{"version"}, nil, nil); !handled || err != nil {
		t.Fatalf("version requires runtime configuration: handled=%v err=%v", handled, err)
	}
}
