package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/soulteary/owlcron/types"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestMainCLIChild(t *testing.T) {
	if os.Getenv("OWLCRON_TEST_MAIN_CHILD") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing subprocess arguments")
	}
	os.Args = append(os.Args[:1], os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet("owlcron", flag.ExitOnError)
	main()
}

func TestMainCLIExitCodes(t *testing.T) {
	t.Setenv("OWLCRON_SECRET_KEY", "")
	config := &types.Config{StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), SecretKey: "test-secret-0123456789abcdef0123456789", UDPBroadcastPort: -1}
	raw, _ := json.Marshal(config)
	configPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(t.TempDir(), "malformed-backup.json")
	if err := os.WriteFile(malformed, []byte("null"), 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"version"}, 0}, {[]string{"setup"}, 0},
		{[]string{"import", "missing-file"}, 1}, {[]string{"import", malformed}, 1},
		{[]string{"export", filepath.Join(t.TempDir(), "missing-dir", "backup.json")}, 1},
		{[]string{"storage-cli", "get", "missing"}, 1}, {[]string{"storage-cli", "unknown"}, 1},
		{[]string{"unknown-command"}, 1},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		args := append([]string{"-test.run=^TestMainCLIChild$", "--", "-config", configPath}, tc.args...)
		command := exec.CommandContext(ctx, executable, args...)
		command.Env = append(os.Environ(), "OWLCRON_TEST_MAIN_CHILD=1")
		output, err := command.CombinedOutput()
		cancel()
		if command.ProcessState == nil || command.ProcessState.ExitCode() != tc.exit {
			t.Fatalf("%v exit=%v err=%v output=%s", tc.args, command.ProcessState, err, output)
		}
	}
}

func TestSecretInitializationPersistsAndRejectsWeakConfig(t *testing.T) {
	t.Setenv("OWLCRON_SECRET_KEY", "")
	dir := t.TempDir()
	a := &types.Config{HTTPPort: 3012, StorageBaseDir: dir, SecretKey: "CHANGE_ME_TO_RANDOM_STRING"}
	if err := initializeSecret(a); err != nil {
		t.Fatal(err)
	}
	if len(a.SecretKey) != 64 {
		t.Fatal("weak generated secret")
	}
	b := &types.Config{HTTPPort: 3012, StorageBaseDir: dir}
	if err := initializeSecret(b); err != nil || b.SecretKey != a.SecretKey {
		t.Fatalf("restart secret changed: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "cluster-secret"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("secret permissions")
	}
	if err := initializeSecret(&types.Config{HTTPPort: 3012, SecretKey: "weak"}); err == nil {
		t.Fatal("weak config accepted")
	}
	if err := initializeSecret(&types.Config{HTTPPort: 65536, SecretKey: a.SecretKey}); err == nil {
		t.Fatal("invalid port accepted")
	}
}

func TestConfigDefaultURLFollowsPort(t *testing.T) {
	t.Setenv("OWLCRON_SECRET_KEY", "owlcron-config-test-secret-0123456789abcdef")
	for _, tc := range []struct {
		name, input, wantURL string
		wantPort             int
	}{
		{"default", `{}`, "http://localhost:3012", 3012},
		{"custom port", `{"http_port":19312,"udp_broadcast_port":0,"scheduler_startup_grace":0}`, "http://localhost:19312", 19312},
		{"explicit URL", `{"http_port":19312,"base_app_url":"https://cron.example.test"}`, "https://cron.example.test", 19312},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			config, err := loadConfig(path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if config.HTTPPort != tc.wantPort || config.BaseAppURL != tc.wantURL {
				t.Fatalf("config port=%d URL=%q", config.HTTPPort, config.BaseAppURL)
			}
			if tc.name == "custom port" && (config.UDPBroadcastPort != 0 || config.SchedulerStartupGrace != 0) {
				t.Fatal("explicit zero values replaced by defaults")
			}
		})
	}
}

func TestConcurrentSecretInitializationPublishesCompleteFile(t *testing.T) {
	t.Setenv("OWLCRON_SECRET_KEY", "")
	dir := t.TempDir()
	var wg sync.WaitGroup
	secrets := make(chan string, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			config := &types.Config{HTTPPort: 3012, StorageBaseDir: dir}
			if err := initializeSecret(config); err != nil {
				t.Errorf("concurrent initialize: %v", err)
				return
			}
			secrets <- config.SecretKey
		}()
	}
	wg.Wait()
	close(secrets)
	var expected string
	for secret := range secrets {
		if expected == "" {
			expected = secret
		}
		if secret != expected || len(secret) != 64 {
			t.Fatal("initializers did not converge on one complete secret")
		}
	}
}

func TestNativeTLSDefaultBaseAppURL(t *testing.T) {
	config := &types.Config{HTTPPort: 8443, ClusterTLSCertFile: "/private/server.pem"}
	applyDefaults(config)
	if config.BaseAppURL != "https://localhost:8443" {
		t.Fatalf("native TLS generated an HTTP link: %s", config.BaseAppURL)
	}
	config.BaseAppURL = "https://public.example/cron"
	applyDefaults(config)
	if config.BaseAppURL != "https://public.example/cron" {
		t.Fatal("explicit public URL was overwritten")
	}
}
