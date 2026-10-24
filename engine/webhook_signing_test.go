package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func signingTestConfig() *types.Config {
	return &types.Config{WebHookSigningKeyID: "deployment-2026", WebHookSigningSecret: strings.Repeat("s", 32)}
}

func TestWebHookSigningConfigValidation(t *testing.T) {
	if signer, err := loadWebHookSigner(&types.Config{}); err != nil || signer != nil {
		t.Fatalf("disabled signer: %v", err)
	}
	cases := map[string]func(*types.Config){
		"missing ID":     func(c *types.Config) { c.WebHookSigningKeyID = "" },
		"invalid ID":     func(c *types.Config) { c.WebHookSigningKeyID = "key\r\nInjected: true" },
		"missing secret": func(c *types.Config) { c.WebHookSigningSecret = "" },
		"short":          func(c *types.Config) { c.WebHookSigningSecret = "short" },
		"newline":        func(c *types.Config) { c.WebHookSigningSecret += "\n" },
		"NUL":            func(c *types.Config) { c.WebHookSigningSecret += "\x00" },
		"cluster reuse":  func(c *types.Config) { c.SecretKey = c.WebHookSigningSecret },
		"SMTP reuse":     func(c *types.Config) { c.SMTPPassword = c.WebHookSigningSecret },
		"two sources":    func(c *types.Config) { c.WebHookSigningSecretFile = "/missing-secret" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c := signingTestConfig()
			change(c)
			if _, err := loadWebHookSigner(c); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestWebHookSigningSecretFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "private-secret")
	secret := strings.Repeat("q", 32)
	if err := os.WriteFile(path, []byte(secret+"\r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c := signingTestConfig()
	c.WebHookSigningSecret = ""
	c.WebHookSigningSecretFile = path
	s, err := loadWebHookSigner(c)
	if err != nil {
		t.Fatal(err)
	}
	if string(s.secret) != secret {
		t.Fatal("file secret changed")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadWebHookSigner(c); err == nil || strings.Contains(err.Error(), path) {
		t.Fatalf("unsafe permission or disclosed path: %v", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	c.WebHookSigningSecretFile = link
	if _, err := loadWebHookSigner(c); err == nil {
		t.Fatal("symlink accepted")
	}
	c.WebHookSigningSecretFile = path
	if err := os.WriteFile(path, []byte(strings.Repeat("a", maxWebHookSigningSecretBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadWebHookSigner(c); err == nil {
		t.Fatal("oversized secret accepted")
	}
}

func expectedWebHookSignature(req *http.Request, body []byte, secret string) string {
	canonical := strings.Join([]string{"v1", req.Header.Get("X-Owlcron-Key-ID"), req.Header.Get("X-Owlcron-Timestamp"), req.Header.Get("Idempotency-Key"), req.Method, req.URL.Scheme, req.URL.Host, req.URL.RequestURI(), ""}, "\n")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(canonical))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestWebHookSignatureAuthenticatesBodyTargetAndIdentity(t *testing.T) {
	c := signingTestConfig()
	s, err := loadWebHookSigner(c)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, "https://hooks.example.test/a%2Fb?x=1", nil)
	body := []byte(`{"event":"complete"}`)
	if err := s.sign(req, body, "notification-123"); err != nil {
		t.Fatal(err)
	}
	sig := req.Header.Get("X-Owlcron-Signature")
	if sig != expectedWebHookSignature(req, body, c.WebHookSigningSecret) {
		t.Fatal("signature mismatch")
	}
	ts, err := strconv.ParseInt(req.Header.Get("X-Owlcron-Timestamp"), 10, 64)
	if err != nil || time.Since(time.Unix(ts, 0)) > 2*time.Second {
		t.Fatal("invalid timestamp")
	}
	if req.Header.Get("X-Owlcron-Key-ID") != c.WebHookSigningKeyID {
		t.Fatal("missing key ID")
	}
	if sig == expectedWebHookSignature(req, []byte(`{"event":"failed"}`), c.WebHookSigningSecret) {
		t.Fatal("body tampering not detected")
	}
	for _, change := range []func(*http.Request){
		func(r *http.Request) { r.Method = "GET" },
		func(r *http.Request) { r.Header.Set("X-Owlcron-Key-ID", "other-key") },
		func(r *http.Request) { r.URL.Host = "other.example.test" },
		func(r *http.Request) { r.URL.RawQuery = "x=2" },
		func(r *http.Request) { r.Header.Set("Idempotency-Key", "other-ID") },
		func(r *http.Request) { r.Header.Set("X-Owlcron-Timestamp", "1") },
	} {
		altered := req.Clone(req.Context())
		change(altered)
		if sig == expectedWebHookSignature(altered, body, c.WebHookSigningSecret) {
			t.Fatal("target/identity tampering not detected")
		}
	}
	if err := s.sign(req, body, "injected\nID"); err == nil {
		t.Fatal("newline ID accepted")
	}
}

func TestWebHookSignerConcurrentAttemptsAndRotation(t *testing.T) {
	c := signingTestConfig()
	s, err := loadWebHookSigner(c)
	if err != nil {
		t.Fatal(err)
	}
	c.WebHookSigningKeyID = "deployment-next"
	c.WebHookSigningSecret = strings.Repeat("n", 32)
	next, err := loadWebHookSigner(c)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, _ := http.NewRequest("POST", "https://hooks.example.test/", nil)
			id := strconv.Itoa(i)
			body := []byte(id)
			if err := s.sign(r, body, id); err != nil {
				t.Error(err)
				return
			}
			if r.Header.Get("X-Owlcron-Key-ID") != "deployment-2026" || r.Header.Get("X-Owlcron-Signature") != expectedWebHookSignature(r, body, strings.Repeat("s", 32)) {
				t.Error("concurrent signing corrupted immutable key")
			}
			old := r.Header.Get("X-Owlcron-Signature")
			if err := next.sign(r, body, id); err != nil {
				t.Error(err)
			}
			if old == r.Header.Get("X-Owlcron-Signature") || r.Header.Get("X-Owlcron-Key-ID") != "deployment-next" {
				t.Error("rotation did not change identity/signature")
			}
		}(i)
	}
	wg.Wait()
}

func TestWebHookSigningSecretRejectsSpecialFiles(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "owlcron-hook-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "socket")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	assertRejected := func(t *testing.T, path string) {
		t.Helper()
		c := signingTestConfig()
		c.WebHookSigningSecret = ""
		c.WebHookSigningSecretFile = path
		done := make(chan error, 1)
		go func() { _, err := loadWebHookSigner(c); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("special file accepted")
			}
		case <-time.After(time.Second):
			t.Fatal("secret loading blocked on special file")
		}
	}
	assertRejected(t, path)
	assertRejected(t, dir)
	t.Run("FIFO", func(t *testing.T) {
		path := filepath.Join(dir, "fifo")
		if err := syscall.Mkfifo(path, 0600); err != nil {
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) {
				t.Skipf("host/container disallows FIFO creation: %v; socket and directory rejection remains verified", err)
			}
			t.Fatal(err)
		}
		assertRejected(t, path)
	})
}
