package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func loginSecurityFixture(t *testing.T) (*Server, http.Handler) {
	t.Helper()
	dir := t.TempDir()
	eng, err := engine.NewEngine(&types.Config{SecretKey: strings.Repeat("login-key-", 4), StorageBaseDir: filepath.Join(dir, "data"), LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), TrustedProxyCIDRs: []string{"10.0.0.0/8"}, LoginRateLimit: 2, LoginAccountRateLimit: 20, LoginMaxConcurrent: 8}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	hash, err := eng.HashPassword("valid-password-123")
	if err != nil {
		t.Fatal(err)
	}
	if err = eng.SaveUserToStorage(&types.User{ID: "proxyuser", Username: "proxyuser", Password: hash, Enabled: true, Privileges: types.Privileges{}}); err != nil {
		t.Fatal(err)
	}
	s := NewServer(eng, zap.NewNop())
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	t.Cleanup(func() { _ = eng.Shutdown() })
	return s, s.AuthenticationMiddleware(mux)
}
func loginSecurityRequest(handler http.Handler, remote, forwarded, proto string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "http://owlcron.local/api/user/login", strings.NewReader(`{"username":"proxyuser","password":"valid-password-123"}`))
	r.RemoteAddr = remote
	r.Header.Set("X-Forwarded-For", forwarded)
	r.Header.Set("X-Forwarded-Proto", proto)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func TestLoginTrustedProxyCookieAndIPBudgetIntegration(t *testing.T) {
	_, handler := loginSecurityFixture(t)
	for i := 0; i < 2; i++ {
		w := loginSecurityRequest(handler, "10.1.2.3:1234", "198.51.100.8", "https")
		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response["code"] != float64(0) {
			t.Fatalf("login failed %s %v", w.Body.Bytes(), err)
		}
		cookies := w.Result().Cookies()
		if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
			t.Fatalf("insecure proxy session cookie: %#v", cookies)
		}
		logout := httptest.NewRequest(http.MethodPost, "http://owlcron.local/api/user/logout", nil)
		logout.RemoteAddr = "10.1.2.3:1234"
		logout.Header.Set("X-Forwarded-Proto", "https")
		logout.AddCookie(cookies[0])
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, logout)
		deleted := out.Result().Cookies()
		if len(deleted) != 1 || !deleted[0].Secure || deleted[0].MaxAge != -1 || deleted[0].Path != cookies[0].Path {
			t.Fatal("logout failed to preserve secure cookie policy")
		}
	}
	blocked := loginSecurityRequest(handler, "10.1.2.3:1234", "198.51.100.8", "https")
	if blocked.Code != 429 || blocked.Header().Get("Retry-After") == "" {
		t.Fatalf("proxy IP budget bypassed: %d %s", blocked.Code, blocked.Body.Bytes())
	}
	other := loginSecurityRequest(handler, "10.1.2.3:1234", "198.51.100.9", "https")
	if other.Code == 429 {
		t.Fatal("different trusted client inherited first client's IP bucket")
	}
}
func TestLoginForgedProxyHeadersConcurrentBudgetIntegration(t *testing.T) {
	_, handler := loginSecurityFixture(t)
	var accepted atomic.Int32
	var limited atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := loginSecurityRequest(handler, "192.0.2.1:4321", "198.51.100."+strconv.Itoa(10+i), "https")
			if w.Code == 429 {
				limited.Add(1)
				return
			}
			accepted.Add(1)
			for _, cookie := range w.Result().Cookies() {
				if cookie.Secure {
					t.Error("untrusted forwarded proto changed Cookie policy")
				}
			}
		}(i)
	}
	wg.Wait()
	if accepted.Load() != 2 || limited.Load() != 10 {
		t.Fatalf("spoofed XFF bypassed limit: accepted=%d limited=%d", accepted.Load(), limited.Load())
	}
}
