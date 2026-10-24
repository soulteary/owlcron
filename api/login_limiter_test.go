package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

func TestLoginLimiterAccountAcrossIPsConcurrent(t *testing.T) {
	l := newLoginLimiter(&types.Config{LoginMaxConcurrent: 100})
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			release, retry := l.acquire(fmt.Sprint(i), "victim")
			if retry == 0 {
				allowed.Add(1)
				release()
			}
		}(i)
	}
	wg.Wait()
	if allowed.Load() != 10 {
		t.Fatalf("allowed %d distributed attempts", allowed.Load())
	}
}

func TestLoginLimiterIPAcrossAccountsConcurrent(t *testing.T) {
	l := newLoginLimiter(&types.Config{})
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.allowIP("192.0.2.1") == 0 {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 20 {
		t.Fatalf("allowed %d attempts", allowed.Load())
	}
}

func TestLoginLimiterCapacityNeverEvictsActiveVictimAndExpires(t *testing.T) {
	l := newLoginLimiter(&types.Config{LoginLimiterMaxEntries: 4, LoginAccountRateLimit: 1})
	now := time.Unix(100, 0)
	l.now = func() time.Time { return now }
	release, retry := l.acquire("first", "victim")
	if retry != 0 {
		t.Fatal(retry)
	}
	release()
	release, retry = l.acquire("second", "other")
	if retry != 0 {
		t.Fatal(retry)
	}
	release()
	for i := 0; i < 100; i++ {
		if _, retry := l.acquire(fmt.Sprint(i), fmt.Sprint(i)); retry == 0 {
			t.Fatal("capacity allowed new bucket")
		}
	}
	if _, retry := l.acquire("first", "victim"); retry != time.Minute {
		t.Fatalf("victim counter reset: %s", retry)
	}
	if len(l.buckets) != 4 || len(l.expiry) != 4 {
		t.Fatal("limiter exceeded bounded capacity")
	}
	for key := range l.buckets {
		if strings.Contains(key, "victim") {
			t.Fatal("raw account stored")
		}
	}
	now = now.Add(time.Minute)
	release, retry = l.acquire("third", "new")
	if retry != 0 {
		t.Fatal("expiry failed", retry)
	}
	release()
	if len(l.buckets) != 2 {
		t.Fatal("expired buckets not cleaned")
	}
}

func TestLoginLimiterConcurrencyAndRelease(t *testing.T) {
	l := newLoginLimiter(&types.Config{LoginMaxConcurrent: 1})
	release, retry := l.acquire("a", "one")
	if retry != 0 {
		t.Fatal(retry)
	}
	if _, retry := l.acquire("b", "two"); retry != time.Second {
		t.Fatal("concurrent password work admitted")
	}
	release()
	release() // Safe even when early release also has a deferred release.
	release, retry = l.acquire("b", "two")
	if retry != 0 {
		t.Fatal("capacity did not recover", retry)
	}
	release()
}

func loginTestServer(t *testing.T, config *types.Config) *Server {
	t.Helper()
	config.SecretKey = "0123456789abcdef0123456789abcdef"
	config.StorageBaseDir, config.LogDir, config.QueueDir = t.TempDir(), t.TempDir(), t.TempDir()
	eng, err := engine.NewEngine(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Shutdown() })
	hash, err := bcrypt.GenerateFromPassword([]byte("test-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveUserToStorage(&types.User{ID: "victim", Username: "victim", Enabled: true, Password: string(hash)}); err != nil {
		t.Fatal(err)
	}
	return NewServer(eng, zap.NewNop())
}

func TestLoginLimitsSharedWithBasicAuthAndVersionedRoutes(t *testing.T) {
	s := loginTestServer(t, &types.Config{LoginAccountRateLimit: 2})
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	// Production wraps the routes again. One attempt must count only once.
	handler := s.AuthenticationMiddleware(mux)
	for i := 0; i < 3; i++ {
		var r *http.Request
		if i == 0 {
			r = httptest.NewRequest(http.MethodPost, "/api/user/login/v1", strings.NewReader(`{"username":"victim","password":"wrong"}`))
		} else {
			r = httptest.NewRequest(http.MethodGet, "/api/app/get_schedule", nil)
			r.SetBasicAuth("victim", "wrong")
		}
		r.RemoteAddr = fmt.Sprintf("192.0.2.%d:5000", i+1)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if i < 2 && w.Code == 429 {
			t.Fatal("attempt counted twice")
		}
		if i == 2 && (w.Code != 429 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), `"code":1`)) {
			t.Fatalf("missing retry response: %d %s %s", w.Code, w.Header(), w.Body)
		}
	}
}

func TestLoginMalformedBodiesConsumeIPBudget(t *testing.T) {
	s := loginTestServer(t, &types.Config{LoginRateLimit: 2})
	for i, body := range []string{strings.Repeat("x", 5000), `{} {}`, `{}`} {
		r := httptest.NewRequest(http.MethodPost, "/api/user/login", strings.NewReader(body))
		w := httptest.NewRecorder()
		s.handleUserLogin(w, r)
		if i == 2 && w.Code != 429 {
			t.Fatalf("malformed requests bypassed budget: %d", w.Code)
		}
	}
	if len(s.loginLimiter.slots) != 0 {
		t.Fatal("malformed body occupied password slots")
	}
}

func TestBasicAuthNestedMiddlewareCountsSuccessfulAttemptOnce(t *testing.T) {
	s := loginTestServer(t, &types.Config{LoginAccountRateLimit: 2})
	var admitted int
	handler := s.AuthenticationMiddleware(s.AuthenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if GetAuthContext(r) == nil {
			t.Error("missing authentication context")
		}
		admitted++
		w.WriteHeader(http.StatusNoContent)
	})))
	for i := 0; i < 3; i++ {
		r := httptest.NewRequest(http.MethodGet, "/api/app/get_schedule", nil)
		r.RemoteAddr = fmt.Sprintf("192.0.2.%d:5000", i+1)
		r.SetBasicAuth("victim", "test-password")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if i < 2 && w.Code != http.StatusNoContent {
			t.Fatalf("successful attempt counted twice: %d", w.Code)
		}
		if i == 2 && w.Code != http.StatusTooManyRequests {
			t.Fatal("Basic Auth successful attempts bypassed rate limit")
		}
	}
	if admitted != 2 {
		t.Fatalf("admitted %d", admitted)
	}
}

func TestLoginDisabledAndUnknownReturnSameError(t *testing.T) {
	s := loginTestServer(t, &types.Config{})
	user, err := s.engine.LoadUserByUsernameFromStorage("victim")
	if err != nil {
		t.Fatal(err)
	}
	user.Enabled = false
	if err := s.engine.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	var previous string
	for _, username := range []string{"victim", "missing"} {
		r := httptest.NewRequest(http.MethodPost, "/api/user/login", strings.NewReader(fmt.Sprintf(`{"username":%q,"password":"test-password"}`, username)))
		w := httptest.NewRecorder()
		s.handleUserLogin(w, r)
		if previous != "" && w.Body.String() != previous {
			t.Fatal("account state disclosed")
		}
		previous = w.Body.String()
		if len(s.loginLimiter.slots) != 0 {
			t.Fatal("failed login leaked password slot")
		}
	}
}
