package api

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func requestSecurityServer(t *testing.T, force bool) *Server {
	t.Helper()
	eng, err := engine.NewEngine(&types.Config{StorageBaseDir: t.TempDir(), LogDir: t.TempDir(), QueueDir: t.TempDir(), TrustedProxyCIDRs: []string{"10.0.0.0/8", "2001:db8:1::/48"}, SessionCookieSecure: force}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Shutdown() })
	return NewServer(eng, zap.NewNop())
}

func TestClientIPTrustBoundary(t *testing.T) {
	s := requestSecurityServer(t, false)
	cases := []struct {
		name, peer, xff, want string
		duplicate             bool
	}{
		{"direct spoof", "198.51.100.1:1200", "203.0.113.9", "198.51.100.1", false},
		{"trusted proxy", "10.0.0.1:1200", "203.0.113.9", "203.0.113.9", false},
		{"trusted chain", "10.0.0.1:1200", "203.0.113.9, 10.0.0.2", "203.0.113.9", false},
		{"untrusted intermediary", "10.0.0.1:1200", "1.1.1.1, 198.51.100.2, 10.0.0.2", "198.51.100.2", false},
		{"malformed left", "10.0.0.1:1200", "garbage, 198.51.100.2", "10.0.0.1", false},
		{"IPv6 chain", "[2001:db8:1::1]:1200", "2001:db8:2::1, 2001:db8:1::2", "2001:db8:2::1", false},
		{"IPv6 zone", "10.0.0.1:1200", "fe80::1%en0", "10.0.0.1", false},
		{"IPv6 port invalid", "10.0.0.1:1200", "[2001:db8:2::1]:123", "10.0.0.1", false},
		{"duplicate header", "10.0.0.1:1200", "203.0.113.9", "10.0.0.1", true},
		{"mapped IPv4", "10.0.0.1:1200", "::ffff:203.0.113.9", "203.0.113.9", false},
		{"oversized chain", "10.0.0.1:1200", strings.Repeat("1.1.1.1,", 33) + "1.1.1.1", "10.0.0.1", false},
		{"unknown peer", "garbage", "1.1.1.1", "unknown", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.peer
			r.Header.Set("X-Forwarded-For", tc.xff)
			if tc.duplicate {
				r.Header.Add("X-Forwarded-For", "1.1.1.1")
			}
			if got := s.clientIP(r); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestSecureCookieProxyBoundaryAndDeletion(t *testing.T) {
	s := requestSecurityServer(t, false)
	cases := []struct {
		name, peer, proto      string
		tls, secure, duplicate bool
	}{
		{"direct TLS", "198.51.100.1:123", "", true, true, false},
		{"spoofed HTTPS", "198.51.100.1:123", "https", false, false, false},
		{"proxy HTTPS", "10.0.0.1:123", "https", false, true, false},
		{"ambiguous HTTPS", "10.0.0.1:123", "https,http", false, false, false},
		{"duplicate HTTPS", "10.0.0.1:123", "https", false, false, true},
		{"HTTP", "10.0.0.1:123", "http", false, false, false},
		{"invalid scheme", "10.0.0.1:123", "HTTPS", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.peer
			r.Header.Set("X-Forwarded-Proto", tc.proto)
			if tc.duplicate {
				r.Header.Add("X-Forwarded-Proto", "https")
			}
			if tc.tls {
				r.TLS = &tls.ConnectionState{}
			}
			for _, value := range []string{"session-value", ""} {
				w := httptest.NewRecorder()
				s.setSessionCookie(w, r, value)
				cookies := w.Result().Cookies()
				if len(cookies) != 1 {
					t.Fatal(cookies)
				}
				c := cookies[0]
				if c.Secure != tc.secure || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || c.Path != "/" {
					t.Fatalf("unsafe cookie %+v", c)
				}
				if value == "" {
					if c.MaxAge != -1 || !c.Expires.Before(time.Now()) {
						t.Fatal("deletion expiry", c)
					}
				} else if c.MaxAge <= 0 || !c.Expires.After(time.Now()) {
					t.Fatal("session expiry", c)
				}
			}
		})
	}
	forced := requestSecurityServer(t, true)
	if !forced.secureCookie(httptest.NewRequest("GET", "/", nil)) {
		t.Fatal("forced HTTPS ignored")
	}
}

func TestProxyPolicyConcurrentRequests(t *testing.T) {
	s := requestSecurityServer(t, false)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				r := httptest.NewRequest("GET", "/", nil)
				r.RemoteAddr = "10.0.0.1:123"
				r.Header.Set("X-Forwarded-For", "203.0.113.9,10.0.0.2")
				r.Header.Set("X-Forwarded-Proto", "https")
				if s.clientIP(r) != "203.0.113.9" || !s.secureCookie(r) {
					t.Error("inconsistent concurrent proxy policy")
				}
			}
		}()
	}
	wg.Wait()
}

func TestValidateTrustedProxyCIDRs(t *testing.T) {
	for _, raw := range []string{"", "localhost", "10.0.0.1", "10.0.0.0/99", "fe80::1%en0/64", "::ffff:10.0.0.0/104"} {
		if types.ValidateHTTPConfig(&types.Config{TrustedProxyCIDRs: []string{raw}}) == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	if err := types.ValidateHTTPConfig(&types.Config{TrustedProxyCIDRs: []string{"10.0.0.0/8", "2001:db8::/32"}}); err != nil {
		t.Fatal(err)
	}
}
