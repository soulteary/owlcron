package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestMetricsAuthenticationAndFormat(t *testing.T) {
	eng, handler := securityServer(t)
	normal := addSecurityUser(t, eng, "normal-metrics", types.Privileges{})
	collector := addSecurityUser(t, eng, "collector", types.Privileges{"view_metrics": true})
	admin := addSecurityUser(t, eng, "admin-metrics", types.Privileges{"admin": true})
	key := &types.APIKey{ID: "collector-key", Key: "metrics-secret-not-a-label", Enabled: true, Privileges: types.Privileges{"view_metrics": true}}
	if err := eng.SaveAPIKeyToStorage(key); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, method, path, session, secret string
		want                                int
	}{
		{"anonymous", "GET", "/api/app/metrics", "", "", 401},
		{"invalid key", "GET", "/api/app/metrics", "", "invalid", 401},
		{"restricted user", "GET", "/api/app/metrics", normal, "", 403},
		{"collector", "GET", "/api/app/metrics", collector, "", 200},
		{"admin", "GET", "/api/app/metrics/v1", admin, "", 200},
		{"key", "GET", "/api/app/metrics", "", key.Key, 200},
		{"method", "POST", "/api/app/metrics", collector, "", 405},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, nil)
			r.Header.Set("X-Session-ID", tt.session)
			r.Header.Set("X-API-Key", tt.secret)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tt.want {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if tt.want != 200 {
				if strings.Contains(w.Body.String(), "owlcron_") {
					t.Fatal("unauthorized metric disclosure")
				}
				return
			}
			if w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("invalid scrape headers")
			}
			lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
			if len(lines) != 21 {
				t.Fatalf("unexpected metric count: %d", len(lines))
			}
			for i := 0; i < len(lines); i += 3 {
				fields := strings.Fields(lines[i+2])
				if len(fields) != 2 {
					t.Fatalf("invalid sample: %q", lines[i+2])
				}
				if !strings.HasPrefix(lines[i], "# HELP "+fields[0]+" ") || lines[i+1] != "# TYPE "+fields[0]+" gauge" {
					t.Fatalf("invalid metadata: %q", lines[i:i+3])
				}
				if _, err := strconv.ParseUint(fields[1], 10, 64); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(fields[0], "{") {
					t.Fatal("unexpected labels")
				}
			}
			if strings.Contains(w.Body.String(), key.Key) {
				t.Fatal("credential leaked")
			}
		})
	}
	key.Enabled = false
	if err := eng.SaveAPIKeyToStorage(key); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/api/app/metrics", nil)
	r.Header.Set("X-API-Key", key.Key)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("disabled key returned %d", w.Code)
	}
}
