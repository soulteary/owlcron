package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadinessHTTPStatusAndPublicAliases(t *testing.T) {
	eng, handler := securityServer(t)
	for _, path := range []string{"/api/app/ready", "/api/app/ready/v1"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"code":0`) {
			t.Fatalf("ready %s: %d %s", path, w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("health response can be cached")
		}
	}
	eng.RequestShutdown()
	for _, path := range []string{"/api/app/ready", "/api/app/ready/v1"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusServiceUnavailable || w.Body.String() != "{\"code\":1,\"description\":\"Service unavailable\"}\n" {
			t.Fatalf("draining %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/app/ping", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("liveness returned %d", w.Code)
	}
}
