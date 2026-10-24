package main

import (
	"github.com/soulteary/owlcron/types"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasePathRouting(t *testing.T) {
	var got string
	h := serveBasePath("/tools/cron", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = r.URL.Path; w.WriteHeader(204) }))
	for _, tc := range []struct {
		path     string
		status   int
		upstream string
	}{{"/tools/cron/api/app/ping", 204, "/api/app/ping"}, {"/tools/cron/ws", 204, "/ws"}, {"/cluster/ws", 204, "/cluster/ws"}, {"/tools/cron", 307, ""}, {"/api/app/ping", 404, ""}, {"/tools/cronx/api", 404, ""}, {"/tools/cron/cluster/ws", 404, ""}, {"/tools/cron%2fapi/app/ping", 404, ""}} {
		got = ""
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status || got != tc.upstream {
			t.Fatalf("%s: %d %s", tc.path, w.Code, got)
		}
	}
}
func TestBasePathConfig(t *testing.T) {
	for _, prefix := range []string{"/cron/", "cron", "//cron", "/a/../b", "/a%2fb", "/cluster", "/a b"} {
		if types.ValidateHTTPConfig(&types.Config{BasePath: prefix}) == nil {
			t.Fatalf("accepted %q", prefix)
		}
	}
	if err := types.ValidateHTTPConfig(&types.Config{BasePath: "/tools/cron", BaseAppURL: "https://example.com/tools/cron"}); err != nil {
		t.Fatal(err)
	}
	if types.ValidateHTTPConfig(&types.Config{BasePath: "/cron", BaseAppURL: "https://example.com/other"}) == nil {
		t.Fatal("mismatched external URL accepted")
	}
}
