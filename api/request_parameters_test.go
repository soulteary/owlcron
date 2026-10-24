package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestRequestParametersRejectAmbiguousSelectors(t *testing.T) {
	for _, tc := range []struct{ name, query, body string }{
		{"conflicting id", "?id=allowed", `{"id":"private"}`},
		{"conflicting category", "?category=allowed", `{"category":"private"}`},
		{"conflicting target", "?target=allowed", `{"target":"private"}`},
		{"array shadowed by query", "?ids=allowed", `{"ids":["private"]}`},
		{"query duplicates", "?id=allowed&id=private", `{}`},
		{"duplicate JSON id", "", `{"id":"private","id":null}`},
		{"duplicate JSON ids", "", `{"ids":["private"],"ids":null}`},
		{"case insensitive id", "", `{"ID":"private"}`},
		{"case insensitive category", "", `{"Category":"private"}`},
		{"case insensitive target", "", `{"TARGET":"private"}`},
		{"unicode case fold", "", `{"idſ":["private"]}`},
		{"query case", "?ID=private", `{}`},
		{"null object", "?id=private", `null`},
		{"multiple objects", "", `{} {"id":"private"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/"+tc.query, strings.NewReader(tc.body))
			if _, err := requestParameters(r); err == nil {
				t.Fatal("ambiguous request accepted")
			}
		})
	}
}

func TestRequestParametersPreserveCompatibleInputs(t *testing.T) {
	for _, tc := range []struct {
		name, method, query, body string
		want                      map[string]interface{}
	}{
		{"query GET", http.MethodGet, "?id=allowed", "", map[string]interface{}{"id": "allowed"}},
		{"query POST", http.MethodPost, "?id=allowed", "", map[string]interface{}{"id": "allowed"}},
		{"equal duplicate scalars", http.MethodPost, "?id=allowed&id=allowed&limit=10&active=true", `{"id":"allowed","limit":10,"active":true}`, map[string]interface{}{"id": "allowed", "limit": float64(10), "active": true}},
		{"JSON array and separate credential", http.MethodPost, "?api_key=secret", `{"ids":["one","two"],"updates":{"timeout":10}}`, map[string]interface{}{"api_key": "secret", "ids": []interface{}{"one", "two"}, "updates": map[string]interface{}{"timeout": float64(10)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "/"+tc.query, strings.NewReader(tc.body))
			for i := 0; i < 2; i++ {
				got, err := requestParameters(r)
				if err != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("parameters = %#v, %v; want %#v", got, err, tc.want)
				}
			}
			raw, err := io.ReadAll(r.Body)
			if err != nil || string(raw) != tc.body {
				t.Fatalf("handler body changed: %q, %v", raw, err)
			}
		})
	}
}

func TestScopedMutationSelectorsAgreeWithHandler(t *testing.T) {
	eng, handler := securityServer(t)
	if err := eng.GetJobManager().SavePluginToStorage(&types.Plugin{ID: "shell", Title: "Shell", Enabled: true, Command: "/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveServerGroupToStorage(&types.ServerGroup{ID: "allowed", Title: "Allowed", Regexp: "^allowed-host$"}); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"category", "group"} {
		p := types.Privileges{"create_events": true, "delete_events": true, "edit_events": true, "abort_events": true}
		if scope == "category" {
			p["cat_limit"], p["cat_allowed"] = true, true
		} else {
			p["grp_limit"], p["grp_allowed"] = true, true
		}
		session := addSecurityUser(t, eng, scope, p)
		secret := scope + "-secret"
		if err := eng.SaveAPIKeyToStorage(&types.APIKey{ID: scope + "-key", Key: secret, Enabled: true, Privileges: p}); err != nil {
			t.Fatal(err)
		}
		for _, auth := range []string{"session", "key"} {
			for _, suffix := range []string{"", "/v1"} {
				t.Run(scope+"/"+auth+suffix, func(t *testing.T) {
					request := func(route, query, body string) map[string]interface{} {
						t.Helper()
						r := httptest.NewRequest(http.MethodPost, "/api/app/"+route+suffix+query, bytes.NewBufferString(body))
						if auth == "session" {
							r.Header.Set("X-Session-ID", session)
						} else {
							r.Header.Set("X-API-Key", secret)
						}
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						var response map[string]interface{}
						if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
							t.Fatal(err)
						}
						return response
					}
					for _, event := range []*types.Event{
						{ID: "visible", Title: "Visible", Category: "allowed", Target: "allowed-host", Plugin: "shell"},
						{ID: "private", Title: "Private", Category: "private", Target: "private-host", Plugin: "shell"},
					} {
						if err := eng.GetScheduler().AddEvent(event); err != nil {
							t.Fatal(err)
						}
					}
					for _, job := range []*types.Job{
						{ID: "visible-job", Category: "allowed", Target: "allowed-host"},
						{ID: "private-job", Category: "private", Target: "private-host"},
					} {
						raw, _ := json.Marshal(job)
						if err := eng.GetStorage().Put(context.Background(), "jobs:"+job.ID, raw); err != nil {
							t.Fatal(err)
						}
					}
					selector, allowed := "category", "allowed"
					if scope == "group" {
						selector, allowed = "target", "allowed-host"
					}
					for _, attack := range []struct{ route, query, body string }{
						{"create_event", "?" + selector + "=" + allowed, `{"id":"injected","title":"Injected","plugin":"shell","category":"private","target":"private-host"}`},
						{"create_event", "?" + selector + "=" + allowed, `{"id":"injected","title":"Injected","plugin":"shell"}`},
						{"create_event", "", `{"id":"injected","title":"Injected","plugin":"shell","Category":"private","Target":"private-host"}`},
						{"delete_event", "?id=visible", `{"id":"private"}`},
						{"abort_event", "?id=visible", `{"id":"private"}`},
						{"delete_event", "", `{"ID":"private"}`},
						{"delete_event", "", `{"id":"private","id":null}`},
						{"abort_job", "?id=visible-job", `{"id":"private-job"}`},
						{"abort_jobs", "?ids=visible-job", `{"ids":["private-job"]}`},
					} {
						resp := request(attack.route, attack.query, attack.body)
						if resp["code"] == float64(0) {
							t.Fatalf("%s accepted ambiguous selector: %#v", attack.route, resp)
						}
						if attack.route == "abort_job" && !strings.Contains(resp["description"].(string), "conflicting request parameter") {
							t.Fatalf("abort reached handler: %#v", resp)
						}
					}
					if _, ok := eng.GetScheduler().GetEvent("injected"); ok {
						t.Fatal("created inaccessible event")
					}
					if _, ok := eng.GetScheduler().GetEvent("private"); !ok {
						t.Fatal("deleted inaccessible event")
					}
					if resp := request("create_event", "", `{"id":"valid","title":"Valid","plugin":"shell","category":"allowed","target":"allowed-host"}`); resp["code"] != float64(0) {
						t.Fatalf("valid create failed: %#v", resp)
					}
					if resp := request("update_event", "?id=valid&title=Updated", `{"enabled":1}`); resp["code"] != float64(0) {
						t.Fatalf("query-based update failed: %#v", resp)
					}
					if event, _ := eng.GetScheduler().GetEvent("valid"); event.Title != "Updated" || !event.Enabled.Bool() {
						t.Fatal("query-based update lost its JSON or query fields")
					}
					if resp := request("get_event", "?id=valid", `{}`); resp["code"] != float64(0) {
						t.Fatalf("query-based read failed: %#v", resp)
					}
					if resp := request("delete_event", "?id=valid", `{"id":"valid"}`); resp["code"] != float64(0) {
						t.Fatalf("equal selector rejected: %#v", resp)
					}
				})
			}
		}
	}
}
