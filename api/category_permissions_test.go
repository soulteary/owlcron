package api

import (
	"bytes"
	"encoding/json"
	"github.com/soulteary/owlcron/types"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDelegatedCategoryPermissions(t *testing.T) {
	e, h := securityServer(t)
	p := types.Privileges{"create_categories": true, "update_categories": true, "delete_categories": true, "cat_limit": true, "cat_allowed": true}
	session := addSecurityUser(t, e, "category-editor", p)
	for _, tc := range []struct {
		route, body string
		allow       bool
	}{
		{"create_category", `{"id":"denied","title":"Denied"}`, false},
		{"create_category", `{"title":"Generated"}`, false},
		{"create_category", `{"id":"allowed","title":"Allowed"}`, true},
		{"create_category", `{"id":"allowed","title":"Duplicate"}`, false},
		{"update_category", `{"id":"allowed","title":"Updated"}`, true},
		{"update_category?id=allowed", `{"id":"denied","title":"Denied"}`, false},
		{"delete_category", `{"id":"allowed"}`, true},
	} {
		resp := securityRequest(t, h, "/api/app/"+tc.route, tc.body, session)
		if (resp["code"] == float64(0)) != tc.allow {
			t.Fatalf("%s: %#v", tc.route, resp)
		}
	}
	if err := e.SaveAPIKeyToStorage(&types.APIKey{ID: "category-key", Key: "category-secret", Enabled: true, Privileges: p}); err != nil {
		t.Fatal(err)
	}
	request := func(route, body string) map[string]interface{} {
		r := httptest.NewRequest(http.MethodPost, "/api/app/"+route+"/v1", bytes.NewBufferString(body))
		r.Header.Set("X-API-Key", "category-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := request("create_category", `{"id":"allowed","title":"Key-created"}`); resp["code"] != float64(0) {
		t.Fatal(resp)
	}
	if resp := request("delete_category", `{"id":"denied"}`); resp["code"] == float64(0) {
		t.Fatal("API key escaped category scope")
	}
	regular := addSecurityUser(t, e, "no-category-rights", types.Privileges{})
	if resp := securityRequest(t, h, "/api/app/delete_category", `{"id":"allowed"}`, regular); resp["code"] == float64(0) {
		t.Fatal("missing operation privilege accepted")
	}
	if err := e.GetScheduler().AddEvent(&types.Event{ID: "assigned", Title: "Assigned", Category: "allowed", Plugin: "shell"}); err != nil {
		t.Fatal(err)
	}
	if resp := request("delete_category", `{"id":"allowed"}`); resp["code"] == float64(0) {
		t.Fatal("in-use category deleted")
	}
}
