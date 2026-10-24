package api

import (
	"context"
	"encoding/json"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCronicleHistoryDetailsAndLogPOSTContract(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "historyadmin", types.Privileges{"admin": true})
	for idx, id := range []string{"jfirst", "jsecond"} {
		job := &types.Job{ID: id, Event: "event", EventTitle: "Event", Complete: true, Code: 0, TimeStart: float64(idx + 1), TimeEnd: float64(idx + 2), Progress: 1}
		data, _ := json.Marshal(job)
		if err := eng.GetStorage().Put(context.Background(), "jobs:"+id, data); err != nil {
			t.Fatal(err)
		}
		if err := storage.HistoryUpsert(context.Background(), eng.GetStorage(), job); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.GetStorage().Put(context.Background(), "job_logs:jsecond", []byte("<html>plain log</html>\n")); err != nil {
		t.Fatal(err)
	}
	history := securityRequest(t, handler, "/api/app/get_history", `{"offset":1,"limit":1}`, session)
	rows, ok := history["rows"].([]interface{})
	if !ok || len(rows) != 1 || rows[0].(map[string]interface{})["id"] != "jfirst" || history["list"].(map[string]interface{})["length"] != float64(2) {
		t.Fatalf("history POST pagination contract: %#v", history)
	}
	if rows[0].(map[string]interface{})["action"] != "job_complete" {
		t.Fatal("legacy history row missing completed action")
	}
	eventHistory := securityRequest(t, handler, "/api/app/get_event_history", `{"id":"event","offset":0,"limit":1}`, session)
	if eventHistory["code"] != float64(0) || len(eventHistory["rows"].([]interface{})) != 1 {
		t.Fatalf("event history POST contract: %#v", eventHistory)
	}
	details := securityRequest(t, handler, "/api/app/get_job_details", `{"id":"jsecond"}`, session)
	if details["code"] != float64(0) || details["job"].(map[string]interface{})["id"] != "jsecond" || details["token"] == "" {
		t.Fatalf("restart details contract: %#v", details)
	}
	log := securityRequest(t, handler, "/api/app/get_job_log", `{"id":"jsecond"}`, session)
	if log["log"] != "<html>plain log</html>\n" {
		t.Fatalf("POST log contract: %#v", log)
	}
	r := httptest.NewRequest(http.MethodGet, "/api/app/get_job_log?id=jsecond&t="+details["token"].(string), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") || w.Body.String() != "<html>plain log</html>\n" {
		t.Fatalf("GET log must be safe plain text: %d %s", w.Code, w.Body.String())
	}
	deleted := securityRequest(t, handler, "/api/app/delete_job", `{"id":"jsecond"}`, session)
	if deleted["code"] != float64(0) {
		t.Fatalf("delete job: %#v", deleted)
	}
	if details := securityRequest(t, handler, "/api/app/get_job_details", `{"id":"jsecond"}`, session); details["code"] == float64(0) {
		t.Fatal("persistent deleted job reappeared")
	}
}

func TestConfigurationPOSTSparseUpdatesAndIndexes(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "configadmin", types.Privileges{"admin": true})
	requests := []struct{ create, update, get, list, remove, body, field string }{
		{"create_plugin", "update_plugin", "get_plugin", "get_plugins", "delete_plugin", `{"id":"pcontrol","title":"Plugin","enabled":1,"command":"/bin/sh","params":[{"id":"choice","type":"select","items":["a","b"],"value":"a"}]}`, "plugin"},
		{"create_category", "update_category", "get_category", "get_categories", "delete_category", `{"id":"ccontrol","title":"Category","enabled":1,"color":"blue","max_children":3}`, "category"},
		{"create_server_group", "update_server_group", "get_server_group", "list_server_groups", "delete_server_group", `{"id":"gcontrol","title":"Group","master":1,"regexp":".*"}`, "group"},
	}
	for _, tc := range requests {
		created := securityRequest(t, handler, "/api/app/"+tc.create, tc.body, session)
		if created["code"] != float64(0) || created["id"] == "" {
			t.Fatalf("create %s: %#v", tc.create, created)
		}
		id := created["id"].(string)
		updated := securityRequest(t, handler, "/api/app/"+tc.update, `{"id":"`+id+`","title":"Changed"}`, session)
		if updated["code"] != float64(0) {
			t.Fatalf("sparse %s: %#v", tc.update, updated)
		}
		got := securityRequest(t, handler, "/api/app/"+tc.get, `{"id":"`+id+`"}`, session)
		record, ok := got[tc.field].(map[string]interface{})
		if !ok || record["title"] != "Changed" {
			t.Fatalf("POST get %s: %#v", tc.get, got)
		}
		if tc.field == "plugin" {
			params := record["params"].([]interface{})
			if record["command"] != "/bin/sh" || len(params[0].(map[string]interface{})["items"].([]interface{})) != 2 {
				t.Fatal("plugin control data was lost")
			}
		}
		if tc.field == "group" && record["master"] != true {
			t.Fatalf("numeric master flag lost: %#v", record)
		}
		listed := securityRequest(t, handler, "/api/app/"+tc.list, `{"offset":0,"limit":1}`, session)
		if listed["list"].(map[string]interface{})["length"] != float64(1) || len(listed["rows"].([]interface{})) != 1 {
			t.Fatalf("list %s: %#v", tc.list, listed)
		}
		deleted := securityRequest(t, handler, "/api/app/"+tc.remove, `{"id":"`+id+`"}`, session)
		if deleted["code"] != float64(0) {
			t.Fatalf("delete %s: %#v", tc.remove, deleted)
		}
		listed = securityRequest(t, handler, "/api/app/"+tc.list, `{}`, session)
		if len(listed["rows"].([]interface{})) != 0 {
			t.Fatalf("deleted record remained indexed: %s", tc.list)
		}
	}
}

func TestCronicleOnDemandFalseTimingContract(t *testing.T) {
	eng, handler := securityServer(t)
	if err := eng.GetJobManager().SavePluginToStorage(&types.Plugin{ID: "shell", Title: "Shell", Enabled: true, Command: "/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	session := addSecurityUser(t, eng, "ondemandadmin", types.Privileges{"admin": true})
	created := securityRequest(t, handler, "/api/app/create_event", `{"id":"ondemand","title":"On Demand","enabled":1,"plugin":"shell","target":"local","timing":false}`, session)
	if created["code"] != float64(0) {
		t.Fatalf("create on demand: %#v", created)
	}
	id := created["id"].(string)
	event, ok := eng.GetScheduler().GetEvent(id)
	if !ok || event.Timing != nil {
		t.Fatalf("on demand event acquired a schedule: %#v", event)
	}
	got := securityRequest(t, handler, "/api/app/get_event", `{"id":"`+id+`"}`, session)
	if got["code"] != float64(0) || got["event"].(map[string]interface{})["timing"] != nil {
		t.Fatalf("on demand get contract: %#v", got)
	}
	var invalid types.Event
	if err := json.Unmarshal([]byte(`{"timing":true}`), &invalid); err == nil {
		t.Fatal("true timing must be rejected")
	}
}

func TestCronicle135EventParametersMustMatchPluginSchema(t *testing.T) {
	eng, handler := securityServer(t)
	if err := eng.GetJobManager().SavePluginToStorage(&types.Plugin{ID: "schema", Title: "Schema", Enabled: true, Command: "/bin/sh", Params: []*types.PluginParam{{ID: "message", Type: "text"}}}); err != nil {
		t.Fatal(err)
	}
	session := addSecurityUser(t, eng, "schemaadmin", types.Privileges{"admin": true})
	bad := securityRequest(t, handler, "/api/app/create_event", `{"id":"badparams","title":"Bad","plugin":"schema","params":{"LD_PRELOAD":"evil"}}`, session)
	if bad["code"] == float64(0) {
		t.Fatal("unknown plugin parameter was persisted")
	}
	created := securityRequest(t, handler, "/api/app/create_event", `{"id":"validparams","title":"Valid","plugin":"schema","params":{"message":"hello"}}`, session)
	if created["code"] != float64(0) {
		t.Fatal(created)
	}
	id := created["id"].(string)
	for _, endpoint := range []string{"update_event", "run_event"} {
		got := securityRequest(t, handler, "/api/app/"+endpoint, `{"id":"`+id+`","params":{"unknown":"evil"}}`, session)
		if got["code"] == float64(0) {
			t.Fatalf("%s accepted unknown effective plugin parameter", endpoint)
		}
	}
}
