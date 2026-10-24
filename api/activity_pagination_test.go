package api

import (
	"github.com/soulteary/owlcron/types"
	"testing"
	"time"
)

func TestActivityPageRetainsTotalAndReadsPostOffset(t *testing.T) {
	eng, handler := securityServer(t)
	if err := eng.SaveUserToStorage(&types.User{ID: "admin", Username: "admin", Enabled: true, Privileges: types.Privileges{"admin": true}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSessionToStorage(&types.Session{ID: "activity-session", UserID: "admin", Expires: time.Now().Unix() + 300}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"first", "second", "third"} {
		eng.LogActivity(action, map[string]interface{}{"username": "admin"})
	}
	got := securityRequest(t, handler, "/api/app/get_activity", `{"offset":1,"limit":1}`, "activity-session")
	if got["code"] != float64(0) {
		t.Fatal(got)
	}
	rows := got["rows"].([]interface{})
	if len(rows) != 1 || rows[0].(map[string]interface{})["action"] != "second" {
		t.Fatal(got)
	}
	if got["list"].(map[string]interface{})["length"] != float64(3) {
		t.Fatal(got)
	}
}
