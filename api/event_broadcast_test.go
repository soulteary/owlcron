package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/soulteary/owlcron/types"
)

func TestEventMutationsBroadcastSchedule(t *testing.T) {
	e, handler := securityServer(t)
	if err := e.GetJobManager().SavePluginToStorage(&types.Plugin{ID: "shell", Title: "Shell", Enabled: true, Command: "/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	session := addSecurityUser(t, e, "browseradmin", types.Privileges{"admin": true})
	mux := handler.(*http.ServeMux)
	e.GetCommunication().RegisterRoutes(mux)
	server := httptest.NewServer(handler)
	defer server.Close()
	socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err = socket.WriteJSON(map[string]interface{}{"type": "auth", "data": map[string]string{"session_id": session}}); err != nil {
		t.Fatal(err)
	}
	readSchedule := func() []map[string]interface{} {
		t.Helper()
		_ = socket.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			var message struct {
				Type string                     `json:"type"`
				Data map[string]json.RawMessage `json:"data"`
			}
			if err := socket.ReadJSON(&message); err != nil {
				t.Fatal(err)
			}
			if raw, ok := message.Data["schedule"]; message.Type == "update" && ok {
				var events []map[string]interface{}
				if err := json.Unmarshal(raw, &events); err != nil {
					t.Fatal(err)
				}
				return events
			}
		}
	}
	if events := readSchedule(); len(events) != 0 {
		t.Fatal("fixture not empty")
	}
	created := securityRequest(t, handler, "/api/app/create_event", `{"id":"broadcaster","title":"Original","enabled":1,"plugin":"shell","target":"local","timing":false}`, session)
	if created["code"] != float64(0) {
		t.Fatalf("create: %#v", created)
	}
	if events := readSchedule(); len(events) != 1 || events[0]["title"] != "Original" {
		t.Fatalf("create not reflected: %#v", events)
	}
	updated := securityRequest(t, handler, "/api/app/update_event", `{"id":"broadcaster","title":"Changed"}`, session)
	if updated["code"] != float64(0) {
		t.Fatalf("update: %#v", updated)
	}
	if events := readSchedule(); len(events) != 1 || events[0]["title"] != "Changed" {
		t.Fatalf("update not reflected: %#v", events)
	}
	deleted := securityRequest(t, handler, "/api/app/delete_event", `{"id":"broadcaster"}`, session)
	if deleted["code"] != float64(0) {
		t.Fatalf("delete: %#v", deleted)
	}
	if events := readSchedule(); len(events) != 0 {
		t.Fatalf("delete not reflected: %#v", events)
	}
}
