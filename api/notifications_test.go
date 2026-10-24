package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestNotificationOperationsRequireAdminAndRedactPayload(t *testing.T) {
	e, handler := securityServer(t)
	admin := addSecurityUser(t, e, "notifyadmin", types.Privileges{"admin": true})
	user := addSecurityUser(t, e, "notifyreader", types.Privileges{})
	// Seed a dead record using the production identity key.
	raw := []byte(`{"id":"dead-notification","kind":"email","state":"dead","attempts":8,"payload":{"password":"secret-body"},"lease_token":"private-claim"}`)
	sum := sha256.Sum256([]byte("dead-notification"))
	key := "notifications/" + hex.EncodeToString(sum[:])
	if err := e.GetStorage().Put(context.Background(), key, raw); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"get_notifications", "retry_notification"} {
		response := securityRequest(t, handler, "/api/app/"+endpoint, `{"id":"dead-notification"}`, user)
		if response["code"] == float64(0) {
			t.Fatalf("nonadmin allowed %s", endpoint)
		}
	}
	response := securityRequest(t, handler, "/api/app/get_notifications", `{"state":"dead"}`, admin)
	if response["code"] != float64(0) {
		t.Fatalf("admin listing failed: %#v", response)
	}
	encoded, _ := json.Marshal(response)
	if strings.Contains(string(encoded), "secret-body") || strings.Contains(string(encoded), "private-claim") {
		t.Fatalf("payload leaked %s", encoded)
	}
	rows, ok := response["rows"].([]interface{})
	if !ok || len(rows) != 1 {
		t.Fatalf("missing dead record: %#v", response)
	}
	retry := securityRequest(t, handler, "/api/app/retry_notification", `{"id":"dead-notification"}`, admin)
	if retry["code"] != float64(0) {
		t.Fatalf("admin replay rejected: %#v", retry)
	}
}

func TestNotificationRetryRequiresPost(t *testing.T) {
	e, handler := securityServer(t)
	admin := addSecurityUser(t, e, "notifypostadmin", types.Privileges{"admin": true})
	request := httptest.NewRequest(http.MethodGet, "/api/app/retry_notification", nil)
	request.Header.Set("X-Session-ID", admin)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var response map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["code"] == float64(0) {
		t.Fatal("GET mutation accepted")
	}
}
