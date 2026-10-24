package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccountEmailPublicConfigurationKeepsRegistrationClosed(t *testing.T) {
	eng, _ := securityServer(t)
	cfg := eng.GetConfig()
	cfg.AccountEmailEnabled = true
	cfg.SMTPPassword = "must-not-be-public"
	cfg.AccountTokenTTLSeconds = 1800
	server := NewServer(eng, nil)
	recorder := httptest.NewRecorder()
	server.handleConfig(recorder, httptest.NewRequest(http.MethodGet, "/api/app/config", nil))
	body := recorder.Body.String()
	start, end := strings.Index(body, "{"), strings.LastIndex(body, "}")
	if start < 0 || end < start {
		t.Fatal("missing configuration response")
	}
	var response struct {
		Config map[string]interface{} `json:"config"`
	}
	if err := json.Unmarshal([]byte(body[start:end+1]), &response); err != nil {
		t.Fatal(err)
	}
	if response.Config["free_accounts"] != float64(0) || response.Config["account_email_enabled"] != true {
		t.Fatal("incorrect public account capabilities")
	}
	for _, secret := range []string{"must-not-be-public", "smtp_password", "account_token_ttl_seconds"} {
		if strings.Contains(body, secret) {
			t.Fatal("private policy exposed", secret)
		}
	}
}
