package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestAccountPasswordChangeNoticeHasNoCredentials(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	if err := e.RequestPasswordRecovery(ctx, user.Username, user.Email); err != nil {
		t.Fatal(err)
	}
	token := actionToken(t, e, user.ID, resetPasswordAction)
	password := "new-password-for-notice"
	if err := e.ConsumePasswordRecovery(ctx, user.ID, token, password); err != nil {
		t.Fatal(err)
	}
	rows, err := e.notifications.List(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Kind != "email" {
			continue
		}
		var mail EmailPayload
		if err := json.Unmarshal(row.Payload, &mail); err != nil {
			t.Fatal(err)
		}
		if mail.Subject != "OwlCron password changed" {
			continue
		}
		found = true
		if !strings.Contains(mail.Body, "All previous sessions and reset links are invalid") {
			t.Fatal("missing revocation notice")
		}
		if strings.Contains(string(row.Payload), password) || strings.Contains(string(row.Payload), token) {
			t.Fatal("notice contains credentials")
		}
	}
	if !found {
		t.Fatal("password-change notice not queued")
	}
}
