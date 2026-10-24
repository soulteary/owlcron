package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNotificationSpoolCorruptionDoesNotBlockRecovery(t *testing.T) {
	e := jobTestEngine(t)
	dir := filepath.Join(e.config.QueueDir, "notifications")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Both an unterminated large JSON string and a syntactically valid oversized
	// payload must be bounded and quarantined while later records are replayed.
	for name, raw := range map[string]string{
		"a.json": `{"id":"bad","kind":"email","payload":"` + strings.Repeat("x", 300<<10),
		"b.json": `{"id":"trailing","kind":"email","payload":{}} {}`,
		"z.json": `{"id":"good","kind":"webhook","payload":{"body":{}}}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.replayNotificationSpool(context.Background()); err == nil {
		t.Fatal("corrupt records not reported")
	}
	if _, err := e.notifications.Get(context.Background(), "good"); err != nil {
		t.Fatal("valid notification blocked", err)
	}
	for _, name := range []string{"a.json.invalid", "b.json.invalid"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.replayNotificationSpool(context.Background()); err != nil {
		t.Fatal("quarantine blocked subsequent pass", err)
	}
}
