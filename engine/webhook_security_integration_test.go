package engine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestWebHookSecuritySignedRedactedQueueAndLegacyRecovery(t *testing.T) {
	secret := strings.Repeat("independent-key-", 3)
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		for _, credential := range []string{secret, "cluster-private-material-0123456789", "smtp-private-material-0123456789", "plain-token-value"} {
			if strings.Contains(string(body), credential) {
				t.Errorf("credential escaped redaction: %s", body)
			}
		}
		timestamp, err := strconv.ParseInt(r.Header.Get("X-Owlcron-Timestamp"), 10, 64)
		if err != nil || time.Now().Unix()-timestamp > 60 || timestamp > time.Now().Unix()+1 {
			t.Error("invalid signature timestamp")
		}
		id := r.Header.Get("Idempotency-Key")
		canonical := strings.Join([]string{"v1", "key-2026", r.Header.Get("X-Owlcron-Timestamp"), id, r.Method, "http", r.Host, r.URL.RequestURI(), ""}, "\n")
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(canonical))
		mac.Write(body)
		expected := "v1=" + hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(expected), []byte(r.Header.Get("X-Owlcron-Signature"))) {
			t.Error("signature does not match delivered body and target")
		}
		if id == "" || r.Header.Get("X-Owlcron-Key-ID") != "key-2026" {
			t.Error("missing signature identity")
		}
		received.Add(1)
		w.WriteHeader(204)
	}))
	defer server.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "signing-key")
	if err := os.WriteFile(path, []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(&types.Config{StorageBaseDir: filepath.Join(dir, "data"), LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), SecretKey: "cluster-private-material-0123456789", SMTPPassword: "smtp-private-material-0123456789", SMTPUsername: "user", WebHookAllowedHosts: []string{"127.0.0.1"}, WebHookSigningKeyID: "key-2026", WebHookSigningSecretFile: path}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.cancel()
	data := map[string]interface{}{"id": "secure-job", "description": secret + " cluster-private-material-0123456789", "nested": map[string]interface{}{"access_token": "plain-token-value"}}
	if err = e.jobManager.webHookManager.enqueueWebHook(context.Background(), server.URL+"/callback?tenant=one", "job_complete", "secure-job", data); err != nil {
		t.Fatal(err)
	}
	records, err := e.notifications.List(context.Background(), "pending", 100)
	if err != nil || len(records) != 1 {
		t.Fatalf("admission %v %v", records, err)
	}
	if strings.Contains(string(records[0].Payload), secret) || strings.Contains(string(records[0].Payload), "plain-token-value") {
		t.Fatal("new queue persisted secrets")
	}
	// Bypass modern preparation to simulate a record created before redaction existed.
	legacy := WebHookPayload{URL: server.URL + "/legacy", Body: json.RawMessage(fmt.Sprintf(`{"description":%q,"password":"plain-token-value","large":9007199254740993}`, secret))}
	if err = e.notifications.Enqueue(context.Background(), "legacy-id", "webhook", legacy); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.notifications.RunOnce(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if received.Load() != 2 {
		t.Fatalf("concurrent delivery duplicated or lost records: %d", received.Load())
	}
	records, err = e.notifications.List(context.Background(), "sent", 100)
	if err != nil || len(records) != 2 {
		t.Fatalf("sent %v %v", records, err)
	}
}

func TestWebHookSecurityBlockedLegacyQueueBecomesDead(t *testing.T) {
	e := jobTestEngine(t)
	var reached atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Add(1) }))
	defer server.Close()
	if err := e.notifications.Enqueue(context.Background(), "unsafe-legacy", "webhook", WebHookPayload{URL: server.URL + "/?token=do-not-log", Body: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := e.notifications.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := e.notifications.Get(context.Background(), "unsafe-legacy")
	if err != nil {
		t.Fatal(err)
	}
	if record.State != "dead" || record.Attempts != 1 || reached.Load() != 0 {
		t.Fatalf("policy boundary failed: %+v reached=%d", record, reached.Load())
	}
	if strings.Contains(record.LastError, "do-not-log") || strings.Contains(record.LastError, "127.0.0.1") {
		t.Fatal("destination escaped into durable error")
	}
}

func TestNotificationErrorsRedactedBeforePersistenceAndRead(t *testing.T) {
	e := jobTestEngine(t)
	secret := e.config.SecretKey
	e.notifications.deliver = func(context.Context, *Notification) error {
		return &NotificationDeliveryError{Err: fmt.Errorf("failure secret_key=%s url=https://example.test/?token=unknown-value", secret), Permanent: true}
	}
	if err := e.notifications.Enqueue(context.Background(), "redact-errors", "email", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if err := e.notifications.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := e.notifications.Get(context.Background(), "redact-errors")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(record.LastError, secret) || strings.Contains(record.LastError, "unknown-value") {
		t.Fatal("durable diagnostic leaked credentials")
	}
	// Older diagnostic records are cleaned on reads as well.
	record.LastError = "Bearer legacy-token-value"
	raw, _ := json.Marshal(record)
	if err = e.storage.Put(context.Background(), notificationKey(record.ID), raw); err != nil {
		t.Fatal(err)
	}
	rows, err := e.ListNotifications(context.Background(), "dead", 100)
	if err != nil || len(rows) != 1 || strings.Contains(rows[0].LastError, "legacy-token-value") {
		t.Fatalf("legacy diagnostic leaked: %v %v", rows, err)
	}
}
