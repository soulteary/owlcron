package engine

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestWebHookRedactionNestedTypedAndRaw(t *testing.T) {
	e := &Engine{config: &types.Config{SecretKey: "cluster-private", SMTPPassword: "mail-private"}}
	type nested struct {
		Password    string `json:"password"`
		Description string `json:"description"`
	}
	data := map[string]interface{}{
		"nested":      map[string][]nested{"items": {{Password: "unknown-password", Description: "cluster-private"}}},
		"raw":         json.RawMessage(`{"access_token":"unknown-token","description":"mail-private"}`),
		"json":        `{"api_key":"unknown-api","text":"hello"}`,
		"url":         "https://alice:unknown-password@example.test/path?token=unknown-token&ok=yes#private",
		"description": "Authorization: Bearer unknown-bearer password=unknown-pass Cookie=unknown-cookie",
	}
	before, _ := json.Marshal(data)
	cleaned := e.sanitizeWebHookData(data)
	encoded, err := json.Marshal(cleaned)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"cluster-private", "mail-private", "unknown-password", "unknown-token", "unknown-api", "unknown-bearer", "unknown-pass", "unknown-cookie"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret %q leaked: %s", secret, encoded)
		}
	}
	after, _ := json.Marshal(data)
	if string(before) != string(after) {
		t.Fatal("input mutated")
	}
	if !strings.Contains(string(encoded), "ok=yes") {
		t.Fatalf("safe URL query lost: %s", encoded)
	}
}

func TestWebHookRedactionConcurrentAndCycle(t *testing.T) {
	e := &Engine{config: &types.Config{SecretKey: "shared-secret"}}
	cycle := map[string]interface{}{"text": "shared-secret"}
	cycle["cycle"] = cycle
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clean := e.sanitizeWebHookData(cycle)
			b, err := json.Marshal(clean)
			if err != nil {
				t.Error(err)
			}
			if strings.Contains(string(b), "shared-secret") {
				t.Error("secret leaked")
			}
		}()
	}
	wg.Wait()
	if cycle["text"] != "shared-secret" {
		t.Fatal("input changed")
	}
}

func TestWebHookRedactionMalformedRawAndDepth(t *testing.T) {
	e := &Engine{}
	values := map[string]interface{}{"raw": json.RawMessage(`{"password":"unterminated`)}
	for i := 0; i < 100; i++ {
		values = map[string]interface{}{"child": values}
	}
	b, err := json.Marshal(e.sanitizeWebHookData(values))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 1000 {
		t.Fatalf("depth limit ineffective: %d", len(b))
	}
	b, err = json.Marshal(e.sanitizeWebHookData(map[string]interface{}{"raw": json.RawMessage(`invalid secret`)}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "invalid secret") {
		t.Fatal("malformed raw leaked")
	}
}

func TestWebHookRedactionLoadedSigningSecretAndEncodedCredentials(t *testing.T) {
	secret := "file-signing-secret-with-special / symbols"
	e := &Engine{webHookSigner: &webHookSigner{secret: []byte(secret)}}
	input := map[string]interface{}{secret: "https://example.test/path?value=file-signing-secret-with-special+%2F+symbols", "description": secret}
	b, err := json.Marshal(e.sanitizeWebHookData(input))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "file-signing-secret") {
		t.Fatalf("file key leaked: %s", b)
	}
	var nilEngine *Engine
	if got := nilEngine.RedactSensitiveText("https://u:p@example.test/?api_key=hidden"); strings.Contains(got, "hidden") || strings.Contains(got, "u:p") {
		t.Fatal(got)
	}
}

func TestWebHookRedactionPreservesNumbersAndBoundsSensitiveKeys(t *testing.T) {
	e := &Engine{}
	body, err := e.cleanWebHookBody([]byte(`{"number":9007199254740993,"raw":"{\"number\":9007199254740993}"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"number":9007199254740993`) || strings.Contains(string(body), `"number":"9007199254740993"`) {
		t.Fatalf("numeric precision lost: %s", body)
	}
	data := make(map[string]interface{}, 20000)
	for i := 0; i < 20000; i++ {
		data["password"+strings.Repeat("x", i%10)+string(rune(i+100))] = "hidden"
	}
	cleaned := e.sanitizeWebHookData(data)
	if len(cleaned) > 10000 {
		t.Fatalf("sensitive keys exceed budget: %d", len(cleaned))
	}
	if _, err := e.cleanWebHookBody([]byte(`{} {}`)); err == nil {
		t.Fatal("accepted multiple JSON documents")
	}
}
