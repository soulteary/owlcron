package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestScriptArgumentsAndProtocolIsolation(t *testing.T) {
	var out, logs bytes.Buffer
	code := run(context.Background(), []string{"/bin/sh", "-c", `printf '<%s><%s>\n' "$1" "$2"; printf '{"complete":true,"code":0}\n'; exit 7`, "fixed"}, strings.NewReader(`{"params":{"args":"[\"a b\",\"\"]","script":"/malicious"}}`), &out, &logs)
	if code != 7 || !strings.Contains(logs.String(), "<a b><>") {
		t.Fatalf("code=%d logs=%q", code, logs.String())
	}
	var result struct {
		Complete bool
		Code     int
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.Complete || result.Code != 7 {
		t.Fatalf("result: %s %v", out.String(), err)
	}
}
func TestScriptRejectsInvalidArgumentsAndMissingCommand(t *testing.T) {
	for _, input := range []string{`{`, `{"params":{"args":["x"]}}`, `{"params":{"args":"[1]"}}`, `{"params":{"args":"null"}}`, strings.Repeat("x", 1024*1024+1)} {
		var out, logs bytes.Buffer
		if run(context.Background(), []string{"/bin/sh", "-c", "echo should-not-run"}, strings.NewReader(input), &out, &logs) == 0 || logs.Len() != 0 {
			t.Fatal("invalid input executed")
		}
	}
	var out bytes.Buffer
	if run(context.Background(), []string{"sh"}, strings.NewReader(`{}`), &out, &out) == 0 {
		t.Fatal("relative command accepted")
	}
}
func TestScriptCancellationAndInputForwarding(t *testing.T) {
	var out, logs bytes.Buffer
	if run(context.Background(), []string{"/bin/cat"}, strings.NewReader(`{"id":"forwarded"}`), &out, &logs) != 0 || !strings.Contains(logs.String(), "forwarded") {
		t.Fatal("job input not forwarded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	out.Reset()
	logs.Reset()
	if run(ctx, []string{"/bin/sleep", "5"}, strings.NewReader(`{}`), &out, &logs) == 0 {
		t.Fatal("cancelled command succeeded")
	}
}
