package plugin

import (
	"bytes"
	"context"
	"errors"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Both 0.9.102 and 0.9.135 use one job JSON line on stdin and newline
// JSON updates, including numeric complete:1 and fractional progress.
func TestCronicleProtocolContract(t *testing.T) {
	pe := NewPluginExecutor(zap.NewNop())
	job := &types.Job{ID: "jcontract", Now: 123, Params: map[string]interface{}{"hello": "world"}}
	script := `read input
printf 'input:%s\n' "$input"
printf '%s\n' '{"progress":0.375}' '{"complete":1,"code":0,"description":"done","perf":{"work":0.2},"chain_data":{"answer":42}}'
printf 'tail after completion\n'
`
	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", script)
	var log bytes.Buffer
	updates := 0
	result, err := pe.ExecutePrepared(context.Background(), cmd, job, &log, nil, func(data map[string]interface{}) {
		updates++
		if p, ok := data["progress"].(float64); ok && p != 0.375 {
			t.Errorf("fractional progress lost: %v", p)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.Code != 0 || result.Progress != 0.375 || result.ChainData["answer"] != float64(42) {
		t.Fatalf("bad result: %+v", result)
	}
	if updates != 2 || !strings.Contains(log.String(), `"hello":"world"`) || !strings.Contains(log.String(), "tail after completion") {
		t.Fatalf("protocol/log drain mismatch %d %s", updates, log.String())
	}
}

type failFirstLogWriter struct{ failed bool }

func (w *failFirstLogWriter) Write(data []byte) (int, error) {
	if !w.failed {
		w.failed = true
		return 0, errors.New("injected log write failure")
	}
	return len(data), nil
}

func TestPluginLogWriteFailureCannotBecomeSuccess(t *testing.T) {
	writer := &protocolWriter{log: &failFirstLogWriter{}, result: &PluginResult{}}
	_, err := writer.Write([]byte("first log\nsecond log\n{\"complete\":1}\n"))
	if err == nil || writer.err == nil {
		t.Fatal("later successful log writes erased the original failure")
	}
}
func TestPluginMissingCompletionFails(t *testing.T) {
	pe := NewPluginExecutor(zap.NewNop())
	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", "echo ordinary-log")
	_, err := pe.ExecutePrepared(context.Background(), cmd, &types.Job{}, &bytes.Buffer{}, nil, nil)
	if err == nil {
		t.Fatal("missing completion was accepted")
	}
}
func TestPluginTimeoutDrainsAndReaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "exec sleep 30")
	_, err := NewPluginExecutor(zap.NewNop()).ExecutePrepared(ctx, cmd, &types.Job{}, &bytes.Buffer{}, nil, nil)
	if err == nil || ctx.Err() == nil {
		t.Fatal("timeout was ignored")
	}
	if cmd.ProcessState == nil {
		t.Fatal("process was not reaped")
	}
}
func TestParseCommandQuotedArguments(t *testing.T) {
	command, args := ParseCommand(`/bin/sh -c 'printf "hello world"' ""`)
	if command != "/bin/sh" || len(args) != 3 || args[1] != `printf "hello world"` || args[2] != "" {
		t.Fatalf("%s %#v", command, args)
	}
}

// Completion must never hide failed log writes, including stderr failures while
// a child keeps running. Wait would otherwise wait indefinitely for that child.
func TestLogWriteFailureStopsPluginImmediately(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			script := `printf '{"complete":1,"code":0}\n'; printf 'ordinary output\n'; exec sleep 30`
			if stream == "stderr" {
				script = `printf '{"complete":1,"code":0}\n'; printf 'ordinary output\n' >&2; exec sleep 30`
			}
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", script)
			failure := errors.New("disk full")
			start := time.Now()
			_, err := NewPluginExecutor(zap.NewNop()).ExecutePrepared(ctx, cmd, &types.Job{}, errorLogWriter{failure}, nil, nil)
			if !errors.Is(err, failure) {
				t.Fatalf("got %v", err)
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("log failure waited for timeout")
			}
		})
	}
}

type errorLogWriter struct{ err error }

func (w errorLogWriter) Write(p []byte) (int, error) { return 0, w.err }
