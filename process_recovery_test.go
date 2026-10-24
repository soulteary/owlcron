package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These tests execute the production daemon entry point in another process and
// crash it without Shutdown.
// File and protocol barriers locate failures after externally observable work.
func processRecoveryEvent(t *testing.T, f *processFixture, script, id string, extras map[string]any) string {
	t.Helper()
	requireProcessOK(t, f.Request(t, "/api/app/create_category", map[string]any{"id": "general", "title": "General", "enabled": 1}))
	requireProcessOK(t, f.Request(t, "/api/app/create_plugin", map[string]any{"id": "recovery", "title": "Recovery", "enabled": 1, "command": "/bin/sh " + script}))
	event := map[string]any{"id": id, "title": "Crash recovery", "enabled": 0, "category": "general", "target": "local", "plugin": "recovery", "params": map[string]any{}, "timing": map[string]any{}, "timezone": "UTC", "max_children": 8}
	for k, v := range extras {
		event[k] = v
	}
	requireProcessOK(t, f.Request(t, "/api/app/create_event", event))
	response := f.Request(t, "/api/app/run_event", map[string]any{"id": id})
	requireProcessOK(t, response)
	ids, ok := response["ids"].([]any)
	if !ok || len(ids) != 1 {
		t.Fatalf("run result: %#v", response)
	}
	return ids[0].(string)
}
func requireProcessOK(t *testing.T, response map[string]any) {
	t.Helper()
	if response["code"] != float64(0) {
		t.Fatalf("API failed: %#v", response)
	}
}
func processRecoveredLog(t *testing.T, f *processFixture, id string) string {
	t.Helper()
	var result []byte
	offset := 0
	for i := 0; i < 64; i++ {
		response := f.Request(t, "/api/app/get_job_log", map[string]any{"id": id, "offset": offset, "limit": 32768})
		requireProcessOK(t, response)
		data, err := base64.StdEncoding.DecodeString(response["log_base64"].(string))
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, data...)
		if response["eof"] == true {
			return string(result)
		}
		next := int(response["next_offset"].(float64))
		if next <= offset {
			t.Fatal("log paging did not advance")
		}
		offset = next
	}
	t.Fatal("log paging exceeded bound")
	return ""
}
func processAssertSingleHistory(t *testing.T, f *processFixture, event, id string) {
	t.Helper()
	history := f.Request(t, "/api/app/get_event_history", map[string]any{"id": event, "limit": 100})
	requireProcessOK(t, history)
	rows := history["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != id {
		t.Fatalf("history duplicated or lost: %#v", history)
	}
}

// Readers race against persisted completion recovery and notification workers in
// a separate daemon address space, rather than sharing in-memory engine state.
func processAssertConcurrentRecoveryReads(t *testing.T, f *processFixture, event, id string) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assertProcessLogSequence(t, processRecoveredLog(t, f, id))
			processAssertSingleHistory(t, f, event, id)
		}()
	}
	wg.Wait()
}

func processLogScript(marker string) string {
	return "read input\nprintf 'execution\\n' >> '" + marker + "'\nawk 'BEGIN { for(i=0;i<3000;i++) printf \"RECOVERY-%06d-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\\n\", i }'\n"
}
func assertProcessLogSequence(t *testing.T, log string) {
	t.Helper()
	next := 0
	scanner := bufio.NewScanner(strings.NewReader(log))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "RECOVERY-") {
			continue
		}
		expected := fmt.Sprintf("RECOVERY-%06d-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", next)
		if next >= 3000 || line != expected {
			t.Fatalf("log missing, duplicated or out of order at line %d: %q", next, line)
		}
		next++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if next != 3000 {
		t.Fatalf("log has %d recovery lines, want 3000", next)
	}
}

func TestProcessSIGKILLRecoversActiveJobAndChunkedLog(t *testing.T) {
	processSIGKILLRecoversActiveJobAndChunkedLog(t, nil)
}

func processSIGKILLRecoversActiveJobAndChunkedLog(t *testing.T, configure func(...*processFixture)) {
	f := newProcessFixture(t)
	if configure != nil {
		configure(f)
	}
	script := filepath.Join(f.Dir, "active.sh")
	marker := filepath.Join(f.Dir, "executions")
	release := filepath.Join(f.Dir, "release")
	escaped := filepath.Join(f.Dir, "escaped")
	body := processLogScript(marker) + "while [ ! -f '" + release + "' ]; do sleep 0.1; done\nprintf escaped > '" + escaped + "'\nprintf '%s\\n' '{\"complete\":1,\"code\":0}'\n"
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	p := f.Start(t)
	id := processRecoveryEvent(t, f, script, "active-recovery", nil)
	pid := 0
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0600)
		if pid > 0 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	})
	awaitProcess(t, 15*time.Second, "active plugin PID and multi-chunk log", func() bool {
		response := f.Request(t, "/api/app/get_job_status", map[string]any{"id": id})
		if job, ok := response["job"].(map[string]any); ok {
			pid = int(job["pid"].(float64))
		}
		data, err := os.ReadFile(filepath.Join(f.Config.LogDir, "jobs", id+".log"))
		return pid > 0 && err == nil && strings.Contains(string(data), "RECOVERY-002999-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n")
	})
	p.KillDaemonOnly(t)
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("fault barrier did not leave plugin alive: %v", err)
	}
	f.Start(t)
	awaitProcess(t, 15*time.Second, "restarted task terminal failure", func() bool {
		response := f.Request(t, "/api/app/get_job_status", map[string]any{"id": id})
		job, ok := response["job"].(map[string]any)
		return ok && job["complete"] == true && job["code"] != float64(0)
	})
	// Releasing the original command after recovery proves that its process group
	// cannot continue executing side effects following daemon death.
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	if _, err := os.Stat(escaped); !os.IsNotExist(err) {
		t.Fatal("orphan plugin continued executing after recovery")
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "execution\n" {
		t.Fatalf("task replayed: %q %v", data, err)
	}
	assertProcessLogSequence(t, processRecoveredLog(t, f, id))
	processAssertConcurrentRecoveryReads(t, f, "active-recovery", id)
}

// A genuine SMTP peer keeps delivery in a recoverable 451 state, then accepts
// DATA after daemon restart. No notification transport is replaced by a mock.
type processSMTPPeer struct {
	listener net.Listener
	recover  atomic.Bool
	mu       sync.Mutex
	messages []string
	failures atomic.Int32
	wg       sync.WaitGroup
}

func newProcessSMTPPeer(t *testing.T) *processSMTPPeer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &processSMTPPeer{listener: l}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(15 * time.Second))
				rw := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
				reply := func(text string) { _, _ = fmt.Fprint(rw, text+"\r\n"); _ = rw.Flush() }
				reply("220 recovery SMTP")
				for {
					line, err := rw.ReadString('\n')
					if err != nil {
						return
					}
					switch {
					case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "HELO"):
						reply("250 recovery")
					case strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"):
						reply("250 OK")
					case strings.HasPrefix(line, "DATA"):
						if !s.recover.Load() {
							s.failures.Add(1)
							reply("451 temporarily unavailable")
							continue
						}
						reply("354 send data")
						var message strings.Builder
						for {
							part, err := rw.ReadString('\n')
							if err != nil {
								return
							}
							if part == ".\r\n" {
								break
							}
							message.WriteString(part)
						}
						s.mu.Lock()
						s.messages = append(s.messages, message.String())
						s.mu.Unlock()
						reply("250 accepted")
					case strings.HasPrefix(line, "QUIT"):
						reply("221 bye")
						return
					default:
						reply("250 OK")
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = l.Close(); s.wg.Wait() })
	return s
}

func TestProcessSIGKILLRecoversMailAndWebHookQueue(t *testing.T) {
	processSIGKILLRecoversMailAndWebHookQueue(t, nil)
}

func processSIGKILLRecoversMailAndWebHookQueue(t *testing.T, configure func(...*processFixture)) {
	f := newProcessFixture(t)
	if configure != nil {
		configure(f)
	}
	smtp := newProcessSMTPPeer(t)
	_, port, _ := net.SplitHostPort(smtp.listener.Addr().String())
	f.Config.SMTPHostname = "127.0.0.1"
	f.Config.SMTPPort, _ = strconv.Atoi(port)
	f.Config.SMTPTLSMode = "none"
	f.Config.SMTPTimeoutSeconds = 2
	f.Config.EmailFrom = "daemon@example.test"
	f.Config.NotificationWorkers = 4
	var recovered atomic.Bool
	var failed atomic.Int32
	var hookMu sync.Mutex
	attemptIDs := map[string]int{}
	successIDs := map[string]int{}
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		id := r.Header.Get("Idempotency-Key")
		hookMu.Lock()
		attemptIDs[id]++
		if recovered.Load() {
			successIDs[id]++
		}
		hookMu.Unlock()
		if !recovered.Load() {
			failed.Add(1)
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer hook.Close()
	f.Config.WebHookAllowedHosts = []string{"127.0.0.1"}
	marker := filepath.Join(f.Dir, "executions")
	script := filepath.Join(f.Dir, "complete.sh")
	if err := os.WriteFile(script, []byte(processLogScript(marker)+"printf '%s\\n' '{\"complete\":1,\"code\":0}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p := f.Start(t)
	id := processRecoveryEvent(t, f, script, "completed-recovery", map[string]any{"web_hook": hook.URL, "notify_success": "one@example.test,two@example.test"})
	awaitProcess(t, 20*time.Second, "SMTP and WebHook durable retry barriers", func() bool { return smtp.failures.Load() >= 2 && failed.Load() >= 2 })
	var response map[string]any
	awaitProcess(t, 5*time.Second, "all failed deliveries durably pending", func() bool {
		response = f.Request(t, "/api/app/get_notifications", map[string]any{"limit": 100})
		rows, ok := response["rows"].([]any)
		if !ok || len(rows) != 4 {
			return false
		}
		for _, raw := range rows {
			row := raw.(map[string]any)
			if row["state"] != "pending" || row["attempts"].(float64) < 1 {
				return false
			}
		}
		return true
	})
	requireProcessOK(t, response)
	before := map[string]string{}
	for _, raw := range response["rows"].([]any) {
		row := raw.(map[string]any)
		before[row["id"].(string)] = row["state"].(string)
	}
	if len(before) != 4 {
		t.Fatalf("expected start+completion hooks and two emails, got %#v", response)
	}
	p.Kill(t)
	recovered.Store(true)
	smtp.recover.Store(true)
	f.Start(t)
	awaitProcess(t, 25*time.Second, "all persisted notifications sent after SIGKILL", func() bool {
		response := f.Request(t, "/api/app/get_notifications", map[string]any{"limit": 100})
		rows, ok := response["rows"].([]any)
		if !ok || len(rows) != len(before) {
			return false
		}
		for _, raw := range rows {
			row := raw.(map[string]any)
			if _, ok := before[row["id"].(string)]; !ok || row["state"] != "sent" {
				return false
			}
		}
		return true
	})
	hookMu.Lock()
	if len(successIDs) != 2 {
		t.Errorf("hook identifiers lost: %#v", successIDs)
	}
	for key, n := range successIDs {
		if key == "" || n != 1 || attemptIDs[key] < 2 {
			t.Errorf("unstable or duplicate hook delivery %q: success=%d attempts=%d", key, n, attemptIDs[key])
		}
	}
	hookMu.Unlock()
	smtp.mu.Lock()
	if len(smtp.messages) != 2 {
		t.Errorf("SMTP successful deliveries=%d want 2", len(smtp.messages))
	}
	for _, message := range smtp.messages {
		if !strings.Contains(message, "Message-ID:") {
			t.Error("SMTP stable Message-ID missing")
		}
	}
	smtp.mu.Unlock()
	data, _ := os.ReadFile(marker)
	if string(data) != "execution\n" {
		t.Fatalf("completed task executed again: %q", data)
	}
	assertProcessLogSequence(t, processRecoveredLog(t, f, id))
	processAssertConcurrentRecoveryReads(t, f, "completed-recovery", id)
}
