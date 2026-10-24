package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The receiver accepts the complete request before the daemon is killed. This
// deliberately preserves the ambiguous delivery window: lease recovery must use
// the same idempotency identity, while the receiver may have seen it already.
func TestProcessSIGKILLRecoversInflightWebHookClaim(t *testing.T) {
	f := newProcessFixture(t)
	stop, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var ids []string
	var arrival []time.Time
	entered := make(chan struct{}, 1)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(400)
			return
		}
		if payload["action"] != "job_complete" {
			w.WriteHeader(204)
			return
		}
		mu.Lock()
		ids = append(ids, r.Header.Get("Idempotency-Key"))
		arrival = append(arrival, time.Now())
		attempt := len(ids)
		mu.Unlock()
		if attempt == 1 {
			entered <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-stop.Done():
			}
			return
		}
		w.WriteHeader(204)
	}))
	defer hook.Close()
	// Ensure any blocked receiver exits before httptest's close waits for it.
	defer cancel()
	f.Config.WebHookAllowedHosts = []string{"127.0.0.1"}
	script := filepath.Join(f.Dir, "inflight.sh")
	if err := os.WriteFile(script, []byte("read input\nprintf 'INFLIGHT-DURABLE-LOG\\n'\nprintf '%s\\n' '{\"complete\":1,\"code\":0}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p := f.Start(t)
	jobID := processRecoveryEvent(t, f, script, "inflight-recovery", map[string]any{"web_hook": hook.URL})
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("completion request never reached blocked receiver")
	}
	var notificationID string
	var leaseUntil int64
	awaitProcess(t, 5*time.Second, "persisted inflight notification claim", func() bool {
		response := f.Request(t, "/api/app/get_notifications", map[string]any{"state": "inflight", "limit": 100})
		rows, ok := response["rows"].([]any)
		if !ok {
			return false
		}
		mu.Lock()
		id := ids[0]
		mu.Unlock()
		for _, raw := range rows {
			row := raw.(map[string]any)
			if row["id"] == id && row["attempts"] == float64(1) {
				notificationID = id
				leaseUntil = int64(row["lease_until"].(float64))
				return true
			}
		}
		return false
	})
	if remaining := time.Until(time.Unix(0, leaseUntil)); remaining < 45*time.Second {
		t.Fatalf("expected real default claim lease, remaining %s", remaining)
	}
	p.KillDaemonOnly(t)
	f.Start(t)
	// The lease is not edited or advanced. A real minute must elapse before a
	// different daemon may reclaim this abandoned delivery.
	awaitProcess(t, 85*time.Second, "natural inflight lease expiry and successful redelivery", func() bool {
		response := f.Request(t, "/api/app/get_notifications", map[string]any{"state": "sent", "limit": 100})
		rows, ok := response["rows"].([]any)
		if !ok {
			return false
		}
		for _, raw := range rows {
			row := raw.(map[string]any)
			if row["id"] == notificationID {
				if row["attempts"] != float64(2) {
					t.Fatalf("recovered attempt count %#v", row)
				}
				return true
			}
		}
		return false
	})
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] == "" || ids[0] != ids[1] {
		t.Fatalf("ambiguous recovery changed identity or duplicated retry: %#v", ids)
	}
	if arrival[1].Before(time.Unix(0, leaseUntil).Add(-time.Millisecond)) {
		t.Fatal("new daemon bypassed active claim lease")
	}
	processAssertSingleHistory(t, f, "inflight-recovery", jobID)
	if got := processRecoveredLog(t, f, jobID); !strings.Contains(got, "INFLIGHT-DURABLE-LOG\n") {
		t.Fatal("completion log missing after inflight recovery")
	}
}
