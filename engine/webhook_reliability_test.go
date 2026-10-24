package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func TestWebHookDeliveryStatusAndIdempotency(t *testing.T) {
	for _, tc := range []struct {
		status    int
		permanent bool
		success   bool
	}{{200, false, true}, {204, false, true}, {400, true, false}, {401, true, false}, {302, true, false}, {408, false, false}, {429, false, false}, {503, false, false}} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Idempotency-Key") != "stable-id" {
					t.Error("missing stable idempotency header")
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Error("missing JSON content type")
				}
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			manager := NewWebHookManager(&Engine{config: &types.Config{WebHookAllowedHosts: []string{"127.0.0.1"}}}, zap.NewNop())
			err := manager.DeliverWebHook(context.Background(), WebHookPayload{URL: server.URL, Body: json.RawMessage(`{"action":"job_complete"}`)}, "stable-id")
			if tc.success {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var failure *NotificationDeliveryError
			if !errors.As(err, &failure) || failure.Permanent != tc.permanent || failure.RetryAfter != 120*time.Second {
				t.Fatalf("unexpected failure: %#v", err)
			}
		})
	}
}

func TestWebHookNoRedirectAndBoundedResponse(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(302)
	}))
	defer server.Close()
	manager := NewWebHookManager(&Engine{config: &types.Config{WebHookAllowedHosts: []string{"127.0.0.1"}}}, zap.NewNop())
	err := manager.DeliverWebHook(context.Background(), WebHookPayload{URL: server.URL, Body: json.RawMessage(`{}`)}, "id")
	if err == nil || redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 128*1024))) }))
	defer large.Close()
	if err := manager.DeliverWebHook(context.Background(), WebHookPayload{URL: large.URL, Body: json.RawMessage(`{}`)}, "id"); err != nil {
		t.Fatal(err)
	}
}

func TestWebHookCancellationAndConcurrentDelivery(t *testing.T) {
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if r.URL.Path == "/slow" {
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
			return
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	manager := NewWebHookManager(&Engine{config: &types.Config{WebHookAllowedHosts: []string{"127.0.0.1"}}}, zap.NewNop())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := manager.DeliverWebHook(ctx, WebHookPayload{URL: server.URL + "/slow", Body: json.RawMessage(`{}`)}, "slow"); err == nil {
		t.Fatal("missing canceled failure")
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancellation was not bounded")
	}
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := manager.DeliverWebHook(context.Background(), WebHookPayload{URL: server.URL, Body: json.RawMessage(`{}`)}, "id"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if received.Load() != 25 {
		t.Fatalf("unexpected deliveries %d", received.Load())
	}
}

func TestWebHookDoesNotExportClusterSecret(t *testing.T) {
	manager := NewWebHookManager(&Engine{config: &types.Config{SecretKey: "private-cluster-secret", WebHookConfigKeys: []string{"secret_key", "base_app_url"}}}, zap.NewNop())
	body, err := json.Marshal(manager.prepareHookData("job_complete", map[string]interface{}{"id": "job"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "private-cluster-secret") || strings.Contains(string(body), "secret_key") {
		t.Fatal("cluster secret exported")
	}
}

func TestWebHookRetryAfterBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		input string
		want  time.Duration
	}{{"-1", 0}, {"garbage", 0}, {"999999999999", 24 * time.Hour}, {now.Add(2 * time.Hour).Format(http.TimeFormat), 2 * time.Hour}, {now.Add(-time.Minute).Format(http.TimeFormat), 0}} {
		if got := parseWebHookRetryAfter(tc.input, now); got != tc.want {
			t.Errorf("%q: got %v want %v", tc.input, got, tc.want)
		}
	}
}
