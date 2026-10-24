package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/soulteary/owlcron/types"
)

// DNS, address classification and dialing regressions live with http-kit/outbound.
// These tests protect the application's configuration and delivery adapter.
func TestWebHookPolicyConfigurationMapping(t *testing.T) {
	config := &types.Config{
		WebHookAllowedHosts: []string{"127.0.0.1"},
		WebHookAllowedCIDRs: []string{"10.2.0.0/16", "fc00::/7"},
	}
	policy := newWebHookPolicy(config)
	config.WebHookAllowedHosts[0] = "127.0.0.2"
	config.WebHookAllowedCIDRs[0] = "10.3.0.0/16"
	for _, target := range []string{"http://127.0.0.1:8080/hook", "http://10.2.3.4/hook", "http://[fd12::1]/hook"} {
		if err := policy.validate(context.Background(), target); err != nil {
			t.Fatalf("trusted destination rejected: %v", err)
		}
	}
	for _, target := range []string{"http://127.0.0.2/hook", "http://10.3.0.1/hook", "https://169.254.169.254/hook"} {
		err := policy.validate(context.Background(), target)
		if !isWebHookPolicyDenied(err) || err.Error() != "webhook destination is prohibited by security policy" {
			t.Fatalf("expected application policy denial, got %v", err)
		}
	}
	defaults := newWebHookPolicy(nil)
	if err := defaults.validate(context.Background(), "https://8.8.8.8/hook"); err != nil {
		t.Fatal(err)
	}
	if err := defaults.validate(context.Background(), "http://8.8.8.8/hook"); !isWebHookPolicyDenied(err) {
		t.Fatalf("default plaintext request accepted: %v", err)
	}
}

func TestWebHookPolicyInvalidConfigurationFailsClosed(t *testing.T) {
	var reached atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer server.Close()
	for _, config := range []*types.Config{
		{WebHookAllowedHosts: []string{"*.example.com"}},
		{WebHookAllowedHosts: []string{"https://example.com"}},
		{WebHookAllowedHosts: []string{"example.com:443"}},
		{WebHookAllowedCIDRs: []string{"invalid"}},
	} {
		if err := validateWebHookPolicy(config); err == nil {
			t.Fatal("invalid configuration accepted")
		}
		policy := newWebHookPolicy(config)
		if err := policy.validate(context.Background(), server.URL); !isWebHookPolicyDenied(err) {
			t.Fatalf("invalid configuration did not fail closed: %v", err)
		}
		request, err := http.NewRequest(http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := policy.client().Do(request)
		if response != nil {
			response.Body.Close()
		}
		if !isWebHookPolicyDenied(err) {
			t.Fatalf("standalone manager client did not fail closed: %v", err)
		}
	}
	if reached.Load() != 0 {
		t.Fatal("invalid configuration reached network")
	}
}

func TestWebHookSharedClientEnforcesPolicyWithoutPrevalidation(t *testing.T) {
	var reached atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer server.Close()
	manager := NewWebHookManager(&Engine{config: &types.Config{}}, nil)
	request, err := http.NewRequest(http.MethodGet, server.URL+"?token=private", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := manager.client.Do(request)
	if response != nil {
		response.Body.Close()
	}
	if !isWebHookPolicyDenied(err) || reached.Load() != 0 {
		t.Fatalf("shared client bypassed policy: %v, reached=%d", err, reached.Load())
	}
}

func TestWebHookTransportPolicyDenialIsPermanentAndRedacted(t *testing.T) {
	var reached atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer server.Close()
	manager := NewWebHookManager(&Engine{config: &types.Config{WebHookAllowedHosts: []string{"127.0.0.1"}}}, nil)
	// Preliminary application validation succeeds. A stricter transport denies the
	// attempt, exercising the shared package's error across net/http's url.Error.
	manager.client = newWebHookPolicy(nil).client()
	err := manager.DeliverWebHook(context.Background(), WebHookPayload{URL: server.URL + "?token=private-token", Body: json.RawMessage("{}")}, "stable-id")
	var failure *NotificationDeliveryError
	if !errors.As(err, &failure) || !failure.Permanent || failure.Err.Error() != "webhook transport failed" {
		t.Fatalf("shared transport denial changed classification or diagnostic: %v", err)
	}
	if reached.Load() != 0 || strings.Contains(err.Error(), "private-token") || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("blocked request reached network or disclosed target: %v", err)
	}
	if !isWebHookPolicyDenied(fmt.Errorf("transport: %w", &webHookPolicyError{})) {
		t.Fatal("wrapped application denial lost its shared error identity")
	}
}
