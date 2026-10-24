package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// This gate forwards to an independently running S3 service. It does not
// implement S3, and these tests are skipped unless that service is configured.
type s3FaultGate struct {
	mode           atomic.Int32 // 0 healthy, 1 stalled, 2 disconnect, 3 commit PUT then lose response
	hits           atomic.Int32
	capabilityOnly atomic.Bool
	server         *httptest.Server
	stop           chan struct{}
	stopOnce       sync.Once
}

func newS3FaultGate(t *testing.T, endpoint string) *s3FaultGate {
	t.Helper()
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	gate := &s3FaultGate{stop: make(chan struct{})}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = transport
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		if gate.mode.Load() == 3 && response.Request.Method == http.MethodPut && !strings.Contains(response.Request.URL.Path, "/capability/") && response.StatusCode >= 200 && response.StatusCode < 300 {
			gate.hits.Add(1)
			// The backend has acknowledged its write; only the client response is lost.
			response.Body.Close()
			select {
			case <-response.Request.Context().Done():
				return response.Request.Context().Err()
			case <-gate.stop:
				return context.Canceled
			}
		}
		return nil
	}
	gate.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := gate.mode.Load()
		if gate.capabilityOnly.Load() && !strings.Contains(r.URL.Path, "/capability/") {
			mode = 0
		}
		switch mode {
		case 1:
			gate.hits.Add(1)
			select {
			case <-r.Context().Done():
			case <-gate.stop:
			}
			return
		case 2:
			gate.hits.Add(1)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				connection.Close()
			}
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		gate.stopOnce.Do(func() { close(gate.stop) })
		gate.server.Close()
		transport.CloseIdleConnections()
	})
	return gate
}

func newS3FaultClient(t *testing.T, endpoint, prefix string) *S3Storage {
	t.Helper()
	client, err := NewS3Storage(&StorageConfig{Engine: "S3", Options: map[string]interface{}{
		"endpoint": endpoint, "bucket": "owlcron-integration", "prefix": prefix,
		"access_key": "owlcron-test", "secret_key": "owlcron-test-secret",
	}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestS3RealTransportFaultRecovery(t *testing.T) {
	endpoint := os.Getenv("OWLCRON_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OWLCRON_TEST_S3_ENDPOINT to a real isolated S3 service")
	}
	prefix := fmt.Sprintf("transport-fault/%d", time.Now().UnixNano())
	direct := newS3FaultClient(t, endpoint, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := direct.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(direct.bucket)}); err != nil {
		if _, headErr := direct.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(direct.bucket)}); headErr != nil {
			t.Fatal(err)
		}
	}
	gate := newS3FaultGate(t, endpoint)
	client := newS3FaultClient(t, gate.server.URL, prefix)
	// A slow first network probe must not make other callers ignore their
	// independent deadlines while waiting to validate the same service.
	gate.capabilityOnly.Store(true)
	gate.mode.Store(1)
	probeContext, stopProbe := context.WithTimeout(ctx, 5*time.Second)
	defer stopProbe()
	probeDone := make(chan error, 1)
	go func() { probeDone <- client.ensureConditionalWrites(probeContext) }()
	deadline := time.Now().Add(3 * time.Second)
	for gate.hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if gate.hits.Load() == 0 {
		t.Fatal("first probe did not reach the transport gate")
	}
	waiterContext, stopWaiter := context.WithTimeout(ctx, 100*time.Millisecond)
	waiterStarted := time.Now()
	waiterErr := client.ensureConditionalWrites(waiterContext)
	stopWaiter()
	stopProbe()
	if !errors.Is(waiterErr, context.DeadlineExceeded) || time.Since(waiterStarted) > time.Second {
		t.Fatalf("probe waiter ignored independent deadline: %v", waiterErr)
	}
	select {
	case <-probeDone:
	case <-time.After(time.Second):
		t.Fatal("canceled network probe did not release its gate")
	}
	// The first attempted mutation cannot cache a failed capability probe.
	gate.capabilityOnly.Store(true)
	for _, mode := range []int32{1, 2} {
		gate.mode.Store(mode)
		before := gate.hits.Load()
		bounded, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		started := time.Now()
		err := client.Put(bounded, "uncommitted", []byte("not committed"))
		stop()
		if err == nil {
			t.Fatalf("fault mode %d unexpectedly succeeded", mode)
		}
		if time.Since(started) > 3*time.Second {
			t.Fatalf("fault mode %d ignored deadline", mode)
		}
		if gate.hits.Load() <= before {
			t.Fatalf("fault mode %d did not reach transport gate", mode)
		}
		if client.capabilityChecked {
			t.Fatal("failed probe was cached as healthy")
		}
	}
	gate.capabilityOnly.Store(false)
	gate.mode.Store(0)
	if err := client.Put(ctx, "durable", []byte("survives reconnect")); err != nil {
		t.Fatal(err)
	}
	// An already established client must propagate outages instead of treating
	// a failed read as a missing object and overwriting persistent state.
	for _, mode := range []int32{1, 2} {
		gate.mode.Store(mode)
		bounded, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		err := client.Update(bounded, "durable", func([]byte) ([]byte, error) { return []byte("corrupt"), nil })
		stop()
		if err == nil {
			t.Fatalf("update through fault mode %d succeeded", mode)
		}
	}
	gate.mode.Store(0)
	reopened := newS3FaultClient(t, gate.server.URL, prefix)
	data, err := reopened.Get(ctx, "durable")
	if err != nil || string(data) != "survives reconnect" {
		t.Fatalf("reconnected data=%q err=%v", data, err)
	}
	// Race independent SDK clients after reconnection; validate the actual set,
	// not merely its length, to catch duplicate retries and lost CAS updates.
	const count = 24
	var wg sync.WaitGroup
	failures := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := client
			if i%2 == 1 {
				store = reopened
			}
			failures <- store.ListPush(ctx, "concurrent", i)
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	items, err := direct.ListGet(ctx, "concurrent", 0, 0)
	if err != nil || len(items) != count {
		t.Fatalf("concurrent items=%d err=%v", len(items), err)
	}
	seen := map[float64]bool{}
	for _, item := range items {
		number, ok := item.(float64)
		if !ok || number < 0 || number >= count || seen[number] {
			t.Fatalf("duplicate or invalid item: %v", item)
		}
		seen[number] = true
	}
}

func TestS3RealCommittedResponseLossRecovery(t *testing.T) {
	endpoint := os.Getenv("OWLCRON_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OWLCRON_TEST_S3_ENDPOINT to a real isolated S3 service")
	}
	prefix := fmt.Sprintf("response-loss/%d", time.Now().UnixNano())
	direct := newS3FaultClient(t, endpoint, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Shared integration bucket is created explicitly, so this case also runs alone.
	if _, err := direct.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(direct.bucket)}); err != nil {
		if _, e := direct.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(direct.bucket)}); e != nil {
			t.Fatal(err)
		}
	}
	gate := newS3FaultGate(t, endpoint)
	client := newS3FaultClient(t, gate.server.URL, prefix)
	if err := client.Put(ctx, "operations", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	apply := func(store *S3Storage, operationContext context.Context) error {
		return store.Update(operationContext, "operations", func(raw []byte) ([]byte, error) {
			operations := map[string]int{}
			if err := json.Unmarshal(raw, &operations); err != nil {
				return nil, err
			}
			operations["stable-operation-id"] = 1
			return json.Marshal(operations)
		})
	}
	gate.mode.Store(3)
	bounded, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	err := apply(client, bounded)
	stop()
	if err == nil || gate.hits.Load() == 0 {
		t.Fatalf("did not observe lost committed response: hits=%d err=%v", gate.hits.Load(), err)
	}
	raw, err := direct.Get(ctx, "operations")
	if err != nil {
		t.Fatal(err)
	}
	var committed map[string]int
	if err := json.Unmarshal(raw, &committed); err != nil || committed["stable-operation-id"] != 1 {
		t.Fatalf("server commit missing: %s %v", raw, err)
	}
	gate.mode.Store(0)
	// Model restarting the caller with its durable operation ID. Non-idempotent
	// callbacks cannot promise exactly-once behavior after an ambiguous response.
	restarted := newS3FaultClient(t, gate.server.URL, prefix)
	if err := apply(restarted, ctx); err != nil {
		t.Fatal(err)
	}
	raw, err = direct.Get(ctx, "operations")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &committed); err != nil || len(committed) != 1 || committed["stable-operation-id"] != 1 {
		t.Fatalf("replayed operation duplicated: %s %v", raw, err)
	}
}

// TestS3RealServerRestartRecovery controls a labeled, explicitly named service
// container. This is a backend process fault, distinct from reconstructing an
// SDK client or restarting the scheduling daemon.
func TestS3RealServerRestartRecovery(t *testing.T) {
	container := os.Getenv("OWLCRON_TEST_S3_CONTAINER")
	if container == "" {
		t.Skip("set OWLCRON_TEST_S3_CONTAINER to enable isolated-service faults")
	}
	endpoint := os.Getenv("OWLCRON_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Fatal("S3 fault acceptance requires OWLCRON_TEST_S3_ENDPOINT")
	}
	control := newRemoteContainerControl(t, container)
	prefix := fmt.Sprintf("server-fault/%d", time.Now().UnixNano())
	a, b := newS3FaultClient(t, endpoint, prefix), newS3FaultClient(t, endpoint, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	if _, err := a.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(a.bucket)}); err != nil {
		if _, headErr := a.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(a.bucket)}); headErr != nil {
			t.Fatal(err)
		}
	}
	if err := a.Put(ctx, "durable", []byte("before-fault")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, "other-client", []byte("probe-complete")); err != nil {
		t.Fatal(err)
	}
	ready := func(t *testing.T, store *S3Storage) {
		t.Helper()
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			bounded, stop := context.WithTimeout(ctx, time.Second)
			data, err := store.Get(bounded, "durable")
			stop()
			if err == nil && string(data) == "before-fault" {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("S3 server did not restore committed data within 45 seconds")
	}
	unavailable := func(t *testing.T) {
		t.Helper()
		started := time.Now()
		var wg sync.WaitGroup
		failures := make(chan error, 12)
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				bounded, stop := context.WithTimeout(ctx, 400*time.Millisecond)
				defer stop()
				if i%2 == 0 {
					_, err := a.Get(bounded, "durable")
					failures <- err
				} else {
					failures <- b.Put(bounded, fmt.Sprintf("uncertain/%d", i), []byte("attempt"))
				}
			}(i)
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			if err == nil {
				t.Error("unavailable S3 server acknowledged an operation")
			}
		}
		if time.Since(started) > 5*time.Second {
			t.Fatal("concurrent remote operations ignored their deadlines")
		}
	}
	t.Run("PausedServerHonorsConcurrentDeadlines", func(t *testing.T) {
		control(t, "pause")
		t.Cleanup(func() { control(t, "unpause") })
		unavailable(t)
	})
	ready(t, a)
	ready(t, b)
	t.Run("StoppedServerDisconnects", func(t *testing.T) {
		control(t, "stop")
		t.Cleanup(func() { control(t, "start") })
		unavailable(t)
	})
	ready(t, a)
	ready(t, b)
	if err := a.Put(ctx, "lease", []byte("old-owner")); err != nil {
		t.Fatal(err)
	}
	_, staleVersion, err := a.read(ctx, "lease")
	if err != nil {
		t.Fatal(err)
	}
	control(t, "restart")
	ready(t, a)
	ready(t, b)
	reopened := newS3FaultClient(t, endpoint, prefix)
	ready(t, reopened)
	if err := b.Put(ctx, "lease", []byte("new-owner")); err != nil {
		t.Fatal(err)
	}
	raw, _, err := b.read(ctx, "lease")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.compareAndSwap(ctx, "lease", raw, staleVersion); !errors.Is(err, ErrConflict) {
		t.Fatalf("server restart accepted stale owner ETag: %v", err)
	}
	if data, err := reopened.Get(ctx, "lease"); err != nil || string(data) != "new-owner" {
		t.Fatalf("new lease owner lost: %q %v", data, err)
	}
	exerciseStorage(t, a, reopened)
}

// This checks only fault-gate cleanup, not S3 service behavior. In particular,
// a failed assertion must not hang cleanup when the peer keeps its connection.
func TestS3FaultGateCleanupReleasesStalledPeer(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer upstream.Close()
	for _, mode := range []int32{1, 3} {
		done := make(chan struct{})
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			gate := newS3FaultGate(t, upstream.URL)
			gate.mode.Store(mode)
			go func() {
				defer close(done)
				request, _ := http.NewRequest(http.MethodPut, gate.server.URL+"/bucket/key", nil)
				response, err := http.DefaultClient.Do(request)
				if err == nil {
					response.Body.Close()
				}
			}()
			deadline := time.Now().Add(3 * time.Second)
			for gate.hits.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if gate.hits.Load() == 0 {
				t.Fatal("peer did not reach stalled gate")
			}
			// Return while the peer remains connected. The registered cleanup must
			// unblock the handler independently of the request context.
		})
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("fault gate leaked a connected peer")
		}
	}
}
