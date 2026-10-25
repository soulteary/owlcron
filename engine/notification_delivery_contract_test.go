package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
)

func TestNotificationDeliveryStaleAcknowledgmentIsFenced(t *testing.T) {
	for _, tc := range []struct {
		name               string
		oldError, newError error
		wantState          string
	}{
		{name: "late-success-cannot-replace-dead", newError: &NotificationDeliveryError{Err: errors.New("permanent rejection"), Permanent: true}, wantState: "dead"},
		{name: "late-failure-cannot-replace-sent", oldError: &NotificationDeliveryError{Err: errors.New("late rejection"), Permanent: true}, wantState: "sent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := notificationTestStore(t)
			clock := time.Unix(1_700_000_000, 0)
			entered, release := make(chan struct{}), make(chan struct{})
			releaseAttempt := sync.OnceFunc(func() { close(release) })
			old := notificationTestQueue(t, store, NotificationQueueConfig{}, func(context.Context, *Notification) error {
				close(entered)
				<-release
				return tc.oldError
			})
			old.now = func() time.Time { return clock }
			if err := old.Enqueue(ctx, "fenced-delivery", "webhook", "original payload"); err != nil {
				t.Fatal(err)
			}
			var workers sync.WaitGroup
			workers.Add(1)
			oldResult := make(chan error, 1)
			go func() { defer workers.Done(); oldResult <- old.RunOnce(ctx) }()
			t.Cleanup(func() { releaseAttempt(); workers.Wait() })
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first delivery did not acquire its claim")
			}

			// A second process's clock has passed the first claim's lease. The
			// first callback remains active, so its eventual ack must be fenced.
			current := notificationTestQueue(t, store, NotificationQueueConfig{}, func(context.Context, *Notification) error { return tc.newError })
			current.now = func() time.Time { return clock.Add(61 * time.Second) }
			if err := current.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := current.Get(ctx, "fenced-delivery")
			if err != nil || before.State != tc.wantState || before.Attempts != 2 {
				t.Fatalf("new owner did not finish: record=%+v err=%v", before, err)
			}
			releaseAttempt()
			if err := <-oldResult; !errors.Is(err, errNotificationSkip) {
				t.Fatalf("stale acknowledgment error=%v, want existing skip error", err)
			}
			after, err := current.Get(ctx, "fenced-delivery")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("stale acknowledgment changed current record: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}

func TestNotificationDeliveryExpiredLeaseStillAcknowledgesCurrentToken(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	var calls atomic.Int32
	q := notificationTestQueue(t, notificationTestStore(t), NotificationQueueConfig{}, func(ctx context.Context, n *Notification) error {
		calls.Add(1)
		if ctx.Err() != nil || n.LeaseToken == "" {
			t.Errorf("delivery did not start with a live context and claim: context=%v record=%+v", ctx.Err(), n)
		}
		// Advance only the persisted lease's wall clock. The attempt's real
		// context remains live, and no second owner acquires another token.
		clock.Store(n.LeaseUntil + int64(time.Second))
		return nil
	})
	q.now = func() time.Time { return time.Unix(0, clock.Load()) }
	if err := q.Enqueue(ctx, "expired-current-token", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatalf("current owner could not acknowledge after lease expiry: %v", err)
	}
	n, err := q.Get(ctx, "expired-current-token")
	if err != nil || n.State != "sent" || n.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("current owner lost its acknowledgment: record=%+v calls=%d err=%v", n, calls.Load(), err)
	}
	if n.UpdatedAt != clock.Load() || n.UpdatedAt <= start.Add(time.Minute).UnixNano() {
		t.Fatalf("acknowledgment was not persisted after lease expiry: %+v", n)
	}
	if n.LeaseToken != "" || n.LeaseUntil != 0 || len(n.Payload) != 0 {
		t.Fatalf("successful acknowledgment did not clear claim and payload: %+v", n)
	}
}

func TestNotificationDeliveryAttemptTimeoutDoesNotAcknowledgeSuccess(t *testing.T) {
	ctx := context.Background()
	clock := time.Unix(1_700_000_000, 0)
	q := notificationTestQueue(t, notificationTestStore(t), NotificationQueueConfig{
		DeliveryTimeout: 5 * time.Millisecond, LeaseDuration: time.Second,
		RetryBase: time.Second, RetryMax: time.Minute,
	}, func(ctx context.Context, _ *Notification) error {
		<-ctx.Done()
		// Returning nil after the deadline cannot turn an uncertain attempt
		// into a durable success, even if a transport forgets to return ctx.Err.
		return nil
	})
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "attempt-timeout", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatalf("persisted transport failure should not fail the scan: %v", err)
	}
	n, err := q.Get(ctx, "attempt-timeout")
	if err != nil || n.State != "pending" || n.Attempts != 1 || n.NextAttempt != clock.Add(time.Second).UnixNano() {
		t.Fatalf("timeout outcome record=%+v err=%v", n, err)
	}
	if n.LeaseToken != "" || n.LeaseUntil != 0 || n.LastError != context.DeadlineExceeded.Error() || len(n.Payload) == 0 {
		t.Fatalf("timeout lost payload, lease cleanup or diagnostic: %+v", n)
	}
}

func TestNotificationDeliveryParentCancellationLeavesClaimForRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := time.Unix(1_700_000_000, 0)
	q := notificationTestQueue(t, notificationTestStore(t), NotificationQueueConfig{}, func(ctx context.Context, _ *Notification) error {
		cancel()
		<-ctx.Done()
		return nil
	})
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "parent-canceled", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation was hidden: %v", err)
	}
	n, err := q.Get(context.Background(), "parent-canceled")
	if err != nil || n.State != "inflight" || n.Attempts != 1 {
		t.Fatalf("canceled attempt changed durable claim: record=%+v err=%v", n, err)
	}
	if n.LeaseToken == "" || n.LeaseUntil != clock.Add(time.Minute).UnixNano() || n.NextAttempt != 0 || n.LastError != "" || len(n.Payload) == 0 {
		t.Fatalf("canceled attempt prematurely acknowledged its claim: %+v", n)
	}
}

func TestNotificationDeliveryWrappedFailurePolicy(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		failure                    error
		priorAttempts, maxAttempts int
		wantState                  string
		wantAttempts               int
		wantDelay                  time.Duration
	}{
		{name: "permanent-overrides-deferred", failure: &NotificationDeliveryError{Err: errors.New("rejected"), Permanent: true, Deferred: true, RetryAfter: time.Hour}, wantState: "dead", wantAttempts: 1},
		{name: "deferred-refunds-last-attempt", failure: &NotificationDeliveryError{Err: errors.New("quota"), Deferred: true, RetryAfter: 2 * time.Hour}, maxAttempts: 1, wantState: "pending", wantDelay: 2 * time.Hour},
		{name: "deferred-zero-uses-base", failure: &NotificationDeliveryError{Err: errors.New("quota"), Deferred: true}, wantState: "pending", wantDelay: 10 * time.Second},
		{name: "deferred-negative-uses-base", failure: &NotificationDeliveryError{Err: errors.New("quota"), Deferred: true, RetryAfter: -time.Second}, wantState: "pending", wantDelay: 10 * time.Second},
		{name: "deferred-has-own-day-cap", failure: &NotificationDeliveryError{Err: errors.New("quota"), Deferred: true, RetryAfter: 48 * time.Hour}, wantState: "pending", wantDelay: 24 * time.Hour},
		{name: "ordinary-retry-after-cannot-reduce-base", failure: &NotificationDeliveryError{Err: errors.New("temporary"), RetryAfter: time.Second}, wantState: "pending", wantAttempts: 1, wantDelay: 10 * time.Second},
		{name: "ordinary-retry-after-is-capped", failure: &NotificationDeliveryError{Err: errors.New("temporary"), RetryAfter: 48 * time.Hour}, wantState: "pending", wantAttempts: 1, wantDelay: time.Minute},
		{name: "ordinary-exponential-delay-is-capped", failure: errors.New("temporary"), priorAttempts: 3, wantState: "pending", wantAttempts: 4, wantDelay: time.Minute},
		{name: "ordinary-last-attempt-becomes-dead", failure: errors.New("temporary"), maxAttempts: 1, wantState: "dead", wantAttempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clock := time.Unix(1_700_000_000, 0)
			store := notificationTestStore(t)
			attemptLimit := tc.maxAttempts
			if attemptLimit == 0 {
				attemptLimit = 8
			}
			q := notificationTestQueue(t, store, NotificationQueueConfig{MaxAttempts: attemptLimit, RetryBase: 10 * time.Second, RetryMax: time.Minute}, func(context.Context, *Notification) error {
				return fmt.Errorf("transport: %w", tc.failure)
			})
			q.now = func() time.Time { return clock }
			if err := q.Enqueue(ctx, "classified-failure", "email", "saved"); err != nil {
				t.Fatal(err)
			}
			seedNotificationDeliveryAttempts(t, store.(storage.AtomicStorage), "classified-failure", tc.priorAttempts)
			if err := q.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			n, err := q.Get(ctx, "classified-failure")
			if err != nil || n.State != tc.wantState || n.Attempts != tc.wantAttempts {
				t.Fatalf("failure classification record=%+v err=%v", n, err)
			}
			var wantNext int64
			if tc.wantDelay != 0 {
				wantNext = clock.Add(tc.wantDelay).UnixNano()
			}
			if n.NextAttempt != wantNext || n.LeaseToken != "" || n.LeaseUntil != 0 || len(n.Payload) == 0 || !strings.HasPrefix(n.LastError, "transport: ") {
				t.Fatalf("failure retry or diagnostic changed: record=%+v want next=%d", n, wantNext)
			}
		})
	}
}

func seedNotificationDeliveryAttempts(t *testing.T, store storage.AtomicStorage, id string, attempts int) {
	t.Helper()
	if attempts == 0 {
		return
	}
	if err := store.Update(context.Background(), notificationKey(id), func(raw []byte) ([]byte, error) {
		var n Notification
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, err
		}
		n.Attempts = attempts
		return json.Marshal(n)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationDeliveryRedactionPrecedesRawByteLimit(t *testing.T) {
	for _, tc := range []struct {
		name, redacted, want string
	}{
		{name: "ascii", redacted: "safe:" + strings.Repeat("x", 1500), want: "safe:" + strings.Repeat("x", 1019)},
		// The historical limit is applied before JSON marshaling. A split
		// UTF-8 character is subsequently replaced by JSON's U+FFFD, so the
		// decoded diagnostic can exceed 1024 bytes without changing that rule.
		{name: "json-replaces-split-utf8", redacted: strings.Repeat("界", 342), want: strings.Repeat("界", 341) + "\uFFFD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const secret = "private-delivery-token"
			q := notificationTestQueue(t, notificationTestStore(t), NotificationQueueConfig{}, func(context.Context, *Notification) error {
				return &NotificationDeliveryError{Err: errors.New(secret + strings.Repeat("raw", 500)), Permanent: true}
			})
			var sawFullInput atomic.Bool
			q.redactError = func(text string) string {
				sawFullInput.Store(strings.Contains(text, secret) && len(text) > 1024)
				return tc.redacted
			}
			if err := q.Enqueue(ctx, "redacted-failure", "webhook", "saved"); err != nil {
				t.Fatal(err)
			}
			if err := q.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			n, err := q.Get(ctx, "redacted-failure")
			if err != nil || n.State != "dead" || !sawFullInput.Load() || n.LastError != tc.want || strings.Contains(n.LastError, secret) {
				t.Fatalf("persisted redaction or byte-limit contract changed: record=%+v redactor saw full input=%v err=%v", n, sawFullInput.Load(), err)
			}
		})
	}
}

func TestNotificationDeliveryLegacyJSONRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		attempts    int
	}{
		{name: "pending-without-hash-or-lease", state: "pending"},
		{name: "inflight-without-lease-token", state: "inflight", attempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := notificationTestStore(t)
			clock := time.Unix(1_700_000_000, 0)
			const payload = `{"url":"https://example.test/hook","body":{}}`
			raw := fmt.Sprintf(`{"id":"legacy-notification","kind":"webhook","payload":%s,"state":%q,"attempts":%d}`, payload, tc.state, tc.attempts)
			// Keep the historical namespace and hash independent of notificationKey.
			if err := store.Put(ctx, "notifications/196e9c91d198012869bb0a74fb06bae5a26593680ea103279e240c5298452a32", []byte(raw)); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			q := notificationTestQueue(t, store, NotificationQueueConfig{}, func(_ context.Context, n *Notification) error {
				calls.Add(1)
				assertLegacyNotificationDeliverySnapshot(t, n, payload, tc.attempts+1)
				return nil
			})
			q.now = func() time.Time { return clock }
			if err := q.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			n, err := q.Get(ctx, "legacy-notification")
			if err != nil || n.State != "sent" || n.Attempts != tc.attempts+1 || n.ContentHash != "" || len(n.Payload) != 0 || calls.Load() != 1 {
				t.Fatalf("legacy recovery record=%+v calls=%d err=%v", n, calls.Load(), err)
			}
			if err := q.Enqueue(ctx, "legacy-notification", "webhook", "different payload"); err != nil {
				t.Fatal(err)
			}
			if err := q.RunOnce(ctx); err != nil || calls.Load() != 1 {
				t.Fatalf("legacy sent marker did not deduplicate: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func assertLegacyNotificationDeliverySnapshot(t *testing.T, n *Notification, payload string, attempts int) {
	t.Helper()
	if n.ID != "legacy-notification" || n.Kind != "webhook" || string(n.Payload) != payload || n.ContentHash != "" || n.Attempts != attempts {
		t.Errorf("legacy delivery snapshot changed: %+v", n)
	}
}
