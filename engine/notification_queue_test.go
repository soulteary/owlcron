package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
)

func notificationTestStore(t *testing.T) storage.Storage {
	t.Helper()
	s, err := storage.NewFilesystemStorage(&storage.StorageConfig{Engine: "Filesystem", BaseDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func notificationTestQueue(t *testing.T, s storage.Storage, cfg NotificationQueueConfig, deliver func(context.Context, *Notification) error) *NotificationQueue {
	t.Helper()
	queueStore, err := newNotificationStore(s)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewNotificationQueue(queueStore, cfg, deliver)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestNotificationQueueConcurrentClaimsAndIdempotency(t *testing.T) {
	ctx := context.Background()
	s := notificationTestStore(t)
	var deliveries atomic.Int32
	gate := make(chan struct{})
	deliver := func(context.Context, *Notification) error { deliveries.Add(1); <-gate; return nil }
	q1 := notificationTestQueue(t, s, NotificationQueueConfig{}, deliver)
	q2 := notificationTestQueue(t, s, NotificationQueueConfig{}, deliver)
	if err := q1.Enqueue(ctx, "job:1:email:x", "email", map[string]string{"body": "first"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	errs := make(chan error, 2)
	go func() { defer wg.Done(); errs <- q1.RunOnce(ctx) }()
	go func() { defer wg.Done(); errs <- q2.RunOnce(ctx) }()
	deadline := time.After(5 * time.Second)
	for deliveries.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("delivery did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if deliveries.Load() != 1 {
		t.Fatalf("deliveries=%d", deliveries.Load())
	}
	if err := q2.Enqueue(ctx, "job:1:email:x", "email", map[string]string{"body": "changed"}); err != nil {
		t.Fatal(err)
	}
	if err := q2.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries.Load() != 1 {
		t.Fatal("sent notification delivered again")
	}
	n, err := q1.Get(ctx, "job:1:email:x")
	if err != nil {
		t.Fatal(err)
	}
	if n.State != "sent" || len(n.Payload) != 0 {
		t.Fatalf("record=%+v", n)
	}
}

func TestNotificationQueueRetryDeadAndManualRecovery(t *testing.T) {
	ctx := context.Background()
	s := notificationTestStore(t)
	clock := time.Now()
	var calls int
	var recoverDelivery bool
	q := notificationTestQueue(t, s, NotificationQueueConfig{MaxAttempts: 2, RetryBase: time.Second, RetryMax: time.Minute}, func(context.Context, *Notification) error {
		calls++
		if recoverDelivery {
			return nil
		}
		return &NotificationDeliveryError{Err: errors.New("temporary unavailable"), RetryAfter: 3 * time.Second}
	})
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "retry", "webhook", map[string]string{"url": "target"}); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := q.Get(ctx, "retry")
	if n.State != "pending" || n.Attempts != 1 || n.NextAttempt != clock.Add(3*time.Second).UnixNano() {
		t.Fatalf("record=%+v", n)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("retried before due")
	}
	clock = clock.Add(4 * time.Second)
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	dead, err := q.List(ctx, "dead", 10)
	if err != nil || len(dead) != 1 {
		t.Fatalf("dead=%v err=%v", dead, err)
	}
	recoverDelivery = true
	if err := q.RetryDead(ctx, "retry"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ = q.Get(ctx, "retry")
	if n.State != "sent" || n.Attempts != 1 {
		t.Fatalf("record=%+v", n)
	}
}

func TestNotificationQueueRestartRecoversExpiredClaim(t *testing.T) {
	ctx := context.Background()
	s := notificationTestStore(t)
	clock := time.Now()
	cancelCtx, cancel := context.WithCancel(ctx)
	q := notificationTestQueue(t, s, NotificationQueueConfig{}, func(ctx context.Context, _ *Notification) error { cancel(); return ctx.Err() })
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "interrupted", "email", map[string]string{"body": "saved"}); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	n, _ := q.Get(ctx, "interrupted")
	if n.State != "inflight" {
		t.Fatalf("record=%+v", n)
	}
	var calls int
	restarted := notificationTestQueue(t, s, NotificationQueueConfig{}, func(context.Context, *Notification) error { calls++; return nil })
	restarted.now = func() time.Time { return clock }
	if err := restarted.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("live lease stolen")
	}
	clock = clock.Add(61 * time.Second)
	if err := restarted.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("expired claim not recovered")
	}
	n, _ = restarted.Get(ctx, "interrupted")
	if n.Attempts != 2 || n.State != "sent" {
		t.Fatalf("record=%+v", n)
	}
}

func TestNotificationQueuePermanentErrorAndPayloadLimit(t *testing.T) {
	ctx := context.Background()
	q := notificationTestQueue(t, notificationTestStore(t), NotificationQueueConfig{}, func(context.Context, *Notification) error {
		return &NotificationDeliveryError{Err: errors.New("invalid recipient"), Permanent: true}
	})
	if err := q.Enqueue(ctx, "large", "email", make([]byte, notificationPayloadLimit)); err == nil {
		t.Fatal("accepted oversized payload")
	}
	if err := q.Enqueue(ctx, "permanent", "email", "body"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := q.Get(ctx, "permanent")
	if n.State != "dead" || n.Attempts != 1 {
		t.Fatalf("record=%+v", n)
	}
}

type notificationFailAckStorage struct {
	storage.Storage
	failed atomic.Bool
}

func (s *notificationFailAckStorage) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return s.Storage.(storage.AtomicStorage).Update(ctx, key, func(data []byte) ([]byte, error) {
		next, err := fn(data)
		if err != nil {
			return nil, err
		}
		var n Notification
		if err := json.Unmarshal(next, &n); err != nil {
			return nil, err
		}
		if n.State == "sent" && !s.failed.Swap(true) {
			return nil, errors.New("simulated ack storage failure")
		}
		return next, nil
	})
}
func (s *notificationFailAckStorage) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	return s.Storage.(storage.PrefixKeyLister).ListKeysPrefix(ctx, prefix)
}
func TestNotificationQueueAckFailureRecoversAfterLease(t *testing.T) {
	ctx := context.Background()
	s := &notificationFailAckStorage{Storage: notificationTestStore(t)}
	clock := time.Now()
	calls := 0
	q := notificationTestQueue(t, s, NotificationQueueConfig{}, func(context.Context, *Notification) error { calls++; return nil })
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "ack-failure", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err == nil {
		t.Fatal("ack failure hidden")
	}
	n, _ := q.Get(ctx, "ack-failure")
	if n.State != "inflight" {
		t.Fatalf("record=%+v", n)
	}
	clock = clock.Add(61 * time.Second)
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ = q.Get(ctx, "ack-failure")
	if n.State != "sent" || calls != 2 {
		t.Fatalf("record=%+v calls=%d", n, calls)
	}
}

func TestNotificationQueueBoundedConcurrencyAndRotatingBatch(t *testing.T) {
	ctx := context.Background()
	s := notificationTestStore(t)
	var active, maximum, delivered atomic.Int32
	q := notificationTestQueue(t, s, NotificationQueueConfig{Workers: 2, BatchSize: 3}, func(context.Context, *Notification) error {
		n := active.Add(1)
		for {
			old := maximum.Load()
			if old >= n || maximum.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		delivered.Add(1)
		return nil
	})
	for i := 0; i < 12; i++ {
		if err := q.Enqueue(ctx, fmt.Sprintf("batch-%d", i), "email", i); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- q.RunOnce(ctx) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() > 2 {
		t.Fatalf("concurrency=%d", maximum.Load())
	}
	if delivered.Load() != 12 {
		t.Fatalf("delivered=%d", delivered.Load())
	}
}

func TestNotificationQueueDeferredDoesNotExhaustAttempts(t *testing.T) {
	ctx := context.Background()
	clock := time.Now()
	deferDelivery := true
	q := notificationTestQueue(t, notificationTestStore(t), NotificationQueueConfig{MaxAttempts: 1}, func(context.Context, *Notification) error {
		if deferDelivery {
			return &NotificationDeliveryError{Err: errors.New("daily quota"), Deferred: true, RetryAfter: 12 * time.Hour}
		}
		return nil
	})
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "deferred", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := q.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		n, _ := q.Get(ctx, "deferred")
		if n.State != "pending" || n.Attempts != 0 || n.NextAttempt != clock.Add(12*time.Hour).UnixNano() {
			t.Fatalf("record=%+v", n)
		}
		clock = clock.Add(13 * time.Hour)
	}
	deferDelivery = false
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := q.Get(ctx, "deferred")
	if n.State != "sent" {
		t.Fatalf("record=%+v", n)
	}
}

func TestNotificationQueueInterruptedLastAttemptBecomesDead(t *testing.T) {
	ctx := context.Background()
	s := notificationTestStore(t)
	clock := time.Now()
	cancelCtx, cancel := context.WithCancel(ctx)
	q := notificationTestQueue(t, s, NotificationQueueConfig{MaxAttempts: 1}, func(ctx context.Context, _ *Notification) error { cancel(); return ctx.Err() })
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "last-attempt", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	recovered := notificationTestQueue(t, s, NotificationQueueConfig{MaxAttempts: 1}, func(context.Context, *Notification) error { t.Error("exhausted claim delivered again"); return nil })
	clock = clock.Add(61 * time.Second)
	recovered.now = func() time.Time { return clock }
	if err := recovered.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := recovered.Get(ctx, "last-attempt")
	if n.State != "dead" || n.Attempts != 1 || n.LeaseToken != "" {
		t.Fatalf("record=%+v", n)
	}
}
