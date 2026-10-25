package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
)

// This store implements only the queue's three persistence operations. Its
// fault controls model a competing committed update and an acknowledgment lost
// after a successful write; they do not implement the queue's state machine.
type notificationContractStore struct {
	mu      sync.Mutex
	records map[string][]byte

	replayRecord    []byte
	replayNext      bool
	replays         int
	failCommitAfter int
	commitError     error
	getError        error
	scanError       error
	removeAfterScan string
}

var _ NotificationStore = (*notificationContractStore)(nil)

func newNotificationContractStore() *notificationContractStore {
	return &notificationContractStore{records: make(map[string][]byte)}
}

func (s *notificationContractStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getError != nil {
		return nil, s.getError
	}
	data, ok := s.records[key]
	if !ok {
		return nil, fmt.Errorf("contract read: %w", storage.ErrNotFound)
	}
	return append([]byte(nil), data...), nil
}

func (s *notificationContractStore) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := fn(append([]byte(nil), s.records[key]...))
	if err != nil {
		return fmt.Errorf("contract callback: %w", err)
	}
	if s.replayNext {
		s.replayNext = false
		s.replays++
		// Another writer wins the first CAS. Discard that callback result and
		// evaluate it again against the other writer's committed record.
		s.records[key] = append([]byte(nil), s.replayRecord...)
		next, err = fn(append([]byte(nil), s.replayRecord...))
		if err != nil {
			return fmt.Errorf("contract repeated callback: %w", err)
		}
	}
	s.records[key] = append([]byte(nil), next...)
	if s.failCommitAfter > 0 {
		s.failCommitAfter--
		if s.failCommitAfter == 0 {
			return s.commitError
		}
	}
	return nil
}

func (s *notificationContractStore) ListKeysPrefix(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scanError != nil {
		return nil, s.scanError
	}
	var keys []string
	for key := range s.records {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	// Callers must not rely on enumeration order to rotate their processing batch.
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	if s.removeAfterScan != "" {
		delete(s.records, s.removeAfterScan)
		s.removeAfterScan = ""
	}
	return keys, nil
}

func (s *notificationContractStore) loseCommitResponseAfter(count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failCommitAfter, s.commitError = count, err
}

func (s *notificationContractStore) replayAgainst(t *testing.T, record *Notification) {
	t.Helper()
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replayRecord, s.replayNext = data, true
}

func newNotificationContractQueue(t *testing.T, store NotificationStore, deliver NotificationDeliverer) *NotificationQueue {
	t.Helper()
	q, err := NewNotificationQueue(store, NotificationQueueConfig{Workers: 1}, deliver)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func getNotificationContractRecord(t *testing.T, q *NotificationQueue, id string) *Notification {
	t.Helper()
	n, err := q.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestNotificationQueueAcceptsMinimalStore(t *testing.T) {
	ctx := context.Background()
	s := newNotificationContractStore()
	var deliveries atomic.Int32
	q := newNotificationContractQueue(t, s, func(_ context.Context, n *Notification) error {
		deliveries.Add(1)
		if n.ID != "minimal" || n.Kind != "email" || string(n.Payload) != `"saved"` {
			t.Errorf("delivery lost its immutable snapshot: %+v", n)
		}
		return nil
	})
	if err := q.Enqueue(ctx, "minimal", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	pending, err := q.List(ctx, "pending", 10)
	if err != nil || len(pending) != 1 || pending[0].ID != "minimal" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n := getNotificationContractRecord(t, q, "minimal")
	if n.State != "sent" || len(n.Payload) != 0 || deliveries.Load() != 1 {
		t.Fatalf("record=%+v deliveries=%d", n, deliveries.Load())
	}
}

func TestNotificationQueueRepeatedClaimCallbackDiscardsStaleClaim(t *testing.T) {
	for _, test := range []struct {
		name, replacementState, wantState string
		attempts                          int
	}{
		{name: "another-worker-already-sent", replacementState: "sent", wantState: "sent", attempts: 1},
		{name: "interrupted-last-attempt-exhausted", replacementState: "pending", wantState: "dead", attempts: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := newNotificationContractStore()
			var deliveries atomic.Int32
			q := newNotificationContractQueue(t, s, func(context.Context, *Notification) error { deliveries.Add(1); return nil })
			if err := q.Enqueue(ctx, "repeated-claim", "email", "saved"); err != nil {
				t.Fatal(err)
			}
			replacement := getNotificationContractRecord(t, q, "repeated-claim")
			replacement.State, replacement.Attempts = test.replacementState, test.attempts
			s.replayAgainst(t, replacement)
			if err := q.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			n := getNotificationContractRecord(t, q, "repeated-claim")
			if deliveries.Load() != 0 || n.State != test.wantState || n.Attempts != test.attempts || n.LeaseToken != "" {
				t.Fatalf("stale callback escaped: record=%+v deliveries=%d", n, deliveries.Load())
			}
			s.mu.Lock()
			replays := s.replays
			s.mu.Unlock()
			if replays != 1 {
				t.Fatalf("CAS replay was not exercised: %d", replays)
			}
		})
	}
}

func TestNotificationQueuePendingIdentityRetainsOriginalSnapshot(t *testing.T) {
	ctx := context.Background()
	s := newNotificationContractStore()
	q := newNotificationContractQueue(t, s, func(context.Context, *Notification) error { return nil })
	if err := q.Enqueue(ctx, "pending-identity", "email", "first rendering"); err != nil {
		t.Fatal(err)
	}
	first := getNotificationContractRecord(t, q, "pending-identity")
	if err := q.Enqueue(ctx, "pending-identity", "email", "different rendering"); err != nil {
		t.Fatal(err)
	}
	repeated := getNotificationContractRecord(t, q, "pending-identity")
	if string(repeated.Payload) != string(first.Payload) || repeated.ContentHash != first.ContentHash || repeated.State != "pending" {
		t.Fatalf("pending replay changed the admitted snapshot: first=%+v repeated=%+v", first, repeated)
	}
	if err := q.Enqueue(ctx, "pending-identity", "webhook", "different kind"); !errors.Is(err, ErrNotificationConflict) {
		t.Fatalf("kind change did not preserve identity conflict: %v", err)
	}
	retained := getNotificationContractRecord(t, q, "pending-identity")
	if retained.Kind != "email" || retained.ContentHash != first.ContentHash {
		t.Fatalf("conflicting kind overwrote the original record: %+v", retained)
	}
}

func TestNotificationQueueEnqueueRecoversLostCommitResponse(t *testing.T) {
	ctx := context.Background()
	s := newNotificationContractStore()
	var deliveries atomic.Int32
	q := newNotificationContractQueue(t, s, func(_ context.Context, n *Notification) error {
		deliveries.Add(1)
		if string(n.Payload) != `"first snapshot"` {
			t.Errorf("replay replaced the admitted payload: %s", n.Payload)
		}
		return nil
	})
	lost := errors.New("enqueue committed but response was lost")
	s.loseCommitResponseAfter(1, lost)
	if err := q.Enqueue(ctx, "uncertain-enqueue", "email", "first snapshot"); !errors.Is(err, lost) {
		t.Fatalf("uncertain enqueue result was hidden: %v", err)
	}
	n := getNotificationContractRecord(t, q, "uncertain-enqueue")
	if n.State != "pending" || string(n.Payload) != `"first snapshot"` {
		t.Fatalf("committed enqueue disappeared: %+v", n)
	}
	if err := q.Enqueue(ctx, "uncertain-enqueue", "email", "later rendering"); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries.Load() != 1 {
		t.Fatalf("enqueue replay duplicated delivery: %d", deliveries.Load())
	}
}

func TestNotificationQueueClaimRecoversLostCommitResponseAfterLease(t *testing.T) {
	ctx := context.Background()
	s := newNotificationContractStore()
	clock := time.Unix(1700000000, 0)
	var deliveries atomic.Int32
	deliver := func(context.Context, *Notification) error { deliveries.Add(1); return nil }
	q := newNotificationContractQueue(t, s, deliver)
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "uncertain-claim", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	lost := errors.New("claim committed but response was lost")
	s.loseCommitResponseAfter(1, lost)
	if err := q.RunOnce(ctx); !errors.Is(err, lost) {
		t.Fatalf("uncertain claim result was hidden: %v", err)
	}
	n := getNotificationContractRecord(t, q, "uncertain-claim")
	if n.State != "inflight" || n.Attempts != 1 || n.LeaseToken == "" || deliveries.Load() != 0 {
		t.Fatalf("uncertain claim was used for delivery: record=%+v deliveries=%d", n, deliveries.Load())
	}
	restarted := newNotificationContractQueue(t, s, deliver)
	restarted.now = func() time.Time { return clock }
	if err := restarted.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries.Load() != 0 {
		t.Fatal("restart stole a still-live uncertain claim")
	}
	clock = clock.Add(61 * time.Second)
	if err := restarted.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	n = getNotificationContractRecord(t, restarted, "uncertain-claim")
	if n.State != "sent" || n.Attempts != 2 || deliveries.Load() != 1 {
		t.Fatalf("expired uncertain claim was not recovered: record=%+v deliveries=%d", n, deliveries.Load())
	}
}

func TestNotificationQueueAckLostResponseRetainsCommittedSentState(t *testing.T) {
	ctx := context.Background()
	s := newNotificationContractStore()
	clock := time.Unix(1700000000, 0)
	var deliveries atomic.Int32
	deliver := func(context.Context, *Notification) error { deliveries.Add(1); return nil }
	q := newNotificationContractQueue(t, s, deliver)
	q.now = func() time.Time { return clock }
	if err := q.Enqueue(ctx, "uncertain-ack", "email", "saved"); err != nil {
		t.Fatal(err)
	}
	lost := errors.New("ack committed but response was lost")
	// The first commit is the claim; only the following successful ack loses its response.
	s.loseCommitResponseAfter(2, lost)
	if err := q.RunOnce(ctx); !errors.Is(err, lost) {
		t.Fatalf("uncertain ack result was hidden: %v", err)
	}
	n := getNotificationContractRecord(t, q, "uncertain-ack")
	if n.State != "sent" || n.Attempts != 1 || len(n.Payload) != 0 || deliveries.Load() != 1 {
		t.Fatalf("committed ack was not retained: record=%+v deliveries=%d", n, deliveries.Load())
	}
	clock = clock.Add(61 * time.Second)
	restarted := newNotificationContractQueue(t, s, deliver)
	restarted.now = func() time.Time { return clock }
	if err := restarted.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if deliveries.Load() != 1 {
		t.Fatal("ack response loss caused an already-sent record to be delivered again")
	}
}

func TestNotificationQueueReadAndScanFailuresAreNotAbsence(t *testing.T) {
	for _, operation := range []string{"get", "list-read", "list-scan", "run-scan"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			s := newNotificationContractStore()
			var deliveries atomic.Int32
			q := newNotificationContractQueue(t, s, func(context.Context, *Notification) error { deliveries.Add(1); return nil })
			if err := q.Enqueue(ctx, "unavailable", "email", "saved"); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("storage unavailable")
			s.mu.Lock()
			if strings.HasSuffix(operation, "scan") {
				s.scanError = failure
			} else {
				s.getError = failure
			}
			s.mu.Unlock()
			var err error
			switch operation {
			case "get":
				_, err = q.Get(ctx, "unavailable")
			case "run-scan":
				err = q.RunOnce(ctx)
			default:
				_, err = q.List(ctx, "", 10)
			}
			if !errors.Is(err, failure) || errors.Is(err, storage.ErrNotFound) || deliveries.Load() != 0 {
				t.Fatalf("storage failure became absence: err=%v deliveries=%d", err, deliveries.Load())
			}
		})
	}
}

func TestNotificationQueueRecordsMayDisappearAfterEnumeration(t *testing.T) {
	for _, operation := range []string{"list", "run"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			s := newNotificationContractStore()
			var deliveries atomic.Int32
			q := newNotificationContractQueue(t, s, func(context.Context, *Notification) error { deliveries.Add(1); return nil })
			if err := q.Enqueue(ctx, "disappearing", "email", "saved"); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			s.removeAfterScan = notificationKey("disappearing")
			s.mu.Unlock()
			if operation == "list" {
				records, err := q.List(ctx, "", 10)
				if err != nil || len(records) != 0 {
					t.Fatalf("vanished key was not skipped: records=%v err=%v", records, err)
				}
			} else if err := q.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Get(ctx, notificationKey("disappearing")); !errors.Is(err, storage.ErrNotFound) || deliveries.Load() != 0 {
				t.Fatalf("vanished key was recreated or delivered: err=%v deliveries=%d", err, deliveries.Load())
			}
		})
	}
}
