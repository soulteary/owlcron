package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/soulteary/owlcron/storage"
)

const notificationPrefix = "notifications/"
const notificationPayloadLimit = 256 * 1024

var errNotificationSkip = errors.New("notification is not due")

// NotificationQueue retains the existing notifications/ key layout and record
// format. Business admission, local spooling and transport selection live in
// notification_integration.go, outside this persistence and retry mechanism.
type NotificationQueue struct {
	store       NotificationStore
	cfg         NotificationQueueConfig
	deliver     NotificationDeliverer
	redactError func(string) string
	slots       chan struct{}
	scanMu      sync.Mutex
	cursor      string
	now         func() time.Time
}

// NewNotificationQueue constructs a queue without starting a background worker.
// Callers schedule RunOnce and own cancellation and error reporting.
func NewNotificationQueue(store NotificationStore, cfg NotificationQueueConfig, deliver NotificationDeliverer) (*NotificationQueue, error) {
	if store == nil {
		return nil, storage.ErrUnsupported
	}
	if deliver == nil {
		return nil, errors.New("notification delivery function is required")
	}
	if cfg.Workers == 0 {
		cfg.Workers = 4
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 8
	}
	if cfg.DeliveryTimeout == 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 60 * time.Second
	}
	if cfg.RetryBase == 0 {
		cfg.RetryBase = 5 * time.Second
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = time.Hour
	}
	if cfg.Workers < 1 || cfg.Workers > 64 || cfg.BatchSize < 1 || cfg.BatchSize > 10000 || cfg.MaxAttempts < 1 || cfg.MaxAttempts > 100 || cfg.DeliveryTimeout <= 0 || cfg.LeaseDuration <= cfg.DeliveryTimeout || cfg.RetryBase <= 0 || cfg.RetryMax < cfg.RetryBase {
		return nil, errors.New("invalid notification queue limits")
	}
	return &NotificationQueue{store: store, cfg: cfg, deliver: deliver, slots: make(chan struct{}, cfg.Workers), now: time.Now}, nil
}

func notificationKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return notificationPrefix + hex.EncodeToString(sum[:])
}

// Enqueue durably admits the first payload for an ID and Kind. An uncertain
// storage result is returned to the caller; replay must use the same identity.
func (q *NotificationQueue) Enqueue(ctx context.Context, id, kind string, payload any) error {
	if id == "" || len(id) > 1024 || kind == "" || len(kind) > 64 {
		return errors.New("invalid notification identity")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(data) > notificationPayloadLimit {
		return errors.New("notification payload exceeds 256 KiB")
	}
	hash := sha256.Sum256(append([]byte(kind+"\x00"), data...))
	digest := hex.EncodeToString(hash[:])
	now := q.now().UnixNano()
	err = q.store.Update(ctx, notificationKey(id), func(old []byte) ([]byte, error) {
		if len(old) > 0 {
			var existing Notification
			if err := json.Unmarshal(old, &existing); err != nil {
				return nil, err
			}
			if existing.ID != id || existing.Kind != kind {
				return nil, ErrNotificationConflict
			}
			return nil, errNotificationSkip
		}
		return json.Marshal(&Notification{ID: id, Kind: kind, Payload: data, ContentHash: digest, State: "pending", CreatedAt: now, UpdatedAt: now})
	})
	if errors.Is(err, errNotificationSkip) {
		return nil
	}
	return err
}

func (q *NotificationQueue) Get(ctx context.Context, id string) (*Notification, error) {
	data, err := q.store.Get(ctx, notificationKey(id))
	if err != nil {
		return nil, err
	}
	var n Notification
	err = json.Unmarshal(data, &n)
	if err == nil && n.ID != id {
		err = ErrNotificationConflict
	}
	return &n, err
}

// List returns at most limit records. Payloads may contain private message data;
// callers must require administrator authorization before exposing them.
func (q *NotificationQueue) List(ctx context.Context, state string, limit int) ([]Notification, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	keys, err := q.store.ListKeysPrefix(ctx, notificationPrefix)
	if err != nil {
		return nil, err
	}
	out := []Notification{}
	for _, key := range keys {
		data, err := q.store.Get(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var n Notification
		if err = json.Unmarshal(data, &n); err != nil {
			return nil, err
		}
		if state == "" || n.State == state {
			out = append(out, n)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (q *NotificationQueue) RetryDead(ctx context.Context, id string) error {
	return q.store.Update(ctx, notificationKey(id), func(data []byte) ([]byte, error) {
		if len(data) == 0 {
			return nil, storage.ErrNotFound
		}
		var n Notification
		if err := json.Unmarshal(data, &n); err != nil {
			return nil, err
		}
		if n.State != "dead" {
			return nil, errors.New("only dead notifications can be retried")
		}
		n.State = "pending"
		n.Attempts = 0
		n.NextAttempt = 0
		n.LeaseUntil = 0
		n.LeaseToken = ""
		n.LastError = ""
		n.UpdatedAt = q.now().UnixNano()
		return json.Marshal(&n)
	})
}

// RunOnce enumerates the namespace and processes a bounded rotating batch.
// Enumeration itself is not bounded. Claims are durable and fenced, so
// multiple processes may run this method against the same storage backend.
// Delivery is at least once: a crash after acceptance but before durable ack
// can cause another attempt. Transports should include the stable ID.
func (q *NotificationQueue) RunOnce(ctx context.Context) error {
	keys, err := q.store.ListKeysPrefix(ctx, notificationPrefix)
	if err != nil {
		return err
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil
	}
	q.scanMu.Lock()
	start := sort.SearchStrings(keys, q.cursor)
	if start >= len(keys) {
		start = 0
	}
	count := q.cfg.BatchSize
	if count > len(keys) {
		count = len(keys)
	}
	batch := make([]string, count)
	for i := range batch {
		batch[i] = keys[(start+i)%len(keys)]
	}
	next := (start + count) % len(keys)
	q.cursor = keys[next]
	q.scanMu.Unlock()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}
	}
	for _, key := range batch {
		select {
		case q.slots <- struct{}{}:
		case <-ctx.Done():
			recordErr(ctx.Err())
			wg.Wait()
			return firstErr
		}
		wg.Add(1)
		go func(key string) { defer wg.Done(); defer func() { <-q.slots }(); recordErr(q.process(ctx, key)) }(key)
	}
	wg.Wait()
	return firstErr
}

func (q *NotificationQueue) process(ctx context.Context, key string) error {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	token := hex.EncodeToString(random[:])
	now := q.now()
	var claimed Notification
	err := q.store.Update(ctx, key, func(data []byte) ([]byte, error) {
		claimed = Notification{}
		if len(data) == 0 {
			return nil, errNotificationSkip
		}
		var n Notification
		if err := json.Unmarshal(data, &n); err != nil {
			return nil, err
		}
		if (n.State != "pending" && n.State != "inflight") || n.NextAttempt > now.UnixNano() || (n.State == "inflight" && n.LeaseUntil > now.UnixNano()) {
			return nil, errNotificationSkip
		}
		if n.Attempts >= q.cfg.MaxAttempts {
			n.State = "dead"
			n.LeaseUntil = 0
			n.LeaseToken = ""
			n.LastError = "delivery attempts exhausted after interrupted delivery"
			n.UpdatedAt = now.UnixNano()
			return json.Marshal(&n)
		}
		n.State = "inflight"
		n.Attempts++
		n.LeaseToken = token
		n.LeaseUntil = now.Add(q.cfg.LeaseDuration).UnixNano()
		n.UpdatedAt = now.UnixNano()
		claimed = n
		return json.Marshal(&n)
	})
	if errors.Is(err, errNotificationSkip) {
		return nil
	}
	if err != nil {
		return err
	}
	if claimed.ID == "" {
		return nil
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, q.cfg.DeliveryTimeout)
	deliveryErr := q.deliver(deliveryCtx, &claimed)
	if deliveryErr == nil {
		deliveryErr = deliveryCtx.Err()
	}
	cancel()
	// A cancelled parent must leave the claim durable for lease-based recovery.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return q.store.Update(ctx, key, func(data []byte) ([]byte, error) {
		var n Notification
		if err := json.Unmarshal(data, &n); err != nil {
			return nil, err
		}
		if n.State != "inflight" || n.LeaseToken != token {
			return nil, errNotificationSkip
		}
		n.LeaseUntil = 0
		n.LeaseToken = ""
		n.UpdatedAt = q.now().UnixNano()
		if deliveryErr == nil {
			n.State = "sent"
			n.Payload = nil
			n.LastError = ""
			n.NextAttempt = 0
			return json.Marshal(&n)
		}
		// Store a bounded diagnostic; transports must redact credentials from errors.
		n.LastError = deliveryErr.Error()
		if q.redactError != nil {
			n.LastError = q.redactError(n.LastError)
		}
		if len(n.LastError) > 1024 {
			n.LastError = n.LastError[:1024]
		}
		var failure *NotificationDeliveryError
		errors.As(deliveryErr, &failure)
		if failure != nil && failure.Deferred && !failure.Permanent {
			n.Attempts--
			n.State = "pending"
			delay := failure.RetryAfter
			if delay < q.cfg.RetryBase {
				delay = q.cfg.RetryBase
			}
			if delay > 24*time.Hour {
				delay = 24 * time.Hour
			}
			n.NextAttempt = q.now().Add(delay).UnixNano()
			return json.Marshal(&n)
		}
		if n.Attempts >= q.cfg.MaxAttempts || (failure != nil && failure.Permanent) {
			n.State = "dead"
			n.NextAttempt = 0
		} else {
			n.State = "pending"
			delay := q.cfg.RetryBase
			for i := 1; i < n.Attempts && delay < q.cfg.RetryMax; i++ {
				if delay > q.cfg.RetryMax/2 {
					delay = q.cfg.RetryMax
				} else {
					delay *= 2
				}
			}
			if failure != nil && failure.RetryAfter > delay {
				delay = failure.RetryAfter
			}
			if delay > q.cfg.RetryMax {
				delay = q.cfg.RetryMax
			}
			n.NextAttempt = q.now().Add(delay).UnixNano()
		}
		return json.Marshal(&n)
	})
}

func (n *Notification) String() string {
	return fmt.Sprintf("notification %s (%s, %s)", n.ID, n.Kind, n.State)
}
