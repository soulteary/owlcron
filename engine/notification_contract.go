package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// NotificationStore is the single-key persistence contract used by the queue.
// It deliberately does not require the application's list, stream, expiry or
// transaction operations. Implementations must coordinate across processes that
// share a namespace, not just across goroutines in one queue.
type NotificationStore interface {
	// Get returns the stored bytes, or an error matching storage.ErrNotFound for
	// a missing or expired record. Other errors must not be treated as absence.
	Get(context.Context, string) ([]byte, error)

	// Update atomically reads, transforms and persists one record under the
	// backend's existing commit guarantees; it adds no replication durability.
	// A missing or expired record is passed to fn as nil. CAS implementations
	// may invoke fn repeatedly with newer bytes; fn must not perform delivery or
	// other external side effects. A callback error aborts that update and must
	// remain discoverable with errors.Is. A nil result with no error writes an
	// empty value; it is neither a delete nor a no-op.
	//
	// Only a nil return confirms the commit. An error may leave the outcome
	// unknown (for example, a response lost after a remote commit). Callers must
	// recover through the stable ID and lease rather than assume no write took
	// place. Implementations must honor ctx and bound their own conflict retries.
	Update(context.Context, string, func([]byte) ([]byte, error)) error

	// ListKeysPrefix enumerates the namespace, without a snapshot guarantee.
	// It may read record bodies to check expiry. Keys can disappear or change
	// before Get/Update; the queue rechecks eligibility in Update. Enumeration
	// is complete, not paged or bounded by NotificationQueueConfig.BatchSize.
	ListKeysPrefix(context.Context, string) ([]string, error)
}

// Notification is the existing durable JSON record. ID and Kind identify the
// first admitted message; repeating them preserves its original payload, even
// when the new payload differs. ContentHash is retained diagnostic metadata,
// not an additional admission constraint. Successful delivery removes Payload
// but retains the record for deduplication. Timestamps use Unix nanoseconds.
type Notification struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	ContentHash string          `json:"content_hash"`
	State       string          `json:"state"`
	Attempts    int             `json:"attempts"`
	NextAttempt int64           `json:"next_attempt,omitempty"`
	LeaseUntil  int64           `json:"lease_until,omitempty"`
	LeaseToken  string          `json:"lease_token,omitempty"`
	CreatedAt   int64           `json:"created_at"`
	UpdatedAt   int64           `json:"updated_at"`
	LastError   string          `json:"last_error,omitempty"`
}

// ErrNotificationConflict reports an existing record with a different ID or
// Kind. Its historical error text is preserved; payload differences alone do
// not produce this error.
var ErrNotificationConflict = errors.New("notification ID already has different content")

// NotificationDeliverer performs one attempt after a durable lease is acquired.
// It must respect ctx, treat the record as read-only, and use its stable ID for
// recipient-side idempotency where supported. Delivery is at least once: a
// timeout or failure to persist the acknowledgment can follow external success.
// Lease fencing prevents stale state updates, not duplicate external effects.
//
// Returning nil means acceptance unless the delivery context expired. A canceled
// parent leaves the claim for lease recovery; otherwise an expired attempt is
// recorded as a delivery error. Ordinary errors consume an attempt and retry;
// NotificationDeliveryError refines that policy. Errors must not expose
// credentials. Payload interpretation and transport security belong to callers.
type NotificationDeliverer func(context.Context, *Notification) error

// NotificationDeliveryError classifies a failed attempt, including when wrapped.
// Permanent takes precedence over Deferred and moves the message to dead.
// Deferred refunds the attempt and delays min(24h, max(RetryBase, RetryAfter)).
// Other failures use exponential backoff, with RetryAfter as a minimum and
// RetryMax as the cap. Exhausted ordinary attempts move the message to dead.
type NotificationDeliveryError struct {
	Err        error
	Permanent  bool
	Deferred   bool
	RetryAfter time.Duration
}

func (e *NotificationDeliveryError) Error() string {
	if e.Err == nil {
		return "notification delivery failed"
	}
	return e.Err.Error()
}

func (e *NotificationDeliveryError) Unwrap() error { return e.Err }

// NotificationQueueConfig bounds one queue instance's workers and processing
// batch, not namespace enumeration. Zero fields select the existing defaults:
// 4 workers, 100 records, 8 attempts, 30s timeout, 60s lease, 5s base, 1h max.
// LeaseDuration must exceed DeliveryTimeout; clocks across nodes must agree
// closely enough for lease expiration. There is no lease renewal.
type NotificationQueueConfig struct {
	Workers, BatchSize, MaxAttempts                     int
	DeliveryTimeout, LeaseDuration, RetryBase, RetryMax time.Duration
}
