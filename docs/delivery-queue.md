# Durable delivery queue contract

English | [简体中文](zh-CN/delivery-queue.md)

This page describes the persistent notification queue inside OwlCron. The queue
remains in `engine`; it is not a separate package or a published `delivery-kit`.
See the [implementation](../engine/notification_queue.go),
[queue regressions](../engine/notification_queue_test.go), and
[application integration](../engine/notification_integration.go).
The narrowed boundary also has [store contract tests](../engine/notification_store_contract_test.go)
and [storage adapter tests](../engine/notification_storage_test.go).

## Interface boundary

[`engine.NotificationStore`](../engine/notification_contract.go) contains only
the operations the queue uses:

```go
Get(context.Context, string) ([]byte, error)
Update(context.Context, string, func([]byte) ([]byte, error)) error
ListKeysPrefix(context.Context, string) ([]string, error)
```

`NewNotificationQueue` accepts that interface and a named
`NotificationDeliverer` callback, whose signature is
`func(context.Context, *Notification) error`. The queue interprets scheduling and
delivery outcomes; the callback interprets `Kind` and `Payload`. The engine's
[storage adapter](../engine/notification_storage.go) checks backend capabilities
and retains `storage.ErrUnsupported` for an unsupported backend. Requiring three methods does not itself prove that a
custom implementation provides the required atomicity or durability.

At the Go call site, a value statically typed as `storage.Storage` needs a
capability check to obtain `NotificationStore`; the wider interface does not
declare `Update` or `ListKeysPrefix`. OwlCron performs that check at startup in
the [storage adapter](../engine/notification_storage.go).

The queue does not require generic list mutations, stream operations, expiration,
transactions, or deletion from [the wider storage API](../storage/storage.go).
It still uses OwlCron's storage errors and its existing, unversioned JSON record
format; this work does not introduce an independent error or format-version contract.

## Store requirements

- `Get` reads a record and identifies a missing record through
  `errors.Is(err, storage.ErrNotFound)`.
- `Update` must serialize a single key's read/modify/write across processes and
  storage instances sharing a namespace. A process-local mutex alone is
  insufficient for a remote backend. Missing or expired records are presented to
  the callback as empty input. A callback error aborts that candidate write and
  must remain discoverable through `errors.Is` if the store wraps it;
  returning `nil, nil` is a value to write, not a portable no-op instruction.
- An update callback may run more than once after compare-and-swap conflicts.
  It must recompute its result from each supplied value and must not send a
  message or perform other external side effects. Stores must honor context
  cancellation and bound their own conflict retries. Claim selection is reset on
  each callback invocation; delivery happens only after the update returns
  success. See [remote CAS](../storage/remote.go) and
  [Filesystem locking](../storage/filesystem.go).
- A successful update inherits the backend's existing commit acknowledgment
  guarantees, not additional replica or node-power-loss durability; current
  [Couchbase Insert/Replace](../storage/couchbase_impl.go) sets no explicit durability level.
  A timeout, cancellation, connection failure, or Filesystem error after
  replacement/synchronization can leave the write outcome uncertain: an error
  is not proof that nothing committed. Retrying
  admission with the same stable ID and recovering claims after their leases
  expire are part of handling this uncertainty.
- `ListKeysPrefix` enumerates live keys from the requested namespace and may
  read record bodies to check expiry. It is not a transactionally consistent
  snapshot of all records. A listed record can disappear before `Get`; `List`
  tolerates that missing record. Enumeration failures must be returned rather
  than presented as an empty queue. See [prefix enumeration](../storage/key_prefix.go).

The queue provides no cross-key transaction or external-side-effect transaction.
Its namespace, `notifications/`, must be shared consistently by cooperating
nodes. Lease times use wall-clock Unix nanoseconds, so clock synchronization and
backend availability remain deployment requirements.

## Admission and persisted identity

The record key is `notifications/` plus the SHA-256 digest of the notification
ID. Admission accepts an ID of 1–1024 bytes, a kind of 1–64 bytes, and a
JSON-encoded payload of at most 256 KiB. The size limit applies after JSON
encoding, not to the caller's input object size.

Admission is **first write wins for the same ID and kind**. A repeated admission
with a different payload succeeds without replacing the original record or
creating another delivery. A record with a different ID or kind at the same key
returns `ErrNotificationConflict`. `ContentHash` records a digest of the initial
kind and encoded payload; it is not currently a strict content-conflict check.
Do not infer stricter deduplication from that field or from the error's name.

Existing JSON field names and state strings remain the persisted contract.
`sent` records retain their identity and content hash while discarding their
payload. They remain deduplication markers; the queue does not expire them
automatically. Dead records retain their payload for manual replay. Removing
these records changes deduplication and recovery behavior.

## Claims, outcomes, and retry budgets

- New records enter `pending`. A due record, or an `inflight` record whose lease
  expired, may be claimed atomically. Claiming increments `Attempts`, records a
  random lease token and expiry, and changes the state to `inflight`.
- An unexpired lease is not taken over. A completion update must still match the
  stored `inflight` state and lease token. There is no lease renewal. A
  superseded worker cannot acknowledge
  a newer claim or overwrite its outcome. This fences storage updates, not an
  external recipient's actions. Lease expiry alone does not invalidate an
  acknowledgment while the same token still owns the record.
- A successful delivery becomes `sent`, with payload and diagnostic cleared.
  An ordinary delivery error retries as `pending`, or becomes `dead` when the
  attempt budget is exhausted. An expired claim already at the maximum becomes
  `dead` without another delivery.
- `NotificationDeliveryError.Permanent` makes the failure terminal.
  `Deferred`, when not permanent, returns the record to `pending` and restores
  the claimed attempt to the budget. Other errors are retryable until the budget
  is exhausted. Classification works through wrapped errors using `errors.As`.
- Ordinary retry delay doubles from `RetryBase`, is raised by a larger
  `RetryAfter`, and is capped at `RetryMax`. Deferred delay is
  `min(24 hours, max(RetryBase, RetryAfter))`; it uses that separate cap.
- `RetryDead` accepts only a dead record and resets its attempt count, schedule,
  claim, and last error. It preserves the original identity and payload.

Delivery is **at least once**. A recipient can accept a message before the
sender crashes or fails to persist acknowledgment. Lease recovery may then
deliver it again. Callbacks must treat the record as read-only, tolerate repeated
invocation, pass the stable ID to a recipient that can deduplicate, and honor
cancellation. The delivery
timeout is a context deadline, not forced termination of callback code. A
callback that ignores it can keep a worker occupied and overlap a later claim.

If the parent context is canceled after claiming, the queue leaves the claim
durable for lease-based recovery. A callback returning success after its delivery
context expired is treated as failed. Delivery diagnostics pass through the
configured application redactor and are truncated to 1024 raw bytes before JSON
encoding. If that cuts a UTF-8 sequence, JSON's replacement of invalid bytes can
make the decoded diagnostic slightly larger than 1024 bytes; this is not a
strict persisted-byte-length guarantee.

`RunOnce` returning nil does not mean every recipient accepted its message:
delivery failures normally become persisted retry or dead-letter state. Storage
and processing failures are returned; a pass reports its first observed error.
Inspect the records to determine delivery outcomes.

## Limits and enumeration cost

Zero configuration values select defaults: 4 workers, a batch of 100, 8 attempts,
a 30-second delivery timeout, a 60-second lease, a 5-second retry base, and a
1-hour retry maximum. Validation permits 1–64 workers, batches of 1–10000,
and 1–100 attempts. Durations must be positive, the lease must exceed the
delivery timeout, and the retry maximum must be at least its base.

`RunOnce` enumerates **all** keys in `notifications/`, sorts them, and then
chooses a rotating batch. `BatchSize` bounds selected records, not enumeration
time, memory, total record count, or backend reads. The cursor is local to a
queue instance and is not a durable ordering guarantee. `Workers` bounds
callbacks within that instance, not total concurrency across nodes.

`List` also enumerates the namespace before reading records and filtering by
state. Its result limit defaults to 100 when outside 1–1000; it does not bound
the number of nonmatching records read. The HTTP API applies its own stricter
request validation and access control.

## Responsibilities retained by the application

The engine retains notification target selection, stable business ID generation,
payload preparation, local spool fallback/replay, maintenance and shutdown,
account-email rules, credentials, error redaction, and SMTP/WebHook transport.
Those paths are not part of the narrowed storage interface. Queue records may
contain private payloads and claim tokens; administrator authorization and API
field removal remain in [the notification handlers](../api/notifications.go).
See [notification configuration](configuration.md#email-webhooks-and-keys) and
[limitations](limitations.md#external-side-effects-and-notifications).

## Conditions for evaluating delivery-kit later

The single-key claim/retry state machine and a narrow store/callback seam are
plausible extraction candidates. A small interface alone does not make them
ready for independent release. Before creating a library, establish:

- A real second application with the same semantics; multiple channels inside
  OwlCron do not by themselves establish that demand.
- Independent error contracts, a versioned persisted-format and compatibility
  plan, and a clear migration path for OwlCron's existing keys and markers.
- A reusable remote-backend contract suite for atomic callback retries, uncertain
  writes, cross-instance claims, stale acknowledgments, and recovery. Existing
  Filesystem queue regressions and backend CAS tests are useful evidence, but
  do not validate every deployment or adapter.
- Explicit deployment boundaries for shared namespaces, clocks, callback
  cooperation, lease overlap and scan scale, with measurements at the intended
  queue sizes. Do not advertise bounded reads or exactly-once delivery on the
  basis of the current batch and lease settings.

This work keeps those decisions inside OwlCron while making the present queue
contract easier to test and review.
