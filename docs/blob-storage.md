# Immutable blob storage

English | [简体中文](zh-CN/blob-storage.md)

## Separation from scheduler state

The `blob` package stores immutable log chunks and index pages independently of
`storage.Storage`. Scheduler state still needs `AtomicStorage.Update`; the blob
adapter does not provide compare-and-swap, append, transactions, or expiry.

The foundation exposes adapters, a registry, and layout initialization and
validation helpers. Connecting the job-log repository, daemon, offline CLI,
complete backups, and migration is handled by the following changes in the
split-storage series. Adding the foundation alone does not switch existing logs
to a new format. Legacy S3 and Redis metadata behavior is unchanged.

Callers allocate each object key before uploading and retry that key only with
identical bytes. A successful object write does not publish a log: publication
belongs to the atomic metadata root. SHA-256 and exact byte length identify the
content; ETags are not part of this protocol. This is application-managed
immutability, not a server-side WORM guarantee.

## Adapter contract

`blob.Store` supports `Put`, `Open`, `Stat`, and `Delete`. The first release limits
each object to 64 KiB, including index pages. `Put` validates the declared length
and SHA-256 before any backend write. Larger content must be split by its owner.

The S3 adapter uses the OtterIO SDK for ordinary, known-length object requests.
It never adds `If-Match` or `If-None-Match` and never constructs the S3 metadata
adapter or its conditional-write probe. Requests preserve raw bytes, have a
30-second timeout through body reads, and keep shorter caller deadlines. An
uncertain write result is returned to the repository for reconciliation against
the same key; permission and missing-bucket failures are not missing objects.

The Filesystem adapter writes and synchronizes a private temporary file, then
publishes it without replacing an existing key. Existing identical content is an
idempotent retry; conflicting content is an integrity error. Parent directories
are synchronized. Object paths reject traversal and symlinks. A later inventory
can remove unpublished temporary files older than one hour.

Filesystem blobs and metadata must use separate directories. A local directory
on one host does not become shared storage merely because another host uses the
same path string.

## Inventory

`blob.Inventory.Scan` accepts a prefix, opaque cursor, and limit of 1–1000. It is
not a consistent snapshot and normal reads never depend on enumeration.

S3 pages use bounded ListObjects requests. Filesystem inventory keeps at most
eight directory walkers and examines at most `max(256, 2 * limit)` entries per
call, without loading or sorting entire directories. Filesystem pages are not
sorted; a page may be empty with a nonempty cursor. Those cursors are single-use,
expire after thirty minutes of inactivity, and do not survive process restarts.
On `blob.ErrInvalidCursor`, restart from the beginning. Periodic inventories must
also restart so that late writes and objects missed during concurrent changes
are eventually discovered.

## Registry and binding identity

`BlobStorageConfig` contains a `write_store` and 1–16 named `stores`. Store IDs
use letters, digits, `_`, and `-`. Each ID must have a distinct physical location.
Filesystem stores require `engine: Filesystem` and `base_dir`. S3 stores require
`engine: S3`, `bucket`, and credentials; custom OtterIO endpoints should use
`path_style: true`. Optional fields include `endpoint`, `region` (default
`us-east-1`), and `prefix`.

S3 credentials can come from `access_key` / `secret_key`, or
`access_key_file` / `secret_key_file`. A value and its file cannot both be set.
When neither is set, the registry uses `OWLCRON_BLOB_ACCESS_KEY_ID` and
`OWLCRON_BLOB_SECRET_ACCESS_KEY`. `session_token` can fall back to
`OWLCRON_BLOB_SESSION_TOKEN`. Keep credential files outside version control.

Bindings fingerprint the bucket and prefix, or the resolved Filesystem directory.
Credentials and endpoints are excluded so they can rotate, but a read-only
namespace-marker check must still prove that the client reaches the same data.
An endpoint change is not permission to silently substitute an empty bucket.

## Explicit layout initialization

`InitializeStorageLayout` is an offline helper for a fresh metadata target. It
first atomically reserves a random namespace in `global/storage_layout`, then
checks ordinary PUT/HEAD/GET/DELETE and writes per-store identity markers. Its
final phase is `ready`, `restoring`, or `migrating`. An interrupted `initializing`
phase resumes the same namespace only with identical bindings and target phase.

`ValidateStorageLayout` accepts only `ready`; the phase-specific variant supports
offline recovery. `Readiness` reads only the small identity markers. It does not
create missing markers, replace bindings, or mutate application data. The set of
bindings is fixed for the initialized layout; changing the selected writer among
those stores leaves existing references bound to their original store ID.

All writers must validate the layout before mutations. Older binaries do not
understand this marker and must be stopped or isolated through a fresh metadata
directory/prefix and deployment credentials. Restoring an old snapshot must use
a fresh namespace; upload sequence history cannot be rewound in place.

The shared `OpenConfiguredMetadata` helper preserves legacy defaults. When blob
storage is configured, the first-release support matrix permits Filesystem or
Couchbase metadata. Couchbase requires `durability: persist_to_majority`, even
when new logs use v1, because existing v2 roots still need durable sequence and
deletion history. Ordinary Couchbase configurations can explicitly choose
`none`, `majority`, `majority_and_persist_to_active`, or `persist_to_majority`;
omitting the option preserves the previous SDK default. Unsupported durability
or topology errors are returned rather than downgraded.
