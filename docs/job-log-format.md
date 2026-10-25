# Immutable job-log format

English | [简体中文](zh-CN/job-log-format.md)

This page describes the storage protocol implemented by `JobLogRepository`.
The repository is an explicit log service over atomic metadata and an independent
blob store. Adding the format does not automatically enable it for an existing
deployment. Runtime configuration, initialization and complete backup support
must be available before enabling v2 writes.

## Compatibility and ownership

The logical metadata key remains `job_log_manifest/<base64url-job-id>`. A missing
`format` or `format: 1` selects the existing chunked format. `format: 2` selects
the immutable format. Unknown versions are rejected. Legacy single-object logs
can be read only when no manifest exists. A missing or corrupt object referenced
by a v2 root is an integrity error, never an empty log or a legacy fallback.

New code can read multiple formats, but all writers accessing a v2 namespace must
understand v2. Older binaries do not recognize a storage-layout version fence;
isolate them with deployment configuration, credentials and storage namespaces.
New v2 owners are limited to 180 bytes so their canonical base64url component fits
filesystem object-path limits.

## Bounded representation

| Structure | Bound |
| --- | --- |
| Logical content chunk | 64 KiB |
| One append | At most 64 KiB |
| One index page | At most 128 chunk references and 64 KiB encoded |
| Log size | At most 1 GiB |
| Current root | At most 128 page references and 32 KiB encoded |
| Pending append | At most two chunks and two index pages |
| One range read | At most 1 MiB of requested content |

The metadata store contains one mutable root per log. Both immutable chunk
versions and immutable index pages live in the blob store. Extending a partial
tail creates a replacement object; it does not overwrite the previous tail.
Only affected index pages are copied. Current metadata size depends on log size,
not append count. Old physical versions still consume space until collection.

The root records its namespace, incarnation, store ID, committed offset, final
and deleted flags, page references, `commit_seq`, `issued_seq`, and optional
Pending plan. Every reference includes length and SHA-256. Page bodies contain
the incarnation, page number and relative chunk references, without physical
bucket, namespace or store configuration.

Object keys are:

```text
<namespace>/logs/<base64url-job-id>/<incarnation>/<upload-seq>-<kind>-<nonce>
```

The sequence is a zero-padded 20-digit unsigned integer. Kind is `c` for a chunk
or `p` for a page; incarnation and nonce are 16 random bytes encoded as lowercase
hex. A new reservation never reuses an old object ID. Replays of the same
reservation use exactly the same bytes and IDs. This is application-level
immutability, not a storage-side WORM guarantee.

## Publication

1. Read the root and affected pages. Verify the old tail, freeze the input,
   construct replacement chunks/pages, and allocate all IDs outside callbacks.
2. Atomically validate the committed root, increment `issued_seq`, and install
   the complete Pending plan. No object PUT occurs before this succeeds or its
   ambiguous result is confirmed by reading the root.
3. Upload chunks, then pages using ordinary blob PUT. Existing retry objects
   must match actual length and SHA-256. A lost PUT response is reconciled with a
   bounded GET; HEAD length alone is insufficient.
4. Atomically match the Pending attempt and base commit sequence, publish page
   references and the new offset, clear Pending, and increment `commit_seq`.

Step 4 is the sole visibility point. There is no cross-store transaction and no
per-append journal. Pending carries the recovery plan; owner/sequence-bearing
keys identify later garbage. Atomic update callbacks can run repeatedly and must
not perform external I/O or allocate new identities.

Identical range/content retries resume the original plan. A live upload reserved
with `final: false` can be retried after the process completes; its original final
intent is preserved. The completion path then seals the committed length with an
empty append. A final root rejects further content. Retries of already committed
bytes compare the actual committed range before returning success.

An error can follow a successful metadata write. Re-read the root before deciding
whether publication happened. An unavailable acknowledgment does not authorize
deleting uploaded objects or allocating a replacement for a still-active plan.

## Deletion and collection

Logical deletion atomically sets `deleted` and `final`, clears Pending and current
page references, and keeps the incarnation and highest issued sequence forever.
The existing task-level deletion flow remains responsible for its own markers.
Expired Pending is revoked through an atomic root update; a deadline alone never
authorizes deletion.

For each owner, collection reads a root snapshot and records watermark
`H = issued_seq`. It fully marks current index-page objects and the chunks they
reference, then marks every object allocated by Pending, including objects not
yet uploaded. It sweeps only unmarked objects of the same incarnation with
sequence at most H. Later reservations have larger sequences and cannot be swept.

The proof requires that retired references are never republished, new references
are registered before PUT, and sequence history never rewinds in the same
namespace/incarnation. Content deduplication and rollback to old index pages are
not supported by this protocol. Restore into a fresh namespace.

Collection uses bounded blob inventory pages rather than materializing metadata
keys. Inventory is not a snapshot. Cursors may expire; restart from the beginning
on `blob.ErrInvalidCursor`. Keep the returned cursor even when a particular
owner's mark fails: that owner is skipped, other owners can progress, and a later
full scan retries it. Root/page errors never mean an empty reference set.

Grace periods reduce races with readers. If an old page disappears after the
root advances, range reads retry against the latest root within a bounded retry
budget. If the latest root still references a missing object, reading fails.
Unknown object modification times are retained when a grace period is configured.

A delayed PUT can finish after DELETE and leave an unreachable object. Repeated
inventory reclaims it; one sweep cannot promise permanent physical emptiness.
The garbage collector never deletes an entire upload prefix, because one upload
can contain both a still-live full chunk and a retired tail version.

## Offline administration and validation

`VisitCommittedObjects` streams verified current pages and chunks, rejects
Pending, and checks that the root did not change during the visit. It is not a
cross-log snapshot: administrative backup must stop writers and collectors.
Rebinding a root preserves sequence/fence information and rewrites its physical
namespace/store binding. Page bodies remain unchanged, allowing byte-for-byte
copying into a fresh namespace.

`go test -race ./storage -run '^TestJobLogV2'` covers publication failures,
ambiguous acknowledgments, competing writers, tiny appends, chunk/page boundaries,
deletion and delayed uploads, collection/read races, format fencing and independent
namespace restoration. Real-service and process-fault acceptance complement these
protocol tests; see [testing](testing.md).
