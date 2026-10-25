# Architecture

English | [简体中文](zh-CN/architecture.md)

This guide describes component responsibilities, persistence boundaries, and recovery. Read the [known limitations](limitations.md) before deploying; recovery mechanisms do not replace backups or business-level idempotency.

## Components

The Go module lives at the repository root; see [go.mod](../go.mod) for toolchain metadata. The server handles scheduling, plugin execution, HTTP APIs, WebSockets, and storage. The browser interface is built into static assets. Running the service does not require the original Cronicle daemon. Deployers must supply any shell, Node.js, Python, or other runtime required by their plugins.

See [compatibility](compatibility.md) for supported Cronicle behavior and migration requirements.

- [Entry point](../main.go): configuration, listener setup, process lifecycle, and exit status.
- [Engine](../engine/engine.go): node roles, maintenance, leadership leases, and shutdown coordination.
- [Scheduler](../engine/scheduler.go): event snapshots, minute scheduling, persistent cursors, catch-up, and event queues.
- [JobManager](../engine/job_manager.go): target selection, concurrency reservations, plugin processes, resource limits, cancellation, retries, and completion archival.
- [Communication](../engine/communication.go) and [node message handling](../engine/websocket_handler.go): authentication, heartbeats, remote dispatch, completion replay, and acknowledgments.
- [Storage](../storage/storage.go): Filesystem, S3, Couchbase, and Redis records, lists, atomic updates, and backups.

An `Event` is configuration, a `Job` is an execution snapshot, and a `Plugin` defines an administrator-approved executable. Queued and remote work retains the parameters accepted at admission time; later event edits must not change an accepted job's execution semantics.

## Persistence and consistency

Filesystem uses cancellable in-process locking and a cross-process exclusive file lock. Writes synchronize temporary files before atomic replacement. A durable redo journal recovers multi-record transactions. Reads take the same lock, so throughput and mount latency affect the whole node. S3 uses conditional requests; Couchbase uses CAS. Redis requires a single Redis 8.4+ instance and uses native SET NX/IFEQ with the same document envelope, logical expiry, and tombstones; the adapter does not execute Lua. Transport errors are not CAS conflicts, and writes are not implicitly replayed. Backend capability, timeouts, and conditional conflicts must be handled explicitly.

Persistent state includes:

- `events:<id>` and event indexes: canonical configuration and recoverable enumeration. Loading traverses all pages and preserves the previous in-memory snapshot if a record or page cannot be read.
- `global/master_lease` and `global/state`: leadership owner, lease expiration, and the generation owning global state.
- `active_jobs`, `global/event_queue/<eventID>`, and `queue_admissions:<token>`: active reservations, queued snapshots, and stable job IDs.
- `jobs:<id>` and `history/completed/<encoded ID>`: canonical job records and per-job history indexes.
- `job_log_manifest/<encoded ID>` and `job_log_chunks/<encoded ID>/…`: committed log offsets, completion state, and chunks.
- `remote_completions`: completed worker results awaiting acknowledgment.
- `notifications/…` and `notification_plans/…`: notification state and the initially selected delivery targets.
- `deleted_jobs:<id>`, log deletion markers, and `history_cleanup/…`: tombstones and recoverable cleanup intentions.

Single-key atomic updates are not cross-key transactions. Job records, history indexes, logs, notifications, and active reservations are committed in stages; stable IDs, completion spools, and replay repair intermediate states. Filesystem journal replay does not provide database snapshot isolation. Cluster nodes must share one namespace with correct backend atomic operations.

## Execution and recovery

Admission reserves concurrency capacity before local or remote dispatch. Plugins receive job JSON on stdin. Structured stdout messages report progress and results; ordinary stdout and stderr become logs. Explicit completion, process exit, cancellation, and log write errors determine the outcome. A successful process exit alone does not satisfy the completion protocol.

Before starting a plugin, the engine persists a random execution marker. It then records the PID and kernel process identity. On restart, it verifies both before reclaiming an orphaned process group and marking the interrupted job failed. It does not kill an unrelated reused PID or automatically repeat a command with possible external side effects. Unverifiable processes require operator attention, and platform visibility may limit automatic recovery. See [process recovery](../engine/process_recovery.go).

Before execution, the engine physically allocates a local `.completion-wal` journal, including space for the complete supported result. Its two slots carry generations, lengths, and checksums; a partial write leaves the previous checkpoint intact. Plugin result fields together are limited to 256 KiB of JSON after report sanitization, including descriptions, reports, performance, chaining, and notification overrides. Insufficient reservation capacity prevents execution. Runtime metadata growth is reserved before acceptance; an oversized plugin result is rejected with a bounded failure diagnostic.

A plugin completion report is synchronized to that journal before any shared-backend update, even while the process is still draining output. Periodic recovery leaves these unsealed reports alone. Startup recovery first reclaims the verified process group, then seals the recorded result and log without replacing the actual code and description with a generic restart failure. The final journal remains until logs, the job record, history, notification admission, reservation removal, and any worker completion outbox have committed. Legacy `.completed.json` spools remain replayable. Put `log_dir` and `queue_dir` on persistent volumes; failed hardware writes or output not yet received and synchronized cannot be recovered from memory after a forced termination.

Workers persist completion envelopes before sending them, and replay unacknowledged envelopes periodically and after reconnecting. The leader acknowledges only after the full completion commit. Already acknowledged duplicates retain the first result and clear residual active state. Finding a completed job record alone does not permit skipping the other commit steps. See [completion storage](../engine/completion_storage.go) and [replay](../engine/state_access.go).

History uses per-job indexes instead of rewriting the complete history list. Logs upload incrementally in 64 KiB chunks with bounded concurrency and per-job size. Batched retention protects active jobs and pending recovery records. Zero retention days disables automatic expiration; per-job limits are not a node-wide disk quota. See [log storage](../storage/job_logs.go) and [retention](../engine/retention.go).

## Clustering, security, and notifications

The leader acquires a shared atomic lease with a random owner. Scheduling, control messages, and global state writes verify that owner; state generations prevent a recovered former leader from overwriting newer state. This is not Raft or majority consensus. It depends on backend atomicity, a shared namespace, and reasonable clock synchronization. See [leadership](../engine/leadership.go) and [state fencing](../engine/state_fencing.go).

Nodes use `/cluster/ws`, require WSS by default, and support a private CA and mandatory client certificates. Ed25519 proofs bind the peer, challenge, role, and lease owner. Node registration, connection identity, and the current lease jointly constrain permissions. Browsers use `/ws`; user sessions cannot authenticate a node.

Heartbeat sending permits at most one pending send per node and participates in shutdown. Slow connections and remote validation do not hold the global peer lock. Read-idle time starts while waiting for the next frame, excluding local processing of the previous frame. Only authenticated inbound messages with the correct lease refresh leader liveness. Outbound status messages do not. Timeout transitions recheck liveness under the role lock, and waiting messages recheck identity and lease so revoked connections cannot regain access.

HTTP handling includes account and key validation, object-scoped authorization, login and Basic Auth rate limits, trusted proxies, secure cookies, and request deadlines. WebHooks enforce destination and DNS restrictions against SSRF through [http-kit/outbound](https://github.com/soulteary/http-kit/tree/v2.1.0/outbound), with a thin [configuration and error adapter](../engine/webhook_policy.go). The shared client checks each request and the actual dial target, including reused connections, and disables proxies and redirects. Signing secrets and sensitive-output redaction remain in OwlCron. Administrator-configured plugins can still run arbitrary programs; they are not sandboxed.

Email supports SMTP authentication, TLS, timeouts, and per-recipient delivery. Email and WebHooks use persistent queues, claim leases, retries, dead-letter state, and local spools. Delivery carries stable identifiers and is at least once: a crash after recipient acceptance but before recording success can cause a duplicate. The queue depends on a three-method storage contract and a delivery callback; OwlCron owns spooling, recipient policy and transports. See the [delivery contracts and extraction assessment](delivery-queue.md), [notification queues](../engine/notification_queue.go), [mail transport](../engine/mail_transport.go), and [WebHook signing](../engine/webhook_signing.go).

Account email is opt-in. One-time verification and recovery actions are bound to
the canonical user, password revision, address and expiry. Token consumption and
the account change share an atomic update; the user index is a discovery aid.
The account mail queue stores references and regenerates/revalidates links at
delivery time. Password changes invalidate existing sessions. See
[account actions](../engine/account_actions.go) and the
[account operations guide](operations.md#account-email-and-password-recovery).
Public registration remains disabled.

See [testing](testing.md) for load and fault scenarios.
