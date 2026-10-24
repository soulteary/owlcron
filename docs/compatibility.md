# Cronicle compatibility

English | [简体中文](zh-CN/compatibility.md)

OwlCron aims to replace Cronicle with a Go implementation and provides selected compatibility to support migration. Compatibility currently covers the behaviors described below; it does not imply that every API, storage format, or node protocol is interchangeable. Go nodes cannot join an original Node.js cluster. Validate existing clients, plugins, and data migrations against the scope of this guide.

See [configuration](configuration.md), [deployment](deployment.md), and [known limitations](limitations.md).

## Scheduling and plugins

- Scheduled and on-demand events, time zones, catch-up, delays, persistent queues, concurrency limits, retries, and success/failure chains are supported. `timing:false/null` means on demand. Manual runs may execute disabled events; disabled chain targets are not queued.
- Plugins receive one line of job JSON on stdin and report progress, completion, results, and chain data as JSON lines on stdout. Ordinary stdout/stderr becomes log output. Non-detached plugins must explicitly report completion; exit code 0 alone is insufficient.
- The command, working directory, uid/gid, and environment come from plugin configuration. Events, API overrides, and job parameters cannot replace that execution context. Parameters are validated against the plugin schema; hidden parameters come from the plugin definition.
- Argument parsing supports quotes and empty arguments. Pass shell expressions explicitly to a shell; not every original `shell-quote` behavior is supported.
- Detached execution is rejected. Background detachment, continued execution across restarts, and takeover of old PIDs are not supported.
- Plugin `update_event` is disabled by default. When enabled, the original event ID remains fixed and updates are validated. HTML reports are sanitized server-side; arbitrary inline styles, custom `sanitize-html` configuration, and arbitrary frontend filter functions are not supported.

## APIs and browser interface

- The included interface is adapted to the Go APIs. Changing the server URL alone is not enough to establish compatibility for external Cronicle clients.
- `get_schedule` returns a full configuration snapshot including schedule, categories, and plugins, not the original endpoint's paginated `rows/list` contract. `get_event` accepts IDs only, not title lookup or the original complete queue/jobs response shape.
- History uses paginated `rows/list` responses; details include the job, completion state, and a log token. Some error codes and HTTP authorization failures differ from the original service.
- Sessions, API keys, categories, and server-group scopes are checked server-side. Authorization applies to existing objects and the effective target after updates or overrides. WebSockets filter by recipient and revalidate credentials. Global statistics remain aggregate values.
- Log tokens are read-only, time-limited, and bound to one job. The browser polls authenticated POST endpoints; the original Socket.IO log-watcher protocol is not implemented.
- Activity records use Go engine fields and do not reproduce every Pixl activity type or display detail.

## Storage, history, and logs

- Filesystem, S3, and Couchbase use OwlCron's own records, indexes, and backup format. They are not migration adapters for an existing Pixl storage directory.
- Cluster nodes must share one namespace with the required atomic update semantics. Filesystem clusters must explicitly enable `shared_storage` and use a filesystem with suitable inter-process locks and durability. Separate local directories do not form shared cluster storage.
- History writes are idempotent by job ID. Durable deletion markers coordinate retention with recovery. Set retention for your workload rather than treating defaults as capacity planning.
- Logs use 64 KiB chunks and committed offsets. Reads default to 64 KiB and are capped at 1 MiB per request. Follow cursors or stream downloads for complete logs; one response need not contain the whole log. The default per-job hard limit is 64 MiB.
- Legacy Filesystem/S3 logs retain a read-compatibility path. Legacy Couchbase binary logs require offline migration.
- During backend outages, local completion spools, logs, and notification queues participate in recovery. Persist these directories. Data not written before a forced termination is not guaranteed to survive. A timeout means the result is unconfirmed, not that the write was never committed.

## Cluster and failure semantics

- Nodes use `/cluster/ws`, WSS by default, mutual Ed25519 challenges, and optional mTLS. Browsers use `/ws`. Ordinary job frames do not have separate per-message signatures. Migrate older plaintext or shared-secret node configuration; `cluster_allow_insecure` is for explicit development testing only.
- Leadership authority comes from the shared-storage CAS lease and its owner. Each frame rechecks node registration, role, and connection generation. Failover uses demotion, reconnection, and fencing rather than the original conflict behavior of simply exiting.
- Only inbound traffic authenticated as the current owner refreshes node liveness. Sending status or seeing a valid shared lease does not prove peer connectivity. Queued frames cannot restore a revoked connection's authority.
- Workers persist completion results and logs, then resend the completion outbox until the leader acknowledges the complete commit. Fully persisted duplicates can be acknowledged immediately. This does not mean a worker has started when dispatch returns, or that every unconfirmed dispatch is automatically resent after disconnecting.
- On restart, the engine verifies execution markers and kernel process identity before reclaiming old plugin process groups and recording interrupted jobs as failed. It does not blindly repeat commands with external side effects. Unverifiable PIDs or unavailable platform identity checks require operator review.
- Stable IDs, idempotent history, leases, and outboxes do not provide exactly-once business side effects. Plugins must handle idempotency and uncertain execution outcomes.

## Email, WebHooks, and HTTP security

- SMTP supports authentication, STARTTLS/implicit TLS, timeouts, and multiple recipients. Email and WebHooks use persistent queues, retries, and dead-letter state. Delivery is at least once, so lost acknowledgment can cause duplicates. Validate real credentials, connectivity, and recipient behavior during deployment.
- WebHooks default to public HTTPS destinations. Private-network or HTTP exceptions require explicit configuration. DNS answers and dialed addresses are checked; environment proxies and redirects are disallowed. Separate HMAC-SHA256 signing secrets and protected key files are supported. Notifications and diagnostics redact sensitive fields.
- Login and Basic Auth share process-local IP/account rate limits and a password-verification concurrency cap, not a cluster-wide limit. Forwarded headers are trusted only from configured proxies. Cookies apply consistent Secure, HttpOnly, and SameSite policies.
- Ordinary HTTP APIs and streaming logs have separate deadlines. WebSockets use their own lifecycle. Cross-origin write checks remain enabled; the original option to disable that protection is not reproduced.
