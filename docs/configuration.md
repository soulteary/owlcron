# Configuration and runtime behavior

English | [简体中文](zh-CN/configuration.md)

See [config.json](../config.json) for a sample and [types.Config](../types/types.go) for the full field definitions. Pass `-config` on the command line to select a file. Relative paths are resolved from the working directory.

For startup, proxy configuration, and administrator initialization, see [deployment](deployment.md). Restart the service after changing configuration; do not assume running nodes reload files automatically.

## Basic settings

- `hostname`: a unique node name. Set it explicitly in a cluster and keep it consistent with node registration, public keys, and certificates.
- `base_app_url`: the external application URL, used for links and origin checks. It does not restrict the listening address.
- `http_port`: the HTTP port, default 3012. The service listens on all interfaces; restrict access with a firewall or proxy.
- `secret_key`: a random secret of at least 32 bytes, also configurable through `OWLCRON_SECRET_KEY`. Protect configuration and key files.
- `storage_engine`: `Filesystem`, `S3`, or `Couchbase`. The local default is Filesystem.
- `storage_base_dir`: the Filesystem data directory, default `data`.
- `log_dir` and `queue_dir`: local job logs and recovery queues, default `logs` and `queue`.
- `udp_broadcast_port`: discovery port. Set it to `-1` to disable discovery. Discovery does not replace node authentication.

The JSON blocks below are configuration fragments, not complete deployment files.

## Storage, backups, and recovery

Supported backends are Filesystem, S3, and Couchbase:

- Filesystem uses cross-process exclusive locking, atomic replacement after synchronization, and a multi-record redo journal. In-process lock waits respect cancellation; an HTTP timeout does not roll back a commit already in progress. Set `shared_storage` explicitly when multiple nodes use the same directory. The mount must support the required locks and atomic replacement; do not assume every NFS implementation does.
- S3 must implement `If-Match`, `If-None-Match`, and concurrent conditional writes correctly. The engine probes support before its first modification and rejects incompatible services. Credentials need object read/write and prefix-list permissions for the test and application namespaces.
- Couchbase uses SDK CAS. Startup recovery and complete enumeration require a 7.6+ server with range scans and appropriate permissions. Remote backends do not provide cross-key ACID transactions; canonical records and indexes use retryable, idempotent updates.

Event loading traverses all pages and preserves the previous in-memory snapshot on failure. Prefix enumeration recovers orphaned legacy event records; custom adapters must implement `storage.PrefixKeyLister`. List pagination may still decode the whole index, so it is not a constant-memory or maximum-capacity guarantee.

After a forced daemon termination, restart recovery verifies execution tokens and kernel process identity, reclaims verified process groups, and marks interrupted jobs failed. It does not resume or repeat plugins. `OWLCRON_JOB_TOKEN` is reserved. An unverifiable live legacy PID prevents startup and requires operator review; never kill a process based only on a reused PID. Preserve OS identity and process-inspection permissions across restarts. Plugins that deliberately detach or remove the marker are outside this recovery contract; detached execution is unsupported.

See [architecture](architecture.md) for completion commits, worker acknowledgments, and log recovery. Persist `log_dir` and `queue_dir`; buffered log tails are not guaranteed to survive. Use job IDs or business keys for plugin idempotency and stable message identifiers for recipient deduplication. See [deployment](deployment.md#backups-upgrades-and-rollback) for backup and import procedures.

### Retention and log limits

Retention is disabled by default. Zero days means no time-based deletion. Example:

```json
{
  "job_data_expire_days": 180,
  "job_log_expire_days": 30,
  "history_cleanup_interval": 60,
  "history_cleanup_batch_size": 100,
  "job_log_hard_max_size": 67108864,
  "log_upload_concurrency": 4
}
```

History expiration uses a completed job's `time_end`; old records without a completion time are not assumed expired. History expiration removes the job record, indexes, and logs. Log-only expiration retains history and details. Active jobs, pending recovery spools, and unacknowledged remote completions are protected. Durable deletion intentions can be replayed; lightweight tombstones remain to prevent late completions from restoring deleted data. Do not manually remove these tombstones or notification idempotency markers.

Completion writes small per-job history indexes instead of rewriting all history. HTTP history queries still aggregate retained jobs; prefix enumeration, local scans, and tombstones grow with data volume. Retention is not a node-wide disk quota. Monitor disk usage and cleanup backlogs separately.

Logs use 64 KiB chunks and a manifest, uploading incrementally from the durable offset. Failures retain local content. Defaults are four concurrent uploads and a 30-second synchronization timeout. The per-job hard limit defaults to 64 MiB; zero selects that default, and the maximum is 1 GiB. Event and category limits cannot exceed the hard cap. Exceeding it or failing a disk write terminates the job.

Log APIs accept byte `offset` and `limit`, defaulting to 64 KiB with a 1 MiB maximum. Decode `log_base64` as raw bytes and follow `next_offset`; `eof` describes only the current snapshot. GET `get_job_log` or `get_live_job_log` with `stream=1&download=1` streams a download. The interface displays roughly the most recent 1 MiB without limiting full downloads. Legacy Filesystem/S3 logs support bounded reads; legacy single-object Couchbase binary logs require offline migration and do not support range reads.

## Login, proxies, and HTTP limits

Disabling, deleting, or changing the password of an account invalidates its sessions. Deleting or disabling an API key takes effect immediately. Ordinary users cannot elevate their own privileges. Logs render as plain text; plugin HTML reports retain only allowed formatting tags.

JSON login and Basic Auth share process-local limits: 20 attempts per IP, 10 per account and IP/account pair, in a 60-second window. Successful attempts also count. Settings are `login_rate_limit`, `login_account_rate_limit`, `login_rate_window_seconds`, `login_max_concurrent` (default 4), and `login_limiter_max_entries` (default 10000). Rejection returns HTTP 429 with Retry-After. Restart clears local counters; multi-node ingress needs a separate shared limit. Use sessions or API keys for frequent polling.

The client IP defaults to the direct peer. Configure only controlled proxy networks in `trusted_proxy_cidrs`. X-Forwarded-For is traversed right to left until an untrusted hop is found, and proxies must overwrite client-supplied forwarding headers. Cookies use HttpOnly and SameSite=Lax. Secure is enabled for direct TLS, `session_cookie_secure: true`, or a trusted peer supplying a single `X-Forwarded-Proto: https` header. Force Secure for HTTPS deployments and restrict direct backend access.

HTTP timeout values are seconds. Zero selects the default; negative values and values above 3600 are rejected:

```json
{
  "http_read_header_timeout_seconds": 10,
  "http_read_timeout_seconds": 30,
  "http_write_timeout_seconds": 35,
  "http_idle_timeout_seconds": 60,
  "http_request_timeout_seconds": 30,
  "http_stream_timeout_seconds": 300
}
```

The ordinary request deadline covers authentication and handling. Expiration cancels the context and returns HTTP 503. Keep the write timeout longer than the request deadline so a structured error can be sent. Streaming logs have a total deadline and a 30-second per-chunk write deadline. Backends must honor context cancellation; committed changes are not rolled back. WebSockets use their own protocol lifecycle, not the short API deadline.

## Email, WebHooks, and keys

Notifications are admitted synchronously to a persistent queue. When shared storage is unavailable, they first use the local `queue_dir/notifications` outbox. Defaults are `notification_workers: 4` (range 1–64) and `notification_max_attempts: 8` (range 1–100), with exponential retry delays from 5 seconds to 1 hour. Permanent or exhausted failures become dead letters; claims interrupted by a crash wait for lease expiration. Payloads are limited to 256 KiB. Sent records retain idempotency markers; dead records retain bounded payloads for replay.

SMTP settings are `smtp_hostname`, `smtp_port`, `email_from`, `smtp_username`, `smtp_password`, `smtp_tls_mode`, and `smtp_timeout_seconds`, with optional `smtp_tls_server_name` and `smtp_tls_ca_file`. TLS defaults to `starttls`; `implicit` is also supported. Plaintext relay requires explicit `none`. Authentication requires TLS and both credentials. Certificates are verified with TLS 1.2+. The timeout defaults to 30 seconds and is capped at 300. RFC address lists are deduplicated and each recipient is queued separately. `max_emails_per_day` is a shared persistent UTC-day limit on SMTP attempts; failures count, and waiting for quota does not consume retries.

WebHook requests time out after 10 seconds and do not follow redirects or environment proxies. Transport failures, 429, and 5xx responses retry, with bounded Retry-After support; other 4xx responses fail permanently. Public HTTPS is allowed by default. `web_hook_allowed_hosts` accepts exact hosts/IPs and `web_hook_allowed_cidrs` accepts address ranges, permitting matching internal or HTTP destinations; they are not a global allowlist for public HTTPS. Metadata, link-local, multicast, and unspecified addresses remain blocked. Every DNS answer and the actual dialed address are checked. URL userinfo, fragments, and IPv6 zones are rejected.

For signing, set `web_hook_signing_key_id` and exactly one of `web_hook_signing_secret` or `web_hook_signing_secret_file`. Use a secret independent of cluster and SMTP credentials, 32–4096 bytes long, without embedded newlines or NUL. Prefer a regular 0600 file; symlinks and permissive modes are rejected. No signing configuration preserves unsigned delivery; incomplete configuration prevents startup.

Each attempt sends `X-Owlcron-Key-ID`, `X-Owlcron-Timestamp`, and `X-Owlcron-Signature: v1=<hex HMAC-SHA256>`. Sign these fields joined by LF, ending with the raw body and no appended LF:

```text
v1
<key ID>
<Unix timestamp>
<Idempotency-Key>
<HTTP method>
<URL scheme>
<URL host including explicit port>
<request URI including raw query>
<raw body bytes>
```

Recipients should compare signatures in constant time, enforce a time window, and deduplicate the stable `Idempotency-Key`. SMTP uses a stable Message-ID. For rotation, allow both key IDs at recipients before updating and restarting senders. Nodes sharing a queue need consistent destination policies and signing configuration.

Payloads and errors are recursively redacted; conservative matching may hide similarly named business fields. Actual URLs and bodies remain private queue data. Old backups and quarantined records are not rewritten in bulk. Administrator `get_notifications` accepts `state` and `limit` (1–1000), excluding bodies and claim tokens. `POST retry_notification` resets the retry budget for a dead notification `id`; both support `/v1`. Do not delete notification queues or idempotency markers when cleaning job history.

## Cluster TLS and node identity

Nodes use `/cluster/ws`; browsers use `/ws`. Their credentials are not interchangeable. Node connections require native HTTPS/WSS and mutual Ed25519 challenges by default. `cluster_allow_insecure: true` is for explicit development testing only.

Give every node a unique `hostname`. Set exactly one of `node_auth_private_key` or `node_auth_key_file`, containing a standard-base64 32-byte seed or valid 64-byte Ed25519 private key. `node_auth_peers` maps exact hostnames to standard-base64 32-byte public keys, including the local node's matching key. Prefer a 0600 key file and do not share private keys. Both peers also check enabled canonical `servers:<hostname>` registration; register nodes and public keys before joining.

`cluster_tls_cert_file` and `cluster_tls_key_file` must be configured together. They enable HTTPS/WSS on the current HTTP port. The certificate SAN must include the node hostname; a registered IP controls only the dial address. System CAs are used unless `cluster_tls_ca_file` selects a CA bundle. Private keys must be regular files with 0400/0600 permissions, at most 64 KiB; certificate and CA files are regular files up to 1 MiB. Symlinks and replacement races are rejected. Set the actual `base_app_url` instead of relying on inference.

mTLS uses `cluster_tls_require_client_cert: true`, an explicit CA, and per-node certificates/keys. Certificates need serverAuth/clientAuth usages and the corresponding node SAN. The HTTPS listener verifies supplied client certificates, but only the node endpoint requires one; ordinary browsers can still use HTTPS.

Node proofs bind roles, both challenges, time, nonce, and the current lease. Clock skew is limited to 60 seconds; keep clocks synchronized. The replay cache holds 16384 entries and fails closed when full. WebSockets are capped at 1024 connections with a fixed 10-second unauthenticated window. The current shared lease governs takeover and leader commands. Old owners, stale connection generations, and disabled nodes cannot control workers or acknowledge completion. Disconnection preserves running jobs and durable completions; reconnection authenticates again.

Shared-storage write access is equivalent to cluster control. Rotate identities by coordinating public-key registration and restarting affected nodes. Only one public key per hostname is supported, so do not assume overlapping key IDs. Rotate CAs using a planned trust bundle. UDP discovery does not grant authorization.

## Per-node job capacity

Set `max_jobs` on a server group to limit active and reserved jobs on each matching
node. Zero means unlimited. The strictest positive limit across matching groups
applies even to direct-host and `local` targets. Admission reserves capacity before
execution; queued, delayed and multiplex jobs use the same check. Existing jobs
are not terminated when a limit is lowered. A full node is excluded from new
selection; an event with no capacity waits only when event queuing is enabled.
The top-level `max_jobs` continues to limit the whole cluster.

## Per-event log retention

Events can set `log_expire_days`: omit it or send `null` to inherit
`job_log_expire_days`, use `0` to retain logs indefinitely, or use 1–36500 days.
The effective value is copied into each new job. Editing an event or the global
policy does not change jobs that already have a snapshot; older jobs without one
still use the global policy. The event editor exposes the same choices.
Expiration starts at job completion. History retention (`job_data_expire_days`)
can remove the entire job, including a log otherwise kept indefinitely. Pending
completion delivery and deletion markers retain their existing protections.

## Browser theme

The header theme selector supports System, Light and Dark. System follows OS
changes; an explicit choice is stored locally in the browser under
`owlcron.theme`. Selecting System restores automatic changes. Storage restrictions
fall back to an in-memory preference and do not prevent login. Theme preferences
are local UI settings, not account settings.

## Account email policy

Account email is opt-in: `account_email_enabled` defaults to `false`. Enabling it requires an explicit `smtp_hostname`, a valid single `email_from` address and a trusted absolute HTTPS `base_app_url`. The URL must contain no credentials, query or fragment, and its path must match `base_path`. Only local testing may set `account_email_allow_insecure: true` to allow HTTP. This does not disable SMTP certificate verification.

Zero or omitted limits select these defaults: `account_token_ttl_seconds: 1800` (allowed nonzero values 60–86400), `account_email_cooldown_seconds: 60`, `account_email_rate_limit: 5`, `account_email_ip_rate_limit: 20`, and `account_email_rate_window_seconds: 3600`. Cooldown/window values are capped at 86400 seconds; request limits are capped at 100000. Negative values are rejected.

Existing email addresses are initially unverified. Verification applies only to the exact current address and is cleared when that address changes. This policy foundation alone does not provide verification or reset endpoints. Public registration remains disabled; administrators continue to create accounts and can reset passwords.

Account emails require SMTP TLS as well as HTTPS links. The explicit `account_email_allow_insecure` override also permits `smtp_tls_mode: "none"` for a local test relay; use it only in isolated local testing, never production. TLS modes still verify certificates when TLS is enabled.
