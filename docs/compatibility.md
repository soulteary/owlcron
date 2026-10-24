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
- A JSON request body must be a single object with unique top-level keys. Scope selectors (`id`, `ids`, `category`, and `target`) use their exact lowercase spelling. Query parameters may repeat only with the same value; when a parameter is also in JSON, only an equal scalar is accepted and its JSON type is preserved. Conflicting values and query/body array or object overlaps are rejected before authorization and execution. Supply bulk `ids` as a JSON array. Event creation, event deletion/abortion, single-job abortion, and category mutations authorize the selectors in their JSON body; query-only selectors do not populate these bodies. These rules also apply to `/v1` routes; separate query credentials and ordinary query-only reads remain supported.
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

### Delegated category API administration

Administrators may grant `create_categories`, `update_categories` and
`delete_categories` separately to users or API keys. These rights do not grant
user, plugin, server or API-key administration. With `cat_limit`, every mutation
must name an explicit category ID allowed by its `cat_<id>` privilege, including
creation. Query parameters cannot override the body ID for authorization.
Without `cat_limit`, category rights apply to all categories. Existing IDs cannot
be recreated, and categories assigned to events cannot be deleted. The browser's
administration area remains administrator-only; delegated clients use the APIs
(`/api/app/create_category`, `update_category`, `delete_category`, optionally
suffixed with `/v1`). The administrator's user/API-key forms expose these rights.

## Browser account and administration workflows

Accounts are provisioned by administrators in **Admin → Users**. The login page's
**Account access help** explains how to request an account or password reset.
Self-service registration remains unavailable. Email verification and password
recovery are disabled by default; operators can enable them with the
[account email policy](configuration.md#account-email-policy). Recovery requires
an enabled account and a previously verified email. Existing Cronicle recovery
links are not accepted; request a new OwlCron link. When recovery is disabled,
old recovery routes show administrator-assisted recovery instructions.
Signed-in users can update their profile and password in **My Account**, using
their current password. An administrator can reset another user's password in
**Admin → Users**. If all administrators are locked out, use the offline
[administrator password reset](deployment.md#resetting-an-administrator-password).
The CLI creates or resets an administrator; do not use it to reset an ordinary
user, because it grants administrator privileges.

The account endpoints are OwlCron's contract, not drop-in replacements for old
Cronicle email flows. Each accepts a JSON object by POST, with optional `/v1`:

- `/api/user/request_email_verification`: an authenticated user supplies `email`
  and `current_password`; the current email stays active until confirmation.
- `/api/user/verify_email`: supply `user_id` and `token` from the verification link.
- `/api/user/forgot_password`: supply the exact, case-sensitive `username` and
  verified `email`; the response does not reveal whether the account matches.
- `/api/user/reset_password`: supply `user_id`, `token`, `new_password` and
  `confirm_password`. A successful reset invalidates prior sessions and links.

All four are disabled unless account email is enabled. Their handlers reject
bodies over 4 KiB, duplicate or unknown fields, and non-POST methods. The existing
client's string `session_id` transport field is accepted; it does not substitute
for the current password or link token. GET requests never consume these tokens.

The administration UI saves supported plugin identities and API-key enabled
state. Welcome emails and API-key descriptions/authors are not provided by the
backend, so the forms do not offer these controls. Share initial account
credentials through a secure channel. Script-plugin commands use reviewed files
through `owlcron-script`; no built-in inline Shell Plugin is installed. See
[script files](script-files.md) for an executable configuration example.

Server-group expressions are validated by Go's regular-expression engine. The
browser previews common expressions with their case sensitivity preserved; it
labels expressions it cannot preview as server-side matching. Configure shared
storage and trusted node identities before adding workers.

The event form edits single success/failure chain targets. For events configured
through the API with target arrays, it displays and preserves those targets;
change the array through the event API. Saving another field must not erase them.
Activity pages read the current nested record format and use the full log count
for pagination. Records without actor details are shown without inventing an
identity.
