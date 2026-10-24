# Operations and metrics

English | [简体中文](zh-CN/operations.md)

## Collect metrics with a dedicated identity

`GET /api/app/metrics` and `/api/app/metrics/v1` expose Prometheus text format
0.0.4. Include `base_path` when configured. Authentication is required: use an
API key in `X-API-Key`, an existing session, or HTTPS Basic authentication.
Grant a dedicated collector only **View Operational Metrics** (`view_metrics`);
administrators also have access. This privilege exposes node-wide aggregate
counts regardless of category restrictions, but grants no job mutation rights.
Missing/invalid credentials return HTTP 401, insufficient privilege returns 403,
and unsupported methods return 405. Rotate and revoke collection credentials as
normal application credentials; never put secrets in URLs, dashboards or logs.

For example, create a dedicated user with only `view_metrics` and configure a
protected password file on the collector:

```yaml
scrape_configs:
  - job_name: owlcron
    scheme: https
    metrics_path: /api/app/metrics
    basic_auth:
      username: owlcron-metrics
      password_file: /run/secrets/owlcron-metrics-password
    static_configs:
      - targets: [cron.example.com]
```

Restrict collection to trusted networks, verify HTTPS certificates, and scrape
each node directly rather than through a load-balanced service. Start with a
30-second interval. Authentication reads the existing credential store; metric
collection itself uses memory only and does not list storage or scan spool files.

## Interpret the gauges

All seven series are gauges without application labels. Job IDs, hostnames,
users, commands, tokens and event parameters are never included.

- `owlcron_role_master`: this node's local master flag with an unexpired local
  lease. This is not a fresh shared-storage proof of sole ownership.
- `owlcron_role_worker`: this node's local worker flag. Both role values can be
  zero during role transitions; do not infer the master by negating worker.
- `owlcron_scheduler_enabled`: scheduling enabled in the local state snapshot.
- `owlcron_jobs_active`: non-pending entries in this node's active-job registry.
  A master's registry can include remote jobs; it is not an OS process count.
- `owlcron_jobs_pending`: pending entries in that same registry, including
  delayed launches or retries. Do not sum master and worker counts as unique jobs.
- `owlcron_internal_queue_tasks`: in-memory delayed queue tasks. It is neither
  durable event queue depth nor a count of notification/completion outboxes.
- `owlcron_shutdown_requested`: shutdown or restart has been requested.

Registry fields are captured under one lock; internal queue depth is sampled
separately. These are operational snapshots, not a transaction across nodes.
There are currently no exported authoritative durable-queue depths, outbox ages,
scheduling lag, storage error rates, disk-space values or backup freshness
metrics. Their absence is not a zero value. Use system/storage monitoring and
application logs until bounded instrumentation is added for those sources.

## Alerts and response

Alert on scraper `up == 0` for more than two minutes. Check network reachability,
TLS trust, credential expiry/revocation and HTTP 401/403 before restarting nodes.
Monitor all expected targets: a missing worker cannot be inferred from the
remaining nodes' gauges. For a cluster expected to have one master, a sustained
sum of `owlcron_role_master` equal to zero or greater than one needs investigation
of leases, connectivity and node logs; it is not evidence to force two masters.

Trend pending jobs and internal delayed tasks against the workload baseline.
Sustained growth requires checking worker availability, concurrency limits,
long-running jobs and retry errors. A rising active count alone does not justify
killing jobs. Inspect completion/notification retry logs when work finishes but
history or delivery appears delayed. Keep fault symptoms and timestamps before
intervening; redact credentials and sensitive job output from incident artifacts.

Monitor free bytes and inodes on data, logs and queue volumes externally. On low
space, pause new scheduling, preserve durable queues and investigate retention
and unusually large job logs. Do not delete queue, history, transaction or lease
files to regain space. Increase capacity or apply documented retention after
backing up affected data; verify recovery and queued work before resuming.

For storage errors, verify service health, TLS, access policy, quotas, network
and latency. Distinguish authentication failures from timeouts and conflict
retries. Do not replace the backend with an empty namespace or restart all nodes
at once to clear errors. The [known limitations](limitations.md) still apply to
remote dispatch and recovery; metrics do not establish exactly-once execution.

Record backup success and age in the backup system. Stop all nodes before
`export`, `import` or `storage-migrate` for a consistent snapshot. Encrypt backups,
retain matching binaries/configuration and regularly restore into an isolated
environment with real notification recipients disabled. Verify accounts,
schedule, history and logs before declaring a restore successful. Follow the
[deployment and rollback procedure](deployment.md#backups-upgrades-and-rollback).

### Verify an account email

When account email is enabled, open **My Account**, enter the intended email and
current password, and select **Send verification email**. The existing email
remains active until the confirmation link is explicitly confirmed. Saving other
profile settings does not apply a pending email change. Existing addresses are
not automatically trusted. Administrators cannot bypass verification by editing
an address; account creation remains an administrator operation.

The link opens a confirmation screen. Opening it alone does not consume the
single-use token. Expired or previously used links require a new request from
My Account. Registration remains disabled.

## Account email and password recovery

Public registration stays disabled. Enable `account_email_enabled` only after
configuring the [account email policy](configuration.md#account-email-policy):
a trusted HTTPS `base_app_url` matching `base_path`, an explicit `smtp_hostname`,
SMTP TLS and `email_from`. Keep `account_email_allow_insecure` false in production.
Test delivery with a dedicated account before announcing recovery availability.
Imported and existing email addresses start unverified; users who cannot sign in
must first use administrator-assisted recovery, then verify their address.

After verification, select **Forgot Password?** on the login page and provide the
exact username and verified email address. The response is intentionally the same
for missing, disabled, unverified and matching accounts. A successful response
does not prove delivery. Open the email link, enter and confirm a new password
(12–72 UTF-8 bytes), then explicitly select **Reset Password**. All previous HTTP
and WebSocket sessions and outstanding account links become invalid. Sign in
again with the new password; recovery never creates or enables an account and
never changes privileges.

Links expire after `account_token_ttl_seconds` (30 minutes by default). Requesting
a new link for the same action replaces the previous link. Opening a link alone
does not change the account. The browser removes the token from its address bar
before loading application assets; refreshing loses the in-memory token, so open
the original email again. Do not copy links into logs, tickets or screenshots.
The API accepts these actions only by POST; it does not consume tokens from GET
requests. Old Cronicle recovery links cannot be used.

Email and password edits, account disablement/deletion and secret-key rotation
revoke outstanding links. Changing the email requires the current password and
verification of the new address; the current address remains active until then.
An email-change notice goes to the old address and a password-change notice goes
to the recovery address. Notices are best effort after the account change: a
notification failure does not roll back a successful change.

Requests have process-local IP/account limits and a persistent per-user cooldown.
Use shared ingress limits for multi-node deployments. The durable mail queue
stores account-action references, not reusable reset tokens, and rechecks expiry
and revocation immediately before delivery. SMTP failures retry under the normal
notification policy, which can outlast a link's lifetime; expired/revoked messages
are discarded. Check SMTP connectivity, TLS, sender policy and notification
errors if messages do not arrive, then request a fresh link after the cooldown.
A storage/queue failure can prevent delivery; never report a generic request
response as a confirmed email delivery.

When restoring backups, treat saved account-action state as sensitive: isolate
the restore, disable real recipients, and rotate the restored `secret_key` before
serving users so historical links cannot become usable again. This also
invalidates existing signed credentials. Preserve offline administrator recovery
as the fallback when email is unavailable.
