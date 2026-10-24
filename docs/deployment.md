# Secure deployment and first startup

English | [简体中文](zh-CN/deployment.md)

Use OwlCron initially for development, evaluation, and controlled environments. Strict sustained cluster convergence remains unvalidated. Read the [known limitations](limitations.md); a single-node smoke test does not establish cluster reliability.

## Installation and startup

Use Go 1.27.3 or a later security patch in the same series, Bun 1.4.2, Python 3, and make. CI validates Go 1.27 only; prefer its latest security patch. The module recommends Go 1.27.3, which older Go installations can select when automatic toolchain download is enabled.

Install the pinned Bun toolchain with `curl -fsSL https://bun.sh/install | bash -s "bun-v1.4.2"`, then verify `bun --version`. Frontend and browser dependencies use separate committed `bun.lock` files; always use `--frozen-lockfile` for reproducible installation. `bunfig.toml` keeps the hoisted layout required by static asset paths and license validation. Node.js and npm are not required for these build and test commands.

```sh
git clone https://github.com/soulteary/owlcron.git
cd owlcron
bun install --cwd htdocs-deps --frozen-lockfile
make frontend build
make licenses-check smoke
cp config.json config.local.json
chmod 600 config.local.json
```

Set `secret_key` in `config.local.json` to a random secret of at least 32 bytes. Store data, queues, and logs in persistent directories owned exclusively by the service account. Do not reuse an example secret. `OWLCRON_SECRET_KEY` can also supply it. Keep secrets out of images, Git, and public logs.

```sh
./owlcron-engine -config config.local.json setup
./owlcron-engine -config config.local.json
```

Before the first start with an empty user database, you may set `OWLCRON_ADMIN_USERNAME` and `OWLCRON_ADMIN_PASSWORD`. Passwords must be 12–72 bytes. Without a supplied password, the service generates one in `bootstrap-admin.json` under the storage directory. This file contains sensitive credentials. Read it in a trusted terminal, confirm that login works, then remove the bootstrap file and stop injecting the initial credentials. Setting those variables again does not reset an existing account.

Do not put passwords directly in shell commands, process arguments, or the repository. Use your deployment platform's secret management or an environment file readable only by the service account. OwlCron does not automatically load `.env` files.

## HTTPS reverse proxy

The following example assumes **Nginx on the same host**. Replace the domain and certificate paths. First use the host firewall to prevent external access to port 3012. Container deployments can publish `127.0.0.1:3012:3012`; do not expose the backend mapping on all interfaces.

Merge this fragment into the application configuration:

```json
{
  "base_app_url": "https://cron.example.com",
  "http_port": 3012,
  "session_cookie_secure": true,
  "trusted_proxy_cidrs": ["127.0.0.1/32", "::1/128"]
}
```

Trust only the actual proxy source addresses. Do not use `0.0.0.0/0` or broad internal subnets. Replace the loopback addresses if the proxy is remote or in a separate container network.

```nginx
# Place this in the http context.
map $http_upgrade $owlcron_connection {
    default upgrade;
    ''      close;
}

server {
    listen 443 ssl;
    server_name cron.example.com;
    ssl_certificate     /etc/nginx/tls/fullchain.pem;
    ssl_certificate_key /etc/nginx/tls/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    location / {
        proxy_pass http://127.0.0.1:3012;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        # A single proxy overwrites client-supplied forwarding headers.
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $owlcron_connection;
        proxy_read_timeout 360s;
        proxy_buffering off;
    }
}
```

A separate port-80 virtual host can redirect HTTP to HTTPS. Multi-proxy deployments must maintain trust hop by hop rather than copying this single-proxy header example unchanged. Check that login cookies over HTTPS include Secure, HttpOnly, and SameSite=Lax. Do not continue logging in over HTTP after forcing Secure cookies.

If both `cluster_tls_cert_file` and `cluster_tls_key_file` are configured, the application's same listening port serves TLS. The proxy upstream must then use HTTPS with a trusted CA, server name, and certificate verification. The plaintext `proxy_pass http://...` example no longer applies. Do not disable upstream certificate checks to work around this mismatch.

## Permissions, networking, and data

- Run as a dedicated non-root service account. Restrict configuration and data directories to that account.
- Grant plugin and job administration only to trusted users. The example plugin is for testing, not isolation.
- Use least-privilege credentials for WebHooks, SMTP, S3, and Couchbase. Verify TLS and restrict outbound access.
- Cluster nodes use WSS, node authentication, and optional mTLS at `/cluster/ws` on the same listening port. A reverse proxy does not replace those checks. See [configuration](configuration.md) and restrict node access to cluster members.
- Set `udp_broadcast_port: -1` if discovery is unnecessary. Do not expose discovery ports publicly.
- Configure concurrency, CPU, memory, log size, and retention for your workload, and monitor disk usage. Per-job limits are not a node-wide disk quota.

## Backups, upgrades, and rollback

Before upgrading, stop accepting new jobs, wait for active work to finish, and shut down gracefully. Back up configuration, complete backend data, queues, and logs. Backups contain password hashes, sessions, tokens, and job output; encrypt them and restrict access.

Prefer a stopped, consistent snapshot for Filesystem storage. Use a consistent backup mechanism for remote backends. Copying a live directory is not a verified consistent backup.

Restore into an isolated environment and check login, jobs, history, and logs before upgrading the deployment. For rollback, retain the previous executable, static assets, plugins, and pre-upgrade snapshot. If formats change, restore the matching snapshot rather than assuming an old binary can read new data. Restore tests must not send duplicate notifications to real recipients.

The [release workflow and version policy](releases.md) define verified archive contents and the supported build matrix. Data-migration compatibility is not yet guaranteed. Validate upgrades, cross-version rollback, and remote-backend disaster recovery against the intended deployment.

The CLI streams backup records to the existing `owlcron-storage-v1` format instead of retaining the full export in memory. File exports are written to a private temporary file, synced, and renamed only after completion; errors before the rename preserve the previous destination. A directory-sync failure after the rename reports unconfirmed durability; the new file may already be visible. The key list and largest individual record still consume memory. Standard-output exports can be partial on failure, so check the exit status before using them. Streaming does not make a live backend snapshot consistent.

## Hosting under a URL prefix

Set `base_path` to a path such as `/tools/cron` (no trailing slash) and
`base_app_url` to `https://cron.example.com/tools/cron`. The proxy must preserve
this prefix when forwarding requests. Serve the UI at `/tools/cron/`; API,
browser WebSocket and log URLs share that prefix. Session cookies are scoped to
that path. Path segments may contain letters, digits, underscores and hyphens.
An empty `base_path` retains root hosting. Do not mount another application under
the same cookie path or rely on a proxy to strip the configured prefix.

The authenticated node transport remains `/cluster/ws` at the root, regardless
of `base_path`. Restrict its access to cluster nodes; browser traffic uses the
prefixed `/ws`. The existing TLS, origin and trusted-proxy requirements still apply.

See [operations and metrics](operations.md) for authenticated collection and incident response.

## Resetting an administrator password

Stop the daemon before using the administrative CLI. Read the password from a protected secret file instead of placing it in process arguments or shell history:

```sh
./owlcron-engine --config config.local.json admin admin --password-stdin < /run/secrets/owlcron-admin-password
```

Input must contain one password of 12–72 bytes, with an optional trailing newline. Leading and trailing spaces are preserved. The legacy positional-password command remains accepted for compatibility, but the standard-input form is preferred.

## Preparing account email

Keep `account_email_enabled` disabled until verification and password-reset support is deployed and tested with your SMTP relay. Choose the public HTTPS `base_app_url` explicitly, including `base_path` when deployed under a subpath; account links must never derive their origin from request headers. Configure a dedicated sender and restrict SMTP credentials to this service. Existing users retain login access without email verification, and administrator password recovery remains available. See [account email policy](configuration.md#account-email-policy) for validation and limits. Public registration remains closed.

Account verification and recovery links contain bearer tokens: keep both public links and SMTP transport encrypted. `account_email_allow_insecure` permits plaintext HTTP and SMTP only for isolated local testing; never enable it in production.
