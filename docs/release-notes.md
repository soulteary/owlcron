# Release notes

English | [简体中文](zh-CN/release-notes.md)

## v0.1.0-rc.1024

This is the first release candidate for evaluating OwlCron as a single-node
alternative to Cronicle with familiar pages and selected compatible workflows.
This prerelease is intended for evaluation and controlled deployments.
The supported binary distribution is Linux amd64.

### Included workflows

- Schedule and run local jobs, configure retries and timeouts, inspect history
  and logs, and follow success or failure chains.
- Use the bundled Echo Plugin and `owlcron-script` adapter. Custom plugins still
  require their own runtimes and a review of their execution contract.
- Administer users, permissions, categories, plugins and API keys. Public
  registration remains closed.
- Enable email verification and password recovery with an explicitly configured
  HTTPS public URL and SMTP TLS relay. Account email is disabled by default.
- Use the Cronicle-style pages on compact viewports and with keyboard navigation,
  including dialog focus, cancellation and table scrolling.
- Deploy at the root URL or a configured URL prefix, with secure sessions behind
  a trusted HTTPS reverse proxy.

### Verification and installation

Check the archive checksum before extraction, and confirm the version and source
commit using `owlcron-engine version` and `BUILD.json`. The archive includes
English and Chinese installation instructions, these notes, the MIT license,
third-party notices and dependency license texts.

The release pipeline builds a clean committed source snapshot on Linux amd64.
It verifies the extracted program without Go, Bun or Node.js, runs Echo and shell
jobs, and checks restart, history, logs and offline restoration into empty storage.
Additional acceptance uses Nginx with a locally trusted test certificate at `/`
and `/tools/cron`, exercising HTTPS assets, session cookies, WSS live logs,
reader permissions and a synthetic Cronicle migration rehearsal.

Chromium, Firefox and WebKit checks cover the main and administration pages,
320/375/768 CSS-pixel layouts, keyboard interaction and isolated account-email
flows. These are automated engine checks, not certification of all browser
versions, physical mobile devices or screen readers. Consult the checks for the
source commit in `BUILD.json`; the presence of these notes alone does not prove
that a particular build passed.

Follow the [deployment guide](https://github.com/soulteary/owlcron/blob/main/docs/deployment.md)
before exposing the service. Use a dedicated service account, private persistent
directories, a new application secret and restricted backend access. Configure
the public URL and proxy trust explicitly. Retain a tested backup.

### Moving from Cronicle

Follow the [migration guide](https://github.com/soulteary/owlcron/blob/main/docs/migration.md).
The separate source-built checker reads Cronicle Data Export v1.0 and reports
conversion requirements. It does not import records or prove application
behavior. Recreate reviewed configuration, credentials and local targets; move
inline shell scripts to reviewed files used by `owlcron-script`. Never pass a
Cronicle export directly to OwlCron's import command or reuse its storage directory.

The bundled rehearsal uses authored synthetic data. Before cutover, validate
your own plugins, time zones, schedules, permissions, notification recipients and
side effects in an isolated deployment. Keep the original scheduler stopped
while enabling the replacement to avoid duplicate work.

### Known boundaries

- This candidate is for evaluation and controlled deployments; it does not
  establish stable APIs, cross-version data compatibility or production capacity.
- Cluster discovery, failover and remote dispatch are outside this release's
  compatibility promise. OwlCron nodes cannot join a Cronicle cluster.
- Compatibility covers selected UI/API/plugin behavior, not a drop-in data or
  cluster upgrade. Completed Cronicle history and logs are not migrated by the
  preflight checker. Detached jobs cannot continue across an engine restart.
- Plugins run with the service account's permissions and are not sandboxed.
  External side effects are not exactly once; use business-level idempotency.
- Long-duration stability, actual SMTP providers, deployment-specific TLS and
  proxy behavior, capacity, and cross-version upgrades/rollback require operator
  validation. Local certificate and SMTP fixtures do not establish those claims.
- Checksums provide integrity checking. Artifacts are not signed and do not yet
  include SBOMs or provenance attestations.

See [compatibility](https://github.com/soulteary/owlcron/blob/main/docs/compatibility.md)
and [known limitations](https://github.com/soulteary/owlcron/blob/main/docs/limitations.md)
before choosing a workload. Report reproducible problems with the source commit,
platform and sanitized steps; keep credentials, exports and private logs out of
public issues.
