# Security policy

English | [简体中文](SECURITY.zh-CN.md)

OwlCron is experimental and does not yet have a stable-release support lifecycle. Maintenance targets the main branch. Validate fixes against the affected deployment before releasing them. See the [known limitations](docs/limitations.md).

## Reporting vulnerabilities privately

Use **Report a vulnerability** on the repository's Security page:

[Open a private report](https://github.com/soulteary/owlcron/security/advisories/new)

Repository maintainers must enable GitHub private vulnerability reporting for this entry point to work. If it is unavailable, open an issue without sensitive details and ask for a private contact channel. Do not publish exploit details, real tokens, passwords, or user data in public issues.

Include the affected version or commit, platform, minimal reproduction, impact, and any mitigations you tried. Coordinate disclosure after maintainers confirm the impact and fix. There is currently no guaranteed response SLA.

## Deployment boundaries

Plugins execute system commands. Treat users who can configure plugins or jobs as having the corresponding service account's command-execution privileges. OwlCron is not a sandbox for untrusted code. Run it as a dedicated non-root user, restrict filesystem and network access, and do not mount a Docker socket or the host root filesystem.

The HTTP service listens on all interfaces. `base_app_url` sets the public application URL; **it does not restrict the listening address**. Use a firewall, security group, or container port mapping to restrict port 3012 to the intended reverse proxy. Use HTTPS for browser access and do not expose the backend management port directly to the internet.

See [deployment](docs/deployment.md) for proxy, cookie, credential, and backup requirements.

Account email is disabled by default and public registration remains closed.
Enabling email verification and recovery requires a trusted public HTTPS URL and
SMTP TLS. Recovery links are credentials: do not publish them in issues, logs or
screenshots. Follow the [account email runbook](docs/operations.md#account-email-and-password-recovery)
for expiry, revocation, delivery failures and restoring backups.

## Dependency checks

CI runs `bun audit` against locked npm dependencies and `govulncheck` against the main module and example plugin. These checks cover known advisories at scan time; they do not rule out unknown vulnerabilities. Rebuild the frontend, run installation smoke tests, and refresh notices with `make licenses-update licenses-check` after dependency changes.

Scanners may report vulnerable packages contained in a module but not imported by this project. Distinguish reachable calls, imported packages, and module-only findings. Verify the call path and document the decision instead of hiding scanner output.
