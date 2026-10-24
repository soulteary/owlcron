# OwlCron

English | [简体中文](README.zh-CN.md)

Schedule tasks, run plugins, and inspect execution history and logs from a Web UI.

[![CI](https://github.com/soulteary/owlcron/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/soulteary/owlcron/actions/workflows/ci.yml)
[![Browser tests](https://github.com/soulteary/owlcron/actions/workflows/browser.yml/badge.svg?branch=main)](https://github.com/soulteary/owlcron/actions/workflows/browser.yml)
[![Container](https://github.com/soulteary/owlcron/actions/workflows/container.yml/badge.svg?branch=main)](https://github.com/soulteary/owlcron/actions/workflows/container.yml)
[![Release](https://img.shields.io/github/v/release/soulteary/owlcron)](https://github.com/soulteary/owlcron/releases)
[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)](docs/releases.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)


OwlCron is a task scheduler written in Go, designed to replace [Cronicle](https://github.com/jhuckaby/Cronicle). It provides a web interface, scheduled and on-demand jobs, plugin execution, job logs, and notifications, with selected compatibility for Cronicle workflows, APIs, and plugin conventions. See [Cronicle compatibility](docs/compatibility.md) for the current migration scope.

The project is experimental. Review the [known limitations](docs/limitations.md), including sustained cluster convergence, remote dispatch acknowledgment, and production capacity, before deploying it.

![OwlCron](.github/assets/owlcron-banner.png)

## What you can do

- **Manage jobs**: configure scheduled tasks and run jobs on demand through the Web UI.
- **Run plugins**: verify the workflow with the Echo Plugin, then connect your own scripts; see [running script files](docs/script-files.md).
- **Investigate execution**: inspect job history, logs, and notifications, with [operations and metrics](docs/operations.md) for diagnostics.
- **Evaluate a Cronicle migration**: run the [migration preflight](docs/migration.md) and check the [compatibility scope](docs/compatibility.md) first.

## Choose an installation path

- **Build from source**: follow the steps below for local evaluation and development.
- **Use a release archive**: read [versions and releases](docs/releases.md) for supported platforms, build metadata, and archive verification; check [GitHub Releases](https://github.com/soulteary/owlcron/releases) for available versions.
- **Containers and Kubernetes**: follow the [container deployment guide](docs/containers.md) to prepare configuration and persistent data.

## Quick start

Install Go, Bun 1.4.2, Python 3, and make. The module recommends Go 1.27.3; see the [deployment requirements](docs/deployment.md) before choosing another toolchain.

```sh
git clone https://github.com/soulteary/owlcron.git
cd owlcron
bun install --cwd htdocs-deps --frozen-lockfile
make frontend build
cp config.json config.local.json
chmod 600 config.local.json
```

Edit `config.local.json` and replace `secret_key` with a random secret of at least 32 bytes. Then initialize storage and start the service:

```sh
./owlcron-engine -config config.local.json setup
./owlcron-engine -config config.local.json
```

On the first start with an empty user database, OwlCron generates administrator credentials in `data/bootstrap-admin.json`. Read this file in a trusted terminal, then sign in at `http://localhost:3012`. You can also supply credentials before the first start; see [installation and startup](docs/deployment.md#installation-and-startup).

Initialization provides a General category and an Echo Plugin. Create a local event using that plugin, run it manually, and inspect its history and logs. `make smoke` checks this workflow automatically with isolated temporary data.

The HTTP service listens on all interfaces. `base_app_url` does not restrict the listening address. Restrict access to the backend port and configure HTTPS before exposing the service. Keep secrets, runtime data, and logs out of version control.

## Documentation

- [Documentation index](docs/README.md): guides for deployment, running jobs, operations, and development.
- [Cronicle migration preflight](docs/migration.md) and [compatibility](docs/compatibility.md): check data and behavior differences before migration.
- [Contributing](CONTRIBUTING.md) and [testing](docs/testing.md): development workflow, check commands, and quality reports.
- [Security policy](SECURITY.md): reporting security issues.

## License

OwlCron's original code uses the [MIT License](LICENSE). Inherited Cronicle code retains its original copyright and MIT terms. Third-party code and fonts retain their respective licenses; see [third-party notices](THIRD_PARTY_NOTICES.md) and `licenses/`.
