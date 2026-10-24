# OwlCron binary distribution

English | [简体中文](README.zh-CN.md)

This archive contains `owlcron-engine`, `owlcron-script`, the Echo Plugin,
frontend assets, example configuration, build metadata, bilingual
[release notes](docs/release-notes.md), and third-party licenses.
Run commands from this directory so the daemon can find the frontend and plugin.

```sh
cp config.json config.local.json
chmod 600 config.local.json
```

Edit `config.local.json`: set `secret_key` to a new random secret of at
least 32 bytes, disable discovery with `udp_broadcast_port: -1` for a standalone
node, and choose persistent data, log and queue directories owned by the service
account. Protect the configuration with mode 600. Then initialize and start:

```sh
./owlcron-engine version
./owlcron-engine --config config.local.json setup
./owlcron-engine --config config.local.json
```

On first startup with empty storage, read the generated administrator credentials
from `bootstrap-admin.json` in the configured storage directory using a trusted
terminal. After verifying login, remove this file. Never put real passwords in
shell arguments or command history. Keep the writable data and logs private, back up
before upgrades, and expose the service only behind restricted access and HTTPS.
The example configuration is not a hardened production configuration.

See the [deployment documentation](https://github.com/soulteary/owlcron/blob/main/docs/deployment.md)
and [release policy](https://github.com/soulteary/owlcron/blob/main/docs/releases.md).
The release remains experimental; do not assume cross-version data rollback.
Verify the adjacent `.sha256` file before extracting a downloaded archive.
`BUILD.json` records its version, source commit, target and Go toolchain. Preserve
[LICENSE](LICENSE), [third-party notices](THIRD_PARTY_NOTICES.md), and `licenses/`.

The first release is intended for a standalone single node. Cluster behavior
remains experimental. The daemon does not require Go, Bun or Node.js; custom
plugins still need their own runtime dependencies. The Cronicle migration
preflight checker is a separate source-build tool, not included in this archive.
