# OwlCron binary distribution

English | [简体中文](README.zh-CN.md)

This archive contains `owlcron-engine`, `owlcron-script`, the Echo Plugin,
frontend assets, example configuration, build metadata, and third-party licenses.
Run commands from this directory so the daemon can find the frontend and plugin.

```sh
./owlcron-engine version
./owlcron-engine --config config.json setup
OWLCRON_ADMIN_USERNAME=admin OWLCRON_ADMIN_PASSWORD='choose-a-strong-password' ./owlcron-engine --config config.json
```

The password above is a placeholder: use a generated password and a protected
secret mechanism for deployment. Keep the writable data and logs private, back up
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
