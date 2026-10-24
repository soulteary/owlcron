# Contributing

English | [简体中文](CONTRIBUTING.zh-CN.md)

Bug fixes, documentation improvements, and regression tests are welcome. When discussing a behavior change, include reproduction steps, the expected result, and the actual result. Follow the [security policy](SECURITY.md) for security issues.

## Development workflow

1. Read the [README](README.md) and relevant documentation. Check whether the behavior is a [known limitation](docs/limitations.md).
2. Keep each change focused and account for existing data formats and protocol boundaries.
3. Format Go code with gofmt. Add meaningful regression coverage for behavior changes.
4. Run the checks in [testing](docs/testing.md). Scheduling, recovery, storage, and cluster changes also need the relevant process and fault tests.
5. Describe the problem, resulting behavior, validation, and any untested scope in the contribution.

## Documentation

English is the default edition: root documentation and `docs/*.md`. Chinese counterparts use `*.zh-CN.md` at the root and matching filenames under `docs/zh-CN/`. Keep the language links on every page.

Update both editions when changing behavior, configuration, commands, or limitations. Code blocks and configuration field names should remain consistent across languages. `make docs-check` verifies local links, language counterparts, and configuration examples.

Use the README as the entry point and `docs/` for detailed guides. Check configuration against `types.Config` and the implementation. Commands must work from a clean source checkout.

Do not commit conversation notes, model prompts, generated work summaries, one-off scan output, local benchmark reports, failure stacks, or private workspace paths. Keep evidence outside the repository or attach it to CI runs. Retain reusable test scripts and regression cases with the code.

TODO/FIXME comments should identify a concrete limitation and an actionable task. Document user-visible limitations in `docs/limitations.md` and its Chinese counterpart. Deleting a comment is not a fix.

## Data and credentials

Never commit real credentials, runtime data, job logs, installed dependencies, or build output. Tests must not connect to production services. Use minimal synthetic fixtures instead of publishing private data.

## Licensing

Original contributions use the root [MIT License](LICENSE). Record the source, version, and license of imported code and retain its copyright notices. Fonts and other assets may use different terms; see [third-party notices](THIRD_PARTY_NOTICES.md).

After updating npm dependencies, run `make licenses-update licenses-check`. Go dependency changes also require updates to `licenses/go-inventory.json` and the corresponding license files. Review the notices rather than changing version numbers alone.

## Dependency updates

Dependabot proposes weekly updates for the engine, independent example plugin, frontend, container bases, and GitHub Actions. Actions are pinned to full commit IDs; retain the version comment when reviewing a pin update. Minor and patch Go/npm updates are grouped, while major changes remain separate. Updates are reviewed and tested before merging; there is no automatic merge. Run the license checks and update inventories when dependencies change, and validate container or plugin behavior where applicable.
