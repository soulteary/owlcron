# Real-browser smoke test

English | [简体中文](README.zh-CN.md)

This isolated development dependency runs one headless Chromium session against a
fresh OwlCron daemon. It bootstraps an administrator in temporary storage, logs in
through the UI, checks persisted light/dark preferences, creates and edits an
event (including log retention), runs it, and opens its history and log. It also
also checks live and archived charts, time-axis tooltips, and chart colors when
switching themes. It fails on uncaught browser JavaScript errors. No existing configuration is read.

From the repository root, using Bun 1.4.2 and the documented Go toolchain:

```sh
bun install --cwd htdocs-deps --frozen-lockfile
bun install --cwd tests/browser --frozen-lockfile
(cd tests/browser && bunx --bun --no-install playwright install --with-deps --only-shell chromium)
make build frontend
bun run --cwd tests/browser licenses
bun run --cwd tests/browser test
```

To use an existing compatible Chrome/Chromium installation locally, set
`OWLCRON_BROWSER_EXECUTABLE` to its absolute executable path. CI uses Playwright's
pinned Chromium revision. Failed runs save a screenshot in
`tests/browser/test-results/`; credentials and daemon state are temporary. Do not
reuse this smoke runner against a deployed instance.

The separate, path-filtered Browser smoke workflow keeps browser installation
out of the existing quick Go checks. It caches the pinned browser and runs only
Chromium, with a five-minute outer timeout for cold installations. This is a
critical-path smoke test, not a claim of full cross-browser or accessibility
coverage. Package upgrades require refreshing the lockfile and license copies:

```sh
bun tests/browser/check-licenses.mjs --write
bun run --cwd tests/browser licenses
bun audit --cwd tests/browser --audit-level=moderate
```
