# Real-browser smoke test

English | [简体中文](README.zh-CN.md)

This isolated development dependency runs one headless Chromium session against a
fresh OwlCron daemon. It bootstraps an administrator in temporary storage, logs in
through the UI, checks persisted light/dark preferences, creates and edits an
event (including log retention), runs it, and opens its history and log. It also
fails on uncaught browser JavaScript errors. No existing configuration is read.

From the repository root, using Node.js 22 and the documented Go toolchain:

```sh
npm ci --prefix htdocs-deps
npm ci --prefix tests/browser
(cd tests/browser && npx playwright install --with-deps --only-shell chromium)
make build frontend
npm run licenses --prefix tests/browser
npm test --prefix tests/browser
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
node tests/browser/check-licenses.mjs --write
npm run licenses --prefix tests/browser
npm audit --prefix tests/browser --audit-level=moderate
```
