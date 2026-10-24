# Browser acceptance tests

English | [简体中文](README.zh-CN.md)

The two runners start isolated OwlCron daemons with temporary storage. They do
not read an existing service configuration or target a deployed instance.

- `test` runs the general UI smoke check: login and account help, profile updates,
  six administration pages and temporary resource changes, theme persistence,
  event editing and execution, history, logs and charts.
- `test:account-mail` starts an in-process SMTP server and enables account email
  only in its temporary configuration. It checks email verification, the recovery
  request form, matching/nonmatching account responses, explicit password reset,
  password-change notification, rejected token reuse, invalidated passwords and
  sessions, and successful login with the new password. It also checks fresh-page
  and same-document email links without exposing their tokens in browser logs.

Both run in headless Chromium and fail on uncaught browser JavaScript errors.
The account runner sends no external email; public registration stays disabled.

From the repository root, using Bun 1.4.2 and the documented Go toolchain:

```sh
bun install --cwd htdocs-deps --frozen-lockfile
bun install --cwd tests/browser --frozen-lockfile
(cd tests/browser && bunx --bun --no-install playwright install --with-deps --only-shell chromium)
make build frontend
bun run --cwd tests/browser licenses
bun run --cwd tests/browser test
bun run --cwd tests/browser test:account-mail
```

To use an existing compatible Chrome/Chromium installation locally, set
`OWLCRON_BROWSER_EXECUTABLE` to its absolute executable path. CI uses Playwright's
pinned Chromium revision. Both runners need permission to bind localhost ports
and launch browser/daemon processes.

The general smoke runner saves failure screenshots in `tests/browser/test-results/`.
Review these before sharing. The account runner deliberately does not save
screenshots or dump SMTP messages, token-bearing URLs or daemon logs. Credentials,
messages and daemon state are temporary and cleaned up when each run exits.

The path-filtered [CI / Browser Tests](../../.github/workflows/browser.yml)
workflow runs both runners, caches the pinned browser, and uses a five-minute
outer timeout for cold installations. It keeps browser installation out of the
quick Go job. These tests cover selected Chromium workflows; they do not establish
complete cross-browser or accessibility coverage.

Package upgrades require refreshing the lockfile and license copies:

```sh
bun tests/browser/check-licenses.mjs --write
bun run --cwd tests/browser licenses
bun audit --cwd tests/browser --audit-level=moderate
```
