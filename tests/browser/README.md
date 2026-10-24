# Browser acceptance tests

English | [简体中文](README.zh-CN.md)

The two runners start isolated OwlCron daemons with temporary storage. They do
not read an existing service configuration or target a deployed instance.

- `test` runs the general UI smoke check: login and account help, profile updates,
  six administration pages and temporary resource changes, theme persistence,
  event editing and execution, history, logs and charts. It also checks timezone/
  timing/retry/timeout/queue persistence, archived reruns, live aborts, failed
  job logs, bounded retry, timeout enforcement, queue admission/flush,
  success/failure chains and a separate read-only account (hidden controls and
  rejected APIs).
- `test:account-mail` starts an in-process SMTP server and enables account email
  only in its temporary configuration. It checks email verification, the recovery
  request form, matching/nonmatching account responses, explicit password reset,
  password-change notification, rejected token reuse, invalidated passwords and
  sessions, and successful login with the new password. It also checks fresh-page
  and same-document email links without exposing their tokens in browser logs.

Both run in the selected headless browser and fail on uncaught browser JavaScript errors. CI runs Chromium, Firefox and WebKit in parallel.
The account runner also checks its forms at a 375px viewport. It sends no external email; public registration stays disabled.

From the repository root, using Bun 1.4.2 and the documented Go toolchain:

```sh
bun install --cwd htdocs-deps --frozen-lockfile
bun install --cwd tests/browser --frozen-lockfile
(cd tests/browser && bunx --bun --no-install playwright install --with-deps chromium firefox webkit)
make build frontend
bun run --cwd tests/browser licenses
bun run --cwd tests/browser test
bun run --cwd tests/browser test:account-mail
OWLCRON_BROWSER=firefox bun run --cwd tests/browser test
OWLCRON_BROWSER=firefox bun run --cwd tests/browser test:account-mail
OWLCRON_BROWSER=webkit bun run --cwd tests/browser test
OWLCRON_BROWSER=webkit bun run --cwd tests/browser test:account-mail
```

To use an existing compatible Chrome/Chromium installation locally, set
`OWLCRON_BROWSER_EXECUTABLE` to its absolute executable path. This override is only supported for Chromium. `OWLCRON_BROWSER` defaults to
`chromium` and accepts `firefox` or `webkit`; invalid values fail explicitly.
CI uses the browser revisions pinned by Playwright. Firefox bypasses inherited
host proxy preferences because every fixture is served on loopback. Both runners need permission to bind localhost ports
and launch browser/daemon processes.

The general smoke runner saves failure screenshots in `tests/browser/test-results/`.
Review these before sharing. The account runner deliberately does not save
screenshots or dump SMTP messages, token-bearing URLs or daemon logs. Credentials,
messages and daemon state are temporary and cleaned up when each run exits.

The path-filtered [CI / Browser Tests](../../.github/workflows/browser.yml)
workflow runs both runners in each browser, caches the pinned browser separately,
and uses a five-minute outer timeout for cold installations. It keeps browser installation out of the
quick Go job. On macOS, WebKit keyboard checks use Option+Tab to include native buttons and
links, matching Safari's full keyboard-navigation shortcut. Other engines and
Linux use Tab. Each independent keyboard scenario starts at document focus;
target controls must still be reached by key events.

Engine coverage does not certify every Chrome, Edge or Safari release, real
mobile devices, or screen-reader accessibility. Keep release notes limited to
the workflows and viewport checks actually exercised.

Package upgrades require refreshing the lockfile and license copies:

```sh
bun tests/browser/check-licenses.mjs --write
bun run --cwd tests/browser licenses
bun audit --cwd tests/browser --audit-level=moderate
```

The maintained [single-node coverage checklist](../../docs/testing.md#single-node-ui-release-gate) separates browser assertions from engine tests and manual release checks. The expanded smoke shares its existing daemon and browser process; it does not wait for minute-boundary cron firings or use external services.
