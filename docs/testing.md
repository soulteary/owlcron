# Testing and validation

English | [简体中文](zh-CN/testing.md)

Run commands from the repository root unless stated otherwise. Use the recommended patched Go toolchain and Bun 1.4.2. See [deployment](deployment.md) for prerequisites and permissions.

## Routine checks

```sh
bun install --cwd htdocs-deps --frozen-lockfile
make frontend build
make fmt-check vet test frontend-test licenses-check docs-check smoke
make race
python3 -m unittest discover -s tests -p 'test_*.py'
(cd bin/echo-plugin && go vet ./... && go test ./...)
```

`make smoke` starts a real daemon with isolated temporary data and verifies static assets, login, node readiness, event execution, the example plugin, history, and logs before graceful shutdown. It needs permission to bind local temporary ports and does not use existing runtime data.

`make docs-check` checks local documentation links, heading anchors, language pairs, and configuration examples offline. It helps prevent references to missing files or private workspace paths.

`go test -short ./...` skips independent-process fixtures for quick feedback; it does not replace full regression testing. The example plugin is a separate module and is not included by the root `go test ./...`. It currently has no dedicated test files, but still needs build and static checks.

## Vulnerabilities and licenses

```sh
bun audit --cwd htdocs-deps --audit-level=moderate
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
(cd bin/echo-plugin && go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...)
```

Scans use the advisory database available at runtime. Distinguish reachable calls, imported packages, and module-only findings. Do not describe an unused package advisory as confirmed exploitation, or ignore a reachable vulnerability.

After Bun dependency upgrades, run `make licenses-update licenses-check` and review changed terms. Go dependency changes also require updating `licenses/go-inventory.json` and the relevant upstream notices. Run `bun install --frozen-lockfile` before checking license inventories.

## Independent-process regression

```sh
make process-integration
make process-integration-repeat
```

These targets use real daemon processes, Filesystem storage, SIGKILL, network partitions, and TLS node connections without external S3/Couchbase services. The repeat target runs the selected cases three times.

## Linux and remote-storage faults

```sh
make linux-acceptance
```

Docker, Python 3, downloadable images, and Go dependencies are required. The script creates dedicated MinIO and Couchbase services and a network for this run. It publishes no host ports and mounts no Docker socket into test containers. Test credentials are disposable; the script cleans up its own services.

`OWLCRON_ACCEPTANCE_RESULTS` selects an external results directory; `OWLCRON_ACCEPTANCE_COUNT` selects 1–3 repetitions. See [run-linux-acceptance.sh](../tests/run-linux-acceptance.sh) for image and cache options. Never let two test runs control the same services.

For dedicated services you provision yourself, use `make remote-storage-faults` or `make remote-process-integration`. The first requires S3/Couchbase endpoints and container control identifiers; the second requires the backend list and JSON configurations. See the [Makefile](../Makefile) and scripts for exact variable names. Unconfigured remote tests skip; a skip is not a successful real-service test.

## Load and sustained faults

```sh
make linux-load-acceptance
```

Default limits are 2 CPUs, 2 GiB of memory, and 512 processes. The entry point runs race checks, repeated loaded-cluster tests, capacity tests, single-node recovery, sustained cluster recovery, and evidence validation. `OWLCRON_LOAD_RESULTS` must name an empty directory; old results cannot fill gaps in a failed run.

Individual stages can also be run:

```sh
make capacity-acceptance
make cluster-load-acceptance
OWLCRON_SOAK_DURATION=3m OWLCRON_PROCESS_SOAK_DURATION=3m \
  OWLCRON_LOAD_SOAK_TIMEOUT=15m make soak-acceptance
```

The last target runs single-node and cluster scenarios sequentially. Its outer timeout must cover both and their setup. Adjust the timeout for longer runs.

Resources, mounts, backends, data volume, and log/notification load affect results. Test RTO, latency percentiles, and sampled RSS are not a service SLA; a short passing run does not establish long-term stability. See the sustained-cluster issue in [known limitations](limitations.md).

## CI and evidence

[CI / Build and Test](../.github/workflows/ci.yml) checks Go 1.27, Bun 1.4.2, builds, unit tests, installation smoke tests, licenses, and vulnerabilities. [CI / Process Fault Acceptance](../.github/workflows/process-acceptance.yml) covers process and Linux fault scenarios.

CI runs on pull requests and pushes to `main`; feature-branch pushes do not start a second copy of the same checks. New commits cancel superseded runs. Dependency and Docker layer caches reduce repeated downloads and builds; tests still execute on cache hits. On main, the Linux fault matrix triggers for Go code, its dependencies, the Makefile, acceptance/load harnesses, and its own workflow. Frontend-only tests and container configuration checks use their relevant CI workflows without rerunning unrelated Linux fault scenarios.

Pull requests use `go test -short` for unit tests while retaining static checks, security scans, builds, frontend checks and installation smoke tests; relevant changes still run container checks. The feedback target is about 1–2 minutes with warm caches and an available runner, not a hard execution limit.

Full fault acceptance runs after relevant changes reach `main`, every day at 03:17 Asia/Shanghai (19:17 UTC), and on manual dispatch. It includes the full Linux race suite, real remote storage, repeated process recovery, and sustained faults. These checks are outside the PR fast-feedback stage, so long-test failures may be discovered after merge; run manual deep acceptance before merging high-risk changes.

Load acceptance runs six parallel stages: full Linux race regression, repeated cluster recovery, capacity with and without the race detector, single-node soak, and cluster soak. Every stage validates fresh evidence, and the final job requires all six stages and their expected records. Resource limits, repeat counts, and both three-minute soak windows remain unchanged. The full race suite and vet run once in the regression stage. Process recovery is split into SIGKILL, cluster, and loaded-cluster groups; every selected test still runs three times. Remote-service faults remain a separate job.

To reproduce a single load stage, set `OWLCRON_LOAD_STAGE` to `regression`, `cluster`, `capacity-race`, `capacity-plain`, `daemon-soak`, or `cluster-soak` before `make linux-load-acceptance`. Omit it for the complete sequential matrix. `OWLCRON_ACCEPTANCE_SKIP_BASELINE=1` is reserved for CI runs that also require the Linux regression stage; standalone remote acceptance includes its baseline by default.

Workflow files define the checks; the GitHub Actions run for a commit is the source of its execution status. Record the commit, toolchain, resources, backend, commands, exit codes, and skipped cases with test results.

Keep local logs, failure stacks, scan output, and benchmark data outside the repository or as CI artifacts. Retain reusable test scripts and regression cases in the repository. Maintain deployment-relevant limitations in product documentation instead of removing them with temporary reports.

## Go quality reports

Go quality reports run on tags, published releases, and manual dispatches. They run tests with the race detector and coverage, check the independent example plugin, and generate Go Report Card results. Download reports and raw test evidence from the workflow artifacts (retained for 14 days). Stable reports are committed only when the tested commit still matches the default branch; otherwise they remain artifacts. The existing CI continues to check pull requests.

[Reports / Go Tests and Report Card](https://github.com/soulteary/owlcron/actions/workflows/go-quality-reports.yml)

The path-filtered [CI / Browser Tests](../.github/workflows/browser.yml) workflow
runs the [browser acceptance tests](../tests/browser/README.md): general UI smoke
checks plus the isolated SMTP/account-email flow. The latter exercises email
verification and password recovery, link privacy, token replay rejection and
session revocation. Run both locally with the commands in the browser guide;
they require local ports and a pinned Playwright browser, but no external SMTP service.

Workflow display names are grouped under `CI /`, `Reports /`, and `Release /`; filenames stay unchanged so existing links remain valid. The README badges follow test status, Go Report Card, release and license order. The Go Tests badge shows the main branch build-and-test workflow status, not a coverage percentage. The Go Report Card badge uses the public assessment service and may differ in freshness from the workflow-generated report. Generated coverage and quality badges are saved as `.github/coverage.svg` and `.github/goreportcard.svg`; before the first successful writeback, inspect the workflow artifacts.

## Single-node UI release gate

The browser smoke runner covers the following pages against a disposable local
Filesystem daemon. It drives visible controls and checks persisted responses;
these checks do not connect to an existing installation.

- **Home:** dashboard and live jobs render; a second singleton run queues, the
  administrator flushes it, and a read-only account cannot see abort or
  queue-flush actions.
- **Schedule:** create and edit an event, reload its title/retention, preserve
  API-managed chain arrays, and persist timezone, daily timing, timeout, retry
  delay, queue limits and both chain selectors. Chain targets use on-demand
  timing; other scheduled fixtures stay disabled to avoid wall-clock dependent
  execution.
- **Job Details:** manual execution, archived Run Again, live Abort, failed job
  logs, a single bounded retry, a one-second timeout, successful/failing chain
  execution, progress/
  resource charts and read-only action visibility. Direct mutation
  API requests from the read-only account must also fail.
- **History:** list-to-job navigation, event statistics and chart tooltips.
- **My Account/Login:** profile persistence and disabled-registration help in
  smoke; email verification, password recovery and session revocation in the
  separate isolated SMTP runner.
- **Administration:** activity, API keys, categories, plugins, server cluster and
  users render; temporary keys/categories/plugins/users are created, edited and
  deleted. Non-admin access redirects to Home and the users API rejects it.

Timing persistence is a browser check, not proof of a scheduled firing. Engine
regressions separately cover timezone/DST matching, catch-up, concurrency,
abort-without-retry, timeout updates and persisted job history after restart
(`engine/scheduler_cluster_test.go` and `engine/job_integration_test.go`).
Before a candidate release, also run the full Go suite and the clean release
package installation check. The browser CI matrix runs Chromium, Firefox and
WebKit independently, including the isolated account-mail flow. It verifies
login and the main/admin pages at 320, 375 and 768 CSS pixels, local table
scrolling and date/time dialogs. Physical keyboard tests cover Tab/Shift+Tab,
Enter/Space, modifier shortcuts, script-editor exit, dialog cancellation and
focus restoration.

Before publishing, manually review long-log readability and notification content,
and verify scheduled execution in the operator's actual timezone. Real mobile
devices, screen readers and every Chrome/Edge/Safari release are not covered by
this engine matrix. Cluster fault tests remain a separate capability gate and
do not expand the single-node compatibility promise or establish full Cronicle
API/plugin compatibility.
