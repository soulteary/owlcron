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

[Routine CI](../.github/workflows/ci.yml) checks Go 1.27, Bun 1.4.2, builds, unit tests, installation smoke tests, licenses, and vulnerabilities. [Fault acceptance](../.github/workflows/process-acceptance.yml) covers process and Linux fault scenarios.

CI runs on pull requests and pushes to `main`; feature-branch pushes do not start a second copy of the same checks. New commits cancel superseded runs. Dependency and Docker layer caches reduce repeated downloads and builds; tests still execute on cache hits. On main, the Linux fault matrix triggers for Go code, its dependencies, the Makefile, acceptance/load harnesses, and its own workflow. Frontend-only tests and container configuration checks use their relevant CI workflows without rerunning unrelated Linux fault scenarios.

Pull requests use `go test -short` for unit tests while retaining static checks, security scans, builds, frontend checks and installation smoke tests; relevant changes still run container checks. The feedback target is about 1–2 minutes with warm caches and an available runner, not a hard execution limit.

Full fault acceptance runs after relevant changes reach `main`, every day at 03:17 Asia/Shanghai (19:17 UTC), and on manual dispatch. It includes the full Linux race suite, real remote storage, repeated process recovery, and sustained faults. These checks are outside the PR fast-feedback stage, so long-test failures may be discovered after merge; run manual deep acceptance before merging high-risk changes.

Load acceptance runs six parallel stages: full Linux race regression, repeated cluster recovery, capacity with and without the race detector, single-node soak, and cluster soak. Every stage validates fresh evidence, and the final job requires all six stages and their expected records. Resource limits, repeat counts, and both three-minute soak windows remain unchanged. The full race suite and vet run once in the regression stage. Process recovery is split into SIGKILL, cluster, and loaded-cluster groups; every selected test still runs three times. Remote-service faults remain a separate job.

To reproduce a single load stage, set `OWLCRON_LOAD_STAGE` to `regression`, `cluster`, `capacity-race`, `capacity-plain`, `daemon-soak`, or `cluster-soak` before `make linux-load-acceptance`. Omit it for the complete sequential matrix. `OWLCRON_ACCEPTANCE_SKIP_BASELINE=1` is reserved for CI runs that also require the Linux regression stage; standalone remote acceptance includes its baseline by default.

Workflow files define the checks; the GitHub Actions run for a commit is the source of its execution status. Record the commit, toolchain, resources, backend, commands, exit codes, and skipped cases with test results.

Keep local logs, failure stacks, scan output, and benchmark data outside the repository or as CI artifacts. Retain reusable test scripts and regression cases in the repository. Maintain deployment-relevant limitations in product documentation instead of removing them with temporary reports.
