# Testing and validation

English | [简体中文](zh-CN/testing.md)

Run commands from the repository root unless stated otherwise. Use the recommended patched Go toolchain and Node.js 22. See [deployment](deployment.md) for prerequisites and permissions.

## Routine checks

```sh
npm ci --prefix htdocs-deps
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
npm audit --prefix htdocs-deps --audit-level=moderate
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
(cd bin/echo-plugin && go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...)
```

Scans use the advisory database available at runtime. Distinguish reachable calls, imported packages, and module-only findings. Do not describe an unused package advisory as confirmed exploitation, or ignore a reachable vulnerability.

After npm upgrades, run `make licenses-update licenses-check` and review changed terms. Go dependency changes also require updating `licenses/go-inventory.json` and the relevant upstream notices. Run `npm ci` before checking license inventories.

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

[Routine CI](../.github/workflows/ci.yml) checks Go 1.27, Node.js 22, builds, unit tests, installation smoke tests, licenses, and vulnerabilities. [Fault acceptance](../.github/workflows/process-acceptance.yml) covers process and Linux fault scenarios.

Workflow files define the checks; the GitHub Actions run for a commit is the source of its execution status. Record the commit, toolchain, resources, backend, commands, exit codes, and skipped cases with test results.

Keep local logs, failure stacks, scan output, and benchmark data outside the repository or as CI artifacts. Retain reusable test scripts and regression cases in the repository. Maintain deployment-relevant limitations in product documentation instead of removing them with temporary reports.
