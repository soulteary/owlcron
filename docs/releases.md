# Versions and releases

English | [简体中文](zh-CN/releases.md)

## Supported toolchain and platforms

Go 1.27 is the supported and CI-tested build series. Use Go 1.27.3 or a newer
security patch in that series. The `go 1.26.0` directive in `go.mod` is the module's
language baseline, not a promise of support or testing on Go 1.26. The separate
`toolchain go1.27.3` directive selects the recommended compiler when automatic
toolchain selection is enabled. Go 1.25 and 1.26 are not in the supported matrix.
Frontend/release builds use Bun 1.4.2, Python 3.9 or later, and make.

The release workflow builds and executes Linux amd64 binaries on its native
runner. The packaging script can also validate a native developer build (such
as macOS arm64), but that does not extend the published support matrix. It
rejects cross-compilation because the resulting binaries could not be run by
its validation steps. A platform becomes supported only after native build and
runtime checks are added.

## First-release scope

The current candidate is `v0.1.0-rc.1024`; see the bilingual
[release notes](release-notes.md). Preparing an archive does not publish a release.

The first release targets a single node with familiar Cronicle pages and
workflows. Run one standalone OwlCron node, validate your events and permissions,
and review the [compatibility boundary](compatibility.md). Cluster discovery,
failover and remote dispatch remain experimental and are outside the initial compatibility scope. This is not a drop-in data or cluster upgrade
from Cronicle; use the [migration preflight](migration.md) and manually recreate
reviewed configuration, credentials, plugins and events.

## Build and verify a local archive

Install the supported toolchain, then run from the repository root:

```sh
make release VERSION=v0.0.0-dev
python3 tools/verify_release.py dist/*.tar.gz
```

The packager exports committed `HEAD` into a clean temporary source tree, installs
the locked frontend dependencies there, and builds only that snapshot. Local
configuration changes, untracked files and ignored files are excluded. Commit
intended release changes first; the archive never includes uncommitted edits.

The archive contains the engine, script adapter, Echo Plugin, materialized
frontend assets, example configuration, installation instructions, and the full
license directory. It has no dependency on the source checkout or node_modules.
`BUILD.json` records the version, source commit, build target and toolchain;
`owlcron-engine version` prints the version and source commit.
The version command does not load configuration or create runtime files. Some
Cronicle compatibility responses retain an upstream protocol version; use the
CLI and `BUILD.json` to identify the OwlCron release.

The adjacent `.tar.gz.sha256` file is compatible with `sha256sum --check` on
Linux. The verification command checks the digest, extracts to a temporary
directory, checks executable metadata and the adapter, and runs a single-node
acceptance test against only the extracted distribution. The test checks the
built frontend and its linked JavaScript, worker and font assets, first boot and
login, actual jobs using the bundled Echo Plugin and script adapter, history and
logs, graceful restart with persistent state, and an offline backup imported into
fresh storage followed by another successful job. It stops
the daemon before both export and import. The test deletes the original
data, log and queue directories before restoration and checks both prior job
histories and their persisted log contents. Storage export includes persisted
job logs, but not configuration, script files or independent local log files;
preserve those separately.

First-boot acceptance uses the documented generated administrator credentials,
checks their private file mode, removes the bootstrap file after successful
login, and confirms that restart and restore do not recreate it. Add
`--runtime-checks` to verify non-UTC timezone support, real process memory
sampling and memory-limit enforcement with the host runtime dependencies from
the [deployment guide](deployment.md#installation-and-startup). These checks use
the normal runtime `PATH` after the separate empty-`PATH` execution check.

The harness uses Python's standard library from the source checkout; the daemon
and its child plugins run outside the checkout with an empty executable `PATH`,
without Go, Bun, Node.js or `node_modules`. `/bin/sh` is required for this test's
shell script. Production custom plugins may require their own interpreters and
dependencies. The release workflow repeats this same verification inside a
separate Linux Python slim container without Go, Bun or Node.js. It mounts only
the built archive and four Python helper files, with no source checkout,
disabled networking, an unprivileged user, a read-only root filesystem and a
temporary writable `/tmp`. Loopback remains available for the application test.
Python is test infrastructure; the extracted Go application does not depend on
it. This extra check validates the Linux archive in an isolated runtime, not the
project's separately built deployment image or every target operating system. A digest
checks file integrity; it is not a signature or independent proof of origin.
The output directory must not already contain the same archive name. Completed
archives and checksums are staged before final filenames are created. Archive
checks remain active under optimized Python; unsafe paths and links are rejected
before any content is extracted or executed.

## Maintainer release process

Use semantic version tags of the form `vMAJOR.MINOR.PATCH` with an optional
prerelease suffix. Until a stable 1.0 policy is announced, APIs, persisted data
and configuration may still change; back up and test upgrades and rollback.
No automatic data rollback guarantee is implied by a version number.

1. Merge reviewed changes, require green CI, and review both language editions
   of the documentation and third-party notices.
2. A maintainer deliberately creates and pushes an annotated version tag for
   that reviewed commit. Ordinary branch pushes do not publish releases.
3. The [release workflow](../.github/workflows/release.yml) builds a native Linux
   amd64 archive, verifies its checksum and extracted runtime, and creates a
   **draft** GitHub release with the archive and checksum attached.
4. Inspect the draft notes and assets, test the intended deployment and recovery
   path, and publish the draft manually when ready. Do not move published tags.

Pull requests affecting packaging validate a `v0.0.0-dev` archive; manual runs
can select an archive version without creating a release. Only the draft-creation job has
repository write permission; build and verification jobs have read permission.
The workflow does not add signatures, SBOMs, or provenance attestations, and
reproducible byte-for-byte archives are not yet guaranteed.

## Prepare the candidate without publishing

After committing the intended changes, build and verify the candidate locally:

```sh
make release VERSION=v0.1.0-rc.1024
python3 tools/verify_release.py dist/*.tar.gz
```

On a Linux host with Nginx and OpenSSL installed, also run the deployment and
synthetic migration checks against that same archive:

```sh
go build -trimpath -o owlcron-migrate-check ./cmd/owlcron-migrate-check
python3 tools/verify_release.py dist/*.tar.gz --deployment-checker ./owlcron-migrate-check
```

For the supported Linux amd64 package, manually dispatch **Release / Build and
Package** against the reviewed branch or commit's branch, setting `version` to
`v0.1.0-rc.1024`. Download the `linux-amd64-release` Actions artifact after all
checks pass. A manual run has no release-write job and creates neither a tag nor
a GitHub Release. Verify that the run's source commit matches `BUILD.json`; do
not substitute a later branch head for the tested commit.

The release workflow performs both checks above, including Nginx at the root
path and `/tools/cron`, and repeats the basic extracted-runtime acceptance in a
separate container without build tools. Store logs and artifact digests with the
workflow run, outside tracked project files. At release time, require green CI
for the final merged source commit and rebuild if its contents changed.

Before publication, the deployment owner must still check their real hostname,
certificate chain, proxy/firewall rules, filesystem ownership, business scripts,
local timezone and any enabled SMTP relay. The synthetic rehearsal cannot attest
to an existing Cronicle installation or authorize production cutover.

## Verify a published download

The [published-release workflow](../.github/workflows/published-release.yml)
downloads the archive and checksum from the public GitHub release URLs, resolves
the release tag's source commit, and requires matching version/commit metadata
before running the extracted files. It repeats generated-account bootstrap,
jobs, restart, fresh-store restore, timezone and resource-monitoring acceptance.
It runs after publication and supports manual verification of a version; it
does not rebuild or replace the published archive. Its evidence log is retained
as a workflow artifact. HTTPS proxy and synthetic migration checks remain part
of the pre-publication release pipeline.

To reproduce on Linux, first install the documented runtime dependencies and
download the archive and adjacent checksum from the release page. Use the
validator from current source without building the application:

```sh
python3 tools/verify_release.py owlcron-v0.1.0-rc.1024-linux-amd64.tar.gz \
  --expected-version v0.1.0-rc.1024 \
  --expected-commit f64ecb8ce6c206fb4b838412ff84eff803bde95c --runtime-checks
```

The expected source for this published candidate is fixed above; for another
version, resolve that tag's commit rather than copying the example. The
validator uses Python as test infrastructure. The installed application still
does not require Python. Existing release tags and binary attachments stay
unchanged when documentation or validators are corrected later.
