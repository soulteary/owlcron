# Versions and releases

English | [简体中文](zh-CN/releases.md)

## Supported toolchain and platforms

Go 1.27 is the supported and CI-tested build series. Use Go 1.27.3 or a newer
security patch in that series. The `go 1.26.0` directive in `go.mod` is the module's
language baseline, not a promise of support or testing on Go 1.26. The separate
`toolchain go1.27.3` directive selects the recommended compiler when automatic
toolchain selection is enabled. Go 1.25 and 1.26 are not in the supported matrix.
Frontend/release builds use Node.js 22, npm, Python 3.9 or later, and make.

The release workflow builds and executes Linux amd64 binaries on its native
runner. The packaging script can also validate a native developer build (such
as macOS arm64), but that does not extend the published support matrix. It
rejects cross-compilation because the resulting binaries could not be run by
its validation steps. A platform becomes supported only after native build and
runtime checks are added.

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
directory, checks executable metadata and the adapter, and runs the existing
installation smoke test against the extracted frontend and plugin. A digest
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

Pull requests affecting packaging and manual workflow runs validate a
`v0.0.0-dev` archive without creating a release. Only the draft-creation job has
repository write permission; build and verification jobs have read permission.
The workflow does not add signatures, SBOMs, or provenance attestations, and
reproducible byte-for-byte archives are not yet guaranteed.
