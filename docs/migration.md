# Checking a Cronicle migration

English | [简体中文](zh-CN/migration.md)

OwlCron aims to replace Cronicle with selected compatibility. Use this read-only preflight before converting data. It does not import records, run plugins, initialize storage, create credentials, or contact a cluster.

## Run the check

Export with Cronicle's own `bin/control.sh export cronicle-export.txt`. The supported input is its **Cronicle Data Export v1.0** format: a comment header followed by `KEY - JSON` lines, with list headers and pages. It is not a JSON array or an OwlCron backup. Keep the original export private: it can contain password hashes, API keys, scripts and environment variables. The export excludes completed job history, logs, sessions and application configuration.

Build and run the separate checker from the source tree:

```sh
go build -o owlcron-migrate-check ./cmd/owlcron-migrate-check
./owlcron-migrate-check --export cronicle-export.txt
./owlcron-migrate-check --export cronicle-export.txt --config cronicle-config.json
```

`--config` optionally inspects an existing Cronicle JSON configuration without using it to initialize OwlCron. The checker has no write or import mode. Input is limited to 32 MiB per file; larger installations must be assessed separately. A successful inspection exits with status 0 even when findings require changes; malformed, incomplete or unreadable input exits with status 1. Consume the JSON `findings` to apply your own migration gate.

## Interpret the report

- `supported`: a specific behavior matches the current schema, such as event timing. This is not approval of the whole event or deployment.
- `requires_conversion`: change storage format, recreate credentials/node identities, map missing targets, or review plugin commands and parameters.
- `unsupported`: detached execution, invalid time zones/timing or other invalid event data, missing references, or configuration outside the OwlCron schema.

`item` is the one-based position in the reconstructed source list, in page order. Counts and findings are deterministic; reports do not contain source IDs, titles, commands, secrets or configuration values. Structural errors identify a list or line number without echoing its contents. Treat exports and optional configuration files as sensitive even though report output excludes their values.

Success (`chain`) and failure (`chain_error`) targets are checked against the exported schedule, using the runtime's string and array forms. Missing targets produce `missing_chain` or `missing_chain_error` findings without exposing target IDs. This checks references only; it does not execute chains or verify their runtime behavior.

Cronicle's inline `shell-plugin.js` needs conversion to reviewed, fixed script files and an `owlcron-script` plugin configuration. Custom plugins require verification of their runtime, command parsing, uid/gid, environment and JSON-line completion contract. The checker never executes their commands and cannot prove their business behavior. See [script files](script-files.md) and [compatibility](compatibility.md).

## Complete the migration deliberately

For the candidate release, also follow the [cutover rehearsal](#rehearse-before-cutover) below.

Resolve every finding, prepare a new OwlCron configuration and identities, and convert only the reviewed supported records into OwlCron's own format. Do not pass the original Cronicle export to `owlcron-engine import`. Test a separate deployment with synthetic or sanitized data, verify time zones, event targets, permissions, notifications, script exit behavior and backups, then plan cutover and rollback. Review [deployment](deployment.md) and [known limitations](limitations.md), including uncertain remote dispatch and external side effects.

Reusable synthetic fixtures live under `migration/testdata` and `plugin/testdata`. The export shape follows [Cronicle's exporter](https://github.com/jhuckaby/Cronicle/blob/master/bin/storage-cli.js); fixture data is authored for tests and contains no installation data. Tests cover paginated input, invalid input, deterministic reports, read-only behavior and fragmented JSON-line plugin output. They establish selected contracts, not full compatibility with every Cronicle version or plugin.

## Rehearse before cutover

The [candidate deployment check](releases.md#prepare-the-candidate-without-publishing)
runs the checker twice against the synthetic export and verifies identical
reports without modifying the input. It recreates the two known fixture events
with new local targets, a new administrator and a reviewed file script. It
verifies preserved daily/on-demand timing and UTC timezone, successful execution,
a nonzero script exit, read-only user restrictions, WSS logs and offline restore.
Fixture events stay disabled and are triggered manually; this does not prove
wall-clock scheduling in a real installation's timezone.

For your own migration, use this order:

1. Keep the original export and configuration private, record the Cronicle
   version, and review every preflight finding. Exit status zero means the file
   was inspected; it does not mean the migration is approved.
2. Create a separate OwlCron instance with a new secret, empty storage and new
   accounts/API keys. Map categories and targets explicitly to the single node.
   Do not copy credentials, node identities or storage files.
3. Review each plugin's command, runtime, working directory, permissions,
   environment and parameter schema. Move inline scripts to fixed files; never
   automatically execute commands from an unreviewed export. Keep recreated
   events disabled until validation is complete.
4. Direct notifications to test recipients and replace external side effects
   with safe test destinations. Check success, failure, timeout, retry and chain
   behavior needed by your workload. Verify the next run and an actual scheduled
   execution in the intended timezone, including DST where relevant.
5. Confirm administrator and restricted-user behavior, logs, restart and an
   offline restore in a new namespace. Save the artifact digest, source commit,
   mapping decisions and sanitized outcomes outside the public repository.
6. At cutover, pause the original scheduler, drain its running/queued work, take
   a final backup and only then enable reviewed OwlCron events. Keep the previous
   installation and backup available. To roll back, stop OwlCron first, reconcile
   work already performed and restore the original scheduler without running
   both against the same business operations.

This process does not import completed Cronicle job history or logs. Synthetic
acceptance is a repeatable baseline, not evidence that production plugins,
notifications or cross-version rollback have been validated.
