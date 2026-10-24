# Running version-controlled script files

English | [简体中文](zh-CN/script-files.md)

Build `owlcron-script` with `make build`. The container image includes it at
`/opt/owlcron/owlcron-script`. Mount reviewed scripts read-only (for example at
`/opt/jobs`) and make them readable by UID 10001. Interpreters beyond `/bin/sh`
must be installed in a derived image. This adapter runs an administrator-selected
command, forwards one job JSON line to its stdin, treats stdout/stderr as log data,
and emits the plugin completion message itself. Ordinary scripts do not need to
implement the plugin protocol. A script cannot report a false success by printing
JSON: its process exit status determines the adapter result.

Create a plugin with a fixed command and working directory, for example:

```json
{
  "id": "daily-report",
  "title": "Daily report script",
  "enabled": 1,
  "command": "/opt/owlcron/owlcron-script -- /bin/sh /opt/jobs/report.sh",
  "cwd": "/opt/jobs",
  "params": [
    {"id": "args", "type": "text", "title": "Arguments (JSON array)", "value": "[]"}
  ]
}
```

An event can supply `"params": {"args": "[\"daily\",\"two words\",\"\"]"}`.
Arguments are passed directly, without shell expansion. `args` must encode at most
256 strings within 64 KiB; job input is limited to 1 MiB. The executable must use
an absolute path. Parameters cannot replace the executable, interpreter, script
path, working directory, environment or OS identity. Native protocol plugins
should run directly, since this adapter intentionally treats progress/report
messages as logs.

Only trusted administrators should manage plugin commands. Scripts run with the
service account's privileges and are not sandboxed. Review the script's own
argument parser: data can still have meaning to that script even without shell
expansion. Deploy reviewed, fixed script revisions to all eligible nodes; do not
pull unreviewed code during each execution. Engine cancellation reclaims the
adapter's process group and ordinary descendants; deliberately detached processes
remain outside that guarantee. The adapter does not create a new process group.
