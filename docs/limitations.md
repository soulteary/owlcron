# Known limitations

English | [简体中文](zh-CN/limitations.md)

OwlCron is intended for development, evaluation, and controlled deployments. It does not yet provide stable-release compatibility or production capacity guarantees. This page records limitations relevant to deployment decisions. See [architecture](architecture.md) for implementation details and [testing](testing.md) for reproduction entry points.

## Sustained cluster convergence

Strict sustained cluster fault scenarios still have unresolved convergence timeouts. After a job finishes execution, its history, logs, notifications, and completion acknowledgment may not all be verifiable before the batch's shared deadline. The current test budget is 15 seconds; it is a test threshold, not a deployment latency guarantee.

Passing ordinary tests or a single recovery cycle does not replace sustained fault testing. Remaining delays need to be diagnosed and tested against the same criteria, without giving each job a new observation deadline.

## Remote dispatch acknowledgment

Remote dispatch does not yet have a separate durable acceptance acknowledgment protocol. Successfully writing a control frame does not mean a worker has durably accepted the job. A disconnect can leave dispatch acceptance uncertain. Event queue recovery and completion replay do not eliminate this window.

## External side effects and notifications

The system does not guarantee exactly-once business side effects. Plugins need business-level idempotency keys. A job recorded as interrupted or failed may already have performed an external operation. Email and WebHook delivery is at least once; recipients should deduplicate using stable identifiers.

## Storage and capacity

Filesystem exclusive locks and synchronous writes, aggregate history reads, directory scans, and persistent deletion markers affect capacity. Retention and per-job log limits are not a node-wide disk quota. Monitor disk usage and cleanup backlogs separately.

Shared storage must provide the required atomic operations and durability. Results from one local mount cannot be generalized to NFS, other remote backends, or different resource allocations.

## Unsupported or unvalidated scenarios

- Go nodes cannot join an original Cronicle cluster or directly use its storage directory.
- Detached plugins cannot continue across engine restarts. Verifiable interrupted processes are reclaimed and their jobs marked failed.
- Plugins are not sandboxed; administrator-configured commands run with the service account's permissions.
- Long-duration stability, cross-host networks, remote-backend capacity, release upgrades and rollback, and business plugins require validation in the target environment.
- Compatibility is not guaranteed for every operating system, filesystem, cloud IAM/TLS configuration, or multi-node storage failover setup.

Include reproducible cases when addressing these limitations and update both language editions. Store raw logs as CI artifacts or external evidence rather than replacing maintained product documentation with one-off reports.
