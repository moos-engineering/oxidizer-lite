# Production Recovery

This guide is a safe investigation and recovery sequence, not a universal
backup/restore procedure. Redis, Kafka, RustFS, AWS S3, and S3 Tables have
different durability and restoration controls. Validate the procedures for
the exact services and versions in the deployment before relying on them.

## Before Intervention

Record the UTC incident time, `formula_id`, `run_id`, `node_id`, last known
checkpoint, relevant message IDs/offsets, and affected connection names. Capture
redacted logs and read-only state before restarting or changing anything.
Never include credentials, full connection documents, or sensitive record
payloads in incident notes.

Do not reset a database, delete a stream/table, acknowledge a pending message,
reset a Kafka offset, or replay a task just to see whether it fixes the run.
First establish whether the destination write committed and what the retry would
write again.

## Health Checks

Use the deployment's normal monitoring and credentials. These checks should not
change service state:

| Component | Read-only check | What it establishes |
| --- | --- | --- |
| Oxidizer API | Request the configured `/api/health` endpoint | The API responds; it does not prove controller or worker progress. |
| Controller/worker | Inspect orchestrator/container status and recent logs | The process is running and whether it is reporting errors; a running process is not proof of progress. |
| Redis | `PING`, `INFO persistence`, then `XLEN`, `XINFO GROUPS`, and `XPENDING` for the relevant stream/group | Connectivity, configured persistence status, backlog, and pending deliveries. These do not show whether an Iceberg write committed. |
| Kafka | `rpk cluster health`, `rpk topic describe`, and `rpk group describe <group>` | Broker health, topic configuration, consumer membership, committed offsets, and lag. Do not reset offsets during diagnosis. |
| RustFS S3 | Check `/health`, then use S3 `HEAD`/`LIST` against the known bucket/prefix | Service and object-store access. Ordinary S3 listing may not expose protected Iceberg catalog metadata. |
| Iceberg destination | Read the run's target table with a bounded `SELECT` through the configured catalog | Whether the expected output rows are visible. Do not infer commit status from a worker log alone. |

For Redis group and run-state interpretation, use [Workflow Run Troubleshooting](../troubleshooting/workflow-runs.md). For controller ordering and persisted updates, use [Controller DEBUG Trace](../troubleshooting/controller-debug-trace.md). Keep SQL read-only and use a row limit.

## Backup And Restore

Backups are deployment responsibilities; this package does not provide a
coordinated backup or restore command. Define retention, encryption, access,
and recovery objectives with the relevant datastore operator.

- **Redis:** Select and monitor RDB/AOF persistence or the managed service's
  backup facility. A backup must include the Formula state and the stream/group
  state needed by the deployment. Test restoration into an isolated Redis
  instance and verify the selected database, run state, streams, and consumer
  groups before considering it usable.
- **S3/RustFS Formula storage:** Back up the configured objects and versions
  using the provider's supported mechanism. Verify restore into a separate
  prefix/bucket and confirm formulas and run states can be read.
- **RustFS Iceberg:** A usable backup must preserve table data and the catalog
  metadata consistently. An object-only copy may omit protected catalog state;
  ordinary S3 listing/deletion is not a substitute for a RustFS-supported
  catalog/volume backup. Test restore into an isolated warehouse and verify
  table discovery plus representative reads.
- **AWS S3 Tables:** Use AWS-supported resilience controls and permissions for
  the service. Test recovery into a separate test table bucket or account where
  supported. Do not treat copying S3 objects or a successful write test as
  proof that the catalog and table can be restored.
- **Kafka/Redpanda:** Broker replication and retention are availability controls,
  not backups. Define a cluster recovery or replication strategy that preserves
  required topics and consumer offsets. Test recovery with the intended group
  IDs; do not reset offsets as a substitute for restore.

Do not test restore by overwriting production state. Use an isolated destination
and record the source snapshot, restore point, and validation queries.

## Restart

Use the deployment orchestrator's approved restart procedure; the following is
the ordering principle, not a portable command sequence:

1. Capture current run/checkpoint state, pending work, offsets, and recent logs.
2. Pause new invocations or dispatch through the normal control plane.
3. Gracefully stop controller and worker processes. Avoid killing a worker
   while its output transaction is active unless deliberately testing that
   failure boundary.
4. Restart or restore a datastore only if its own health checks indicate it is
   necessary. Preserve its data volumes and configuration.
5. Verify datastore and catalog health/readability before restarting workers
   and controllers.
6. Confirm run state, consumer lag/pending work, and destination readback before
   resuming new workload.

Restarting roles does not automatically repair a task left pending or a run
left in `running`. Do not delete persistent volumes or reset streams as part of
a routine restart.

## Replay And Pending Work

Oxidizer does not currently provide a general automatic recovery command for
abandoned work. Redis pending messages are not automatically reclaimed by the
normal worker read path. Kafka resumes from configured consumer-group offsets;
the adapter does not provide an operator offset-reset workflow. Do not imply
that restart alone guarantees redelivery.

Before an operator-approved replay:

1. Identify the exact task/source message and saved checkpoint; confirm whether
   the worker published a checkpoint or acknowledgement.
2. Read the destination to determine whether the write committed. Append can
   duplicate rows on replay; keyed upsert/SCD2 may converge for identical input,
   but that does not make other outputs or streams exactly once.
3. Decide whether replay is supported for that workflow and define how duplicate
   effects will be detected or handled. If not, stop and escalate rather than
   manually acknowledging or changing offsets.
4. Use a controlled, reviewed recovery procedure and preserve the original run
   identity and evidence. Afterward verify state, checkpoint, source delivery,
   and destination rows.

The [recovery-boundary integration test](../../tests/integration/test_recovery_boundaries.py)
demonstrates the current RustFS append behavior for process death before
checkpoint publication and after publication but before task acknowledgement.
It deliberately re-enqueues work to measure duplicate behavior; it does not
implement automatic pending-task recovery or establish exactly-once semantics.

## Validation Required Before Production

For every deployed backend combination, exercise health checks, restart, backup
restore into an isolated environment, and replay behavior with realistic
failures. Include process death after output commit and at each checkpoint/ack
boundary. Record the expected duplicate/loss behavior and recovery owner. The
RustFS integration test covers only its documented boundary; AWS recovery and
provider-specific backup/restore remain deployment-specific until separately
validated.