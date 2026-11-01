# Troubleshooting Workflow Runs

## Goal

Find the component that stopped advancing a workflow run using read-only evidence first. Do not repair state or manipulate messages until the run identity, current state, and intended recovery are clear.

## Intake

Capture these facts before diagnosing:

- `formula_id`, `run_id`, and affected `node_id`.
- Formula backend and the configured connection names/types.
- Last controller tick and worker invocation time.
- Exact node status and checkpoint metadata.
- The complete exception type/message and nearby structured logs.
- Relevant invocation, task, and controller update message IDs.

Never include passwords, API tokens, session tokens, or full connection secrets in a report.

## Read-Only Investigation Order

### 1. Confirm the run exists

Load the run state through the configured `Formula` connection. Inspect only `status`, `dag.status`, the target node's `inputs`/`outputs`, and its `checkpoint_metadata`; avoid dumping node output payloads because they may contain large or sensitive records.

Questions:

- Does the requested `(formula_id, run_id)` exist in the configured backend?
- Is the node `pending`, `scheduled`, `dispatched`, `running`, `ready`, `success`, or `failed`?
- Do node IDs and upstream references use the same formula prefix?

### 2. Follow the controller sequence

The controller tick consumes invocation, then one controller update, then attempts to dispatch ready nodes. A worker may emit `started` and a final `checkpoint` in one execution, but the controller consumes those updates on separate ticks when `count=1`. The final checkpoint, rather than a separate worker `success` update, makes the node successful.

If a node is `ready` after a checkpoint, run/observe a controller tick before concluding redispatch failed. If it remains `running`, the controller has not received a terminal/checkpoint update or the update was not successfully persisted.

### 3. Inspect stream state without changing it

For Redis, inspect stream length, consumer groups, lag, and pending entries with read-only client calls such as `XLEN`, `XINFO GROUPS`, and `XPENDING`. The key streams are:

- `oxidizer:invocations`
- `oxidizer:reagent`
- `oxidizer:controller`
- Data streams named by node IDs or explicit method configuration.

Redis Catalyst wraps written values in a `message` field; task/update readers unwrap that envelope. Kafka writes JSON values and commits consumer offsets when `ack_message` is called. Kafka's `group_id` is connection configuration; the Catalyst `consumer_group` method is not a Redis-style group creation operation.

Do not `XACK`, `XDEL`, delete stream keys, reset offsets, or change consumer groups during diagnosis.

For MCP clients, use `list_formula_runs(formula_id)` to discover run IDs, then
`get_transport_health(stream_name, group_name)` for read-only stream/topic
metadata. REST users can call `GET /api/transport/health` with the same query
parameters. This inspects the server's primary Catalyst only. Redis reports
group pending/lag; Kafka reports partition offsets and optional group lag using
the real configured Kafka group ID. A null lag is unknown, not zero. Neither
tool consumes messages or repairs pending work. See
[transport inspection](../reference/topology-api-and-tools.md#read-only-transport-inspection)
for fields and limitations.

### 4. Correlate logs

For a successful baseline and a role-specific LLM handoff, choose [Worker DEBUG trace](worker-debug-trace.md) for task/input/handler/output issues or [Controller DEBUG trace](controller-debug-trace.md) for invocation/dispatch/state-update issues. Both support container logs and persisted API/MCP log retrieval.

Use `formula_id`, `run_id`, `node_id`, stream name, and message ID to connect `Oxidizer`, `Reagent`, `Catalyst`, `Formula`, `Anvil`, and `Crucible` events. Residue logs carry a component name and can include IDs as structured fields.

For a DEBUG investigation, follow the [expected process trace](../reference/logging-and-sinks.md#expected-process-trace) from invocation preparation through input, handler, output, checkpoint, acknowledgement, and controller state processing. Match the same task/run IDs and identify the first start marker without its completion marker. The guide documents exact event names, role-specific DEBUG settings, and the next check for each unfinished boundary.

Before treating missing logs as evidence, verify the emitting process's log level, that a sink was attached if using persisted logs, and sink-specific durability. Redis writes immediately but can trim or expire its whole stream; sink failures are non-fatal and reported once to stderr. The container entrypoints log to stdout without automatically attaching a Redis sink. Follow [Logging and sinks](../reference/logging-and-sinks.md) for retrieval and configuration checks.

For a worker exception in the current Reagent wrapper, look for `REAGENT: Worker task failed`; it carries the run/node identifiers, batch index, exception type, and message. Pair it with the subsequent `failed` controller update and persisted `error_details`. A failed update may be visible in Formula state even if the original exception log was filtered, the sink was not attached, or the sink write failed.

### 5. Classify the failure boundary

| Observation | First boundary to inspect | Useful evidence |
| --- | --- | --- |
| Invocation written but no state | Controller/formula loading | Invocation message, formula config key, connection references, controller error. |
| Node pending but not dispatched | DAG readiness | Parent statuses, reverse edges, node status, persisted state. |
| Node dispatched but worker sees no task | Worker task stream | Consumer group, pending/lag, worker stream name, dedicated stream setting. |
| Worker fails before user code | DataHandler/input | Method type, resolved connection name/type, endpoint/ref, `s3_path`/format/`order_by`, trigger stream and claimed value, cursor, safe exception text. |
| User code ran but output missing | DataHandler/output | Output method, destination stream/table, output exception, source acknowledgements. |
| Node stays running after batch | Checkpoint/controller | Controller update backlog, checkpoint update persistence, node status after update, redispatch result. |
| Run succeeds with too few rows | Batch/finality | `batch_index`, `records_processed`, `batch_final`, `is_final`, stream lag, API paginator/cursor, trigger consumer-group pending/lag (groups are shared across runs). |

### Worker memory grows across tasks

Compare the same worker process after equivalent tasks; a cold-start number is
not a useful comparison once libraries are loaded. Read cgroup and process
memory without restarting the worker:

```bash
container exec mock-worker cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.stat /proc/1/smaps_rollup
container exec mock-worker python -c 'import os; print(os.environ.get("MALLOC_ARENA_MAX"))'
```

`memory.current` is total container charge. In `memory.stat`, `anon` is anonymous
process memory and `file` is file cache. `smaps_rollup` reports worker RSS and
anonymous RSS. Compare idle readings after the same workflow batch, and include
the peak during work; an RSS rise alone does not prove live objects are retained.
Checkpoint `peak_*_memory_bytes` fields are cgroup phase peaks, not isolated
allocations owned by a node. The optional `MALLOC_ARENA_MAX=2` limits glibc arenas
on Linux; it is not a memory cap and does not control DuckDB or Arrow allocators.
See [SETUP.md](../../SETUP.md) for the local workload validation and caveats.

Do not add forced garbage collection or allocator purge calls as a first response.
If memory approaches the container limit, stop submitting new load, preserve the
run state and logs, and get operator approval before changing or resetting data.

## Recovery Boundary

Only recommend recovery after read-only evidence identifies the failed message and confirms what will be replayed. Explain duplicate-write risk and checkpoint consequences. Require operator approval before any action that acknowledges, deletes, seeks, replays, resets, or overwrites persisted state/configuration.

## Verification

After an approved fix, verify the same run identity (or a deliberately new run ID), node status transitions, checkpoint values, source acknowledgement timing, destination row/message counts, and final run status. State which checks were not possible.

## Related Operator Guides

- [Run state and checkpoints](../reference/run-state-and-checkpoints.md)
- [Logging and sinks](../reference/logging-and-sinks.md)
- [Formulas and connections](../reference/formulas-and-connections.md)
- [Topology API and MCP tools](../reference/topology-api-and-tools.md)
