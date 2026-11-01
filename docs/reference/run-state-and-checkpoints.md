# Run State and Checkpoints

## Scope

This page describes the current Oxidizer/Reagent controller protocol: a worker handles one input batch per task; a non-final batch reports a checkpoint; the controller persists it and redispatches the node with the checkpoint attached.

## Run Identity

Correlate all evidence with:

- `formula_id`: workflow identity.
- `run_id`: one invocation of that workflow.
- `node_id`: fully qualified node, normally `<formula_id>.<layer>.<node>`.
- `invocation_id`, task message ID, or controller update message ID when available.

The worker context includes `formula_id`, `run_id`, `layer_id` (`<formula_id>.<layer_name>`), `layer_name`, `node_id` (`<layer_id>.<node_name>`), `node_name`, and `lattice_id` (the local implementation currently falls back to `formula_id` because `TaskMessage` has no separate lattice field). The context also includes the zero-based `batch_index` for the current worker task. Formula, layer, and node names must not contain dots.

## Lifecycle

`Oxidizer.oxidize_once()` processes one tick in this order (`Oxidizer.oxidize()` repeats ticks until stopped):

1. Read invocation messages and create initial Formula state for valid invocations.
2. Read one Reagent controller update and persist it before acknowledging the update message.
3. Dispatch every currently ready node by writing a task to `oxidizer:reagent` and changing its state to `dispatched`.

Relevant update transitions:

| Update | State effect | Follow-up |
| --- | --- | --- |
| `started` | Node becomes `running` | No redispatch while the worker owns the task. |
| `checkpoint` with `is_final=false` | Checkpoint is persisted; node becomes `ready` | The same controller tick can redispatch a task with the new checkpoint metadata. |
| `checkpoint` with `is_final=true` | Final checkpoint is persisted; node becomes `success` | Downstream nodes may become ready. Worker output is not sent to or persisted by the controller. |
| `failed` | Node becomes `failed`; error details are saved | No automatic retry is implemented by this transition. |

Run state exposes `started_at` and `completed_at` as UTC RFC 3339 timestamps ending in `Z`. Both are null when the invocation is first persisted. `started_at` is set on the first worker lifecycle update observed by the controller. `completed_at` is set when aggregate run status first becomes `success` or `failed`; it remains null while the run is unfinished. The top-level `status` continues to hold the current run outcome.

## Active Runs And Archival

Run states stay at their original locations: S3 `states/<formula_id>/<run_id>.yml`
or RedisJSON `oxidizer:states:<formula_id>:<run_id>`. Archival is logical; it does
not move or delete history, logs, or output data.

Nonterminal runs have a small marker at S3 `active/<formula_id>/<run_id>.json`
under the configured prefix, or RedisJSON `oxidizer:active:<formula_id>:<run_id>`.
State writes happen first, then marker synchronization. Terminal statuses are
`success`, `failed`, `cancelled`, `timed_out`, and `skipped`. Their markers are
deleted; paused, retrying, waiting, and apparently stuck runs remain active.
The controller's normal scan lists only markers, not historical state objects.
Terminal runs are not dispatched again, and late worker updates are acknowledged
without changing their saved state. Already published worker tasks are not cancelled.

`Formula.list_formula_runs()` lists all runs; `list_active_formula_runs()` lists
the marker index. REST run listings include both active and archived history,
and reads by run ID use the same location before and after archival.

Before upgrading existing storage, pause controllers and repair the marker index:

```python
formula.reconcile_active_formula_runs()
```

Use the same maintenance call after an interrupted state/marker write. It scans
all authoritative states, adds missing active markers, and removes terminal or
orphan markers. Normal controller scans also remove stale terminal markers they
encounter. Missing markers require explicit reconciliation, not a history scan
on every tick. The two writes are not atomic; run reconciliation while controllers
are paused. Markers are not locks and do not make concurrent dispatch exactly-once.

## Output Routing

The handler result stays in Reagent. Reagent routes it directly through the node's configured output methods, such as Catalyst streams or S3 Tables, before reporting the batch checkpoint. `ReagentUpdateMessage` contains lifecycle status, checkpoint metadata, errors, and heartbeat data; it has no output field. Consequently, run-state API responses include the node's output configuration but not the produced records. Downstream workflows consume data through their configured input methods rather than reading a parent node's output from Formula state.

An exception raised by the user-supplied handler is caught by `Reagent.react()`: it is logged as `REAGENT: Worker task failed`, sent to the controller as a `failed` update, stored in `Reagent.last_error`, and the wrapper returns `None` rather than propagating that handler exception. The source batch and task are not acknowledged. Input, output, or other infrastructure failures are not treated as user-handler failures and may still propagate from the worker.

A valid controller update is acknowledged only after Formula state persistence succeeds. A malformed update is logged and left unacknowledged.

## Checkpoint Fields

`TaskMessage.checkpoint_metadata` is passed to the next dispatched task. Current worker checkpoint fields include:

| Field | Meaning |
| --- | --- |
| `batch_index` | Number of input fetches already completed; the next worker context receives this index. |
| `batch_cursors` | Per-input cursor used by the next fetch: an API next-page URL, an integer SQL/S3 offset, or, for `input_trigger` inputs, a mapping with the claimed trigger message (`ack`), its `value`, and the `next` page URL or offset. |
| `batch_final` | Per-input finality; completed inputs are not fetched again while other inputs continue. |
| `records_processed` | Input rows observed in the current checkpoint's batch, summed across inputs. |
| `total_records_so_far` | Cumulative input rows observed across all batches for the node. |
| `total_late_arrivals_ignored` | Cumulative ignored SCD2 rows across the node's batches. |
| `peak_preprocess_memory_bytes` | Highest sampled worker-cgroup memory usage during task retrieval and input fetching, maxed across batches. |
| `peak_function_memory_bytes` | Highest sampled worker-cgroup memory usage while the user handler runs, maxed across batches. |
| `peak_postprocess_memory_bytes` | Highest sampled worker-cgroup memory usage while output methods run, maxed across batches. |
| `is_final` | True when all configured inputs are complete. |

Memory peaks are byte counts sampled every 250 ms and at phase boundaries. Reagent reads the worker container's cgroup v2 `memory.current`, falling back to cgroup v1 `memory.usage_in_bytes`. These are observed container-usage high-water marks during each phase, not isolated allocations attributable to that phase; the cgroup includes memory charged to processes in the worker container, but excludes the API, controller, Redis, and storage containers. If neither cgroup counter is readable, the corresponding peak remains `null`. The peak fields retain the maximum across batches and are not additive; accumulated runtime fields remain seconds summed across batches. Older `accumulated_*_memory` fields are no longer emitted.

With multiple table outputs, the late-arrival counters sum skipped rows reported by each table write; the same source row can count once per destination. The warning log includes the output table and per-write ignored count. The default SCD2 policy rejects late arrivals; `write.late_arrivals: ignore` enables skipping strictly older records. Equal-order conflicts and out-of-order rows within a batch remain errors.

For a stream input with `batch_size=50`, a full read is not proof that the stream is exhausted. With exactly 100 available entries, the worker performs two productive batches and a final empty read. The checkpoint indices are therefore 1, 2, and 3; the final checkpoint has `records_processed=0`, `total_records_so_far=100`, the cumulative late-arrival count from prior batches, and `is_final=true`.

Inputs with a stream `input_trigger` (API, SQL, and S3) claim one trigger message per task. While that message's pages continue, the cursor carries the claimed message, and the message is acknowledged with the batch that reads its last page. That batch is non-final with a null cursor, so the next task claims the next message. The input becomes final only after an empty trigger read. With N trigger messages and single-page results, the final checkpoint therefore has `batch_index` N + 1 and `records_processed=0`.

## Expected Batch Protocol

For every task:

1. Worker reads one batch using the saved cursor/finality state.
2. Worker calls the user function with `(data, context)` and routes configured outputs.
3. Worker advances batch metadata.
4. If input remains, it emits `checkpoint`, acknowledges source messages and the task, then returns. The controller persists and redispatches.
5. It emits `checkpoint` with `is_final` reflecting input completion, then acknowledges the source/task messages. The controller persists the checkpoint and decides whether the node becomes `ready` or `success`; final output data has already been routed through the node's output methods.

Source stream messages are acknowledged only after that batch's outputs succeed. A failure before successful output must not acknowledge that batch.

Redis stream outputs use non-transactional pipelines of at most 1,000 `XADD` commands. Each record remains a separate stream entry, and returned IDs follow input order. Other writers may interleave entries. Pipeline errors propagate before checkpoint publication or source acknowledgement, but earlier commands or chunks may already have succeeded: retry can duplicate output. This is not an atomic or exactly-once write. Kafka and injected clients without bulk publishing retain the single-write path.

## Diagnosis

- `dispatched` with no `started`: inspect `oxidizer:reagent`, its consumer group, pending entries, and worker availability.
- `running` with no recent updates: inspect worker logs and task delivery/ownership; do not assume the controller will redispatch a running node.
- `ready` after checkpoint but no task: confirm the next controller tick processed the checkpoint and ran `oxidize_formulas()`.
- `failed`: inspect `error_details`, the last saved checkpoint, and whether task/source messages remain pending before proposing replay.
- `success` with fewer rows than expected: compare input batch size, checkpoint `records_processed`, source stream lag, and finality fields.

For operator steps to inspect persisted run state, streams, and worker logs, see [Workflow run troubleshooting](../troubleshooting/workflow-runs.md).
