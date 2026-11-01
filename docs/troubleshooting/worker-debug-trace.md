# Worker DEBUG Trace

Use this guide when a workflow task fails, stalls, or produces unexpected output. Compare the successful Reagent worker path below with the affected task. For invocation, scheduling, or state-persistence issues, use the [controller guide](controller-debug-trace.md).

## Enable DEBUG

Merge this into the existing container configuration, preserving other worker settings:

```yaml
worker:
  log_level: DEBUG
```

If `--worker` selects another section, change that section instead. Apply the setting at an authorized worker restart, not during in-flight work. Changing the API's level does not enable worker DEBUG. New phase markers require a build containing the tracing changes.

For a Python application, configure the shared trace explicitly and pass it to the worker. Here `catalyst` is your existing configured dependency; construct it with the same trace when its adapter events are also required:

```python
from oxidizer import Reagent, Residue

trace = Residue(log_level="DEBUG")
worker = Reagent(catalyst, residue=trace, auto_react=False)
```

The container entrypoint performs the same YAML-to-parameter mapping and defaults to `INFO` when `log_level` is omitted. No environment-variable changes are needed for this DEBUG setup.

DEBUG cannot recover logs from an earlier INFO-only execution. Reproducing a failure means another execution and possible duplicate output; obtain approval before invoking or replaying work.

## Collect Evidence

Capture `formula_id`, `run_id`, `node_id`, worker `task_id`, batch index, and the relevant time window. Keep INFO, WARNING, and ERROR events too. Redact secrets and avoid sharing full input/output or connection payloads.

### Container Logs

For the local Mock setup, use the command below; substitute your actual worker name:

```bash
container logs mock-worker
```

Container logs include startup, task decoding, and process failures that may lack run IDs. The container entrypoint does not automatically attach a Redis log sink; stdout is the default source.

### REST API

If the worker has a `RedisStreamLogSink` attached to its shared Residue and Topology reads the same Redis database, retrieve persisted events and node state:

```bash
BASE_URL=http://localhost:8022
FORMULA_ID=your-formula
RUN_ID=your-run-id
NODE_ID=your-formula.layer.node

curl --fail-with-body --silent --show-error --get \
  "$BASE_URL/api/formulas/$FORMULA_ID/logs" \
  --data-urlencode "run_id=$RUN_ID" \
  --data-urlencode "limit=1000"

curl --fail-with-body --silent --show-error \
  "$BASE_URL/api/formulas/$FORMULA_ID/runs/$RUN_ID/nodes/$NODE_ID"
```

The log response contains `logs` and `returned`. Events are newest first: reverse them for the sequence below. Match the node/task locally using IDs and `REAGENT:` events; there is no server-side node/component filter. At most 1,000 matching events are returned from a scan of 10,000 recent formula-stream entries, without pagination. Trimming, expiration, and scan limits can hide earlier phases; use container logs when history is incomplete. An empty response does not prove the worker never ran.

### MCP Tools

Ask the connected Topology MCP client to call:

| Tool | Arguments |
| --- | --- |
| `list_formula_logs` | `formula_id`, `run_id`, `limit: 1000` |
| `get_node_state` | `formula_id`, `run_id`, `node_id` |
| `get_formula_state` | `formula_id`, `run_id` |

The sink requirement, newest-first order, and history limits are identical to REST. Neither interface reads container stdout. Node state supplies checkpoint metadata and stored error details; review these for sensitive data before sharing. See [logging and sink configuration](../reference/logging-and-sinks.md#configuration-and-attachment) if durable events are absent.

## Successful Worker Sequence

Illustrative final batch with one input, not a literal complete log dump. All shown events are DEBUG; adapter and user logs may appear between them. Match the same formula/run/node; from task receipt onward, use the same `task_id`.

```text
REAGENT: Sent update                  update_type=started
REAGENT: Received task                task_id=task-1
REAGENT: Input fetch started          batch_index=0
REAGENT: Fetched node input           input_name=orders input_method=stream is_final=true
REAGENT: Input fetch completed        batch_index=0 input_count=1
REAGENT: Handler started              batch_index=0 handler_mode=data_context
REAGENT: Handler completed            batch_index=0
REAGENT: Output routing started       batch_index=0
REAGENT: Output routing completed     batch_index=0
REAGENT: Batch checkpoint prepared    batch_index=1 records_processed=2 is_final=true
REAGENT: Sent update                  update_type=checkpoint
REAGENT: Acknowledged batch           task_id=task-1 source_message_count=2
```

The `started` update precedes the receipt marker. Execution index `0` produces checkpoint index `1`. Per-input logs appear only for fetched inputs, not inputs already marked finished. A node without inputs or outputs still emits enclosing start/completion markers; those alone do not prove rows were read or written.

For a non-final batch, `is_final=false`; the controller persists `ready` and redispatches another task. Final batches also send `checkpoint`, not a separate `success` update. Publication does not prove controller persistence, and acknowledgement completion is not an exactly-once guarantee. Confirm destination readback, checkpoint metadata, and controller node status before declaring success.

## Find The First Missing Completion

First confirm worker DEBUG is enabled and the log source is complete. Missing markers can also mean work is still running or the process was terminated.

| Last confirmed boundary | Start troubleshooting here |
| --- | --- |
| Controller dispatched, no worker receipt | Worker health, selected task stream/group, pending/lag, task decoding, and initial `started` update publication. Check startup errors in container logs. |
| Input started, no completion | Input method, trigger availability, endpoint/query/file access, cursor, and last per-input event. Blocking inputs can be waiting rather than failed. |
| Handler started, no completion | Custom router or user function. Inspect `REAGENT: Worker task failed`, exception type/message, and persisted traceback. |
| Output started, no completion | Destination method, schema, permissions, adapter exception, and availability. With several output methods, use adapter evidence to identify the failing one. |
| Output completed, no checkpoint prepared/sent | Checkpoint construction or controller-stream publication. Output may already exist; replay can duplicate it. |
| Checkpoint sent, no acknowledged-batch marker | Source/task acknowledgement or termination. Inspect pending messages without acknowledging them; partial acknowledgements are possible. |
| Worker completed, state still running | Switch to the [controller guide](controller-debug-trace.md): inspect update consumption and state persistence. |

`REAGENT: Worker task failed` is ERROR, not DEBUG. A later `REAGENT: Sent update` with `update_type=failed` means failure reporting returned; absence can mean reporting also failed. Keep `REAGENT: Worker cycle failed` and nearby adapter errors in the evidence packet.

## LLM Handoff

Provide this document, IDs, correlated log window, safe error details, and current node/checkpoint state. Use this prompt:

```text
Use the Worker DEBUG Trace guide to compare this task with the successful sequence.
Identify the last confirmed boundary and first missing completion for the same
formula_id/run_id/node_id/task_id. Account for batches, filtering, and retention.
Separate facts from hypotheses. Name the likely failing component and the next
read-only check, including evidence that would disprove the hypothesis.
Do not invoke/replay work, acknowledge/delete messages, reset data, or change config
without approval. State any evidence that is unavailable.
```

See [Workflow run troubleshooting](workflow-runs.md) for stream checks and recovery rules, and [Logging and sinks](../reference/logging-and-sinks.md#expected-process-trace) for the complete event inventory.