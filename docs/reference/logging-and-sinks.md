# Logging and Residue Sinks

## Purpose

Use this page when workflow state and stream messages do not explain a failure, or when expected log events are missing. Logs are supporting evidence; Formula state and Catalyst stream state remain the authoritative workflow records.

## Event Shape

`Residue.emit()` creates structured events with:

- `component`: bound component name, such as `oxidizer`, `formula`, `catalyst`, or `reagent`.
- `event`: human-readable event message.
- `timestamp`: UTC timestamp in ISO format.
- `level`: lowercase severity (`debug`, `info`, `warning`, `error`, `critical`).
- Any bound context and per-event fields, commonly `formula_id`, `run_id`, `node_id`, `stream`, `message_id`, or backend.

Use `formula_id`, `run_id`, and `node_id` to correlate events. Sinks receive the same event object as structured logging, subject to the configured level filter.

## Secret Handling Contract

`Residue` sanitizes its bound context, event message, and structured fields before console logging, event capture, or sink delivery. `RedisStreamLogSink` also sanitizes directly supplied events and its failure warnings. Sanitization copies mappings/lists/tuples rather than modifying the original application payload.

- Sensitive field names are matched case-insensitively after removing punctuation: password/passwd, secret, token, API/access keys, credential, authorization/authentication, and username fragments. Their values become `[REDACTED]`.
- URL userinfo and sensitive query parameters are masked using URL parsing; nonsensitive query parameters are retained. Malformed recognized URLs are replaced with `[REDACTED URL]`.
- Bearer/Basic credentials and common labeled assignments such as `password=...` are masked in event/exception text. Exception objects supplied as fields are converted to sanitized text.

This is a defined best-effort contract, not a guarantee that arbitrary unlabeled secrets in text, binary data, custom objects, URL paths, or unknown field names will be detected. The field-name heuristic can also mask non-secret fields whose names contain those fragments. Do not emit full worker tasks or connection objects, and do not pre-bind secrets to externally supplied loggers. Direct `print()` calls, third-party logging, and direct writes that bypass these interfaces are outside the contract.

Sanitization is for logging only. It does not modify worker task payloads, connection documents, executable node configs, or pagination/checkpoint values. New run states retain connection names only, but arbitrary invocation/error/checkpoint data is not globally sanitized by Oxidizer. Topology returns stored state and log records without another masking pass. Keep stored operational payloads free of unnecessary credentials and restrict API/backend access; old records are not retroactively cleaned by reading them.

Regression tests inspect console output, captured events, directly stored Redis event JSON, sink failure warnings, and unchanged original payloads using fake credentials. Ordinary fields and run correlation are retained, and repeated sanitization is tested for URL stability.

## Configuration and Attachment

Prefer role YAML settings in container deployments and explicit Python parameters in applications. The entrypoints translate YAML into `Residue(log_level=...)`; core components receive the trace through their `residue` parameter, not by loading YAML themselves.

| Setting/API | Current behavior |
| --- | --- |
| `controller.log_level` / selected worker section's `log_level` | Container entrypoints pass this YAML setting explicitly to `Residue`; default `INFO`. |
| `Residue(capture_events=True)` | Opts a trace into in-memory event capture. Bound child traces share its event buffer. |
| `Residue(log_level="DEBUG")` | Enables DEBUG and higher-severity events for this trace and its bound children, including console, capture, and attached sinks. |
| `Residue.dump()` | Returns the captured events and clears the buffer. The caller chooses when and where to publish them. |
| `Residue.add_sink(sink)` | Attaches a sink to a trace; bound child Residues share the sink list. Duplicate sinks with the same type/connection identity are ignored. |
| `Oxidizer(..., sinks=[...])` | Explicitly attaches the provided sinks to its shared root trace before binding Formula and Catalyst. |

For persisted logs, construct `RedisStreamLogSink` with an existing `RedisConnection` and pass it in `Oxidizer(sinks=...)`, or attach it to a shared `Residue` passed to the components. Sink construction and attachment are explicit.

Some standalone examples attach a `RedisStreamLogSink` to their root `Residue`. The container API/controller/worker entrypoints do not automatically attach one; use their stdout logs unless your application explicitly adds a sink. Topology's formula-log tools only see events actually written to Redis, not container stdout.

## Enable A DEBUG Trace

For the container setup, merge these settings into the existing YAML configuration; keep the Formula, Catalyst, and custom-router settings:

```yaml
controller:
    log_level: DEBUG
worker:
    log_level: DEBUG
```

If `--worker` selects another worker section, set `log_level` in that section instead. The API has a separate `--log-level debug` CLI option. Apply configuration changes at the next authorized process restart; do not interrupt in-flight tasks just to enable tracing.

For application code, pass the level explicitly:

```python
from oxidizer import Residue

trace = Residue(log_level="DEBUG")
```

Construct dependencies with the shared trace and pass it as `residue=trace` to `Oxidizer` or `Reagent`. Bound child traces inherit its level and sinks. See the [worker](../troubleshooting/worker-debug-trace.md#enable-debug) and [controller](../troubleshooting/controller-debug-trace.md#enable-debug) examples.

DEBUG includes INFO, WARNING, and ERROR events too; do not filter a diagnostic trace down to only `level=debug`. Read all participating roles, not just the API:

```bash
container logs mock-api
container logs mock-controller
container logs mock-worker
```

### Compatibility And Configuration Scope

YAML-to-parameter configuration makes the effective level visible in the role config, avoids an import-time environment dependency for that level, and lets separate application traces use different levels. It does not enable hot reload: existing components keep their trace settings, and container configuration changes still require an authorized restart. Keep sink attachment explicit regardless of the chosen level.

Environment support remains for compatibility, not as a required DEBUG setup step. `Residue()` without a level still uses `OXIDIZER_LOG_LEVEL` as read at import time, defaulting to `INFO`; explicit parameters override it. Removing that fallback would change existing environment-configured applications. The container roles already pass their YAML level or `INFO` explicitly.

Console formatting is still process-wide and selected at import time through `OXIDIZER_LOG_FORMAT` (`console` by default or `json`); there is currently no `log_format` constructor or role YAML setting. Moving formatting to parameters/YAML would require an API change, not just a new YAML key. `OXIDIZER_LOG_SINK` / `OXIDIZER_LOG_SINKS` are legacy inputs to opt-in helpers and do not automatically attach sinks to the core controller or worker.

## Expected Process Trace

For standalone troubleshooting handoffs, use [Worker DEBUG trace](../troubleshooting/worker-debug-trace.md) or [Controller DEBUG trace](../troubleshooting/controller-debug-trace.md). Each includes a successful sequence, container/API/MCP retrieval, and a procedure for locating the first unfinished boundary.

Match exact `event` strings below, then correlate `formula_id`, `run_id`, `node_id`, and worker `task_id`. Worker phase events include `batch_index`; checkpoint preparation increments it, so execution index `0` produces checkpoint index `1`. Repeated batches and node tasks are expected. The table describes logical boundaries, not a global timestamp order: controller and worker logs interleave, and the worker can receive a task before the controller finishes logging dispatch.

All events in this table are DEBUG. They identify boundaries without logging task, input, output, or connection payloads.

| Boundary | Expected `event` | Meaning and useful fields |
| --- | --- | --- |
| Invocation consumption | `OXIDIZER: Read invocations` | Stream read returned; inspect `invocation_count`, `topic_name`, and consumer fields. Count `0` is normal when idle, not evidence of an accepted invocation. |
| Config/DAG/state preparation | `OXIDIZER: Preparing invocation` then `OXIDIZER: Prepared invocation` | The first includes `invocation_id` and run IDs. The second follows config/connection/schema resolution, DAG construction, and initial-state creation; includes `node_count`. |
| Invocation acknowledgement | `OXIDIZER: Acknowledged invocation` | Acknowledgement returned after initial state was saved; includes run IDs, `invocation_id`, `topic_name`, `group_name`, and `consumer_name`. Failed preparations stay pending. This is acceptance into durable orchestration, not workflow completion. |
| Task dispatch | `OXIDIZER: Dispatched ready nodes` | Includes run IDs, `node_ids`, `dispatched_count`, and `worker_stream`. Positive counts follow task publication and state persistence. Count `0` means no eligible dispatch in this call. |
| Worker receipt | `REAGENT: Sent update` with `update_type=started`, then `REAGENT: Received task` | The started update is published before the receipt log. Receipt includes the task delivery's `task_id` and run/node IDs; it does not mean the controller has applied `started` yet. |
| Input routing | `REAGENT: Input fetch started` then `REAGENT: Input fetch completed` | Completion means all unfinished configured inputs returned; `input_count` counts input aliases, not rows. `REAGENT: Fetched node input` appears per fetched input with `input_name`, `input_method`, and `is_final`; already-finished inputs skip that per-input event. |
| User handler | `REAGENT: Handler started` then `REAGENT: Handler completed` | User-code boundary. Start includes `handler_mode`; completion means the handler returned, not that output was written. |
| Output routing | `REAGENT: Output routing started` then `REAGENT: Output routing completed` | Completion means all configured output methods returned. With no methods, both markers still appear; confirm destination readback rather than assuming rows were written. |
| Checkpoint preparation | `REAGENT: Batch checkpoint prepared` | In-memory metadata has advanced; includes `batch_index`, `records_processed`, and `is_final`. Not yet proof of controller persistence. |
| Checkpoint publication | `REAGENT: Sent update` with `update_type=checkpoint` | Checkpoint publication to the controller stream returned. Both final and non-final batches use this update type. |
| Source/task acknowledgement | `REAGENT: Acknowledged batch` | All source/task acknowledgement calls returned; includes `task_id` and requested `source_message_count`. This follows output routing and checkpoint publication. It is not an exactly-once guarantee; inspect pending entries and adapter acknowledgement counts if needed. |
| Controller state processing | `OXIDIZER: Processed reagent update` | Includes `update_id`, `update_type`, run/node IDs, `node_status`, and `run_status`. For active runs, the state write returned before this marker; controller acknowledgement follows. Late updates to terminal runs are ignored and report the existing terminal status. |

For non-final checkpoints, expect `node_status=ready` and another dispatch once eligible. For final checkpoints, expect `node_status=success`. A node finishing does not imply the entire run finished; confirm `run_status` and persisted Formula state. There is no separate required run-success event.

### Adapter And Failure Evidence

Adapter events provide detail inside those boundaries, when the corresponding method/backend is used:

- Redis Catalyst emits `CATALYST: Wrote message`, `CATALYST: Wrote message batch`, `CATALYST: Read messages`, and `CATALYST: Acknowledged message` or `CATALYST: Acknowledged messages`. Correlate `topic_name`, message IDs/counts, and consumer group. Batch acknowledgement also reports `acknowledged_count`. A read returning zero messages is normal.
- API inputs can emit `REAGENT: Fetched API input`; S3 file pages emit `Read S3 input page`. These are method-specific, not universal checkpoints for SQL or other input methods.
- The universal container handler emits `WORKER: Processing node` with `input_sizes`. Custom handlers need not emit it; use the core handler boundaries instead.
- `REAGENT: Worker task failed` is ERROR and includes run/node IDs, batch index, exception type, and message. Pair it with `REAGENT: Sent update` with `update_type=failed` and persisted `error_details`. If failure reporting itself fails, a failed-update marker may be absent.
- `OXIDIZER: Failed to read invocations`, `OXIDIZER: Invalid reagent update`, `OXIDIZER: Controller tick failed`, and `REAGENT: Worker cycle failed` are ERROR events for the surrounding orchestration. Read nearby events and state before deciding the failure is in user code.

### Locate The First Unfinished Boundary

First rule out filtering, missing sinks, and retention using the [missing-log decision path](#missing-log-decision-path). Then use the last matched boundary for the same task/run, not the last line from another concurrent task:

| Last confirmed evidence | Next investigation |
| --- | --- |
| Preparing invocation, no prepared marker | Config/connection/schema resolution, DAG validation, or initial-state write. Check controller exception and whether state exists. |
| Positive dispatch, no worker receipt | Worker availability, correct task stream, consumer-group lag/pending, and task decoding. A failed started-update publication also occurs before the receipt marker. |
| Input fetch started, no completion | Input method, trigger availability, upstream endpoint/query/file access, cursor, and input exception. A blocking input can be waiting rather than failed. |
| Handler started, no completion | User code or custom router; inspect the worker error/traceback. No error can also mean code is still running or the process was terminated. |
| Output routing started, no completion | Destination method, schema, write permissions, table/stream availability, and output exception. With several methods, inspect adapter evidence to identify which one; the core markers bracket the group. |
| Output completed, no checkpoint publication | Checkpoint construction or controller-stream write. Output may already exist; replay can duplicate it. |
| Checkpoint sent, no acknowledged-batch marker | Source/task acknowledgement or worker termination. Partial acknowledgements are possible; inspect pending entries without modifying them. |
| Worker checkpoint sent, no controller processed marker | Controller update backlog, message validation, state load/write, or controller failure. Inspect `oxidizer:controller` and persisted state. |
| Controller processed marker, update remains pending | Controller acknowledgement after state processing. State may already be advanced; do not blindly replay. |

DEBUG is a phase-level trace, not a record-by-record audit or a Python call trace. Missing completion markers narrow the investigation but alone do not prove a failure. Preserve the correlated sequence, error details, and state before proposing a fix.

## Sink Behavior

### RedisStreamLogSink

- Writes each event immediately. Events with a `formula_id` go to `oxidizer:logs:{formula_id}`; events without one (for example controller start/stop) go to `oxidizer:logs`. There is no stream-name setting; `log_stream_name(formula_id)` returns the stream for a formula.
- Stores the full serialized event in the `event` field, plus `run_id`, `component`, and `level` convenience fields.
- Uses approximate `maxlen` trimming (default `10000`). Optional `ttl` applies expiration to the entire stream key, not individual log events.
- Write failures are caught and reported once to stderr. They do not fail the workflow operation.

## Missing-Log Decision Path

If an expected event is absent, check in this order:

1. Confirm the emitting trace's effective level, including explicit YAML/application settings that override `OXIDIZER_LOG_LEVEL`; `DEBUG` events are filtered at the default `INFO` level.
2. Confirm the sink was actually attached to that process's `Residue`. An environment variable alone is not proof of attachment.
3. For Redis, inspect the formula's stream (`oxidizer:logs:{formula_id}`) and `oxidizer:logs` for events without a formula, plus max length, TTL, and whether expiry/trimming removed older records.
4. Check stderr for the sink's one-time warning. Sink write failures are intentionally non-fatal and can explain missing durable logs while the workflow state still advances.
5. Compare logs with Formula state and Catalyst stream evidence; do not conclude that a phase did not run from missing sink output alone.

## Read-Only Retrieval Examples

Latest events for one formula:

```python
entries = redis_connection.client.xrevrange("oxidizer:logs:sample", count=100)
for message_id, fields in entries:
    event = json.loads(fields["event"])
    print(message_id, event.get("run_id"), event.get("component"), event.get("event"))
```

Events without a formula (process start/stop, connection setup):

```python
entries = redis_connection.client.xrevrange("oxidizer:logs", count=100)
for message_id, fields in entries:
    print(message_id, fields.get("run_id"), fields.get("component"), fields.get("level"))
```

These are read-only checks. Do not print the contents of a sensitive event payload into shared logs or tickets without reviewing it first.

## Related Operator Guides

- [Workflow run troubleshooting](../troubleshooting/workflow-runs.md): correlate events with run state and distinguish missing logs from missing work.
- [Topology API and MCP tools](topology-api-and-tools.md): retrieve formula-scoped log events and inspect runs.
- [Local setup](../local-setup.md): start the local services and check their health.
