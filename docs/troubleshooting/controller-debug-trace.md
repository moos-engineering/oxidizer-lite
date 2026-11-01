# Controller DEBUG Trace

Use this guide when an invocation never creates state, a node is not dispatched, or worker progress is not reflected in run state. Compare the successful Oxidizer controller path below with the affected run. For input, user-code, or output failures, use the [worker guide](worker-debug-trace.md).

## Enable DEBUG

Merge this into the existing container configuration, preserving other settings:

```yaml
controller:
  log_level: DEBUG
```

Apply the setting at an authorized controller restart. The API's `--log-level debug` affects the API, not the controller. New phase markers require a build containing the tracing changes.

For a Python application, configure the shared trace explicitly and pass it to the controller. Here `formula` and `catalyst` are your existing configured dependencies; the controller binds them to the shared trace:

```python
from oxidizer import Oxidizer, Residue

trace = Residue(log_level="DEBUG")
controller = Oxidizer(formula, catalyst, residue=trace)
```

The container entrypoint performs the same YAML-to-parameter mapping and defaults to `INFO` when `log_level` is omitted. No environment-variable changes are needed for this DEBUG setup.

Earlier INFO-only runs cannot acquire DEBUG logs retroactively. Do not start a reproduction or replay messages without approval; existing output/state may already have advanced.

## Collect Evidence

Capture `formula_id`, `run_id`, affected `node_id`, `invocation_id`, controller `update_id`, and time window. Include worker task IDs when available. Keep INFO, WARNING, and ERROR events; do not share credentials or complete stored operational payloads without review.

### Container Logs

For the local Mock setup, use the command below; substitute your actual controller name:

```bash
container logs mock-controller
```

Container logs include startup, idle reads, and orchestration errors without run IDs. The container entrypoint does not automatically attach a Redis log sink; stdout is the default source.

### REST API

If the controller has a `RedisStreamLogSink` attached to its shared Residue and Topology reads the same Redis database, retrieve persisted events, run state, and node state:

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
  "$BASE_URL/api/formulas/$FORMULA_ID/runs/$RUN_ID"

curl --fail-with-body --silent --show-error \
  "$BASE_URL/api/formulas/$FORMULA_ID/runs/$RUN_ID/nodes/$NODE_ID"
```

The log response contains `logs` and `returned`, newest first. Reverse events for comparison with the sequence below and select matching `OXIDIZER:` events locally; there is no server-side role/node filter. At most 1,000 matching events are returned from a scan of 10,000 recent formula-stream entries, without pagination. Missing older events can reflect trimming, expiration, or scan limits rather than missing work.

Formula-log retrieval excludes events without a `formula_id`, including startup, aggregate read/update counts, and some errors. Use container logs for those. API health only confirms the API process responds, not controller progress. Missing run state can be the symptom being investigated, not a reason to invent a new run.

### MCP Tools

Ask the connected Topology MCP client to call:

| Tool | Arguments |
| --- | --- |
| `list_formula_logs` | `formula_id`, `run_id`, `limit: 1000` |
| `get_formula_state` | `formula_id`, `run_id` |
| `get_node_state` | `formula_id`, `run_id`, `node_id` |

The same sink, ordering, history, and formula-scope limits apply as REST; MCP does not read container stdout. Inspect run `status`, `dag.status`, checkpoint metadata, and safe error details rather than dumping full node payloads. See [logging and sink configuration](../reference/logging-and-sinks.md#configuration-and-attachment) if durable logs are absent.

## Successful Controller Sequence

Illustrative one-node, one-final-batch run; adapter events and idle ticks are omitted. These are DEBUG event names, not a literal complete log dump:

```text
OXIDIZER: Read invocations             invocation_count=1
OXIDIZER: Preparing invocation         invocation_id=invocation-1 formula_id=sample run_id=run-1
OXIDIZER: Prepared invocation          node_count=1
OXIDIZER: Acknowledged invocation      invocation_id=invocation-1
OXIDIZER: Dispatched ready nodes       dispatched_count=1 node_ids=[sample.bronze.fetch]
OXIDIZER: Processed reagent update     update_type=started node_status=running run_status=running
OXIDIZER: Processed reagent updates    processed_count=1
OXIDIZER: Processed reagent update     update_type=checkpoint node_status=success run_status=success
OXIDIZER: Processed reagent updates    processed_count=1
```

Preparation completion follows config/connection/schema resolution, DAG construction, and initial-state creation. `OXIDIZER: Acknowledged invocation` follows the invocation acknowledgement call and includes run IDs, `invocation_id`, stream, group, and consumer. Failed preparations remain pending; successful invocations do not wait for workflow completion to be acknowledged. Positive dispatch follows task publication and persistence of `dispatched` state. Per-update processing includes formula/run/node IDs and `update_id`; for active runs, it follows successful state persistence and precedes controller-stream acknowledgement. The aggregate processed count follows successful acknowledgements.

Each tick consumes invocations, then updates, then dispatches eligible nodes. `started` and `checkpoint` can be processed on separate ticks. Worker execution overlaps controller work, so this is logical order, not a guaranteed global timestamp order. `OXIDIZER: Controller started` and `OXIDIZER: Controller stopped` are INFO process events, not per-run success markers.

For multiple batches, a non-final checkpoint results in `node_status=ready`, followed by another eligible dispatch. A final checkpoint results in `node_status=success`. Other unfinished nodes can keep `run_status` nonterminal; confirm the entire persisted DAG, destination output, and `completed_at`. There is no separate required run-success event. Late updates to terminal runs are ignored and report the existing terminal status, not a new transition.

## Find The First Missing Boundary

First confirm the correct controller, DEBUG setting, IDs, and log-source completeness. Empty reads and `dispatched_count=0` are normal on idle ticks, not errors.

| Last confirmed evidence | Start troubleshooting here |
| --- | --- |
| Invocation enqueued, no preparing marker | Controller health, invocation stream/group, backlog/pending, payload decoding, and missing run context. Enqueue acceptance does not prove consumption. |
| Preparing invocation, no prepared marker | Workflow validation, connection/schema resolution, DAG construction, or initial-state persistence. Inspect `OXIDIZER: Failed to read invocations` and stored state/config. |
| Prepared invocation, no acknowledgement marker | Invocation-stream acknowledgement or controller termination after the state save. State may already exist; inspect state and pending entries before any replay. |
| Prepared state, no positive dispatch | Node/parent statuses, scheduling eligibility, connection loading, tick errors, and state-write failures. No eligible node is not itself a dispatch failure. |
| Positive dispatch, no worker receipt | Task stream/group, worker availability, decoding, and initial update publication; continue with the [worker guide](worker-debug-trace.md). |
| Worker sent update, no controller processing marker | `oxidizer:controller` backlog/pending, update validation, state loading/writing, and `OXIDIZER: Invalid reagent update`. That error can describe persistence failure as well as malformed input. |
| Per-update processing logged, update still pending | Controller acknowledgement after persistence; inspect `CATALYST: Acknowledged message` and group state. Do not replay just because acknowledgement failed. |
| Non-final checkpoint became ready, no next dispatch | Later controller tick, dependency eligibility, active-run discovery, and dispatch/storage errors. |
| Final node success but run not successful | Other node states, failure details, unfinished dependencies, and persisted aggregate status. |

Keep `OXIDIZER: Controller tick failed` and nearby ERROR events. For Redis, inspect `XLEN`, `XINFO GROUPS`, and `XPENDING` through an approved read-only connection; API/MCP log tools do not expose arbitrary stream inspection. Never acknowledge, delete, or reset messages as a diagnostic step.

This acknowledgement behavior does not automatically replay or clean up older pending invocations. Redelivery is not an exactly-once guarantee; do not replay an invocation with existing state without considering overwrite and duplicate-run risks.

## LLM Handoff

Provide this document, IDs, correlated controller/worker log windows, safe error details, and persisted run/node state. Use this prompt:

```text
Use the Controller DEBUG Trace guide to compare this run with the successful path.
Locate the first missing invocation, dispatch, update-processing, or acknowledgement
boundary. Account for idle ticks, batches, concurrent workers, terminal-run updates,
and API/MCP log limitations. Separate confirmed state from log-based hypotheses.
Identify the likely component and next read-only check, including evidence that would
disprove the hypothesis. Do not invoke/replay work, acknowledge/delete messages, reset
data, or change config without approval. State missing evidence explicitly.
```

See [Workflow run troubleshooting](workflow-runs.md) for stream checks and recovery rules, and [Logging and sinks](../reference/logging-and-sinks.md#expected-process-trace) for the complete event inventory.