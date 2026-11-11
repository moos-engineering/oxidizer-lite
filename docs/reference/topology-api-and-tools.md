# Topology API and MCP Tools

Topology exposes workflow/run inspection, transport diagnostics, table queries,
and configuration management through a FastAPI parent app with REST routes and
the FastMCP protocol mounted at `/mcp`. Serve `topology.app` for HTTP; MCP tools
remain available from `topology.mcp`. This gives the REST surface a natural home
for shared FastAPI middleware and dependencies. The API image also provides
local semantic documentation search; see the
[two-image build and deployment guide](../../docker/README.md).

## Start the Skeleton Server

From the repository root:

```bash
PYTHONPATH=. .venv/bin/python examples/skeleton/api.py
```

The example defaults to HTTP on `127.0.0.1:8000`. Configure it with:

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `OXIDIZER_TOPOLOGY_TRANSPORT` | `http` | Streamable HTTP only; serves the combined FastAPI app (`/api/...` and MCP at `/mcp`). |
| `OXIDIZER_TOPOLOGY_HOST` | `127.0.0.1` | HTTP bind host. |
| `OXIDIZER_TOPOLOGY_PORT` | `8000` | HTTP bind port. |

The example binds to loopback by default. The API has no authentication; `POST /api/configs` can replace stored configuration and `POST /api/invocations` can start workflows, so do not expose the server to an untrusted network without access control.

All routes return JSON. Send `Content-Type: application/json` with POST requests.

## Health Route

`GET /api/health` returns:

```json
{"status": "ok", "service": "oxidizer-topology"}
```

This is a basic process health response, not a readiness check for Formula storage, Redis, or downstream services.

## Read-Only Transport Inspection

Use MCP `get_transport_health(stream_name, group_name=None)` or the equivalent
REST route to inspect one stream/topic on the server's **primary Catalyst**:

```bash
curl --fail --get http://127.0.0.1:8027/api/transport/health \
	--data-urlencode 'stream_name=oxidizer:reagent' \
	--data-urlencode 'group_name=reagent-consumer-group'
```

For Kafka, pass the actual configured group ID, such as `quickstart-workers`,
not the Redis-style method group label. The adapter translates control names
such as `oxidizer:reagent` to `oxidizer.reagent`.

| Backend | Metadata returned |
| --- | --- |
| Redis | `length`, and up to 100 `groups` containing `name`, `consumers`, `pending`, `last_delivered_id`, and `lag`; `groups_truncated` indicates omitted groups. An optional group filters the result. |
| Kafka | `topic`, optional `group`, and up to 100 `partitions` containing `start_offset`, `end_offset`, `committed_offset`, `lag`, and `offset_out_of_range`. |

Redis lag can be `null` when the server cannot determine it. Kafka committed
offset and lag are `null` when no group is supplied or no committed offset
exists. An offset outside the retained range is flagged and its lag is `null`,
not misleadingly reported as zero. Offset differences are not exact record
counts for compacted or transactional topics. Snapshots can change while a
workflow is active and are not atomic across metadata calls.

These calls do not read payloads, consume, subscribe, create topics/groups,
commit offsets, acknowledge, delete, or replay work. Kafka uses admin APIs with
10-second request/future timeouts and requires an admin client with metadata and
group-offset permissions. It never uses the consumer, so inspection does not
join or rebalance its group. Topics with more than 100 partitions are rejected.

Invalid names/nonstream Redis keys return HTTP 400; missing Redis streams or
groups return 404. Kafka metadata/offset lookup and backend availability failures
return a generic 503 without connection details. This is not a worker-liveness
check or proof of output commit/run completion, and named per-input transports
are not inspected. The server still needs trusted access control.

## Invoke A Formula

`POST /api/invocations` accepts a known `formula_id`, generates a UUID `run_id`, and writes `{formula_id, run_id}` to `oxidizer:invocations`:

```bash
curl -X POST http://127.0.0.1:8000/api/invocations \
	-H 'Content-Type: application/json' \
	-d '{"formula_id":"sample"}'
```

It returns `202 Accepted` with `formula_id`, `run_id`, and the Catalyst stream `invocation_id`. This confirms enqueueing only; use the run routes to observe controller progress. Unknown formulas return `404`; malformed input returns `400`; Redis enqueue failures return a generic `503`.

Example response:

```json
{
	"formula_id": "sample",
	"run_id": "a5c539c8-23c0-46d6-a4b7-0899bb3af255",
	"invocation_id": "1710000000000-0"
}
```

## Formula And Run Routes

| Route | Response |
| --- | --- |
| `GET /api/formulas` | `{"formulas":["..."]}` |
| `GET /api/docs/status` | Documentation search readiness, hybrid mode, model identity, and structured error; HTTP 503 when unavailable. |
| `GET /api/transport/health?stream_name=...&group_name=...` | Read-only primary-Catalyst metadata for one stream/topic; group is optional. |
| `GET /api/connections` | `{"connections":["..."]}` containing connection names sorted by name. Optional repeated `type` filters match stored connection types (e.g. `?type=redis&type=kafka`); omitted filters select `s3_tables`. Empty filters return 400. Settings and credentials are never returned. |
| `GET /api/connections/{connection_name}/schemas` | Iceberg namespaces/SQL schemas and table metadata from Crucible. |
| `POST /api/connections/{connection_name}/query` | Bounded SQL query results from Crucible; body includes `sql` and optional `max_rows`. |
| `GET /api/formulas/{formula_id}/runs` | Formula ID and normalized run records containing `formula_id`, `run_id`, `status`, `started_at`, and `completed_at`. |
| `GET /api/formulas/{formula_id}/runs/active` | Same run-summary shape, limited to indexed nonterminal runs. |
| `GET /api/formulas/{formula_id}/runs/{run_id}` | Formula ID, run ID, and persisted `state`, including run timestamps, without response-time masking. |
| `GET /api/formulas/{formula_id}/runs/{run_id}/nodes/{node_id}` | Node ID, stored node/checkpoint data, parents, and children without response-time masking. |
| `GET /api/formulas/{formula_id}/logs?run_id=...&limit=100` | Recent matching log events, newest first. `run_id` is optional; `limit` is 1-1,000. |
| `POST /api/invocations` | Enqueue an existing formula; generates a UUID run ID and returns `202 Accepted`. |
| `POST /api/configs` | Stores or replaces one version 1 workflow, connection, or schema document. |

The runs route lists all run IDs returned by Formula for that formula, not only nonterminal runs. State IDs are normalized in the REST response even though Formula backends use different separators. `started_at` is set on the first worker lifecycle update persisted by the controller; `completed_at` remains null until aggregate status becomes `success` or `failed`. Both timestamps are UTC RFC 3339 strings ending in `Z`. Missing formulas, runs, or nodes return HTTP 404.

The active route uses Formula's active-marker index rather than scanning run
history. It loads only indexed states for the requested formula and skips
missing states and terminal statuses (`success`, `failed`, `cancelled`,
`timed_out`, `skipped`). Pending, paused, and stuck nonterminal runs count as
active; this does not establish worker liveness. An empty index returns an empty
list. Backend failures return a generic 503 without connection details.

Listing never repairs markers or writes state. An interrupted index write can
therefore leave a run absent from this view until authorized maintenance
reconciliation; see [active runs and archival](run-state-and-checkpoints.md#active-runs-and-archival).

The connection route uses Formula's connection listing and loading methods to select documents whose `connection.type` is `s3_tables`, including AWS and RustFS table-catalog connections. It returns names only, never connection settings or credentials. No matching connections returns an empty list; backend failures return HTTP 503 with a generic error. This is table-catalog inspection, not general inspection of API or `type: s3` object-storage connections.

## Connection Discovery And Queries

Run history includes logically archived terminal runs independently of the
controller's active-marker index. Archival leaves state paths and run-detail
URLs unchanged; it does not delete logs or output data.

Both routes resolve a stored Formula connection with `connection.type: s3_tables` and a non-empty `aws_s3_table_bucket`. AWS credentials/profile settings and RustFS S3/catalog endpoints come from that connection document. Each request creates and closes its own DuckDB engine through Crucible; no connection settings are included in responses. The API image needs the DuckDB integration and network access to the catalog, object storage, and DuckDB extension downloads.

`GET /api/connections/local_lake/schemas` delegates to Crucible's metadata queries and returns:

```json
{
	"connection_name": "local_lake",
	"catalog": "local_catalog",
	"namespaces": ["analytics"],
	"schemas": ["analytics"],
	"tables": [{"namespace": "analytics", "table": "mock_entity_history", "type": "BASE TABLE"}]
}
```

Iceberg namespaces are SQL schemas, so `namespaces` and `schemas` contain the same names. Discovery follows Crucible's existing 1,000-row limit per metadata query; this is not a paginated catalog listing.

Run a query using the returned catalog/schema/table names:

```bash
curl -X POST http://127.0.0.1:8022/api/connections/local_lake/query \
	-H 'Content-Type: application/json' \
	-d '{"sql":"SELECT entity_id FROM local_catalog.analytics.mock_entity_history LIMIT 5","max_rows":5}'
```

The response contains `connection_name`, `catalog`, `query`, `columns`, `rows` (objects keyed by column name), `row_count`, and `truncated`. `max_rows` defaults to 100 and must be an integer from 1 to 1,000; Crucible reads one additional row to determine truncation. Dates/timestamps use ISO strings, decimals and UUIDs use strings, and binary values use base64 strings.

Crucible permits a single `SELECT`/`WITH` statement and rejects known write/session-changing keywords. This guard is not a SQL security sandbox: SELECT functions may access files or external services, and a row cap does not bound execution time or memory. Restrict these endpoints/tools to trusted users and apply least-privilege backend credentials. Missing connections return 404; invalid input or connection configuration returns 400; other discovery/query failures return generic 503 responses without backend exception details.

## Run Response Examples

Example run-list response:

```json
{
	"formula_id": "sample-dag",
	"runs": [
		{"formula_id": "sample-dag", "run_id": "run-41", "status": "created", "started_at": null, "completed_at": null},
		{"formula_id": "sample-dag", "run_id": "run-42", "status": "running", "started_at": "2026-10-05T12:00:00Z", "completed_at": null}
	]
}
```

An individual run response includes the persisted state without response-time masking. New controller-created states store connection names, not resolved connection documents:

```json
{
	"formula_id": "sample-dag",
	"run_id": "run-42",
	"state": {
		"status": "running",
		"started_at": "2026-10-05T12:00:00Z",
		"completed_at": null,
		"connection_names": ["users_api"],
		"dag": {
			"status": {"sample-dag.extract.fetch": "running", "sample-dag.transform.clean": "pending"},
			"nodes": {
				"sample-dag.extract.fetch": {"checkpoint_metadata": {"batch_index": 2, "records_processed": 20, "total_records_so_far": 120}},
				"sample-dag.transform.clean": {}
			},
			"edges": {"sample-dag.extract.fetch": ["sample-dag.transform.clean"]},
			"reverse_edges": {"sample-dag.transform.clean": ["sample-dag.extract.fetch"]}
		}
	}
}
```

Example node response:

```json
{
	"formula_id": "sample-dag",
	"run_id": "run-42",
	"node_id": "sample-dag.extract.fetch",
	"status": "running",
	"node": {"checkpoint_metadata": {"batch_index": 2, "records_processed": 20, "total_records_so_far": 120}},
	"parents": [],
	"children": ["sample-dag.transform.clean"]
}
```

The formula-list and formula-detail responses are not authenticated or redacted in this initial version. Keep secrets in named connection documents, not workflow formulas, and keep this server on loopback or behind trusted access control.

### Formula Log Route Details

The logs route reads `oxidizer:logs:{formula_id}` without consuming or acknowledging entries. It decodes JSON stored in either the `event` field (`RedisStreamLogSink`) or the `message` field (`Catalyst.write_to_stream`). Results are newest first; `run_id` is optional and `limit` must be 1-1,000. The implementation scans at most the newest 10,000 stream entries, so older matching events can be omitted. Kafka historical range reads are unsupported; an incompatible Catalyst returns `503`.

Recognizable sensitive fields and text are sanitized by Residue/RedisStreamLogSink before emission. Topology does not mask records when reading them, and direct stream writes or older log entries are not automatically sanitized. See the [logging contract](logging-and-sinks.md#secret-handling-contract).

Example log response:

```json
{
	"formula_id": "sample-dag",
	"run_id": "run-42",
	"returned": 1,
	"logs": [
		{
			"formula_id": "sample-dag",
			"run_id": "run-42",
			"event": "Worker batch completed",
			"component": "reagent",
			"level": "info",
			"records_processed": 120,
			"message_id": "1710000000001-0",
			"stream": "oxidizer:logs:sample-dag"
		}
	]
}
```

The upload endpoint accepts a JSON object with YAML text in its `content` field:

```json
{"content":"templateVersion: 1\nname: users_api\ntype: connection\nconnection:\n  type: api\n  base_url: https://api.example.test\n"}
```

Supported document types are `workflow`, `connection`, and `schema`. Each requires `templateVersion: 1` and a safe `name`; the name and type select the Formula storage operation. The content limit is 1 MB. Uploading an existing name replaces its current document. Success returns the type and name only, not config contents. Invalid YAML/envelopes return HTTP 400; storage failures return a generic HTTP 500.

Example success response:

```json
{"type": "connection", "name": "users_api", "stored": true}
```

## HTTP Errors

- `400 Bad Request`: invalid query parameters, malformed JSON/YAML, unsupported document type, invalid name/version, or invalid invocation input.
- `404 Not Found`: requested formula, run, node, Redis stream, or Redis consumer group does not exist.
- `500 Internal Server Error`: unexpected storage or handler failure; responses do not expose exception details.
- `503 Service Unavailable`: invocation enqueue, transport inspection, or the configured Catalyst's log range read fails.

## MCP Tools

### Documentation Lookup

Chisel provides read-only MCP tools for looking up documentation matching the
installed package. `search_docs` accepts natural-language questions as well as
exact identifiers, using **local vector search plus keyword ranking**. The API
image includes embedded LanceDB, the FastEmbed `BAAI/bge-small-en-v1.5` embedding
model, and a precomputed documentation index. It requires no model API key,
external vector service, runtime model download, or shared writable volume.

`search_docs` and `read_doc` are MCP-only. Readiness is available through both
MCP `get_docs_search_status()` and REST `GET /api/docs/status`.

| Tool | Arguments | Result |
| --- | --- | --- |
| `search_docs` | Required `query`; optional `limit` (default 5, range 1-10) | Hybrid-ranked Markdown chunk excerpts with `path`, `section`, `start_line`, `excerpt`, and `score`. |
| `read_doc` | Required relative Markdown `path`; optional exact `section` heading | Document/section content with `path`, `section`, `start_line`, `headings`, and `truncated`. |
| `get_docs_search_status` | None | `ready`, `mode`, `model`, and `error` (`null` when ready, otherwise a `code` and actionable `message`). |

Example MCP tool calls (tool name and JSON arguments, not REST requests):

```json
{"name":"search_docs","arguments":{"query":"Why are records written twice after a worker restarts?","limit":3}}
```

Choose a returned path and heading, then request the surrounding context:

```json
{"name":"read_doc","arguments":{"path":"reference/run-state-and-checkpoints.md","section":"Expected Batch Protocol"}}
```

The section selector is case-insensitive and includes nested subsections, but
not following sibling sections. Omit `section` to read the document; an
ambiguous repeated heading requires reading the document instead. Chisel splits
sections into overlapping chunks and combines cosine-vector and keyword ranks
using reciprocal rank fusion. Heading matches receive additional keyword weight.
Scores are ranking signals, not confidence probabilities. Semantic neighbors
can be returned even without a literal keyword match; verify their relevance
with `read_doc`. Multiple chunks may come from the same section. `start_line`
identifies the source chunk; an excerpt may begin later within it. Search
responses include `mode: hybrid`; `truncated` indicates omitted ranked candidates,
not an exhaustive catalog-wide match count.

Both responses include `package`, installed `version` (or `null` if package
metadata is unavailable), and `source`. Bundled `oxidizer/docs` takes precedence
(`source: package`); a source checkout can fall back to its `docs` directory
(`source: checkout`). Root-level internal notes and example files are outside
the lookup directory. Only content intended for publication should be placed
in the bundled docs; these tools do not redact document text.

Search accepts queries of at most 300 characters and returns excerpts of at
most 1,000 characters each. Reads return at most 12,000 content characters and
100 headings, each limited to 300 characters. Inspect `truncated` before treating
a result as complete. Documents larger than 500,000 bytes are rejected. Absolute
paths, parent traversal, symlinks, non-Markdown files, and arbitrary network
access are not allowed. Invalid inputs, missing paths/headings, and unavailable
documentation produce MCP tool errors.

Topology opens and validates the index/model at construction. The manifest must
match the installed package version, documentation content hash, indexing
format, model identity, embedding-library versions, row count, and vector
dimension. Serving never builds or repairs an index. Missing or incompatible
assets leave search unavailable, with **no silent keyword fallback**:

```json
{"ready":false,"mode":"hybrid","model":"BAAI/bge-small-en-v1.5","error":{"code":"documentation_search_unavailable","message":"Bundled documentation index/model is missing or incompatible; rebuild the API image with matching release assets."}}
```

The status route returns 200 when ready or 503 when unavailable. A failed search
reports `documentation_search_unavailable` or `documentation_search_failed` as
an MCP tool error, without backend paths or exception details. `read_doc` and
the other inspection tools remain usable when search is unavailable.
`/api/health` remains process liveness only; it does not imply search readiness.
The `[mcp]` extra includes REST/MCP and vector-search dependencies,
but installing it alone does not build model/index assets. For source development,
install `[mcp]` and explicitly build the assets as described in the
[image guide](../../docker/README.md#source-development).

Use the tools together during troubleshooting:

1. Search for the symptom and read the matching section for expected behavior
	and limitations.
2. Use `list_formula_runs` or `list_active_formula_runs`, followed by
	`get_formula_state`, `get_node_state`, and `get_transport_health`, to collect
	actual runtime evidence.
3. Distinguish documented expectations from observed state. Documentation is
	reference material, not authorization to replay, acknowledge, reset, or
	modify a system.

The tools declare MCP read-only and idempotent hints. Those hints are not an
authentication boundary; keep the server behind trusted access control. Rebuild
the package/container and restart the API when deploying newly added tools or
updated bundled documentation.

### Runtime Inspection And Configuration

`list_formula_runs(formula_id)` returns normalized run summaries with IDs,
statuses, and start/completion timestamps, matching the REST run listing. Use it
to discover run IDs before calling `get_formula_state` and `get_node_state`.

`list_active_formula_runs(formula_id)` returns the same fields for indexed
nonterminal runs only, matching the REST active route. It declares read-only and
idempotent MCP hints and does not scan historical states or reconcile markers.

`get_transport_health(stream_name, group_name=None)` returns the metadata
described above. Both new tools declare MCP read-only and idempotent hints;
these describe behavior, not an authorization boundary. Start with
`oxidizer:invocations`, `oxidizer:reagent`, or `oxidizer:controller`, then inspect
the source stream identified by the workflow configuration.

`list_connections` takes no arguments and returns a sorted list of S3 Tables connection names from Formula, such as `["aws_lake", "local_lake"]`. It applies the same filtering as `GET /api/connections` and does not return credentials or other settings.

`list_schemas(connection_name)` returns the same namespace/schema/table metadata as the connection schemas route. `query(connection_name, sql, max_rows=100)` returns the same bounded, JSON-compatible results as the query route, using Crucible's query guard. Both use stored S3 Tables connections and the `local_catalog` alias.

`list_formulas` takes no arguments and returns formula IDs from the configured Formula backend:

```json
["daily-users", "sample"]
```

`get_formula` accepts a `formula_id` and returns the stored workflow configuration. Credentials should be stored in named connection documents, not embedded in workflow formula configs.

`list_formula_states` takes no arguments and returns state IDs from Formula. IDs preserve the configured backend format: S3/RustFS returns `<formula_id>/<run_id>`, while Redis returns `<formula_id>:<run_id>`. It lists identifiers only; it does not load run-state contents.

`get_formula_state` accepts `formula_id` and `run_id` and returns that persisted run state unchanged. New controller-created states store connection names only. Arbitrary operational metadata and old state documents are not masked by this response. Formula remains the source of truth for backend-specific state behavior.

`get_node_state` accepts `formula_id`, `run_id`, and `node_id`. It returns that node's status and stored state, checkpoint metadata if present, parent node IDs, and child node IDs without additional masking.

`upload_config` accepts YAML text in `content` and applies the same validation and Formula dispatch as `POST /api/configs`. It returns storage metadata only.

The server is constructed with `Topology(formula, catalyst)`. Formula enumeration, config/state loading, and storage remain owned by Formula; Catalyst owns log stream access. Log sanitation belongs to Residue/RedisStreamLogSink before emission, not to Topology. See [the logging contract](logging-and-sinks.md#secret-handling-contract); use trusted access control for stored state and logs.

For run-state, stream, and worker-log diagnostics, see [Workflow run troubleshooting](../troubleshooting/workflow-runs.md).
