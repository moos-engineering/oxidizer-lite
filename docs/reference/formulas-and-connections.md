# Formulas and Connections

## Concepts

A **formula** is a workflow definition: layers, nodes, input methods, output methods, dependencies, and connection references. A **connection** is a globally named backend configuration loaded when a formula is invoked. Formula nodes refer to connections by name; credentials belong in the connection configuration or runtime credential provider, not inline in a workflow.

Topology provides Formula/run inspection and a generic config-upload endpoint/tool for version 1 workflow, connection, and schema documents. Formula's `create_formula_config()`, `create_formula_connection()`, and `create_formula_schema()` methods persist each config class using the selected backend.

### Stream Payloads

Catalyst publishes JSON-serializable Python dictionaries or lists to Redis and
Kafka. This applies to both single writes and each message in a batch. Other
top-level values, including strings containing JSON text, raise `TypeError`
before publishing. Batch writes validate all top-level payload types before
writing any records; serialization or broker failures can still leave partial
output.

Wrap a scalar in a dictionary, such as `{"value": "123"}`, rather than sending
`"123"` directly. Nested string values like `"123"`, `"true"`, and `"null"` remain
strings after a round trip. Scalar publishing is no longer supported; decoding
of existing stream messages is unchanged.

## Formula Document

A workflow file may use the versioned envelope shown by the skeleton. `Formula.dag()` accepts either that envelope or the inner workflow object because Oxidizer unwraps the `workflow` field when present.

```yaml
templateVersion: 1
name: daily-users
type: workflow

workflow:
  layers:
    - name: bronze
      nodes:
        - name: fetch_users
          type: scheduled
          inputs:
            - ref: null
              alias: users
              methods:
                - method: api
                  connection: users_api
                  endpoint: /users
                  http_method: GET
          outputs:
            methods:
              - method: stream
    - name: silver
      nodes:
        - name: transform_users
          inputs:
            - ref: daily-users.bronze.fetch_users
              alias: source_users
              methods:
                - method: stream
                  batch_size: 50
          outputs:
            methods:
              - method: stream
```

Create the formula through the public facade:

```python
from pathlib import Path
import yaml
from oxidizer import Formula, S3Connection

storage = S3Connection(
    bucket="oxidizer-configs",
    prefix="oxidizer",
    region_name="us-west-2",
)
formula = Formula(storage)
config = yaml.safe_load(Path("daily-users.yml").read_text())
formula.create_formula_config("daily-users", config)
```

Formula IDs are storage identifiers; the YAML `name` is descriptive and is not used to choose the storage key. The example stores the document at `formulas/daily-users.yml` under the configured S3 prefix, or `oxidizer:formulas:daily-users` in RedisJSON.

S3 workflow discovery follows continuation tokens across all pages, including
when more than 1,000 documents are stored. Only `.yml` keys under the configured
`formulas/` prefix are included.

## Connection Document

Connections use a versioned envelope. The `connection` mapping contains backend-specific settings; `name` is the stable global name referenced by workflow methods.

```yaml
templateVersion: 1
name: users_api
type: connection

connection:
  type: api
  base_url: https://api.example.test/v1
  authentication:
    auth_type: bearer
    credentials:
      token: ${USERS_API_TOKEN}
```

Do not put literal credentials in checked-in YAML. Resolve secrets from a protected configuration process or runtime secret provider before storing the connection. The current API does not expand `${...}` placeholders automatically; it is illustrative unless your deployment performs that substitution.

For services that require multiple fixed authentication headers, use `auth_type: headers` and provide a `credentials.headers` mapping. Both `api_key` and `headers` authentication require valid HTTP-token header names and non-empty string values without line breaks; transport-control headers such as `Host` and `Content-Length` are rejected. Keep credentials out of checked-in connection documents. See the [Alpaca example](../../examples/alpaca/README.md) for a seeder that injects its key pair from environment variables.

An AWS S3 Tables connection can use the existing SSO profile:

```yaml
templateVersion: 1
name: lake
type: connection

connection:
  type: s3_tables
  provider: rustfs
  aws_sso_profile: sandbox-oxidizer
  aws_region: us-west-2
  aws_s3_table_bucket: oxidizer-tables-test
```

To assume a role, add `role_arn`, with optional `role_session_name` and `external_id`. The configured profile (including an SSO profile) supplies the source credentials; if no profile or explicit keys are set, boto3's normal credential chain supplies them. S3 Tables writes and topology queries use the connection's STS AssumeRole settings. S3 input uses DuckDB's `credential_chain` STS provider and supports `role_arn` and `external_id` with an AWS profile or the default credential chain. The API does not interpolate environment placeholders in YAML, so provide any required `external_id` through your protected configuration process.

```yaml
connection:
  type: s3_tables
  aws_region: us-west-2
  aws_s3_table_bucket: production-tables
  role_arn: arn:aws:iam::123456789012:role/OxidizerS3Tables
  role_session_name: oxidizer-workflow
```

For RustFS, use the Iceberg REST endpoint and S3-compatible credentials:

```yaml
templateVersion: 1
name: local_lake
type: connection

connection:
  type: s3_tables
  provider: s3
  endpoint_url: http://localhost:9000
  access_key_id: ${RUSTFS_ACCESS_KEY}
  secret_access_key: ${RUSTFS_SECRET_KEY}
  region_name: us-east-1
  aws_s3_table_bucket: oxidizer-tables-test
  catalog_endpoint: http://localhost:9000/iceberg
```

The RustFS bucket must already exist and be enabled as a table warehouse. The warehouse and S3 bucket names must match.

An object-storage connection for `method: s3` inputs uses `type: s3`. `bucket` is the default bucket for relative paths. Omit `endpoint_url` for AWS S3; omit keys to use the AWS credential chain or `aws_sso_profile`.

```yaml
templateVersion: 1
name: local_files
type: connection

connection:
  type: s3
  bucket: oxidizer-data-test
  endpoint_url: http://localhost:9000
  access_key_id: ${RUSTFS_ACCESS_KEY}
  secret_access_key: ${RUSTFS_SECRET_KEY}
  region_name: us-east-1
  addressing_style: path
```

## Store a Global Connection

S3-backed Formula storage stores a connection at `connections/<name>.yml` under the configured S3 prefix. Redis-backed Formula storage stores JSON at `oxidizer:connections:<name>`.

```python
from pathlib import Path
import yaml
from oxidizer import Formula, S3Connection

storage = S3Connection(
    bucket="oxidizer-configs",
    prefix="oxidizer",
    region_name="us-west-2",
)
formula = Formula(storage)
connection_name = "users_api"
connection_config = yaml.safe_load(Path("users-api.yml").read_text())
key = storage.object_key(f"connections/{connection_name}.yml")
storage.client.put_object(
    Bucket=storage.bucket,
    Key=key,
    Body=yaml.safe_dump(connection_config).encode("utf-8"),
    ContentType="application/x-yaml",
)
```

RedisJSON equivalent:

```python
connection_config = yaml.safe_load(Path("users-api.yml").read_text())
formula.connection.client.json().set(
    "oxidizer:connections:users_api",
    "$",
    connection_config,
)
```

Then verify names and load the stored value:

```python
assert "users_api" in formula.list_formula_connections()
stored = formula.load_formula_connection("users_api")
```

## Reference Resolution

During invocation, Oxidizer recursively searches the workflow configuration for string-valued `connection` fields, loads each unique name from the global connection store, and includes those records in each `TaskMessage.connections` mapping. A missing connection name therefore fails invocation preparation before a worker handles the node.

A node method references the name, not the whole credential mapping:

```yaml
- method: api
  connection: users_api
  endpoint: /users
```

Topology exposes Formula/run/node reads, Formula-scoped logs, config upload, and S3 Tables connection/schema/query inspection. Connection inspection is limited to stored connections with `connection.type: s3_tables`; it does not inspect API or general object-storage connections. Formula configs should reference named connections rather than contain credentials. Log sanitation happens at emission; state and log retrieval do not add masking. See [Topology API and MCP tools](topology-api-and-tools.md) for the available routes and their limits.

Controller-created run state stores only `connection_names`; full connection documents are resolved from Formula for each worker-task dispatch and are not copied back into state. Credential rotation or endpoint changes therefore affect subsequent dispatched tasks, while already-queued tasks retain their message configs. Missing connection documents fail dispatch before publishing the affected task.

Legacy `formula_connections` maps remain readable: the controller uses their names, reloads current documents, and removes the old map on the next dispatch/update state write. This is not a bulk cleanup of completed historical states or old backend object versions. Connection documents and worker streams still contain full configs and require restricted access and retention policies. Names-only connection storage does not sanitize arbitrary secrets embedded in invocation payloads, node configs, or error/checkpoint metadata; keep credentials in named connection documents.

## SQL Inputs

The `sql` input method reads S3 Tables through Crucible using DuckDB. It returns records as dictionaries to the existing worker handler, with cursor/finality tracked in the normal checkpoint. The older oxidizer-lite SQL input was used as a behavioral reference only; no legacy engine or state format is required.

All SQL inputs use a non-empty `catalogs` list. Each entry specifies a stored connection name and its SQL catalog alias. For a query against one connection, list one entry:

```yaml
inputs:
  - alias: events
    methods:
      - method: sql
        catalogs:
          - connection: local_lake
            catalog: local_catalog
        query: |
          SELECT entity_id, observed_at, metric_value
          FROM local_catalog.analytics.mock_entity_history
          WHERE is_current
          ORDER BY entity_id, observed_at
        batch_size: 1000
```

For a join across connections, add entries to the same `catalogs` list. Each binding references a stored `s3_tables` connection and assigns a unique SQL catalog alias. Top-level SQL `connection` and `catalog` fields are not supported; use the list for both single-connection reads and joins:

```yaml
inputs:
  - alias: enriched
    methods:
      - method: sql
        catalogs:
          - connection: sales_lake
            catalog: sales
          - connection: people_lake
            catalog: people
        query: |
          SELECT o.order_id, p.customer_name
          FROM sales.analytics.orders AS o
          JOIN people.analytics.customers AS p ON o.customer_id = p.customer_id
          WHERE o.order_id >= ?
          ORDER BY o.order_id
        parameters: [100]
        batch_size: 1000
```

The controller's existing recursive reference resolution loads all bindings into `TaskMessage.connections`. Crucible attaches them to a single DuckDB engine for the page, so DuckDB executes the join; it does not fetch separate pages and join them in Python. Connections may use AWS S3 Tables or RustFS Iceberg REST settings, each with its own credentials, warehouse, and catalog endpoint. Catalog aliases must be identifier-safe and unique; missing bindings, non-S3 Tables connections, or connections without `aws_s3_table_bucket` are rejected. Use distinct storage bucket paths when separate RustFS endpoints need different credentials; DuckDB's S3 secret scopes are URI-path based, not endpoint-isolated.

`query` must be a single SELECT/WITH statement accepted by Crucible's existing query guard. `parameters` is an optional list of positional values. `batch_size` defaults to 1,000 and must be a positive integer. This workflow batch size is independent of the interactive API query's 1,000-row response cap. Large pages and joins can consume significant memory; a row limit does not cap query execution work or make SQL a security sandbox. Run trusted SQL with least-privilege credentials.

Each task opens a short-lived engine, reapplies catalog attachments, and runs DuckDB `LIMIT batch_size + 1 OFFSET cursor`. One-row lookahead determines whether another page exists. Non-final pages save an integer offset under `batch_cursors.<alias>`; the final page returns a null cursor and marks `batch_final.<alias>` true. An exact-size final SQL page needs no extra empty read. SQL inputs do not acknowledge source rows; only the worker task is acknowledged after output/checkpoint publication succeeds.

Use a deterministic `ORDER BY` including a unique tie-breaker, and keep source data unchanged for the entire run. Pagination reexecutes the query in a new engine per task and does not preserve a shared snapshot across pages or catalogs. Concurrent source changes can skip or duplicate rows, and large offsets may be expensive. Alias joined columns uniquely. If SQL reading fails, no successful checkpoint or task acknowledgement is emitted; existing controller failure/retry behavior is unchanged.

### Triggered Parameters

Instead of static `parameters`, a SQL input can take them from an upstream stream. Each trigger message runs the full paged query once with its own parameters:

```yaml
inputs:
  - ref: orders.source.list_customers
    alias: customer_orders
    methods:
      - method: sql
        catalogs:
          - connection: sales_lake
            catalog: sales
        query: |
          SELECT order_id, customer_id, total
          FROM sales.analytics.orders
          WHERE customer_id = ? AND status = ?
          ORDER BY order_id
        input_trigger:
          type: stream
          attribute: parameters
        batch_size: 1000
```

An upstream record such as `{"parameters": [42, "open"]}` supplies the positional values in placeholder order. A single non-list value is wrapped as a one-element list, and a record without a `parameters` key is used as the value itself. Mapping values are rejected because positional order would be ambiguous. Triggered values replace any static `parameters`.

Pages for one message carry the claimed message and next offset in the cursor. The message is acknowledged with its last page, and the next task claims the next message. The input is final when the trigger stream is empty, following the same rules as [stream-triggered API inputs](#stream-triggered-api-inputs).

## Stream-Triggered API Inputs

An API input can take each request's endpoint or POST payload from a stream:

```yaml
inputs:
  - ref: bronze.ticker_list
    alias: ticker_list
    methods:
      - method: api
        connection: polygon_api
        input_trigger:
          type: stream
          attribute: endpoint   # or payload (requires http_method: POST)
        http_method: GET
        path: $.results
        paginator: next_url
```

Each task claims one message from the `ref` stream (override with `input_trigger.stream`; `consumer_group`, `consumer_name`, and `block` default like stream inputs). If the record contains the `attribute` key, its value is used; otherwise the whole record is used. Endpoint values must be non-empty HTTP(S) strings, absolute or relative to the connection `base_url`. With a base URL, absolute endpoints must keep the same scheme, hostname, and effective port. Authenticated API connections require a base URL; URL credentials and automatic redirects are rejected. Configure the final endpoint explicitly. Payload triggers replace `payload` and post to the configured `endpoint`.

With a `paginator`, later pages use the trigger's next URL from the checkpoint cursor. The same trigger value stays attached, so POST pages resend the same payload. The trigger message is acknowledged only after its last page's outputs and checkpoint succeed. A failed request leaves the message unacknowledged in the consumer group's pending list. As with stream inputs, it is not redelivered by new-message reads. The input becomes final when the trigger stream returns no message within `block`.

## S3 File Inputs

`method: s3` reads Parquet or CSV objects through Crucible in a short-lived DuckDB session. Use a static `s3_path`, or an `input_trigger` that takes paths from an upstream stream:

```yaml
inputs:
  - alias: products
    methods:
      - method: s3
        connection: local_files
        s3_path: s3-input/products.csv     # or s3://bucket/key, a glob, or a list
        order_by: [product_id]
        batch_size: 1000
  - ref: s3_orders.source.list_files
    alias: orders
    methods:
      - method: s3
        connection: local_files
        input_trigger:
          type: stream
          attribute: s3_path
        format: parquet                    # optional
        order_by: [order_id, line_no DESC]
        batch_size: 1000
```

- Paths without a scheme are relative to the connection `bucket`. A trigger message can carry one path or a list of paths, which are read together.
- `format` is `parquet` or `csv`. When omitted, it is inferred from `.parquet` or `.csv`/`.csv.gz` extensions, which must all match.
- Each task reads `ORDER BY <order_by> LIMIT batch_size OFFSET cursor`, with a one-row lookahead. `order_by` takes a column name or list of names, each optionally followed by `ASC`/`DESC`. Use a unique ordering so pages do not skip or repeat rows.
- Static paths save an integer offset under `batch_cursors.<alias>` until the final page. Triggered paths page each message's files and then acknowledge the message, following the same cursor and finality rules as stream-triggered API inputs.
- Rows are JSON-compatible. Decimals and UUIDs become strings, dates, times, and timestamps become ISO strings, and bytes become base64. Parquet `DECIMAL` therefore arrives as a string, while CSV numbers infer as numbers.

## Validation Checklist

- Formula document parses as YAML and has `layers` under its workflow root.
- Each node has a unique name within its layer; references use the fully qualified upstream node ID.
- Each method's `connection` value matches a stored global connection name.
- The stored connection envelope has `type: connection`, a matching `name`, and backend settings nested under `connection`.
- The selected Formula backend and credentials can list/load the config and connection before invocation.
- S3 Tables/RustFS table buckets exist and are enabled before querying; query access is separate from connection discovery.

## Related Operator Guides And Examples

- [Workflow run troubleshooting](../troubleshooting/workflow-runs.md): diagnose missing connections, stalled dependencies, input errors, and output failures using run evidence.
- [Topology API and MCP tools](topology-api-and-tools.md): inspect formulas, runs, nodes, and supported connection queries.
- [Mock workflow](../../examples/mock/README.md): live-tested SQL readback and triggered SQL parameters.
- [Stream-triggered API workflow](../../examples/placeholder/README.md) and [S3 file input workflow](../../examples/s3_input/README.md): runnable examples of the input methods described here.
