# Schema Configuration Design

## Purpose

A schema document defines the durable shape and write semantics of a dataset produced by an Oxidizer workflow. Workflow output methods will reference a named schema in the same way they reference named connections.

Schema configuration is declarative. It describes columns, keys, and the intended write operation. Backend adapters remain responsible for translating that declaration into SQL, Iceberg, S3 Tables, or another storage engine.

The version 1 contract is implemented for named schema storage and strict record validation, with ordered-upsert, SCD1, and SCD2 support in the DuckDB adapter. Crucible creates missing tables and checks physical columns before non-empty writes. AWS S3 Tables supports automatic nullable-column additions and conservative numeric widening; `IcebergRestConnection` does not alter existing schemas. SCD2 has live AWS and RustFS E2E coverage; concurrent writers, broader migrations, views, historical schema lookup, and exactly-once writes remain deferred.

## Storage And References

Schema documents use the same versioned envelope as formulas and connections:

```yaml
templateVersion: 1
name: market_quotes
type: schema

schema:
  # schema definition
```

The stable schema identifier is `name`. Formula stores the current document as:

```text
S3/RustFS: schemas/<name>.yml
RedisJSON:  oxidizer:schemas:<name>
```

A workflow output would reference it by name:

```yaml
outputs:
  methods:
    - method: s3_tables
      connection: local_lake
      schema: market_quotes
      namespace: analytics
      table: events
```

`connection` selects the physical backend. `schema` selects the logical data contract. `namespace` and `table` select the physical relation when the backend needs them.

Schema documents are intentionally independent of Formula documents. A schema may be shared by multiple formulas and output methods.

`templateVersion` versions the document envelope. `schema.version` is a positive integer that versions the schema contract for one stable `name`; increment it whenever the contract changes. Version 1 output methods reference schemas by name only and do not carry a separate version selector. When a run is prepared, Oxidizer resolves the current document and stores its name, version, and resolved contents in the run state. All batches and retries for that run use this snapshot; later runs resolve the then-current document. Formula backends store one current document per name, so historical schema versions are not independently addressable in version 1.

Schema creation and replacement remain an external deployment/operator action. There is no schema registry API, promotion flow, or automatic publication in version 1. A publishing process must validate the document and increment `schema.version` before replacing the current named document.

## Proposed Document

```yaml
templateVersion: 1
name: market_quotes
type: schema

schema:
  version: 1
  description: Latest quote observations from Finnhub.

  columns:
    - name: symbol
      type: string
      nullable: false
      description: Finnhub ticker symbol.
    - name: observed_at
      type: timestamp
      nullable: false
      description: UTC instant at which the quote was observed.
    - name: price
      type: double
      nullable: false
      description: Current market price in the quote currency.
    - name: change
      type: double
      nullable: true
      description: Absolute price change from the previous close.
    - name: change_percent
      type: double
      nullable: true
      description: Percentage price change from the previous close.
    - name: day_high
      type: double
      nullable: true
      description: Highest price during the current trading day.
    - name: day_low
      type: double
      nullable: true
      description: Lowest price during the current trading day.
    - name: open_price
      type: double
      nullable: true
      description: Opening price for the current trading day.
    - name: previous_close
      type: double
      nullable: true
      description: Previous trading session closing price.

  keys:
    - symbol
    - observed_at

  write:
    operation: append

```

## Logical Types

Schema files should use backend-neutral logical types:

| Type | Meaning |
| --- | --- |
| `boolean` | True/false value |
| `int32` | Signed 32-bit integer |
| `int64` | Signed 64-bit integer |
| `float` | Single-precision floating point |
| `double` | Double-precision floating point |
| `decimal` | Exact numeric value, with optional precision and scale |
| `string` | UTF-8 text |
| `date` | Calendar date |
| `timestamp` | UTC timestamp |
| `json` | Structured value retained as JSON |
| `binary` | Opaque bytes |

Physical adapters may map these types to DuckDB, Iceberg, S3 Tables, RedisJSON, or another backend type. A schema should not contain backend-specific SQL types unless placed under an explicit backend override.

Column definitions support optional descriptive metadata and may later support additional constraints:

```yaml
- name: amount
  type: decimal
  precision: 18
  scale: 4
  nullable: false
  default: 0
  description: Monetary amount in the source currency.
```

  An optional `ref` maps an incoming JSON object value to the named output column. It uses JSONPath syntax and is evaluated once for each input record before type/nullability validation:

  ```yaml
  - name: current_price
    ref: $.c
    type: double
    nullable: true
  ```

  With this declaration, an input record `{"c": 227.5}` is normalized to `{"current_price": 227.5}`. Columns without `ref` continue to read the input field matching `name`. A ref with no match follows the column's `default`/`nullable` rules; a ref matching multiple values is rejected. JSONPath refs require the `jsonpath-ng` dependency, included in the `duckdb`, `test`, and `all` extras.

`description` is documentation metadata. It does not change storage, validation, or write behavior.

## Keys And Identity

`keys` defines the logical identity of a record. It is required for `upsert`, `scd1`, and `scd2` operations. It is optional for pure append datasets.

For compound identity, key order is significant and must be stable. A backend must reject writes when a required key is missing or null.

A schema may also define a separate event ordering field:

```yaml
keys:
  - symbol
ordering:
  column: observed_at
  direction: descending
```

The ordering field is used to resolve duplicate records and late-arriving updates. It must be comparable by the selected backend.

## Write Operations

### Append

Append writes every input record as a new row.

```yaml
write:
  operation: append
```

Append is appropriate for immutable observations, events, audit records, and time-series snapshots. It does not prevent duplicate logical keys.

### Upsert

Upsert inserts records that do not exist and updates records whose logical key already exists.

```yaml
keys:
  - symbol
  - observed_at

write:
  operation: upsert
  conflict:
    order_by: observed_at
    direction: descending
  update:
    mode: all_columns
```

The initial DuckDB adapter implementation enforces these invariants:

- Matching is performed only on `keys`.
- At least one key is required, key values must be non-null, and duplicate keys within one input batch are rejected.
- `update.mode: all_columns` requires `conflict.order_by` and a direction. The incoming row updates a match only when its ordering value wins; stale rows are ignored.
- An equal ordering value with different update values is rejected. Replaying the same row is a no-op.
- `update.mode: insert_only` inserts missing keys and leaves matching rows unchanged. `listed_columns` is not implemented.
- The return value is the number of input rows processed, not a breakdown of inserted, updated, and unchanged rows.

This is retry-safe for repeated records under a single-writer assumption. Concurrency behavior and MERGE support against live Iceberg catalogs still need backend contract and integration tests.

`insert_only` is the explicit form of "insert when not matched":

```yaml
write:
  operation: upsert
  update:
    mode: insert_only
```

### SCD Type 1

SCD1 keeps the latest version of each business entity and overwrites changed attributes in place. History is not retained.

```yaml
keys:
  - symbol

write:
  operation: scd1
  ordering:
    column: observed_at
    direction: descending
  compare:
    columns:
      - price
      - change
      - change_percent
      - day_high
      - day_low
      - open_price
      - previous_close
```

SCD1 requires `keys`, `write.ordering.column`, `write.ordering.direction`, and a non-empty `write.compare.columns` list. A missing key inserts a current row. For a matched key, only an incoming row that wins the ordering comparison replaces the existing non-key values; stale rows are ignored. Duplicate keys in one input batch are rejected. If ordering ties and a compared value differs, the write fails; an identical replay is a no-op. Changes outside `compare.columns` do not trigger a tie conflict, and an equal-order row is not applied.

The DuckDB implementation returns the number of input rows processed. Retry behavior is idempotent for repeated rows under a single-writer assumption. Concurrent-writer behavior and MERGE support against live Iceberg catalogs still require backend integration tests.

### SCD Type 2

SCD2 retains history by closing the current version and inserting a new version when tracked attributes change.

Model the key from the entity grain and the ordering field from the version time. For a rolling forecast, each `(place, observed_at)` pair is a separate entity: `observed_at` is part of the key, while a `retrieved_at` timestamp records when that forecast snapshot was obtained and orders revisions. Stamp all rows from one fetch with the same retrieval timestamp. Re-fetching unchanged compared values then creates no new version; changed values at a later retrieval time create a new version.

Using only `place` as the key and `observed_at` as the order for a rolling forecast makes earlier forecast hours appear late on the next fetch. By default, Crucible rejects those records. Set `write.late_arrivals: ignore` to skip rows strictly older than the current version and continue processing newer rows. Crucible emits a warning with the input and ignored-row counts; Reagent stores a cumulative `total_late_arrivals_ignored` checkpoint field across batches. `Crucible.write()` continues to return the number of input records, including ignored ones. This is lossy, does not ignore equal-order conflicts, and does not permit out-of-order records within one batch. Preserve original ordering values on retries when exact replay identity matters.

```yaml
keys:
  - symbol

write:
  operation: scd2
  late_arrivals: reject
  ordering:
    column: observed_at
    direction: ascending
  history:
    valid_from: valid_from
    valid_to: valid_to
    current_flag: is_current
    record_hash: record_hash
    open_ended_value: null
  compare:
    columns:
      - price
      - change
      - change_percent
      - day_high
      - day_low
      - open_price
      - previous_close
```

The DuckDB SCD2 implementation requires ascending `write.ordering`, the four configured history columns, and non-empty `compare.columns`. Repeated keys in a batch are supported when their new observations are ordered chronologically; contradictory same-time compared values are rejected. History columns are managed by Crucible; values supplied for them in records are ignored. `valid_from` is initialized from the ordering value, `valid_to` from `open_ended_value` (null by default), `current_flag` to true, and `record_hash` to a SHA-256 hash of the canonical compared values.

Following the [DuckDB SCD2 MERGE guide](https://duckdb.org/docs/lts/guides/sql_features/merge), the DuckDB adapter bulk-stages `upsert`, SCD1, and SCD2 inputs using PyArrow batches of at most 65,536 rows. Upsert and SCD1 evaluate ordering/tie rules against the staged relation and perform one set-based target `MERGE`; SCD2 additionally computes version transitions with SQL window functions and uses one action-tagged target `MERGE` to close and insert versions. Staging, validation, and mutation share one transaction; temporary tables and registered Arrow batches are cleaned up. This is not snapshot synchronization: keys absent from an incoming batch are not expired.

When a key is new, Crucible inserts one current row. When the compared values change at a later ordering value, it closes the current row by setting `valid_to` to the incoming ordering value and `current_flag` to false, then inserts the new current row. A later record with unchanged compared values creates no new version. Exact retries of a recorded version are no-ops; equal ordering values with different compared values fail. Late arrivals older than the current version are rejected by default unless they exactly match a previously recorded version. With `write.late_arrivals: ignore`, strictly older rows are filtered before version planning and counted in a structured warning; equal-time conflicts and non-ascending order within the remaining batch still fail. All accepted records in a batch share one transaction, so a rejected record rolls back earlier transitions in that batch.

SCD2 invariants:

- At most one row per business key has `is_current = true`.
- A current row has `valid_to = null` or the configured open-ended value.
- Historical rows are never overwritten except to close their validity interval.
- Late-arriving records are rejected by default; an explicit ignore policy can skip strictly older input records.
- Deletes require an explicit policy, such as `ignore`, `tombstone`, or `close_current`.

The default policy rejects late-arriving records; the opt-in `ignore` policy skips only records older than the current version and logs their count. Deletes are not implemented. The return value remains the number of input rows, including ignored rows. Tests cover bulk version transitions, retry behavior, timestamp ordering, ignored late-row counts, mixed logical types, batch rollback, and a single target MERGE per batch. A disposable RustFS Iceberg probe verified initialization, interval boundaries, and exact replays. AWS S3 Tables and concurrent writers still need verification. Input records and normalized values are still held in Python memory; Arrow bounds additional ingestion buffers, not the total batch size. Production-scale throughput and memory use require representative benchmarks.

## Version 1 Scope

Every version 1 table output must reference a named schema. The Crucible table-write API requires the resolved schema document; schema-less table writes are not supported. Version 1 supports `append`, ordered `upsert`, SCD1, and SCD2 in the DuckDB adapter. `write.operation` is required; Crucible must reject a missing or unsupported operation rather than assume append. SCD2 has focused live RustFS verification, but AWS and concurrency checks remain outstanding. Views are not writable workflow outputs in version 1.

For append writes:

- Crucible creates a missing namespace/table from the schema, checks existing column types, and can add nullable columns on AWS S3 Tables. It never drops columns.
- Unsupported type changes, removed columns, and nullable declarations against physically required columns are rejected. AWS permits only the numeric widening rules below. Required input values are enforced by Crucible even when the physical catalog reports nullable columns.
- Crucible validates the complete batch against the resolved schema before sending a write operation.
- Required non-null columns and declared keys must be present and non-null. If a nullable column is absent, use its declared default when present, otherwise `null`.
- Fields not declared by the schema are ignored. Crucible constructs normalized records in schema column order so every row has the same physical shape.
- `keys`, when supplied, are validated but do not deduplicate append records. Append may create duplicate logical keys.
- A validation error rejects the whole batch before table mutation. Version 1 does not promise rollback after a physical write has begun unless the selected backend explicitly guarantees it.

The append path has live RustFS coverage and an AWS creation/additive-schema/readback probe. Ordered upsert currently has in-memory DuckDB coverage; validate MERGE against RustFS and AWS S3 Tables before claiming those catalog paths support it. Broader schema migrations remain deferred. Redis is a Formula schema-document storage backend, not a schema-backed dataset sink in this version. Version 1 does not coerce malformed values into schema types; invalid values fail validation before writing.

### Views

Views define a read model over one or more schema-managed datasets. They do not accept normal row inserts.

```yaml
templateVersion: 1
name: current_market_quotes
type: schema

schema:
  kind: view
  view:
    sql: |
      SELECT symbol, price, observed_at
      FROM analytics.events
      WHERE is_current = true
```

A view configuration should eventually support:

- `kind: view` for a logical database view.
- `materialization: virtual` or `materialized`.
- Explicit refresh policy for materialized views.
- Backend-specific SQL only under an adapter override.

Views should be read-only from the workflow output path. A workflow may reference a view for downstream reads, but it should not write directly to it. The main Oxidizer orchestrator owns dependency ordering, readiness, and execution decisions.

## Backend Mapping

The schema contract is backend-neutral, but each adapter must declare its supported operations and limitations.

```yaml
schema:
  backend_overrides:
    iceberg:
      table_properties:
        format-version: "2"
    redis:
      key_prefix: market_quotes
```

The DuckDB adapter implements `append` directly and uses Arrow staging with one target `MERGE` for ordered `upsert`, SCD1, and SCD2. SCD2 also uses set-based history planning. Install `oxidizer[duckdb]` to include PyArrow. Views remain future capabilities. Other adapters must reject unsupported operations rather than silently degrading them to append.

## Workflow Contract

A table output method must reference a named schema:

```yaml
outputs:
  methods:
    - method: s3_tables
      connection: local_lake
      schema: market_quotes
      namespace: analytics
      table: events
```

Version 1 does not support inline schemas or schema-less table outputs. A `schema` value must name a stored schema document, and the resolved document's `name` must match the reference.

### Workflow Field Decision

There are two possible meanings for a workflow-level `schema` field:

- **Output-method-only:** each output declares the schema it writes. This is explicit and allows one node to write different representations to different destinations.
- **Reserved workflow field:** a formula-level field supplies a default schema for nodes that omit one. This is convenient but introduces inheritance and override rules.

Version 1 should treat `schema` as an **output-method-only field**. A future formula-level default can be added without changing the stored schema document format.

Crucible should validate before writing:

1. Resolve the named connection and schema.
2. Resolve the target namespace/table or view.
3. Validate required columns and logical types.
4. Validate keys and operation-specific metadata.
5. Reconcile the physical schema within the supported provisioning/additive rules, then apply the write operation using the selected adapter.
6. Return the inserted count for append; the initial upsert path returns the number of input rows processed. Future operations may return inserted, updated, unchanged, and rejected counts.

## Table Creation And Additive Changes

After validating the complete input batch, `Crucible.write()` inspects the target relation. If absent, it creates the namespace and table using the declared column names, types, and nullability. Empty writes return zero without provisioning. `Crucible.ensure_table(..., schema=document)` performs the same reconciliation explicitly without requiring records.

For existing tables, column order is not significant. Types must match unless an AWS numeric widening rule below applies. Logical `json` is stored as serialized `VARCHAR` for new Iceberg tables; existing DuckDB `JSON` columns are also accepted. Physical columns not declared in the schema are treated as attempted removals and rejected. A physically nullable column can accept a logically required column because Crucible validates every input value; a physically required column cannot accept a nullable declaration. The live AWS catalog reports nullable fields even after creation DDL includes `NOT NULL`, so physical enforcement must not be assumed.

On AWS `S3TablesConnection`, new columns may be added only when declared nullable. Existing rows receive null; application-level defaults apply to new input records, not as a historical backfill. Existing columns support these Iceberg numeric promotions:

- `int32` to `int64` (`INTEGER` to `BIGINT`).
- `float` to `double` (`FLOAT` to `DOUBLE`).
- Decimal precision increase with unchanged scale, up to precision 38, such as `decimal(8,2)` to `decimal(18,2)`.

Widening preserves stored values and runs before the data write. Narrowing, decimal scale changes, cross-family conversions (such as integer to double or string to numeric), renames/removals, adding required columns, and physical nullability changes are not automated. `IcebergRestConnection` supports creation and compatibility checks but never issues schema alteration; missing columns or requested widening on an existing table fail clearly. General migrations and a `schema.evolution` policy block remain unsupported.

All compatibility checks run before alteration. DDL is separate from the data transaction: successful creation/additions/widenings may remain if a later write fails, and multiple schema changes are not promised to commit atomically. Concurrent provisioning/migration and automated schema-version enforcement are not covered; increment and publish `schema.version` externally when changing the contract. Older run snapshots requesting narrower types will fail compatibility checks after widening. Widening does not recompute existing SCD2 hashes; changing a compared column's logical type changes hash inputs and can cause retry conflicts or new history versions, so it requires a reviewed history migration. AWS nullable additions and all three widening rules have been verified with existing-row preservation, subsequent writes, repeated reconciliation, and readback on disposable tables.

## Idempotency And Batches

Idempotency applies only to SQL/table writes performed through Crucible. Redis Streams, Kafka, and other stream outputs keep their existing message and acknowledgment semantics and do not use this write identity.

Exactly-once SQL writes are not guaranteed by version 1. Append is at-least-once: if a table insert succeeds but the worker fails before checkpoint/task acknowledgment is durable, retry may append duplicate rows. Unit fault-injection tests demonstrate duplicate append after checkpoint-publication or acknowledgment failure, verify that controller state-persistence failure leaves its update unacknowledged, and exercise repeated identical upsert and SCD2 writes. The live [recovery-boundary integration test](../../tests/integration/test_recovery_boundaries.py) kills a worker after a real RustFS append commit, both before checkpoint publication and after publication but before task acknowledgment; it verifies pending work and duplicate behavior on deliberate retry. It does not test automatic pending-task reclaim or other Iceberg catalogs. Do not claim retry-safe or exactly-once behavior for SQL append without an idempotency mechanism. A future Crucible write identity should include:

| Failure/replay case | Current expected behavior |
| --- | --- |
| Append commits; worker fails before checkpoint publication | Retrying the task can append the same rows again. |
| Checkpoint is published; task/source acknowledgment fails | The unacknowledged input can be replayed and append duplicate rows. |
| Controller fails to persist a worker update | The controller leaves that update unacknowledged so it can be delivered again. |
| Identical keyed upsert or unchanged SCD2 batch is replayed | Table keys/ordering and SCD2 comparison make the tested table result converge; this does not cover other outputs or stream records. |

A candidate design is to write the data rows and a SQL-side batch marker in one transaction, then use the marker on retry to skip an already committed batch. A disposable DuckDB/RustFS Iceberg probe confirmed rollback, commit, and marker recovery after reconnect for two tables in one catalog. Commit-time process failure and concurrent duplicate writers have not been tested, so this is feasibility evidence rather than an exactly-once guarantee. Any marker must be committed with the SQL output; a Redis-only marker is insufficient.

```yaml
write:
  idempotency:
    key_columns:
      - formula_id
      - run_id
      - node_id
      - batch_index
```

For future SCD and upsert operations, business keys and ordering columns remain part of the data contract; a batch identity alone must not overwrite a newer source record.

## Timestamp And Decimal Conventions

Canonical representations prevent DuckDB, Iceberg, and source adapters from interpreting the same value differently.

### Timestamps

- Logical `timestamp` means an absolute UTC instant.
- Serialized values use RFC 3339 with a `Z` suffix, for example `2026-10-01T05:12:12Z`.
- Physical DuckDB/Iceberg columns should use a timezone-aware timestamp where supported.
- Date-only business fields use logical `date` and must not be converted to midnight timestamps.
- Naive timestamps from an input source must be rejected or assigned an explicitly configured source timezone; they must not be guessed silently.

### Decimals

- Monetary and exact-measurement values use `decimal` rather than `float` or `double`.
- Precision and scale are required for a decimal column, for example `decimal(18, 4)`.
- Values with more fractional digits must follow an explicit rounding or rejection policy.
- Binary floating-point values from market APIs may remain `double` when approximate values are acceptable, as with the Finnhub quote example.

## Rejected Records And Task Safety

Schema validation must not lose the original workflow task. Candidate policies are:

1. **Fail the batch:** reject the entire batch, leave the task unacknowledged, and retry after the schema or source is corrected. This should be the default for required-column, key, and type failures.
2. **Skip with metrics:** discard invalid records and continue. This is appropriate only for explicitly non-critical fields and must emit counts and reasons.
3. **Fail-fast before write:** validate the complete batch before mutating the destination, preventing partial writes where the backend supports that guarantee.

The initial implementation should default to fail-the-batch and fail-fast validation. Partial acceptance should require an explicit schema policy and return inserted, rejected, and unchanged counts.

## Views And Portability

View portability describes whether a view's SQL can run on more than one backend. A view may declare:

```yaml
schema:
  kind: view
  view:
    portability: portable
    refresh:
      owner: scheduler
      mode: on_write
    sql: |
      SELECT symbol, price
      FROM analytics.events
```

The portability choices are:

- `portable`: use the supported common SQL subset and validate the definition against each target backend.
- `backend_specific`: provide SQL under a backend key such as `iceberg.sql` or `duckdb.sql`.
- `opaque`: the schema registry tracks the view but does not validate SQL.

Refresh ownership answers who is responsible for keeping a materialized view current:

- `on_write`: the output operation refreshes it after a successful write.
- `scheduler`: a controller or external scheduler refreshes it.
- `manual`: an operator or explicit command refreshes it.
- `virtual`: there is no refresh operation because the view is evaluated at read time.

View support is deferred. If introduced later, begin with virtual views and define portability and refresh behavior before supporting materialized views.

## Future Work

The following capabilities are intentionally deferred:

### Schema Lifecycle Management

Schema upload, publication, promotion, rollback, lifecycle history, and deployment authorization are outside the current schema contract. For now, a new or updated schema document is treated as an intentional configuration change performed by an external upload/deployment process.

### Quarantine Storage

Quarantine handling is future work. A later implementation may write rejected records and structured reason codes to a Redis dead-letter stream, an Iceberg quarantine table, or both. The original task must remain retryable until the chosen quarantine write succeeds.

### Automatic Provisioning

Automatic namespace/table creation, AWS nullable-column addition, and the supported numeric widening rules are implemented by the DuckDB adapter. General migrations, schema alteration on Iceberg REST, backfills, and concurrent migration coordination remain future capabilities.

## Version 1 Decisions And Deferred Work

Version 1 decisions:

1. Schema references are output-method-only and identify the current named document.
2. The document's positive `schema.version` is incremented on contract changes; each run snapshots the resolved version and contents for retry consistency.
3. Crucible owns schema interpretation and data validation; Formula only stores/retrieves documents, and Reagent only forwards resolved schema data.
4. Every table write requires a named schema and uses the Crucible schema-aware API; there is no schema-less write facade. Version 1 validates records, reconciles physical columns, creates missing tables, and supports AWS nullable additions and conservative numeric widening.
5. Schema publication remains an external deployment/operator action with pre-publication document validation.
6. Append is at-least-once and can duplicate on retry after an ambiguous write.

Deferred: broader schema migrations, concurrent migration coordination, other upsert policies, additional live catalog MERGE/transaction verification, historical version lookup, workflow-level schema defaults, a schema registry/publication API, views, quarantine, and exactly-once writes. Revisit each with a concrete use case and focused tests before adding it to the runtime contract.
