# Designing Schema Configurations

A schema document defines the logical columns, record identity, and write policy for a table output. Formula stores it by name; Oxidizer snapshots the resolved document for each run; Crucible validates records and applies the write mode. This page is an authoring guide. The [schema contract](schema-contract.md) is the detailed specification.

## Start With The Contract

Every schema document uses this envelope:

```yaml
templateVersion: 1
name: daily_temperature_history
type: schema

schema:
  version: 1
  description: Daily weather observations with change history.
  columns: []
  keys: []
  write:
    operation: append
```

Keep `name` stable because workflow outputs refer to it. Set `schema.version` to a positive integer and increment it whenever the data contract changes. `templateVersion` versions the document format; it is not the dataset revision. A run uses the schema snapshot resolved at invocation time, including for retries.

## Define Columns And Keys

Declare each column once, with a backend-neutral logical type and an explicit nullability rule. Column names must be valid identifiers. Optional `default` values are validated against the column type; missing nullable fields become null. Extra record fields not declared in the schema are ignored. `description` is documentation metadata and does not affect validation or storage.

Supported logical types are `boolean`, `int32`, `int64`, `float`, `double`, `decimal`, `string`, `date`, `timestamp`, `json`, and `binary`. Decimal columns require `precision` and `scale`; timestamps must be UTC RFC3339 strings ending in `Z`. See [logical types and constraints](schema-contract.md#logical-types).

Choose `keys` based on the identity of a logical record:

- For append-only events, keys are optional and do not prevent duplicate rows.
- For upsert, SCD1, and SCD2, declare one or more non-null key columns. Keep compound key membership and meaning stable.

## Choose A Write Mode

- `append` adds every input row. Use it for immutable events or observations where duplicate delivery is acceptable or handled separately.
- `upsert` keeps one row per key. `all_columns` requires an ordering column and direction so stale updates can be ignored; `insert_only` inserts missing keys without updating existing ones.
- `scd1` keeps one current row per key and overwrites non-key columns when a newer ordering value arrives. `compare.columns` identifies fields used to reject conflicting equal-order updates; it does not retain history.
- `scd2` retains versions when `compare.columns` values change. Repeated keys in one batch are allowed, but their ordering values must be chronological.

## Choose SCD2 Keys And Ordering

Choose the key from the **entity grain**: what one independently versioned history represents. Choose `write.ordering.column` from the **version time**: when the source snapshot or change became known. These are often different timestamps.

For forecasts, one row represents one forecast hour for one place. Use `[place, observed_at]` as the key, where `observed_at` is the forecasted hour. Add `retrieved_at` as the ordering column and set it once per API fetch for every row in that batch. A later fetch then has a later version time for the same forecast hour. If the temperature is unchanged, Crucible creates no new version; if it changed, Crucible closes the old version and inserts the new one.

Do not key a whole rolling forecast only by `place` and order it by `observed_at`. On the next fetch, earlier forecast hours are older than the current timestamp and Crucible correctly rejects them as late arrivals. Likewise, ordering by ingestion time is only appropriate if ingestion time represents the desired version history.

SCD2 requires ascending order. By default, records older than the current version are rejected. Set `write.late_arrivals: ignore` to skip those older rows and continue with newer rows; Crucible emits a warning and the worker checkpoint records per-batch and cumulative ignored-row counts. This is lossy by design and does not ignore equal-order conflicts or out-of-order records within a batch. If a source can deliver multiple changed versions for the same key in one batch, each needs a distinct deterministic ordering value. Preserve original ordering values on retries when exact replay identity matters.

## Complete SCD2 Example

This example matches the Open-Meteo history workflow. The four history columns are required in the schema but are managed by Crucible; incoming records do not need to supply them.

```yaml
templateVersion: 1
name: daily_temperature_history
type: schema

schema:
  version: 1
  description: Daily weather observations with change history.
  columns:
    - name: place
      type: string
      nullable: false
    - name: observed_at
      type: timestamp
      nullable: false
    - name: retrieved_at
      type: timestamp
      nullable: false
    - name: temperature_2m
      type: double
      nullable: false
    - name: valid_from
      type: timestamp
      nullable: false
    - name: valid_to
      type: timestamp
      nullable: true
    - name: is_current
      type: boolean
      nullable: false
    - name: record_hash
      type: string
      nullable: false
  keys:
    - place
    - observed_at
  write:
    operation: scd2
    late_arrivals: reject
    ordering:
      column: retrieved_at
      direction: ascending
    compare:
      columns:
        - temperature_2m
    history:
      valid_from: valid_from
      valid_to: valid_to
      current_flag: is_current
      record_hash: record_hash
      open_ended_value: null
```

For SCD2, `valid_from` and `valid_to` must use the ordering column's type. `current_flag` must be a non-null boolean, and `record_hash` a non-null string. The four history fields must be distinct, declared columns, and cannot be keys or the ordering column. `compare.columns` must be non-empty and cannot include keys, the ordering field, or managed history columns.

With the default `late_arrivals: reject`, late records fail unless they exactly replay a previously recorded version. `late_arrivals: ignore` skips records strictly older than the current version and logs the ignored count; equal-order rows with conflicting compared values still fail. A key absent from an incremental batch is not expired; this is not snapshot synchronization.

## Reference The Schema From A Workflow

Outputs select a named schema, independently of the physical table and connection:

```yaml
outputs:
  methods:
    - method: s3_tables
      connection: local_lake
      schema: daily_temperature_history
      catalog: local_catalog
      namespace: analytics
      table: temperature_history
```

Formula stores schema documents under `schemas/<name>.yml` for S3/RustFS or `oxidizer:schemas:<name>` in RedisJSON. Schema publication/replacement remains an external deployment step; there is no schema registry. Crucible checks physical columns, creates missing namespace/tables, and supports nullable-column additions and numeric widening on AWS S3 Tables: int32 to int64, float to double, and increased decimal precision with unchanged scale. It does not alter existing `IcebergRestConnection` schemas or automate narrowing, column removals, or general migrations. See [the schema contract](schema-contract.md#table-creation-and-additive-changes) for compatibility and transaction limits.

## Before Publishing

- Confirm the document envelope, stable name, and incremented schema version.
- Check every column name, logical type, nullability, decimal precision/scale, and default.
- Confirm key columns match the actual business identity and are non-null in source records.
- Confirm the selected write mode has all required fields and ordering policy.
- For SCD2, ensure same-key events are ordered and the physical table includes all four managed history columns.
- Validate representative valid and invalid records with Crucible tests before replacing the current named document.

For exact mode semantics, constraints, and deferred features, see the [schema contract](schema-contract.md). For Arrow staging and adapter verification limits, see [table write modes](table-write-modes.md).
