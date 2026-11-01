# Table Write Modes

Crucible requires a named schema for every table write. See the [schema authoring guide](schema-configurations.md) for a complete configuration walkthrough and the [schema contract](schema-contract.md) for the complete version 1 rules defining `append`, ordered `upsert`, SCD1, and SCD2.

## DuckDB Merge Path

The DuckDB adapter uses PyArrow to stage `upsert`, SCD1, and SCD2 input in batches of up to 65,536 rows. Upsert and SCD1 check ordering ties against that staged relation and issue one set-based target `MERGE`. SCD2 additionally validates and plans validity intervals with SQL window functions before its action-tagged target `MERGE`. Staging, validation, and mutation share a transaction, and temporary tables and Arrow registrations are cleaned up afterward.

The Arrow chunks limit extra ingestion-buffer size, not total process memory: the input records and normalized batch are still held in Python. Crucible's write API is not streaming. Install `oxidizer[duckdb]` to use DuckDB and PyArrow.

## Performance and Verification

An in-memory DuckDB benchmark processed 100,000 matched updates in about 43 ms for upsert and 44 ms for SCD1. Record generation and schema normalization were excluded. These numbers compare adapter paths only; they are not end-to-end or live Iceberg throughput guarantees.

SCD2 initialization and interval behavior have been exercised against local RustFS. Upsert and SCD1 currently have in-memory DuckDB coverage; live RustFS/AWS behavior and concurrent writers remain to be verified. Treat these as validation gaps when investigating a table-write failure. For run state, logs, pending messages, and safe read-only checks, see [Workflow run troubleshooting](../troubleshooting/workflow-runs.md).
