# Oxidizer Operations Docs

This documentation is organized for both operators and LLM troubleshooting assistants. Prefer observable state, correlated IDs, and source-backed behavior over guesses based on a symptom alone.

The Markdown pages are included in the installed `oxidizer` package and are available offline with the distribution.

## Find Your Situation

- **You need a local environment or CPU-aware worker memory guidance:** follow [Local container setup](local-setup.md) for Apple container prerequisites, startup, verification, and allocator tuning.
- **A run failed, stalled, or is missing output:** start with [Workflow run troubleshooting](troubleshooting/workflow-runs.md). It gives a read-only investigation order, symptom table, and verification checklist.
- **You need a successful worker trace to compare with a failure:** use [Worker DEBUG trace](troubleshooting/worker-debug-trace.md) for container/API/MCP retrieval, expected phases, and an LLM handoff.
- **Invocation, dispatch, or controller state progress is unclear:** use [Controller DEBUG trace](troubleshooting/controller-debug-trace.md) for the successful controller sequence and boundary-specific checks.
- **You have a run ID and need to understand node progress:** use [Run state and checkpoints](reference/run-state-and-checkpoints.md) to interpret statuses, cursors, finality, and phase memory peaks.
- **Logs are missing or incomplete:** use [Logging and sinks](reference/logging-and-sinks.md) to check log level, sink attachment, buffering, retention, and retrieval.
- **The REST API or MCP tools are unavailable or return unexpected data:** check [Topology API and MCP tools](reference/topology-api-and-tools.md).
- **An LLM needs version-matched documentation:** use semantic MCP `search_docs` and `read_doc`; check `get_docs_search_status` first. See [Documentation lookup](reference/topology-api-and-tools.md#documentation-lookup) and the [API image guide](../docker/README.md).
- **A workflow reference, connection, or input configuration is rejected:** see [Formulas and connections](reference/formulas-and-connections.md).
- **You need dynamic API query parameters from upstream records:** see [Stream-triggered API inputs](reference/formulas-and-connections.md#stream-triggered-api-inputs) and the [runnable quickstart](../examples/quickstart/README.md#dynamic-api-query-parameters).
- **A table write fails or produces unexpected history:** see [Designing schema configurations](reference/schema-configurations.md), the [Schema contract](reference/schema-contract.md), and [Table write modes](reference/table-write-modes.md).
- **You need an approved production restart, backup/restore, or replay procedure:** use [Production Recovery](operations/production-recovery.md).

## Diagnostic Rules

Use observations from the affected run, not assumptions from a similar workflow. Collect `formula_id`, `run_id`, `node_id`, current status, checkpoint metadata, safe error details, and relevant stream group counts. Correlate state and logs using those identifiers. Separate confirmed facts from hypotheses and name the next read-only check that distinguishes them.

Do not acknowledge, delete, purge, seek, reset, or replay messages; overwrite run state; or change stored workflow configuration during diagnosis unless the operator explicitly authorizes that action. Never request credentials or include tokens and secrets in diagnostic output.

## Evidence Priority

1. Run and node state: authoritative status, error details, cursor, finality, and timestamps.
2. Stream metadata: length, consumer-group pending count, and lag; inspect without changing group state.
3. Correlated component logs: use the same formula, run, and node IDs; confirm log level and sink delivery before treating missing logs as evidence.
4. Destination readback: verify output row/message counts and representative values.
5. Recovery: only after the failed boundary and replay consequences are understood and the operator approves any mutation.

These pages are operational references for users and troubleshooting assistants. They focus on observable behavior, safe checks, and recovery risks; understanding a runtime incident should not require navigating the source tree.
