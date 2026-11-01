# Oxidizer Lite

Oxidizer Lite helps you turn Python functions into data workflows. You describe
what runs, in what order, and where the data comes from and goes in YAML.
The controller keeps things moving, while workers process batches and save
checkpoints along the way. Use it to pull data from APIs or object storage,
pass records through Redis or Kafka, and write results to Iceberg tables.

**Active development:** interfaces and configuration may change between
releases. 

## Key Concepts

| Concept | Purpose |
| --- | --- |
| Controller | Starts runs, schedules ready tasks, and tracks progress and checkpoints. |
| Worker | Executes Python task functions and handles configured inputs and outputs. |
| API / MCP (Topology) | Exposes run inspection, diagnostics, table queries, and configuration management to HTTP and MCP clients. The REST API also starts workflows. |
| Workflow (formula) | YAML describing task nodes, dependencies, and input/output methods. |
| Connection | Named backend settings referenced by workflows, separate from task logic. |
| Schema | Defines table columns, record identity, and write policies such as upsert or SCD2 history. |

See [workflows and connections](docs/reference/formulas-and-connections.md),
[schema configuration](docs/reference/schema-configurations.md), and
[run state and checkpoints](docs/reference/run-state-and-checkpoints.md).

## Quickstart

The supplied Redis demo reads a million-row Parquet fixture and writes Iceberg
upsert and history tables using local RustFS storage.

From the repository root:

```bash
cd examples/quickstart
docker compose -f compose.redis.yml up --build -d
docker compose -f compose.redis.yml logs seed
```

Wait for `Ready to invoke quickstart_observations_redis` in the seed log.
The first API image build downloads its documentation-search model; serving
does not require a model download or API key.

### REST API

Check readiness, then invoke the workflow:

```bash
curl --fail http://localhost:8027/api/health
curl --fail http://localhost:8027/api/docs/status

curl --fail-with-body --silent --show-error \
  --request POST http://localhost:8027/api/invocations \
  --header 'Content-Type: application/json' \
  --data '{"formula_id":"quickstart_observations_redis"}'
```

Use the returned `run_id` to inspect progress:

```bash
curl --fail-with-body --silent --show-error \
  "http://localhost:8027/api/formulas/quickstart_observations_redis/runs/<run-id>"
```

The [quickstart guide](examples/quickstart/README.md) includes result queries,
expected counts, the Kafka alternative, customization, and cleanup instructions.

### MCP

Connect an MCP client using **Streamable HTTP** at
`http://localhost:8027/mcp`. Example tool calls (not REST requests):

```json
{"name":"list_formulas","arguments":{}}
{"name":"get_formula_state","arguments":{"formula_id":"quickstart_observations_redis","run_id":"<run-id>"}}
{"name":"search_docs","arguments":{"query":"How do I troubleshoot a stalled workflow?","limit":3}}
```

See the [API and MCP reference](docs/reference/topology-api-and-tools.md) for
routes, tools, and documentation lookup.

The API/MCP server has no built-in authentication. Keep it on loopback or behind
trusted access control. Quickstart credentials and storage are disposable demo
settings, not production configuration.

## Documentation

- [Documentation index](docs/README.md)
- [Docker images and deployment](docker/README.md)
- [Workflow troubleshooting](docs/troubleshooting/workflow-runs.md)
- [Production recovery](docs/operations/production-recovery.md)
