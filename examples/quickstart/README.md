# Parquet To Iceberg Quickstart

Run the supplied Oxidizer container with YAML configuration and a mounted Python
router. No custom API, controller, or worker implementation is needed.

The two workflows perform the same work with different primary stream backends:

| Workflow | Transport | Nodes |
| --- | --- | --- |
| `quickstart_observations_redis` | Redis for control and data streams | `bronze.raw_observations` -> `silver.transform_observation_history` |
| `quickstart_observations_kafka` | Kafka/Redpanda for control and data topics | `bronze.raw_observations` -> `silver.transform_observation_history` |

The bronze node reads ordered Parquet pages, normalizes timestamps, publishes
records to a stream/topic, and upserts an Iceberg table. The silver node consumes
those records and applies the schema's SCD2 policy to build history. The silver
Python function is deliberately pass-through: Crucible performs the SCD2 write.

In the Redis demo, the silver node sets `task_stream:
oxidizer:reagent:special-worker` in [workflow.redis.yml](workflows/workflow.redis.yml),
so the controller dispatches its tasks to the dedicated `special-worker`
service (configured in [runtime.redis.yml](runtime.redis.yml)) instead of the
shared worker stream. Use this pattern to pin nodes to specific machines, such
as GPU instances.

The Redis demo sets `controller.retention: 3m` in
[runtime.redis.yml](runtime.redis.yml) and `retention: 3m` on the bronze stream
output in [workflow.redis.yml](workflows/workflow.redis.yml). These expire entire
stream keys, including messages and consumer groups. Task writes and non-empty
control reads refresh control TTLs; non-empty output writes refresh the data
TTL. Use longer durations for slower runs or outages, or omit retention to
leave expiration unset. Kafka retention is configured separately at the broker.

## Start

You need Docker with Compose v2 and at least 12 GiB available to Docker for the
million-row demo. These services use local demo credentials, not production
settings.

From the repository root:

```bash
cd examples/quickstart
docker compose -f compose.redis.yml up -d
docker compose -f compose.redis.yml logs seed
```

Choose [compose.redis.yml](compose.redis.yml) for Redis or
[compose.kafka.yml](compose.kafka.yml) for Kafka/Redpanda. Each is a standalone
stack with RustFS, its selected broker, and the API/controller/worker roles.
Both pull `ghcr.io/moos-engineering/oxidizer-lite:latest` for seed/controller/worker
and `ghcr.io/moos-engineering/oxidizer-lite:latest-api` for REST/MCP with embedded
semantic documentation search. Each startup checks for the latest published
images. Releases publish `linux/amd64` and `linux/arm64` variants; Docker selects
the host's architecture automatically. Older AMD64-only releases require
`DOCKER_DEFAULT_PLATFORM=linux/amd64` on ARM hosts until a multi-platform release
is published.
Serving does not need a model download or API key.
The seeder generates a reproducible 1,000,000-row Parquet fixture and uploads
the data and YAML documents. Wait for `Ready to invoke
quickstart_observations_redis` in the seed log before invoking.

Storage is disposable. Removing containers discards their data; Redis also loses
its data when stopped. The generated Parquet file stays in this directory.

## Select Redis Or Kafka

For Kafka, run these commands instead:

```bash
docker compose -f compose.kafka.yml up -d
docker compose -f compose.kafka.yml logs seed
```

No YAML edits or override files are needed. Kafka uses separate controller/worker
groups and a single-partition, single-replica topic setup. The files share the
project name and API port, so run only one at a time. Purge the current demo
before switching and wait for the selected workflow's seed ready message.
Do not switch during an active run.

## Invoke And Verify

Wait for seed completion and API readiness:

```bash
curl --fail http://localhost:8027/api/health
curl --fail http://localhost:8027/api/docs/status
```

Invoke the Redis workflow (use `quickstart_observations_kafka` for Kafka):

```bash
curl --fail-with-body --silent --show-error \
  --request POST http://localhost:8027/api/invocations \
  --header 'Content-Type: application/json' \
  --data '{"formula_id":"quickstart_observations_redis"}'

curl --fail-with-body --silent --show-error \
  "http://localhost:8027/api/formulas/quickstart_observations_redis/runs/<run-id>"
```

Substitute the returned `run_id` and the selected workflow ID. Wait for both
nodes to reach `success`, then read the table results:

```bash
curl --fail-with-body --silent --show-error \
  --request POST http://localhost:8027/api/connections/quickstart_lake/query \
  --header 'Content-Type: application/json' \
  --data '{"sql":"SELECT COUNT(*) AS rows, COUNT(DISTINCT entity_id) AS entities FROM local_catalog.analytics.quickstart_observation_upsert","max_rows":1}'

curl --fail-with-body --silent --show-error \
  --request POST http://localhost:8027/api/connections/quickstart_lake/query \
  --header 'Content-Type: application/json' \
  --data '{"sql":"SELECT COUNT(*) AS versions, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows FROM local_catalog.analytics.quickstart_observation_history","max_rows":1}'
```

Expect 1,000,000 upsert rows across 10 entities, 1,000,000 history versions, and
10 current rows. Replaying the same fixture should leave these table counts
unchanged, but appends another copy to the raw stream/topic. This is not an
exactly-once guarantee. Verify counts, particularly for Kafka, whose short-poll
finality and recovery behavior remain limited.

## Customize

| File | Change here |
| --- | --- |
| [runtime.redis.yml](runtime.redis.yml), [runtime.kafka.yml](runtime.kafka.yml) | Formula store, primary transport, log level, worker router, dedicated worker task stream, selected workflow |
| [workflow.redis.yml](workflows/workflow.redis.yml), [workflow.kafka.yml](workflows/workflow.kafka.yml) | Bronze/silver nodes, dependencies, input paths, batch sizes, outputs |
| [local-files.yml](connections/local-files.yml), [local-lake.yml](connections/local-lake.yml) | Named S3 input and Iceberg output connections |
| [upsert-observations.yml](schemas/upsert-observations.yml) | Typed fields, entity/time keys, winning source revision |
| [observation-history.yml](schemas/observation-history.yml) | SCD2 keys, ordering, comparison fields, history columns |
| [custom.py](custom.py) | `quickstart_router` and the two node functions |
| [parquet_generator.py](parquet_generator.py) | Fixture size, entity count, and values |

Keep connection and runtime bucket names in sync. The custom module includes a
local RustFS metadata-log workaround scoped to `quickstart-tables`.

After changing documents or custom code, rerun the seeder and restart the roles:

```bash
docker compose -f compose.redis.yml run --rm seed
docker compose -f compose.redis.yml restart api controller worker special-worker
```

Use `-f compose.kafka.yml` for Kafka. Reseeding overwrites documents and
uploads the fixture; it does not purge streams, tables, or run state. Avoid
reseeding during an active run.

## Stop Or Start Over

Stop containers (Redis's in-memory data is lost):

```bash
docker compose -f compose.redis.yml stop
```

**Purge this demo:** the following deletes this Compose project's containers,
anonymous volumes, buckets/catalog data, streams, and Kafka offsets. Use only
when all quickstart data can be discarded:

```bash
docker compose -f compose.redis.yml down --volumes --remove-orphans
```

Use `-f compose.kafka.yml` when stopping or purging the Kafka demo. Repeat the
chosen startup command to start over. Remove `observations.parquet` from
this directory only if you also want to regenerate the fixture. No reset script
is needed.

## Reference

- [Workflow and connection configuration](../../docs/reference/formulas-and-connections.md)
- [Table schemas and write modes](../../docs/reference/schema-configurations.md)
- [Run state and checkpoints](../../docs/reference/run-state-and-checkpoints.md)
- [Troubleshooting](../../docs/troubleshooting/workflow-runs.md)