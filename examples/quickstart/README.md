# Parquet To Iceberg Quickstart

Run the supplied Oxidizer container with YAML configuration and a mounted Python
router. No custom API, controller, or worker implementation is needed.

The two Parquet workflows perform the same work with different primary stream backends:

| Workflow | Transport | Nodes |
| --- | --- | --- |
| `quickstart_observations_redis` | Redis for control and data streams | `bronze.raw_observations` -> `silver.transform_observation_history` |
| `quickstart_observations_kafka` | Kafka/Redpanda for control and data topics | `bronze.raw_observations` -> `silver.transform_observation_history` |
| `quickstart_api_params` | Either selected backend | `source.weather_requests` -> `weather.hourly_forecasts` |

The additional API workflow demonstrates dynamic GET query parameters against
Open-Meteo's weather forecast endpoint. Both stacks seed it automatically.

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

## Dynamic API Query Parameters

Invoke `quickstart_api_params` on either stack after seed completion and API
readiness:

```bash
curl --fail-with-body --silent --show-error \
  --request POST http://localhost:8027/api/invocations \
  --header 'Content-Type: application/json' \
  --data '{"formula_id":"quickstart_api_params"}'

curl --fail-with-body --silent --show-error \
  "http://localhost:8027/api/formulas/quickstart_api_params/runs/<run-id>"
```

[workflow.api-params.yml](workflows/workflow.api-params.yml) runs two nodes:

1. `source.weather_requests` emits mock city inputs for New York and London
   with real Open-Meteo query parameters:
   ```json
   {"params":{"latitude":40.7128,"longitude":-74.0060,"hourly":"temperature_2m","forecast_days":1,"timezone":"UTC"}}
   {"params":{"latitude":51.5074,"longitude":-0.1278,"hourly":"temperature_2m","forecast_days":1,"timezone":"UTC"}}
   ```
2. `weather.hourly_forecasts` uses each record's `params` mapping as GET
   query parameters for `https://api.open-meteo.com/v1/forecast`, through
   [open-meteo.yml](connections/open-meteo.yml). `input_trigger.attribute: params`
   keeps the endpoint fixed while changing query parameters per request.
   The HTTP adapter handles URL encoding. Triggered parameters replace static
   method-level `params`, rather than merging them.

Workers need outbound HTTPS access. Open-Meteo's free non-commercial API
requires no API key; follow its [usage terms and limits](https://open-meteo.com/en/terms).
Weather data is provided by [Open-Meteo](https://open-meteo.com/) under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); retain attribution
when reusing results. See the [forecast API documentation](https://open-meteo.com/en/docs).

Both nodes route through [custom.py](custom.py). The forecast transform flattens
the parallel `hourly.time` and `hourly.temperature_2m` arrays into one record
per hour. The output stream/topic
`quickstart_api_params.weather.hourly_forecasts` contains 48 records on a clean
successful run: 24 UTC hours for each location. Records include grid coordinates,
`forecast_at`, timezone, temperature, and unit. Grid coordinates can differ
slightly from the requested coordinates; forecast values change over time.

Wait for both nodes to reach `success`, then inspect the output. For Redis:

```bash
docker compose -f compose.redis.yml exec redis \
  redis-cli XLEN quickstart_api_params.weather.hourly_forecasts
docker compose -f compose.redis.yml exec redis \
  redis-cli XRANGE quickstart_api_params.weather.hourly_forecasts - + COUNT 2
```

For Kafka:

```bash
docker compose -f compose.kafka.yml exec redpanda \
  rpk topic consume quickstart_api_params.weather.hourly_forecasts -n 48
```

Reinvocation appends outputs; it does not clear earlier requests or forecasts.
HTTP errors fail the task and leave its trigger unacknowledged. This API returns
one day's hourly data in a single response, so no paginator is configured.

This workflow requires a worker image containing `params` trigger support;
older published images reject this attribute. To try unreleased source with
an already-running Redis stack, build the runtime image from the repository
root, then reseed and recreate workers without pulling over the local image:

```bash
docker build --target runtime -f docker/oxidizer-lite.dockerfile \
  -t ghcr.io/moos-engineering/oxidizer-lite:latest .
cd examples/quickstart
docker compose -f compose.redis.yml run --rm --pull never seed
docker compose -f compose.redis.yml up -d --pull never --force-recreate worker special-worker
```

For Kafka, use `-f compose.kafka.yml` and recreate only `worker`. Avoid reseeding
or recreating workers during an active run. See [local image builds](../../docker/README.md#local-builds).

## Customize

| File | Change here |
| --- | --- |
| [runtime.redis.yml](runtime.redis.yml), [runtime.kafka.yml](runtime.kafka.yml) | Formula store, primary transport, log level, worker router, dedicated worker task stream, selected workflow |
| [workflow.redis.yml](workflows/workflow.redis.yml), [workflow.kafka.yml](workflows/workflow.kafka.yml) | Bronze/silver nodes, dependencies, input paths, batch sizes, outputs |
| [workflow.api-params.yml](workflows/workflow.api-params.yml), [open-meteo.yml](connections/open-meteo.yml) | Dynamic GET query-parameter trigger and the weather API connection |
| [local-files.yml](connections/local-files.yml), [local-lake.yml](connections/local-lake.yml) | Named S3 input and Iceberg output connections |
| [upsert-observations.yml](schemas/upsert-observations.yml) | Typed fields, entity/time keys, winning source revision |
| [observation-history.yml](schemas/observation-history.yml) | SCD2 keys, ordering, comparison fields, history columns |
| [custom.py](custom.py) | `quickstart_router` and node functions for both demos |
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