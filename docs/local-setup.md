# Local Container Setup

Run RustFS, Redis, and the Oxidizer API/controller/worker locally with Apple's `container` CLI and the Mock workflow. Run shell commands from the repository root, not the docs directory. This guide requires the source checkout; an installed package alone does not include the example scripts and Dockerfile used below.

These are development settings, not a production deployment. The example uses `rustfsadmin` credentials. Replace them consistently in the service, example configuration, and seed script before exposing services beyond your machine. Do not include real credentials in diagnostic output.

For an existing environment, inspect container status and health before starting or recreating anything. Do not reset data, flush Redis, delete volumes, or rerun seed operations during diagnosis without operator authorization. See [Workflow run troubleshooting](troubleshooting/workflow-runs.md) for read-only investigation.

## Prerequisites

- Apple silicon Mac with macOS 26 or later and Apple's `container` CLI installed.
- Available host ports: Redis `6379`, Oxidizer API `8022`, RustFS S3 `9000`, and RustFS console `9001`.
- A repository checkout with project dependencies installed in `.venv` for the host-side seed script.

Start the container service:

```bash
container system start
container system status
```

Configure Apple's embedded DNS in `~/.config/container/config.toml`:

```toml
[dns]
domain = "test"
```

If you changed that file, restart the service during an authorized maintenance window:

```bash
container system stop
container system start
```

Use Apple's default network for all services. The Mock configuration resolves `oxidizer-rustfs.test` and `oxidizer-redis.test` inside containers; host-side seed and health commands use published `localhost` ports. No host DNS alias is needed.

## Start Dependencies

For a new environment, create persistent volumes:

```bash
container volume create oxidizer-rustfs-data
container volume create oxidizer-redis-data
```

Start RustFS:

```bash
export RUSTFS_ACCESS_KEY=rustfsadmin
export RUSTFS_SECRET_KEY=rustfsadmin

container run --detach --name oxidizer-rustfs \
  --publish 9000:9000 \
  --publish 9001:9001 \
  --volume oxidizer-rustfs-data:/data \
  --env "RUSTFS_ACCESS_KEY=$RUSTFS_ACCESS_KEY" \
  --env "RUSTFS_SECRET_KEY=$RUSTFS_SECRET_KEY" \
  --env RUSTFS_ADDRESS=:9000 \
  --env RUSTFS_CONSOLE_ADDRESS=:9001 \
  --env RUSTFS_CONSOLE_ENABLE=true \
  rustfs/rustfs:latest /data
```

Start Redis with persistence and at least 4 GB for this load-test workload:

```bash
container run --detach --name oxidizer-redis \
  --memory 4G \
  --publish 6379:6379 \
  --volume oxidizer-redis-data:/data \
  redis:7-alpine redis-server --appendonly yes --dir /data
```

The default 1 GB can be insufficient when Mock streams accumulate or Redis forks for a background save. Streams are not automatically trimmed; each 1M-row Mock run added about 170 MB to its source stream in local tests. Plan retention and capacity before repeated runs. Memory pressure does not authorize flushing the database.

Verify both services:

```bash
curl --fail http://localhost:9000/health
container exec oxidizer-redis redis-cli ping
```

Expect a successful RustFS health response and Redis `PONG`. Named volumes do not require host ownership changes. A host bind mount instead requires ownership compatible with RustFS UID `10001`.

## Seed And Enable Tables

For a new environment, write the Mock formula, connections, and schemas:

```bash
PYTHONPATH=. .venv/bin/python examples/mock/seed.py
```

The seed script connects to `localhost:9000` and creates the `oxidizer-test` and `oxidizer-tables-test` buckets if needed. Review its writes before using it against existing data.

Enable the table bucket with a signed empty request, using the same credentials as the RustFS service:

```bash
curl --fail-with-body --silent --show-error \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" \
  --header "x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" \
  --request PUT \
  "http://localhost:9000/iceberg/v1/buckets/oxidizer-tables-test"
```

Expect `"enabled":true` and `"catalog-entry-present":true`. Inside containers, the Iceberg REST catalog is `http://oxidizer-rustfs.test:9000/iceberg`, the warehouse is `oxidizer-tables-test`, and the S3 endpoint is `http://oxidizer-rustfs.test:9000`.

## Build And Start Oxidizer

Build both image targets from the repository root. Rebuild after source changes;
the API target bundles local semantic documentation search while workers remain
free of embedding dependencies. See the [image guide](../docker/README.md).

```bash
container build \
  --file docker/oxidizer-lite.dockerfile \
  --tag oxidizer:latest \
  .
container build \
  --file docker/oxidizer-lite.dockerfile \
  --target api \
  --tag oxidizer:latest-api \
  .

CONFIG="$PWD/examples/mock/test.yml"
CUSTOM="$PWD/examples/mock/custom.py"
```

Start each role separately:

```bash
container run --detach --name mock-api \
  --publish 8022:8000 \
  --volume "$CONFIG:/oxidizer/runtime/configs/mock.yml:ro" \
  oxidizer:latest-api \
  python -m app.api --config /oxidizer/runtime/configs/mock.yml \
    --host 0.0.0.0 --port 8000 --log-level info

container run --detach --name mock-controller \
  --volume "$CONFIG:/oxidizer/runtime/configs/mock.yml:ro" \
  oxidizer:latest \
  python -m app.controller --config /oxidizer/runtime/configs/mock.yml

container run --detach --name mock-worker \
  --volume "$CONFIG:/oxidizer/runtime/configs/mock.yml:ro" \
  --volume "$CUSTOM:/oxidizer/runtime/scripts/custom.py:ro" \
  oxidizer:latest \
  python -m app.worker --config /oxidizer/runtime/configs/mock.yml --worker worker
```

The API and controller read Formula documents from RustFS. The worker also mounts the Mock router. Generator size, time unit, entity count, seed, revision, and optional end time are constants in `examples/mock/custom.py`; no `MOCK_*` environment variables are needed.

Apple `container run` does not accept Docker's `--restart` option. Existing stopped containers should be started explicitly, not recreated blindly.

## Worker Memory And CPU-Aware Arena Tuning

Optionally add this setting to the worker's `container run` command before the image name:

```bash
--env MALLOC_ARENA_MAX=2
```

`MALLOC_ARENA_MAX` limits glibc allocation arenas, which can reduce memory retained after tasks finish. Choose a trial value based on the CPUs allocated to the worker: one arena per CPU is a starting point, not a rule. The example uses `2`; lower caps may reduce memory retention but increase allocation contention, so compare throughput before adopting it. Set it before worker startup; it is not a memory limit and applies only to Linux/glibc workers.

## Verify A Run

Check service state and health:

```bash
container list --all
container logs mock-api
container logs mock-controller
container logs mock-worker
curl --fail http://localhost:8022/api/health
curl --fail http://localhost:8022/api/docs/status
```

Invoking the workflow writes data. Once that is intended, trigger the seeded formula:

```bash
curl --fail-with-body --silent --show-error \
  --request POST http://localhost:8022/api/invocations \
  --header 'Content-Type: application/json' \
  --data '{"formula_id":"mock_event_history"}'
```

Use the returned `run_id`:

```bash
curl --fail-with-body --silent --show-error \
  "http://localhost:8022/api/formulas/mock_event_history/runs/<run-id>"
```

Expect terminal success for all nodes and output in `analytics.mock_entity_history` in RustFS. The checked-in workflow consumes Redis in batches of 2,000. An accepted invocation alone is not proof of completion; inspect state, correlated logs, and destination readback. Follow [Workflow run troubleshooting](troubleshooting/workflow-runs.md) for failures, stalls, missing output, or memory growth.

## Stop And Resume

Stop roles without deleting data:

```bash
container stop mock-worker mock-controller mock-api
container stop oxidizer-redis oxidizer-rustfs
```

Resume dependencies before roles:

```bash
container system start
container start oxidizer-rustfs
container start oxidizer-redis
container start mock-api
container start mock-controller
container start mock-worker
```

Confirm dependency health before starting roles. Named volumes survive container deletion. Deleting a volume permanently removes its data; deleting RustFS objects alone can leave stale Iceberg catalog registrations. Do not delete either as an automatic recovery step.

For an explicitly authorized destructive reset, see [the root setup guide](../SETUP.md#stop-restart-and-reset). Redis `FLUSHDB` destroys keys, streams, consumer groups, and state in the selected database; `FLUSHALL` affects every database. Neither is a routine diagnostic action. After an approved reset, repeat seeding and table enablement as appropriate.

## References

- [Apple container CLI](https://github.com/apple/container)
- [Apple container networking](https://github.com/apple/container/blob/main/docs/networking.md)
- [Apple container volumes](https://github.com/apple/container/blob/main/docs/volumes.md)
- [RustFS container installation](https://docs.rustfs.com/en/installation/container/docker)
- [RustFS S3 Tables](https://docs.rustfs.com/en/administration/data/s3-tables)