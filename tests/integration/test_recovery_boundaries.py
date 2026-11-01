"""Live worker crash test for the Iceberg-commit/checkpoint boundary.

Service endpoints and testcontainers settings are read from the adjacent YAML
file. External mode requires an isolated Redis database and an Iceberg-enabled
table bucket. The test leaves a uniquely named namespace and table there; use a
disposable bucket if those artifacts are not wanted.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.external_services]
CONFIG_PATH = Path(__file__).with_name("recovery-boundaries.yml")


@contextmanager
def _services():
    config = yaml.safe_load(CONFIG_PATH.read_text())
    credentials = config["credentials"]
    access_key = credentials["access_key_id"]
    secret_key = credentials["secret_access_key"]

    if config["mode"] == "external":
        external = config["external"]
        yield (
            external["redis_url"],
            external["rustfs_endpoint"].rstrip("/"),
            external["table_bucket"],
            access_key,
            secret_key,
            False,
        )
        return
    if config["mode"] != "testcontainers":
        raise ValueError("recovery-boundaries.yml mode must be external or testcontainers")

    testcontainers_core = pytest.importorskip("testcontainers.core.container")
    testcontainers_redis = pytest.importorskip("testcontainers.community.redis")
    testcontainers_config = config["testcontainers"]
    port = int(testcontainers_config["rustfs_port"])

    with ExitStack() as stack:
        redis_container = stack.enter_context(
            testcontainers_redis.RedisContainer(testcontainers_config["redis_image"])
        )
        rustfs_container = testcontainers_core.DockerContainer(testcontainers_config["rustfs_image"])
        rustfs_container = (
            rustfs_container
            .with_env(testcontainers_config["access_key_env"], access_key)
            .with_env(testcontainers_config["secret_key_env"], secret_key)
            .with_env("RUSTFS_VOLUMES", "/data")
            .with_exposed_ports(port)
        )
        if testcontainers_config.get("rustfs_command"):
            rustfs_container = rustfs_container.with_command(testcontainers_config["rustfs_command"])
        rustfs_container = stack.enter_context(rustfs_container)

        redis_url = (
            f"redis://{redis_container.get_container_host_ip()}"
            f":{redis_container.get_exposed_port(6379)}/0"
        )
        rustfs_endpoint = (
            f"http://{rustfs_container.get_container_host_ip()}"
            f":{rustfs_container.get_exposed_port(port)}"
        )
        table_bucket = f"{testcontainers_config['table_bucket_prefix']}-{uuid4().hex[:12]}"
        yield redis_url, rustfs_endpoint, table_bucket, access_key, secret_key, True


def _s3_client(endpoint: str, access_key: str, secret_key: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _wait_for_s3(client) -> None:
    for _ in range(60):
        try:
            client.list_buckets()
            return
        except Exception:
            time.sleep(0.5)
    client.list_buckets()


def _enable_iceberg_bucket(endpoint: str, bucket: str, access_key: str, secret_key: str) -> None:
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    url = f"{endpoint}/iceberg/v1/buckets/{bucket}"
    request = AWSRequest(
        method="PUT",
        url=url,
        data=b"",
        headers={"x-amz-content-sha256": hashlib.sha256(b"").hexdigest()},
    )
    SigV4Auth(Credentials(access_key, secret_key), "s3", "us-east-1").add_auth(request)
    prepared = request.prepare()
    signed_request = Request(
        prepared.url,
        data=b"",
        headers={str(key): str(value) for key, value in prepared.headers.items()},
        method="PUT",
    )
    with urlopen(signed_request, timeout=30) as response:
        if response.status >= 300:
            raise RuntimeError(f"Iceberg table bucket enable failed: HTTP {response.status}")


def _iceberg_connection(endpoint: str, bucket: str, access_key: str, secret_key: str):
    from oxidizer import IcebergRestConnection, S3Connection

    storage = S3Connection(
        bucket=bucket,
        lazy_client=True,
        endpoint_url=endpoint,
        access_key_id=access_key,
        secret_access_key=secret_key,
        region_name="us-east-1",
        addressing_style="path",
    )
    return IcebergRestConnection(
        storage,
        warehouse=bucket,
        catalog_endpoint=f"{endpoint}/iceberg",
    )


def _row_count(endpoint: str, bucket: str, namespace: str, table: str, access_key: str, secret_key: str) -> int:
    from oxidizer import Crucible

    crucible = Crucible(_iceberg_connection(endpoint, bucket, access_key, secret_key))
    try:
        result = crucible.query_sql(
            bucket,
            "local_catalog",
            f'SELECT COUNT(*) AS row_count FROM local_catalog."{namespace}"."{table}"',
            max_rows=1,
        )
        return result["rows"][0]["row_count"]
    finally:
        crucible.close()


def _task_payload(
    run_id: str,
    task_stream: str,
    endpoint: str,
    bucket: str,
    namespace: str,
    table: str,
    access_key: str,
    secret_key: str,
) -> dict:
    formula_id = "recovery-boundary"
    node_id = f"{formula_id}.append"
    schema_name = "append_events"
    schema_document = {
        "templateVersion": 1,
        "name": schema_name,
        "type": "schema",
        "schema": {
            "version": 1,
            "columns": [
                {"name": "id", "type": "int64", "nullable": False},
                {"name": "value", "type": "string", "nullable": False},
            ],
            "write": {"operation": "append"},
        },
    }
    return {
        "type": "start_node",
        "formula_id": formula_id,
        "run_id": run_id,
        "node_id": node_id,
        "layer_id": f"{formula_id}.layer",
        "checkpoint_metadata": {
            "node_id": node_id,
            "batch_index": 0,
            "batch_cursors": {},
            "batch_final": {},
            "records_processed": 0,
            "total_records_so_far": 0,
            "is_final": False,
        },
        "node": {
            "layer_id": f"{formula_id}.layer",
            "layer_name": "layer",
            "node_name": "append",
            "inputs": [],
            "outputs": {
                "methods": [{
                    "method": "s3_tables",
                    "connection": "lake",
                    "table_bucket": bucket,
                    "catalog": "local_catalog",
                    "namespace": namespace,
                    "table": table,
                    "schema": schema_name,
                    "schema_document": schema_document,
                }]
            },
        },
        "connections": {
            "lake": {
                "connection": {
                    "type": "s3_tables",
                    "provider": "rustfs",
                    "endpoint_url": endpoint,
                    "catalog_endpoint": f"{endpoint}/iceberg",
                    "aws_s3_table_bucket": bucket,
                    "access_key_id": access_key,
                    "secret_access_key": secret_key,
                    "region_name": "us-east-1",
                    "addressing_style": "path",
                }
            }
        },
    }


def _run_reagent_task(
    redis_url: str,
    task_stream: str,
    controller_stream: str,
    output_records: list[dict],
    crash_gate=None,
    crash_boundary: str | None = None,
) -> None:
    from oxidizer import Catalyst, Reagent, RedisConnection

    catalyst = Catalyst(RedisConnection(url=redis_url))
    reagent = Reagent(catalyst, auto_react=False)
    reagent.oxidizer_controller_stream = controller_stream
    if crash_boundary == "before_checkpoint":
        publish_checkpoint = reagent.outgoing_oxidizer_checkpoint

        def pause_before_checkpoint(*args, **kwargs):
            crash_gate.send("iceberg_write_complete")
            crash_gate.recv()
            return publish_checkpoint(*args, **kwargs)

        reagent.outgoing_oxidizer_checkpoint = pause_before_checkpoint
    elif crash_boundary == "after_checkpoint_publish":
        publish_update = reagent.outgoing_oxidizer_update

        def pause_after_checkpoint_publish(update_type, *args, **kwargs):
            update = publish_update(update_type, *args, **kwargs)
            if update_type == "checkpoint":
                crash_gate.send("checkpoint_published")
                crash_gate.recv()
            return update

        reagent.outgoing_oxidizer_update = pause_after_checkpoint_publish
    elif crash_boundary is not None:
        raise ValueError(f"Unsupported crash boundary: {crash_boundary}")

    @reagent.react(dedicated_stream=task_stream)
    def process(_data, _context):
        return output_records

    process()


@pytest.mark.parametrize("crash_boundary", ["before_checkpoint", "after_checkpoint_publish"])
def test_worker_crash_around_checkpoint_ack_boundary(crash_boundary):
    """A real process death before acknowledgment leaves work pending and append replay duplicates."""
    multiprocessing_context = multiprocessing.get_context("spawn")
    redis_package = pytest.importorskip("redis")
    from oxidizer import Catalyst, RedisConnection

    with _services() as (redis_url, endpoint, table_bucket, access_key, secret_key, create_bucket):
        redis_client = redis_package.Redis.from_url(redis_url, decode_responses=True)
        s3 = _s3_client(endpoint, access_key, secret_key)
        _wait_for_s3(s3)
        if create_bucket:
            s3.create_bucket(Bucket=table_bucket)
            _enable_iceberg_bucket(endpoint, table_bucket, access_key, secret_key)
        else:
            s3.head_bucket(Bucket=table_bucket)

        unique_id = uuid4().hex[:12]
        task_stream = f"recovery:tasks:{unique_id}"
        controller_stream = f"{task_stream}:updates"
        group_name = "reagent-consumer-group"
        run_id = f"run-{unique_id}"
        namespace = f"recovery_{unique_id}"
        table = f"append_{unique_id}"
        output_records = [{"id": 1, "value": "committed"}]
        task_payload = _task_payload(
            run_id, task_stream, endpoint, table_bucket, namespace, table, access_key, secret_key
        )
        catalyst = Catalyst(RedisConnection(client=redis_client))
        catalyst.create_consumer_group(task_stream, group_name)

        process = None
        second_process = None
        parent_gate = None
        try:
            first_task_id = catalyst.write_to_stream(task_stream, task_payload)
            parent_gate, child_gate = multiprocessing_context.Pipe()
            process = multiprocessing_context.Process(
                target=_run_reagent_task,
                args=(redis_url, task_stream, controller_stream, output_records, child_gate, crash_boundary),
            )
            process.start()
            child_gate.close()
            if not parent_gate.poll(120):
                process.terminate()
                process.join(10)
                pytest.fail("worker did not finish its Iceberg write before the crash boundary")
            expected_gate_message = (
                "iceberg_write_complete"
                if crash_boundary == "before_checkpoint"
                else "checkpoint_published"
            )
            assert parent_gate.recv() == expected_gate_message
            process.terminate()
            process.join(10)
            assert not process.is_alive()

            assert _row_count(endpoint, table_bucket, namespace, table, access_key, secret_key) == 1
            pending = redis_client.xpending_range(task_stream, group_name, min="-", max="+", count=10)
            assert {entry["message_id"] for entry in pending} == {first_task_id}
            updates = [
                json.loads(fields["message"])
                for _, fields in redis_client.xrange(controller_stream)
                if "message" in fields
            ]
            assert any(update["type"] == "started" and update["run_id"] == run_id for update in updates)
            has_checkpoint = any(update["type"] == "checkpoint" and update["run_id"] == run_id for update in updates)
            assert has_checkpoint is (crash_boundary == "after_checkpoint_publish")

            second_task_id = catalyst.write_to_stream(task_stream, task_payload)
            second_process = multiprocessing_context.Process(
                target=_run_reagent_task,
                args=(redis_url, task_stream, controller_stream, output_records),
            )
            second_process.start()
            second_process.join(120)
            if second_process.is_alive():
                second_process.terminate()
                second_process.join(10)
                pytest.fail("replayed task did not finish within 120 seconds")
            assert second_process.exitcode == 0

            assert _row_count(endpoint, table_bucket, namespace, table, access_key, secret_key) == 2
            pending = redis_client.xpending_range(task_stream, group_name, min="-", max="+", count=10)
            assert {entry["message_id"] for entry in pending} == {first_task_id}
            updates = [
                json.loads(fields["message"])
                for _, fields in redis_client.xrange(controller_stream)
                if "message" in fields
            ]
            assert any(update["type"] == "checkpoint" and update["run_id"] == run_id for update in updates)
            assert second_task_id not in {entry["message_id"] for entry in pending}
        finally:
            for worker_process in (process, second_process):
                if worker_process is not None and worker_process.is_alive():
                    worker_process.terminate()
                    worker_process.join(10)
            if parent_gate is not None:
                parent_gate.close()
            redis_client.delete(task_stream, controller_stream)
            redis_client.close()