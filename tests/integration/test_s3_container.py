import os
import time

import pytest

pytestmark = pytest.mark.integration


def _s3_client(endpoint_url: str, access_key: str, secret_key: str):
    boto3 = pytest.importorskip("boto3")
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _wait_for_s3(client):
    for _ in range(30):
        try:
            client.list_buckets()
            return
        except Exception:
            time.sleep(0.5)
    client.list_buckets()


def _run_s3_compatible_smoke(endpoint_url: str, access_key: str, secret_key: str):
    from oxidizer import S3Connection

    client = _s3_client(endpoint_url, access_key, secret_key)
    _wait_for_s3(client)

    connection = S3Connection(
        bucket="oxidizer-integration",
        prefix="formulae",
        endpoint_url=endpoint_url,
        access_key_id=access_key,
        secret_access_key=secret_key,
        addressing_style="path",
    )

    client.create_bucket(Bucket=connection.bucket)
    client.put_object(Bucket=connection.bucket, Key=connection.object_key("demo.yml"), Body=b"ok: true")
    response = client.get_object(Bucket=connection.bucket, Key=connection.object_key("demo.yml"))

    assert response["Body"].read() == b"ok: true"


def test_rustfs_s3_compatible_container_smoke():
    testcontainers_core = pytest.importorskip("testcontainers.core.container")

    image = os.getenv("OXIDIZER_RUSTFS_IMAGE", "rustfs/rustfs:latest")
    access_key = os.getenv("OXIDIZER_RUSTFS_ACCESS_KEY", "rustfsadmin")
    secret_key = os.getenv("OXIDIZER_RUSTFS_SECRET_KEY", "rustfsadmin")
    port = int(os.getenv("OXIDIZER_RUSTFS_PORT", "9000"))
    command = os.getenv("OXIDIZER_RUSTFS_COMMAND")

    container = (
        testcontainers_core.DockerContainer(image)
        .with_env(os.getenv("OXIDIZER_RUSTFS_ACCESS_KEY_ENV", "RUSTFS_ACCESS_KEY"), access_key)
        .with_env(os.getenv("OXIDIZER_RUSTFS_SECRET_KEY_ENV", "RUSTFS_SECRET_KEY"), secret_key)
        .with_env("RUSTFS_VOLUMES", "/data")
        .with_exposed_ports(port)
    )
    if command:
        container = container.with_command(command)

    with container:
        endpoint_url = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(port)}"

        _run_s3_compatible_smoke(endpoint_url, access_key, secret_key)
