"""Provision local quickstart buckets and publish the selected workflow and fixture."""

import argparse
import hashlib
from pathlib import Path
import time
from urllib.request import Request, urlopen

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError, EndpointConnectionError
import yaml

from parquet_generator import write_observations


ROOT = Path(__file__).parent


def _document(filename):
    path = (ROOT / filename).resolve()
    if not path.is_relative_to(ROOT.resolve()) or not path.is_file():
        raise ValueError(f"Document must exist inside the quickstart: {filename}")
    return yaml.safe_load(path.read_text())


def _enable_tables(settings, bucket):
    url = f"{settings['endpoint_url'].rstrip('/')}/iceberg/v1/buckets/{bucket}"
    request = AWSRequest(
        method="PUT", url=url, data=b"",
        headers={"x-amz-content-sha256": hashlib.sha256(b"").hexdigest()},
    )
    SigV4Auth(
        Credentials(settings["access_key_id"], settings["secret_access_key"]),
        "s3", settings["region_name"],
    ).add_auth(request)
    with urlopen(Request(url, data=b"", headers=dict(request.headers), method="PUT"), timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"Could not enable Iceberg warehouse: HTTP {response.status}")


def main(config_path=ROOT / "runtime.redis.yml"):
    config = yaml.safe_load(Path(config_path).read_text())
    settings = config["formula"]
    seed = config["quickstart_seed"]
    workflow = _document(seed["workflow_file"])
    documents = [workflow] + [
        _document(filename)
        for filename in (
            "connections/local-files.yml", "connections/local-lake.yml",
            "schemas/upsert-observations.yml", "schemas/observation-history.yml",
        )
    ]
    client = boto3.client(
        "s3", endpoint_url=settings["endpoint_url"],
        aws_access_key_id=settings["access_key_id"],
        aws_secret_access_key=settings["secret_access_key"],
        region_name=settings["region_name"],
        config=Config(
            signature_version="s3v4", s3={"addressing_style": "path"},
            connect_timeout=3, read_timeout=10, retries={"max_attempts": 0},
        ),
    )
    for attempt in range(60):
        try:
            client.list_buckets()
            break
        except EndpointConnectionError:
            if attempt == 59:
                raise
            time.sleep(1)
    for bucket in (settings["bucket"], seed["data_bucket"], seed["table_bucket"]):
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            client.create_bucket(Bucket=bucket)
    _enable_tables(settings, seed["table_bucket"])
    fixture = ROOT / "observations.parquet"
    if not fixture.is_file():
        print(f"Generated {write_observations(fixture):,} Parquet records", flush=True)
    prefix = settings.get("prefix", "").strip("/")
    folders = {"workflow": "formulas", "connection": "connections", "schema": "schemas"}
    for document in documents:
        key = "/".join(filter(None, (prefix, folders[document["type"]], f"{document['name']}.yml")))
        client.put_object(
            Bucket=settings["bucket"], Key=key,
            Body=yaml.safe_dump(document, sort_keys=False).encode(),
            ContentType="application/x-yaml",
        )
        print(f"Seeded {key}", flush=True)
    client.upload_file(str(fixture), seed["data_bucket"], seed["data_key"])
    print(f"Ready to invoke {workflow['name']}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "runtime.redis.yml")
    main(parser.parse_args().config)