"""Seed the one-node workflow and provide its worker function."""

import time
from pathlib import Path

from botocore.exceptions import ClientError, EndpointConnectionError

from app.utils.helpers import create_formula_connection, load_config
from oxidizer import Formula, Residue


def hello(data, context):
    return [{"message": "Hello from the authenticated quickstart"}]


def seed():
    root = Path(__file__).parent
    settings = load_config(str(root / "runtime.yml"))["formula"]
    workflow = load_config(str(root / "workflow.yml"))
    residue = Residue()
    connection = create_formula_connection(settings, residue)
    for attempt in range(60):
        try:
            connection.client.head_bucket(Bucket=settings["bucket"])
            break
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            connection.client.create_bucket(Bucket=settings["bucket"])
            break
        except EndpointConnectionError:
            if attempt == 59:
                raise
            time.sleep(1)
    Formula(connection, residue=residue).create_formula_config(workflow["name"], workflow)
    print(f"Seeded {workflow['name']}", flush=True)


if __name__ == "__main__":
    seed()
