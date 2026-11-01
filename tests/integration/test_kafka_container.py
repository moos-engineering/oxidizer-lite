import time
import uuid

import pytest

pytestmark = pytest.mark.integration


def _wait_for_kafka_message(catalyst, topic_name: str, group_name: str, consumer_name: str):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        messages = catalyst.read_from_stream(topic_name, group_name, consumer_name, count=1, block=1000)
        if messages:
            return messages
    return []


def _run_kafka_smoke(bootstrap_servers: str):
    from oxidizer import Catalyst, KafkaConnection

    topic_name = f"oxidizer-integration-{uuid.uuid4().hex}"
    group_name = f"oxidizer-workers-{uuid.uuid4().hex}"

    producer_connection = KafkaConnection(
        bootstrap_servers=bootstrap_servers,
        create_consumer=False,
    )
    producer_catalyst = Catalyst(producer_connection)
    producer_catalyst.create_stream(topic_name)
    producer_catalyst.write_to_stream(topic_name, {"ok": True})

    consumer_connection = KafkaConnection(
        bootstrap_servers=bootstrap_servers,
        create_admin_client=False,
        create_producer=False,
        create_consumer=True,
        group_id=group_name,
        consumer_config={
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        },
    )
    consumer_catalyst = Catalyst(consumer_connection)

    messages = _wait_for_kafka_message(consumer_catalyst, topic_name, group_name, "worker-1")

    assert messages
    assert messages[0][1] == {"ok": True}
    consumer_catalyst.ack_message(topic_name, group_name, messages[0][0], consumer_name="worker-1")
    consumer_connection.consumer.close()


def test_confluent_kafka_container_smoke():
    pytest.importorskip("confluent_kafka")
    testcontainers_kafka = pytest.importorskip("testcontainers.kafka")

    with testcontainers_kafka.KafkaContainer() as container:
        _run_kafka_smoke(container.get_bootstrap_server())
