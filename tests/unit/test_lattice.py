from oxidizer import KafkaConnection, OllamaConnection, RedisConnection, S3Connection, S3TablesConnection
import pytest


def test_redis_connection_accepts_injected_client():
    client = object()

    connection = RedisConnection(client=client)

    assert connection.client is client


def test_s3_connection_accepts_injected_client_without_session():
    client = object()

    connection = S3Connection(bucket="demo", client=client)

    assert connection.client is client
    assert connection.object_key("/formulas/demo.yml") == "formulas/demo.yml"


def test_s3_connection_applies_prefix_to_object_key():
    connection = S3Connection(bucket="demo", prefix="oxidizer", client=object())

    assert connection.object_key("/formulas/demo.yml") == "oxidizer/formulas/demo.yml"


def test_s3_tables_connection_accepts_injected_client_without_session():
    client = object()

    connection = S3TablesConnection(client=client)

    assert connection.client is client


def test_kafka_connection_accepts_injected_client_without_confluent_import():
    client = object()

    connection = KafkaConnection(client=client)

    assert connection.client is client
    assert connection.admin_client is None
    assert connection.producer is None
    assert connection.consumer is None


def test_kafka_created_consumer_disables_auto_commit(monkeypatch):
    import confluent_kafka

    configs = []
    monkeypatch.setattr(confluent_kafka, "Consumer", lambda config: configs.append(config) or object())
    KafkaConnection(
        bootstrap_servers="localhost:9092", group_id="workers",
        create_admin_client=False, create_producer=False, create_consumer=True,
    )
    assert configs[0]["enable.auto.commit"] is False


def test_kafka_topic_consumer_uses_effective_group_and_preserves_config(monkeypatch):
    import confluent_kafka

    configs = []
    monkeypatch.setattr(confluent_kafka, "Consumer", lambda config: configs.append(config) or object())
    connection = KafkaConnection(
        bootstrap_servers="localhost:9092", group_id="workers",
        consumer_config={"group.id": "configured", "auto.offset.reset": "earliest"},
        consumer=object(),
    )
    connection._create_consumer(group_id_suffix="data.events")
    assert configs[0]["group.id"] == "configured.data.events"
    assert configs[0]["auto.offset.reset"] == "earliest"
    assert configs[0]["enable.auto.commit"] is False
    assert connection.consumer_config["group.id"] == "configured"


@pytest.mark.parametrize("options", [{"consumer_config": {"enable.auto.commit": True}},
                                   {"enable.auto.commit": "true"}])
def test_kafka_created_consumer_rejects_auto_commit(options):
    with pytest.raises(ValueError, match="enable.auto.commit=false"):
        KafkaConnection(
            bootstrap_servers="localhost:9092", group_id="workers",
            create_admin_client=False, create_producer=False, create_consumer=True,
            **options,
        )


def test_ollama_connection_accepts_injected_clients_without_driver_import():
    client = object()
    async_client = object()

    connection = OllamaConnection(client=client, async_client=async_client)

    assert connection.client is client
    assert connection.async_client is async_client