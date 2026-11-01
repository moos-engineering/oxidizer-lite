from oxidizer import KafkaConnection, OllamaConnection, RedisConnection, S3Connection, S3TablesConnection


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


def test_ollama_connection_accepts_injected_clients_without_driver_import():
    client = object()
    async_client = object()

    connection = OllamaConnection(client=client, async_client=async_client)

    assert connection.client is client
    assert connection.async_client is async_client