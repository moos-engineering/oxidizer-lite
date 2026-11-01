import pytest

from oxidizer import Catalyst, KafkaConnection

pytestmark = pytest.mark.contract


class FakeKafkaFuture:
    def __init__(self, error=None):
        self.error = error

    def result(self):
        if self.error:
            raise self.error
        return None


class FakeKafkaAdminClient:
    def __init__(self):
        self.topics = set()
        self.create_calls = 0

    def create_topics(self, topics):
        self.create_calls += 1
        for topic in topics:
            self.topics.add(topic.topic)
        return {topic.topic: FakeKafkaFuture() for topic in topics}


class FakeKafkaProducer:
    def __init__(self):
        self.messages = []
        self.flushed = False

    def produce(self, topic_name, value):
        self.messages.append((topic_name, value))

    def flush(self):
        self.flushed = True


class FakeKafkaRecord:
    def __init__(self, value, partition=0, offset=1):
        self._value = value
        self._partition = partition
        self._offset = offset

    def error(self):
        return None

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeKafkaConsumer:
    def __init__(self):
        self.subscriptions = []
        self.committed = []

    def subscribe(self, topics):
        self.subscriptions.append(topics)

    def consume(self, num_messages=1, timeout=1.0):
        return [FakeKafkaRecord(b'{"event": "created"}')]

    def commit(self, offsets=None, asynchronous=False):
        self.committed.append((offsets, asynchronous))


def test_kafka_catalyst_uses_confluent_admin_producer_and_consumer_contracts():
    admin_client = FakeKafkaAdminClient()
    producer = FakeKafkaProducer()
    consumer = FakeKafkaConsumer()
    connection = KafkaConnection(admin_client=admin_client, producer=producer, consumer=consumer)
    catalyst = Catalyst(connection)

    catalyst.create_stream("events")
    catalyst.create_consumer_group("events", "workers")
    catalyst.write_to_stream("events", {"event": "created"})
    messages = catalyst.read_from_stream("events", "workers", "worker-1")
    catalyst.ack_message("events", "workers", "0:1", consumer_name="worker-1")
    assert admin_client.topics == {"events"}
    assert producer.messages == [("events", '{"event": "created"}')]
    assert producer.flushed is True
    assert consumer.subscriptions == [["events"]]
    assert messages == [("0:1", {"event": "created"})]
    assert [(offset.topic, offset.partition, offset.offset) for offset in consumer.committed[0][0]] == [("events", 0, 2)]
    assert consumer.committed[0][1] is False


def test_kafka_catalyst_maps_redis_style_control_topic_names():
    admin_client = FakeKafkaAdminClient()
    producer = FakeKafkaProducer()
    consumer = FakeKafkaConsumer()
    catalyst = Catalyst(KafkaConnection(admin_client=admin_client, producer=producer, consumer=consumer))

    catalyst.create_stream("oxidizer:reagent")
    catalyst.write_to_stream("oxidizer:reagent", {"task": "started"})
    catalyst.read_from_stream("oxidizer:reagent", "workers", "worker-1")

    assert admin_client.topics == {"oxidizer.reagent"}
    assert producer.messages == [("oxidizer.reagent", '{"task": "started"}')]
    assert consumer.subscriptions == [["oxidizer.reagent"]]


def test_kafka_catalyst_prepares_topic_when_creating_consumer_group():
    admin_client = FakeKafkaAdminClient()
    catalyst = Catalyst(KafkaConnection(
        admin_client=admin_client,
        producer=FakeKafkaProducer(),
        consumer=FakeKafkaConsumer(),
    ))

    catalyst.create_consumer_group("oxidizer:invocations", "controllers")

    assert admin_client.topics == {"oxidizer.invocations"}


def test_kafka_catalyst_reuses_existing_stream():
    from confluent_kafka import KafkaError, KafkaException

    class ExistingTopicAdminClient(FakeKafkaAdminClient):
        def create_topics(self, topics):
            self.create_calls += 1
            return {topic.topic: FakeKafkaFuture(KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)))
                    for topic in topics}

    admin_client = ExistingTopicAdminClient()
    catalyst = Catalyst(KafkaConnection(
        admin_client=admin_client,
        producer=FakeKafkaProducer(),
        consumer=FakeKafkaConsumer(),
    ))

    assert catalyst.create_stream("events") is None
    assert admin_client.create_calls == 1


def test_kafka_catalyst_delete_and_ttl_are_passive_noops():
    connection = KafkaConnection(
        admin_client=FakeKafkaAdminClient(),
        producer=FakeKafkaProducer(),
        consumer=FakeKafkaConsumer(),
    )
    catalyst = Catalyst(connection)

    assert catalyst.delete_message("events", "1", group_name="workers", consumer_name="worker-1") is None
    assert catalyst.set_ttl("events", "1", 60) is None


def test_kafka_catalyst_rejects_redis_historical_range_read():
    import pytest

    connection = KafkaConnection(
        admin_client=FakeKafkaAdminClient(),
        producer=FakeKafkaProducer(),
        consumer=FakeKafkaConsumer(),
    )
    catalyst = Catalyst(connection)
    with pytest.raises(NotImplementedError, match="only supported by Redis"):
        catalyst.read_stream_range("oxidizer:logs")