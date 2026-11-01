import json

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

    def flush(self, timeout=None):
        self.flushed = True
        return 0

    def poll(self, timeout):
        return 0


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

    def subscribe(self, topics, **callbacks):
        from confluent_kafka import TopicPartition

        self.subscriptions.append(topics)
        self.callbacks = callbacks
        callbacks["on_assign"](self, [TopicPartition(topic, 0) for topic in topics])

    def consume(self, num_messages=1, timeout=1.0):
        return [FakeKafkaRecord(b'{"event": "created"}')]

    def commit(self, offsets=None, asynchronous=False):
        self.committed.append((offsets, asynchronous))


class DeliveredKafkaConsumer(FakeKafkaConsumer):
    def __init__(self, batches):
        super().__init__()
        self.batches = list(batches)
        self.fail_commit = False

    def consume(self, **kwargs):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets=None, asynchronous=False):
        if self.fail_commit:
            raise RuntimeError("commit unavailable")
        return super().commit(offsets=offsets, asynchronous=asynchronous)


def kafka_reader(batches):
    consumer = DeliveredKafkaConsumer(batches)
    catalyst = Catalyst(KafkaConnection(
        admin_client=FakeKafkaAdminClient(), producer=FakeKafkaProducer(), consumer=consumer,
    ))
    return catalyst, consumer


def test_kafka_read_waits_for_assignment_instead_of_returning_premature_empty_stream():
    from confluent_kafka import TopicPartition

    class AssigningConsumer(FakeKafkaConsumer):
        def subscribe(self, topics, **callbacks):
            self.callbacks = callbacks
            self.calls = 0

        def consume(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return []
            self.callbacks["on_assign"](self, [TopicPartition("events", 0)])
            return [FakeKafkaRecord(b'{}', offset=9)]

    consumer = AssigningConsumer()
    catalyst = Catalyst(KafkaConnection(consumer=consumer))
    assert catalyst.read_from_stream("events", "workers", "worker") == [("0:9", {})]
    assert consumer.calls == 2
    catalyst.ack_message("events", "workers", "0:9")


def test_kafka_assignment_timeout_is_explicit(monkeypatch):
    class UnassignedConsumer(FakeKafkaConsumer):
        def subscribe(self, topics, **callbacks):
            pass

        def consume(self, **kwargs):
            return []

    times = iter([0, 31])
    monkeypatch.setattr("oxidizer.core.catalyst.time.monotonic", lambda: next(times))
    catalyst = Catalyst(KafkaConnection(consumer=UnassignedConsumer()))
    with pytest.raises(TimeoutError, match="assignment timed out"):
        catalyst.read_from_stream("events", "workers", "worker")


def test_kafka_out_of_order_ack_waits_for_earlier_delivered_record_across_reads():
    catalyst, consumer = kafka_reader([
        [FakeKafkaRecord(b'{}', offset=9)],
        [FakeKafkaRecord(b'{}', offset=10)],
    ])
    catalyst.read_from_stream("events", "workers", "worker")
    catalyst.read_from_stream("events", "workers", "worker")
    assert consumer.subscriptions == [["events"]]
    assert catalyst.ack_messages("events", "workers", ["0:10"]) == 1
    assert consumer.committed == []
    catalyst.ack_message("events", "workers", "0:9")
    offsets, asynchronous = consumer.committed[0]
    assert [(item.partition, item.offset) for item in offsets] == [(0, 11)]
    assert asynchronous is False


def test_kafka_processed_prefix_allows_real_offset_gaps_and_independent_partitions():
    catalyst, consumer = kafka_reader([[
        FakeKafkaRecord(b'{}', offset=9),
        FakeKafkaRecord(b'{}', offset=12),
        FakeKafkaRecord(b'{}', partition=1, offset=30),
        FakeKafkaRecord(b'{}', offset=15),
    ]])
    catalyst.read_from_stream("events", "workers", "worker", count=4)
    catalyst.ack_messages("events", "workers", ["0:15", "0:9", "1:30", "0:9"])
    assert [(item.partition, item.offset) for item in consumer.committed[0][0]] == [(0, 10), (1, 31)]
    catalyst.ack_message("events", "workers", "0:12")
    assert [(item.partition, item.offset) for item in consumer.committed[1][0]] == [(0, 16)]


def test_kafka_unknown_ack_rejects_whole_batch_and_commit_failure_is_retryable():
    catalyst, consumer = kafka_reader([[FakeKafkaRecord(b'{}', offset=9), FakeKafkaRecord(b'{}', offset=10)]])
    catalyst.read_from_stream("events", "workers", "worker", count=2)
    with pytest.raises(ValueError, match="record delivered"):
        catalyst.ack_messages("events", "workers", ["0:9", "0:999"])
    catalyst.ack_message("events", "workers", "0:10")
    assert consumer.committed == []
    consumer.fail_commit = True
    with pytest.raises(RuntimeError, match="commit unavailable"):
        catalyst.ack_message("events", "workers", "0:9")
    consumer.fail_commit = False
    catalyst.ack_message("events", "workers", "0:9")
    assert consumer.committed[0][0][0].offset == 11


@pytest.mark.parametrize("callback", ["on_assign", "on_revoke", "on_lost"])
def test_kafka_rebalance_discards_stale_acknowledgements(callback):
    from confluent_kafka import TopicPartition

    catalyst, consumer = kafka_reader([[FakeKafkaRecord(b'{}', offset=9), FakeKafkaRecord(b'{}', offset=10)]])
    catalyst.read_from_stream("events", "workers", "worker", count=2)
    catalyst.ack_message("events", "workers", "0:10")
    consumer.callbacks[callback](consumer, [TopicPartition("events", 0)])
    with pytest.raises(ValueError, match="record delivered"):
        catalyst.ack_message("events", "workers", "0:9")
    assert consumer.committed == []


def test_kafka_control_and_data_consumers_keep_independent_pending_records(monkeypatch):
    catalyst, consumer = kafka_reader([[FakeKafkaRecord(b'{}', offset=9)]])
    data_consumer = DeliveredKafkaConsumer([[FakeKafkaRecord(b'{}', offset=10)]])
    catalyst.connection.bootstrap_servers = "broker:9092"
    factory_calls = []
    def create_consumer(**kwargs):
        factory_calls.append(kwargs)
        return data_consumer
    monkeypatch.setattr(catalyst.connection, "_create_consumer", create_consumer)
    catalyst.read_from_stream("events", "workers", "worker")
    catalyst.read_from_stream("other", "workers", "worker")
    catalyst.ack_message("other", "workers", "0:10")
    catalyst.ack_message("events", "workers", "0:9")
    assert consumer.committed[0][0][0].topic == "events"
    assert consumer.committed[0][0][0].offset == 10
    assert data_consumer.committed[0][0][0].topic == "other"
    assert data_consumer.committed[0][0][0].offset == 11
    assert consumer.subscriptions == [["events"]]
    assert data_consumer.subscriptions == [["other"]]
    assert factory_calls == [{"group_id_suffix": "other"}]


def test_kafka_partition_commit_error_keeps_prefix_retryable(monkeypatch):
    from types import SimpleNamespace

    catalyst, consumer = kafka_reader([[FakeKafkaRecord(b'{}', offset=9)]])
    catalyst.read_from_stream("events", "workers", "worker")
    with monkeypatch.context() as patch:
        patch.setattr(consumer, "commit", lambda **kwargs: [SimpleNamespace(error="broker failure")])
        with pytest.raises(RuntimeError, match="failed to commit"):
            catalyst.ack_message("events", "workers", "0:9")
    catalyst.ack_message("events", "workers", "0:9")
    assert consumer.committed[0][0][0].offset == 10


def test_kafka_decode_failure_does_not_allow_commit_past_failed_record():
    catalyst, consumer = kafka_reader([[
        FakeKafkaRecord(b'\xff', offset=9),
        FakeKafkaRecord(b'{}', offset=10),
    ]])
    with pytest.raises(UnicodeDecodeError):
        catalyst.read_from_stream("events", "workers", "worker", count=2)
    catalyst.ack_message("events", "workers", "0:10")
    assert consumer.committed == []


@pytest.mark.parametrize("message", ["123", "true", "null", '{"id": 1}', b"123", 123, True, None, 1.5, (1,), {1}])
@pytest.mark.parametrize("direct_adapter", [False, True])
def test_kafka_rejects_scalar_payloads_before_any_writes(message, direct_adapter):
    producer = FakeKafkaProducer()
    catalyst = Catalyst(KafkaConnection(
        admin_client=FakeKafkaAdminClient(), producer=producer, consumer=FakeKafkaConsumer(),
    ))
    writer = catalyst.catalyst if direct_adapter else catalyst
    with pytest.raises(TypeError, match="dictionary or list"):
        writer.write_to_stream("events", message)
    with pytest.raises(TypeError, match="dictionary or list"):
        writer.write_many_to_stream("events", [{"id": n} for n in range(5001)] + [message])
    assert producer.messages == []
    assert producer.flushed is False


@pytest.mark.parametrize("message", [{}, [], {"values": ["123", "true", "null"]}, ["123", "true", "null"]])
def test_kafka_structured_payload_round_trip(message):
    producer = FakeKafkaProducer()

    class Consumer(FakeKafkaConsumer):
        def consume(self, num_messages=1, timeout=1.0):
            return [
                FakeKafkaRecord(value.encode(), offset=index)
                for index, (_, value) in enumerate(producer.messages[:num_messages])
            ]

    catalyst = Catalyst(KafkaConnection(
        admin_client=FakeKafkaAdminClient(), producer=producer, consumer=Consumer(),
    ))
    catalyst.write_to_stream("events", message)
    assert catalyst.write_many_to_stream("events", [message]) == [None]
    assert [json.loads(value) for _, value in producer.messages] == [message, message]
    assert catalyst.read_from_stream("events", "workers", "worker-1", count=2) == [
        ("0:0", message), ("0:1", message),
    ]


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