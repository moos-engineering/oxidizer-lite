import json

import pytest

from oxidizer import Catalyst, RedisConnection, Residue

pytestmark = pytest.mark.contract


class ResponseError(Exception):
    pass


class FakeRedisExceptions:
    ResponseError = ResponseError


class FakeRedisStreamClient:
    exceptions = FakeRedisExceptions

    def __init__(self):
        self.streams = {}
        self.groups = set()
        self.acked = []
        self.deleted = []
        self.expirations = {}

    def exists(self, topic_name):
        return topic_name in self.streams

    def xadd(self, topic_name, data):
        stream = self.streams.setdefault(topic_name, [])
        message_id = f"{len(stream) + 1}-0"
        stream.append((message_id, data))
        return message_id

    def pipeline(self, transaction=False):
        client = self

        class Pipeline:
            def __init__(self):
                self.entries = []

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def xadd(self, topic_name, data):
                self.entries.append((topic_name, data))

            def execute(self):
                return [client.xadd(topic, data) for topic, data in self.entries]

        return Pipeline()

    def xtrim(self, topic_name, maxlen):
        self.streams[topic_name] = self.streams.get(topic_name, [])[-maxlen:] if maxlen else []
        return 0

    def xgroup_create(self, topic_name, group_name, id="0", mkstream=False):
        if mkstream:
            self.streams.setdefault(topic_name, [])
        group_key = (topic_name, group_name)
        if group_key in self.groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        self.groups.add(group_key)

    def xreadgroup(self, group_name, consumer_name, streams, count=1, block=1000):
        topic_name = next(iter(streams))
        messages = self.streams.get(topic_name, [])[:count]
        return [(topic_name, messages)] if messages else []

    def xrevrange(self, topic_name, count=10):
        return list(reversed(self.streams.get(topic_name, [])))[:count]

    def xack(self, topic_name, group_name, message_id):
        self.acked.append((topic_name, group_name, message_id))
        return 1

    def xdel(self, topic_name, message_id):
        self.deleted.append((topic_name, message_id))
        return 1

    def expire(self, topic_name, ttl):
        self.expirations[topic_name] = ttl
        return True


@pytest.fixture
def redis_catalyst():
    client = FakeRedisStreamClient()
    connection = RedisConnection(client=client)
    return Catalyst(connection), client


@pytest.mark.parametrize("message", ["123", "true", "null", '{"id": 1}', b"123", 123, True, None, 1.5, (1,), {1}])
@pytest.mark.parametrize("direct_adapter", [False, True])
def test_redis_rejects_scalar_payloads_before_any_writes(redis_catalyst, message, direct_adapter):
    catalyst, client = redis_catalyst
    writer = catalyst.catalyst if direct_adapter else catalyst
    with pytest.raises(TypeError, match="dictionary or list"):
        writer.write_to_stream("demo", message)
    with pytest.raises(TypeError, match="dictionary or list"):
        writer.write_many_to_stream("demo", [{"id": n} for n in range(1001)] + [message])
    assert client.streams == {}


@pytest.mark.parametrize("message", [{}, [], {"values": ["123", "true", "null"]}, ["123", "true", "null"]])
def test_redis_structured_payload_round_trip(redis_catalyst, message):
    catalyst, _ = redis_catalyst
    first = catalyst.write_to_stream("demo", message)
    second = catalyst.write_many_to_stream("demo", [message])[0]
    assert catalyst.read_from_stream("demo", "workers", "worker-1", count=2) == [
        (first, {"message": message}), (second, {"message": message}),
    ]


def test_redis_batch_fallback_rejects_all_payload_types_before_writes(redis_catalyst, monkeypatch):
    catalyst, client = redis_catalyst
    monkeypatch.setattr(catalyst.catalyst, "write_many_to_stream", None)
    with pytest.raises(TypeError, match="dictionary or list"):
        catalyst.write_many_to_stream("demo", [{"id": 1}, "123"])
    assert client.streams == {}
    assert catalyst.write_many_to_stream("demo", [{"id": 1}, ["true"]]) == ["1-0", "2-0"]


def test_redis_catalyst_creates_stream(redis_catalyst):
    catalyst, client = redis_catalyst

    catalyst.create_stream("demo")

    assert client.streams["demo"] == []


def test_redis_catalyst_creates_consumer_group_idempotently(redis_catalyst):
    catalyst, client = redis_catalyst

    catalyst.create_consumer_group("demo", "workers")
    catalyst.create_consumer_group("demo", "workers")

    assert ("demo", "workers") in client.groups


def test_redis_catalyst_writes_reads_acks_deletes_and_expires(redis_catalyst):
    catalyst, client = redis_catalyst

    message_id = catalyst.write_to_stream("demo", {"event": "created"})


    messages = catalyst.read_from_stream("demo", "workers", "worker-1")
    catalyst.ack_message("demo", "workers", message_id, consumer_name="worker-1")
    catalyst.delete_message("demo", message_id, group_name="workers", consumer_name="worker-1")
    catalyst.set_ttl("demo", message_id, 60)

    assert messages == [(message_id, {"message": {"event": "created"}})]
    assert json.loads(client.streams["demo"][0][1]["message"]) == {"event": "created"}
    assert client.acked == [("demo", "workers", message_id)]
    assert client.deleted == [("demo", message_id)]
    assert client.expirations == {"demo": 60}


def test_redis_catalyst_reads_stream_range_without_consumer_group_side_effects(redis_catalyst):
    catalyst, client = redis_catalyst
    catalyst.write_to_stream("oxidizer:logs", {
        "formula_id": "daily-users", "run_id": "run-1", "event": "first",
    })
    catalyst.write_to_stream("oxidizer:logs", {
        "formula_id": "daily-users", "run_id": "run-2", "event": "second",
    })

    entries = catalyst.read_stream_range("oxidizer:logs", count=1)

    assert entries == [("2-0", {"message": {
        "formula_id": "daily-users", "run_id": "run-2", "event": "second",
    }})]
    assert client.groups == set()
    assert client.acked == []


def test_residue_dump_can_be_published_through_catalyst_without_recapturing_write_logs():
    client = FakeRedisStreamClient()
    trace = Residue(capture_events=True)
    catalyst = Catalyst(RedisConnection(client=client, residue=trace), residue=trace)
    trace.emit(trace.ash.ERROR, "worker failed", run_id="run-123", node_id="node-1")
    events = trace.dump()

    message_id = catalyst.write_to_stream("diagnostic-logs", events[0], emit_log=False)

    assert message_id == "1-0"
    assert len(client.streams["diagnostic-logs"]) == 1
    assert json.loads(client.streams["diagnostic-logs"][0][1]["message"]) == events[0]
    assert trace.dump() == []