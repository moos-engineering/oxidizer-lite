import json

from oxidizer import RedisStreamLogSink, Residue


class FakeRedisClient:
    def __init__(self):
        self.entries = []
        self.expirations = {}

    def xadd(self, stream_name, fields, maxlen=None, approximate=True):
        self.entries.append((stream_name, fields, maxlen, approximate))
        return "1-0"

    def expire(self, stream_name, ttl):
        self.expirations[stream_name] = ttl


class FakeRedisConnection:
    def __init__(self, client):
        self.client = client


def test_redis_stream_log_sink_writes_to_existing_connection():
    client = FakeRedisClient()
    sink = RedisStreamLogSink(connection=FakeRedisConnection(client), maxlen=100, ttl=60)

    sink.emit({"event": "first", "run_id": "run-123", "component": "formula", "level": "debug"})

    stream_name, fields, maxlen, approximate = client.entries[0]
    assert stream_name == "oxidizer:logs"
    assert maxlen == 100
    assert approximate is True
    assert fields["run_id"] == "run-123"
    assert json.loads(fields["event"])["event"] == "first"
    assert client.expirations == {"oxidizer:logs": 60}


def test_residue_flush_flushes_attached_residue_sinks():
    flushed = []

    class FakeSink:
        def flush(self):
            flushed.append(True)

    residue = Residue()
    residue.add_sink(FakeSink())

    residue.flush()

    assert flushed == [True]


def test_residue_capture_is_opt_in_shared_by_children_and_dump_clears_buffer():
    residue = Residue(capture_events=True)
    child = residue.bind(component_name="worker", run_id="run-123")
    child.emit(child.ash.INFO, "worker started", node_id="node-1")

    events = residue.dump()

    assert len(events) == 1
    assert events[0]["component"] == "worker"
    assert events[0]["run_id"] == "run-123"
    assert events[0]["event"] == "worker started"
    assert residue.dump() == []


def test_residue_does_not_capture_by_default():
    residue = Residue()
    residue.emit(Residue().ash.INFO, "not buffered")

    assert residue.dump() == []


def test_redis_sink_sanitizes_raw_events_and_failure_warnings(capsys):
    client = FakeRedisClient()
    sink = RedisStreamLogSink(connection=FakeRedisConnection(client))
    original = {"event": "request failed password=event-secret", "run_id": "run-123", "nested": [{"api_token": "field-secret"}]}
    sink.emit(original)
    stored = json.loads(client.entries[0][1]["event"])
    assert "event-secret" not in str(stored)
    assert stored["nested"][0]["api_token"] == "[REDACTED]"
    assert original["nested"][0]["api_token"] == "field-secret"

    class FailingClient:
        def xadd(self, *args, **kwargs):
            raise RuntimeError("redis://warning-user:warning-secret@localhost:6379/0 password=failure-secret")

    sink = RedisStreamLogSink(connection=FakeRedisConnection(FailingClient()))
    sink.emit(original)
    sink.emit(original)
    warning = capsys.readouterr().err
    for secret in ("warning-user", "warning-secret", "failure-secret"):
        assert secret not in warning
    assert warning.count("oxidizer residue sink warning") == 1


def test_redis_stream_log_sink_routes_formula_events_to_formula_stream():
    client = FakeRedisClient()
    sink = RedisStreamLogSink(connection=FakeRedisConnection(client), ttl=60)

    sink.emit({"event": "dispatched", "formula_id": "open_meteo", "run_id": "run-1"})
    sink.emit({"event": "controller started", "formula_id": None})

    assert [entry[0] for entry in client.entries] == ["oxidizer:logs:open_meteo", "oxidizer:logs"]
    assert client.expirations == {"oxidizer:logs:open_meteo": 60, "oxidizer:logs": 60}


def test_residue_log_level_filters_sinks_and_is_inherited_by_children():
    client = FakeRedisClient()
    residue = Residue(log_level="DEBUG")
    residue.add_sink(RedisStreamLogSink(connection=FakeRedisConnection(client)))
    residue.bind(component_name="worker").emit(residue.ash.DEBUG, "debug kept", formula_id="weather")

    quiet = Residue(log_level="WARNING")
    quiet.add_sink(RedisStreamLogSink(connection=FakeRedisConnection(client)))
    quiet.bind(component_name="worker").emit(quiet.ash.INFO, "info dropped")

    assert [json.loads(entry[1]["event"])["event"] for entry in client.entries] == ["debug kept"]
    assert client.entries[0][0] == "oxidizer:logs:weather"


def test_residue_rejects_unknown_log_level():
    import pytest

    with pytest.raises(ValueError, match="Unknown log level"):
        Residue(log_level="LOUD")
