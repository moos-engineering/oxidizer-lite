import pytest

from oxidizer import Crucible, DataHandler, InputFetchResult, Reagent, ReagentUpdateType, Residue, S3TablesConnection, TaskMessage
from oxidizer.core.catalyst import KafkaCatalyst, RedisCatalyst


class FakeCatalyst:
    def __init__(self, messages=None, responses=None, batches=None):
        self.messages = messages or []
        self.responses = responses or {}
        self.batches = {stream: list(results) for stream, results in (batches or {}).items()}
        self.created_groups = []
        self.reads = []
        self.writes = []
        self.acks = []
        self.ack_batches = []
        self.operations = []
        self.created_streams = []

    def create_consumer_group(self, topic_name, group_name):
        self.created_groups.append((topic_name, group_name))

    def create_stream(self, topic_name):
        self.created_streams.append(topic_name)

    def read_from_stream(self, topic_name, group_name, consumer_name, count=1, block=1000):
        self.reads.append((topic_name, group_name, consumer_name, count, block))
        if topic_name in self.batches and self.batches[topic_name]:
            return self.batches[topic_name].pop(0)
        return self.responses.get(topic_name, self.messages)

    def write_to_stream(self, topic_name, message):
        self.writes.append((topic_name, message))
        self.operations.append(("write", topic_name, message))
        return f"{len(self.writes)}-0"

    def ack_message(self, topic_name, group_name, message_id, consumer_name=None):
        self.acks.append((topic_name, group_name, message_id, consumer_name))
        self.operations.append(("ack", topic_name, group_name, message_id, consumer_name))

    def ack_messages(self, topic_name, group_name, message_ids, consumer_name=None):
        self.ack_batches.append((topic_name, group_name, list(message_ids), consumer_name))
        self.operations.append(("ack_batch", topic_name, group_name, list(message_ids), consumer_name))
        for message_id in message_ids:
            self.ack_message(topic_name, group_name, message_id, consumer_name)


class FakeRedisPipelineClient:
    def __init__(self, fail_batch=None):
        self.batches = []
        self.transactions = []
        self.fail_batch = fail_batch
        self.message_count = 0

    def pipeline(self, *, transaction):
        self.transactions.append(transaction)
        return self

    def __enter__(self):
        self.queued = []
        return self

    def __exit__(self, *args):
        return False

    def xadd(self, stream, fields):
        self.queued.append((stream, fields))

    def execute(self):
        self.batches.append(list(self.queued))
        if len(self.batches) == self.fail_batch:
            raise RuntimeError("pipeline failed")
        ids = [f"{self.message_count + index}-0" for index in range(len(self.queued))]
        self.message_count += len(self.queued)
        return ids


@pytest.mark.parametrize("count,chunk_sizes", [(0, []), (1000, [1000]), (2001, [1000, 1000, 1])])
def test_redis_bulk_write_chunks_and_preserves_payloads(count, chunk_sizes):
    import json
    from types import SimpleNamespace

    client = FakeRedisPipelineClient()
    adapter = RedisCatalyst(SimpleNamespace(client=client), Residue())
    messages = ([{"id": 1}, [2], {"value": "text"}, [3], {"value": None}] * 401)[:count]

    ids = adapter.write_many_to_stream("output", messages, emit_log=False)

    assert [len(batch) for batch in client.batches] == chunk_sizes
    assert client.transactions == [False] * len(chunk_sizes)
    assert ids == [f"{index}-0" for index in range(count)]
    assert [entry for batch in client.batches for entry in batch] == [
        ("output", {"message": json.dumps(message)})
        for message in messages
    ]


def test_reagent_bulk_write_failure_does_not_checkpoint_or_ack():
    from types import SimpleNamespace

    client = FakeRedisPipelineClient(fail_batch=2)
    adapter = RedisCatalyst(SimpleNamespace(client=client), Residue())
    task_payload = _task_payload()
    task_payload["node"]["outputs"] = {"methods": [{"method": "stream", "stream": "output"}]}
    catalyst = FakeCatalyst(messages=[("task-1", {"message": task_payload})])
    catalyst.write_many_to_stream = adapter.write_many_to_stream
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return [{"id": index} for index in range(1001)]

    with pytest.raises(RuntimeError, match="pipeline failed"):
        process()

    assert client.message_count == 1000
    assert catalyst.acks == []
    assert [message["type"] for _, message in catalyst.writes] == ["started", "failed"]


def test_data_handler_uses_bulk_write_and_legacy_fallback():
    catalyst = FakeCatalyst()
    handler = DataHandler(catalyst)
    records = [{"id": 1}, {"id": 2}]
    assert handler.outgoing_data({"method": "stream", "stream": "output"}, data=records) == ["1-0", "2-0"]
    assert catalyst.writes == [("output", record) for record in records]

    calls = []

    def write_many(stream, messages):
        calls.append((stream, messages))
        return ["3-0", "4-0"]

    catalyst.write_many_to_stream = write_many
    assert handler.outgoing_data({"method": "stream", "stream": "output"}, data=records) == ["3-0", "4-0"]
    assert calls == [("output", records)]


class FakeAnvil:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, endpoint, params=None):
        self.calls.append(("GET", endpoint, params))
        return self.response

    def post(self, endpoint, data=None):
        self.calls.append(("POST", endpoint, data))
        return self.response


class FakeCrucibleAdapter:
    def __init__(self, *, fail_insert=False):
        self.inserts = []
        self.fail_insert = fail_insert
        self.table_checks = 0
        self.ensured = []

    def _ensure_table_schema(self, table_bucket, catalog, namespace, table, columns):
        self.ensured.append((table_bucket, catalog, namespace, table, columns))

    def table_columns(self, *args):
        self.table_checks += 1
        return [{
            "column_name": "id",
            "data_type": "BIGINT",
            "is_nullable": "NO",
            "column_default": None,
        }]

    def _insert_records(self, table_bucket, catalog, namespace, table, records, *, logical_types=None):
        if self.fail_insert:
            raise RuntimeError("simulated adapter write failure")
        self.inserts.append((table_bucket, catalog, namespace, table, records))
        return len(records)


@pytest.mark.parametrize("injected", [False, True])
@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("http_method", ["GET", "POST"])
def test_api_closes_only_owned_clients_on_success_and_failure(monkeypatch, injected, fail, http_method):
    from oxidizer import Anvil

    closed = []
    close = Anvil.close

    def tracked_close(api):
        closed.append(api)
        close(api)

    def request(api, endpoint, **kwargs):
        if fail:
            raise RuntimeError("request failed")
        return [{"id": 1}]

    monkeypatch.setattr(Anvil, "close", tracked_close)
    monkeypatch.setattr(Anvil, http_method.lower(), request)
    connection = Anvil({}) if injected else {"type": "api", "base_url": "https://api.test"}
    try:
        method = {"method": "api", "endpoint": "/items", "http_method": http_method}
        if fail:
            with pytest.raises(RuntimeError, match="request failed"):
                DataHandler().incoming_data(method, connection)
        else:
            assert DataHandler().incoming_data(method, connection).data == [{"id": 1}]
        assert len(closed) == (0 if injected else 1)
    finally:
        if injected:
            connection.close()


@pytest.mark.parametrize("injected", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_table_output_closes_only_owned_engines_on_success_and_failure(monkeypatch, injected, fail):
    from types import SimpleNamespace

    closed = []
    engine = SimpleNamespace(close=lambda: closed.append(True))
    connection = S3TablesConnection(client=object())
    if injected:
        connection = Crucible(connection, engine=engine)

    def write(crucible, *args, **kwargs):
        crucible.adapter.engine = engine
        if fail:
            raise RuntimeError("write failed")
        return 1

    monkeypatch.setattr(Crucible, "write", write)
    method = {
        "method": "s3_tables", "table_bucket": "warehouse", "catalog": "lake",
        "namespace": "analytics", "table": "events", "schema": "events",
        "schema_document": {"name": "events"},
    }
    if fail:
        with pytest.raises(RuntimeError, match="write failed"):
            DataHandler().outgoing_data(method, connection, data=[{"id": 1}])
    else:
        assert DataHandler().outgoing_data(method, connection, data=[{"id": 1}]) == 1
    assert closed == ([] if injected else [True])
    if injected:
        connection.close()


class CaptureSink:
    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)


def _task_payload():
    return {
        "type": "start_node",
        "formula_id": "daily-users",
        "run_id": "run-123",
        "node_id": "daily-users.bronze.fetch_data",
        "layer_id": "daily-users.bronze",
        "node": {"layer_id": "daily-users.bronze", "layer_name": "bronze", "node_name": "fetch_data"},
        "connections": {"sample_api": {"connection": {"type": "api"}}},
        "checkpoint_metadata": {"node_id": "daily-users.bronze.fetch_data"},
    }


def _table_output_task(crucible):
    schema_document = {
        "templateVersion": 1,
        "name": "events",
        "type": "schema",
        "schema": {
            "version": 1,
            "columns": [{"name": "id", "type": "int64", "nullable": False}],
            "write": {"operation": "append"},
        },
    }
    task = _task_payload()
    task["connections"] = {"lake": crucible}
    task["node"]["inputs"] = []
    task["node"]["outputs"] = {"methods": [{
        "method": "s3_tables",
        "connection": "lake",
        "schema": "events",
        "schema_document": schema_document,
        "table_bucket": "warehouse",
        "catalog": "local_catalog",
        "namespace": "analytics",
        "table": "events",
    }]}
    return task


def test_reagent_reads_task_and_emits_started_update():
    catalyst = FakeCatalyst(messages=[("1-0", {"message": _task_payload()})])
    reagent = Reagent(catalyst, auto_react=False)

    task, task_id = reagent.incoming_oxidizer_task(consumer_name="worker-1", count=1, block=50)

    assert task_id == "1-0"
    assert task == TaskMessage.from_dict(_task_payload())
    assert catalyst.created_groups == [("oxidizer:reagent", "reagent-consumer-group")]
    assert catalyst.reads == [("oxidizer:reagent", "reagent-consumer-group", "worker-1", 1, 50)]
    assert catalyst.writes[0][0] == "oxidizer:controller"
    assert catalyst.writes[0][1]["type"] == ReagentUpdateType.STARTED.value
    assert catalyst.acks == []


def test_reagent_outgoing_checkpoint_emits_checkpoint_then_acks():
    catalyst = FakeCatalyst()
    reagent = Reagent(catalyst, auto_react=False)
    task = TaskMessage.from_dict(_task_payload())
    task.checkpoint_metadata["is_final"] = True
    input_ack = ("orders-input", "worker-group", "input-1", "reagent")

    update = reagent.outgoing_oxidizer_checkpoint("1-0", task, [input_ack])

    assert update.type == ReagentUpdateType.CHECKPOINT.value
    assert catalyst.writes[0] == (
        "oxidizer:controller",
        {
            "type": "checkpoint",
            "formula_id": "daily-users",
            "run_id": "run-123",
            "node_id": "daily-users.bronze.fetch_data",
            "checkpoint_metadata": {
                "node_id": "daily-users.bronze.fetch_data",
                "is_final": True,
            },
            "error_details": None,
            "heartbeat": None,
        },
    )
    assert catalyst.acks == [
        input_ack,
        ("oxidizer:reagent", "reagent-consumer-group", "1-0", "reagent"),
    ]


def test_reagent_batches_checkpoint_acknowledgements_in_chunks():
    catalyst = FakeCatalyst()
    reagent = Reagent(catalyst, auto_react=False)
    task = TaskMessage.from_dict(_task_payload())
    input_acks = [
        ("orders-input", "worker-group", f"{index}-0", "source")
        for index in range(2001)
    ]

    reagent.outgoing_oxidizer_checkpoint("task-1", task, input_acks)

    assert [len(batch[2]) for batch in catalyst.ack_batches] == [1000, 1000, 1, 1]
    assert catalyst.writes[0][1]["type"] == ReagentUpdateType.CHECKPOINT.value
    assert len(catalyst.acks) == 2002
    assert catalyst.acks[:2] == [
        ("orders-input", "worker-group", "0-0", "source"),
        ("orders-input", "worker-group", "1-0", "source"),
    ]
    assert catalyst.acks[-1] == (
        "oxidizer:reagent", "reagent-consumer-group", "task-1", "reagent"
    )
    assert catalyst.operations[0][0] == "write"
    assert all(operation[0] != "write" for operation in catalyst.operations[1:])


def test_reagent_acknowledges_individually_without_bulk_catalyst_method():
    class LegacyCatalyst:
        def __init__(self):
            self.acks = []

        def create_consumer_group(self, topic_name, group_name):
            pass

        def ack_message(self, topic_name, group_name, message_id, consumer_name=None):
            self.acks.append((topic_name, group_name, message_id, consumer_name))

    catalyst = LegacyCatalyst()
    reagent = Reagent(catalyst, auto_react=False)

    reagent._acknowledge_messages([
        ("orders", "workers", "1-0", "consumer"),
        ("orders", "workers", "2-0", "consumer"),
    ])

    assert catalyst.acks == [
        ("orders", "workers", "1-0", "consumer"),
        ("orders", "workers", "2-0", "consumer"),
    ]


@pytest.mark.parametrize("binary", [False, True])
def test_redis_transport_health_reads_only_metadata(binary):
    from types import SimpleNamespace

    class Client:
        def type(self, name):
            assert name == "orders"
            return b"stream" if binary else "stream"

        def xinfo_groups(self, name):
            values = {"name": "workers", "consumers": 2, "pending": 3, "last-delivered-id": "4-0", "lag": None}
            return [{
                key.encode() if binary else key: value.encode() if binary and isinstance(value, str) else value
                for key, value in values.items()
            }]

        def xlen(self, name):
            return 10

    adapter = RedisCatalyst(SimpleNamespace(client=Client()), Residue())
    assert adapter.get_transport_health("orders", "workers") == {
        "backend": "redis", "stream": "orders", "length": 10,
        "groups": [{"name": "workers", "consumers": 2, "pending": 3, "last_delivered_id": "4-0", "lag": None}],
        "groups_truncated": False,
    }
    with pytest.raises(LookupError, match="group not found"):
        adapter.get_transport_health("orders", "unknown")


@pytest.mark.parametrize("group_name", [None, "workers"])
@pytest.mark.parametrize("committed,expected_lag,out_of_range", [(3, 7, False), (-1001, None, False), (0, None, True)])
def test_kafka_transport_health_uses_only_admin_calls(committed, expected_lag, out_of_range, group_name):
    from concurrent.futures import Future
    from types import SimpleNamespace

    def completed(value):
        future = Future()
        future.set_result(value)
        return future

    class Admin:
        def __init__(self):
            self.offset_calls = 0

        def describe_topics(self, topics, *, request_timeout):
            assert topics.topic_names == ["oxidizer.reagent"]
            assert request_timeout == 10
            return {"oxidizer.reagent": completed(SimpleNamespace(partitions=[SimpleNamespace(id=0)]))}

        def list_offsets(self, offsets, *, request_timeout):
            self.offset_calls += 1
            return {
                partition: completed(SimpleNamespace(offset=2 if self.offset_calls == 1 else 10))
                for partition in offsets
            }

        def list_consumer_group_offsets(self, requests, *, request_timeout):
            assert group_name == "workers"
            assert requests[0].group_id == "workers"
            return {"workers": completed(SimpleNamespace(topic_partitions=[
                SimpleNamespace(partition=0, offset=committed, error=None),
            ]))}

    adapter = KafkaCatalyst(SimpleNamespace(admin_client=Admin()), Residue())
    result = adapter.get_transport_health("oxidizer:reagent", group_name)
    assert result["topic"] == "oxidizer.reagent"
    assert result["group"] == group_name
    assert result["partitions"] == [{
        "partition": 0, "start_offset": 2, "end_offset": 10,
        "committed_offset": committed if group_name is not None and committed >= 0 else None,
        "lag": expected_lag if group_name is not None else None,
        "offset_out_of_range": out_of_range if group_name is not None else False,
    }]


@pytest.mark.parametrize("key_type,error", [(b"none", LookupError), ("string", ValueError)])
def test_redis_transport_health_rejects_missing_or_nonstream_keys(key_type, error):
    from types import SimpleNamespace

    adapter = RedisCatalyst(SimpleNamespace(client=SimpleNamespace(type=lambda name: key_type)), Residue())
    with pytest.raises(error):
        adapter.get_transport_health("orders")


@pytest.mark.parametrize("stream,group", [("", None), (" ", None), (None, None), ("tasks", ""), ("tasks", 1)])
def test_transport_health_facade_validates_names_before_backend_calls(stream, group):
    from oxidizer import Catalyst, RedisConnection

    catalyst = Catalyst(RedisConnection(client=object()))
    with pytest.raises(ValueError):
        catalyst.get_transport_health(stream, group)


def test_kafka_transport_health_requires_admin_client():
    from types import SimpleNamespace

    adapter = KafkaCatalyst(SimpleNamespace(admin_client=None), Residue())
    with pytest.raises(RuntimeError, match="admin client"):
        adapter.get_transport_health("orders")


def test_redis_catalyst_ack_messages_uses_one_xack_for_batch():
    from types import SimpleNamespace

    class RedisClient:
        def __init__(self):
            self.xack_calls = []

        def xack(self, topic_name, group_name, *message_ids):
            self.xack_calls.append((topic_name, group_name, message_ids))
            return len(message_ids)

    client = RedisClient()
    catalyst = RedisCatalyst(SimpleNamespace(client=client), Residue())

    assert catalyst.ack_messages("orders", "workers", ["1-0", "2-0"]) == 2
    assert client.xack_calls == [("orders", "workers", ("1-0", "2-0"))]


def test_kafka_catalyst_ack_messages_commits_specific_offsets():
    from types import SimpleNamespace

    class Consumer:
        def __init__(self):
            self.commits = []

        def commit(self, *, offsets, asynchronous):
            self.commits.append((
                [(offset.topic, offset.partition, offset.offset) for offset in offsets],
                asynchronous,
            ))

        def subscribe(self, topics, **callbacks):
            pass

        def consume(self, **kwargs):
            return [
                SimpleNamespace(
                    error=lambda: None, value=lambda: b'{}',
                    partition=lambda p=partition: p, offset=lambda o=offset: o,
                )
                for partition, offset in [(0, 1), (0, 2), (1, 4)]
            ]

    consumer = Consumer()
    catalyst = KafkaCatalyst(SimpleNamespace(consumer=consumer), Residue())

    catalyst.read_from_stream("orders", "workers", "worker", count=3, block=1)
    assert catalyst.ack_messages("orders", "workers", ["0:1", "0:2", "1:4"]) == 3
    assert catalyst.ack_messages("orders", "workers", []) == 0
    assert consumer.commits == [([("orders", 0, 3), ("orders", 1, 5)], False)]


def test_kafka_catalyst_bulk_write_flushes_bounded_batches():
    from types import SimpleNamespace

    class Producer:
        def __init__(self):
            self.pending = []
            self.flush_sizes = []

        def produce(self, topic, *, value):
            self.pending.append((topic, value))

        def poll(self, _timeout):
            return 0

        def flush(self, timeout):
            self.flush_sizes.append(len(self.pending))
            self.pending.clear()
            return 0

    producer = Producer()
    catalyst = KafkaCatalyst(SimpleNamespace(producer=producer, consumer=None), Residue())

    assert catalyst.write_many_to_stream("events", [{"id": n} for n in range(5001)], emit_log=False) == [None] * 5001
    assert producer.flush_sizes == [5000, 1]


def test_reagent_react_decorator_success_path():
    catalyst = FakeCatalyst(messages=[("1-0", {"message": _task_payload()})])
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react(handler_mode="task")
    def handler(task):
        return {"node_id": task.node_id}

    assert handler() == {"node_id": "daily-users.bronze.fetch_data"}
    assert [message[1]["type"] for message in catalyst.writes] == ["started", "checkpoint"]
    assert catalyst.writes[-1][1]["checkpoint_metadata"]["is_final"] is True
    assert catalyst.acks == [("oxidizer:reagent", "reagent-consumer-group", "1-0", "reagent")]


@pytest.mark.parametrize("log_level", ["DEBUG", "INFO"])
def test_reagent_trace_boundaries_are_ordered_correlated_and_filtered(log_level):
    residue = Residue(capture_events=True, log_level=log_level)
    catalyst = FakeCatalyst(messages=[("task-1", {"message": _task_payload()})])
    reagent = Reagent(catalyst, residue=residue, auto_react=False)

    @reagent.react()
    def process(data, context):
        return {"private_payload": "not-for-logs"}

    process()
    events = residue.dump()
    if log_level == "INFO":
        assert events == []
        return

    assert [event["event"] for event in events] == [
        "REAGENT: Sent update",
        "REAGENT: Received task",
        "REAGENT: Input fetch started",
        "REAGENT: Input fetch completed",
        "REAGENT: Handler started",
        "REAGENT: Handler completed",
        "REAGENT: Output routing started",
        "REAGENT: Output routing completed",
        "REAGENT: Batch checkpoint prepared",
        "REAGENT: Sent update",
        "REAGENT: Acknowledged batch",
    ]
    for event in events:
        assert event["level"] == "debug"
        assert event["formula_id"] == "daily-users"
        assert event["run_id"] == "run-123"
        assert event["node_id"] == "daily-users.bronze.fetch_data"
    assert events[0]["update_type"] == "started"
    assert all(event["task_id"] == "task-1" for event in events[1:])
    assert events[2]["batch_index"] == 0
    assert events[8]["batch_index"] == 1
    assert events[8]["is_final"] is True
    assert events[9]["update_type"] == "checkpoint"
    assert events[-1]["task_id"] == "task-1"
    assert "not-for-logs" not in str(events)


@pytest.mark.parametrize("phase,missing_event", [
    ("input", "REAGENT: Input fetch completed"),
    ("handler", "REAGENT: Handler completed"),
    ("output", "REAGENT: Output routing completed"),
    ("checkpoint", "REAGENT: Acknowledged batch"),
    ("acknowledgement", "REAGENT: Acknowledged batch"),
])
def test_reagent_trace_stops_at_failed_boundary(monkeypatch, phase, missing_event):
    residue = Residue(capture_events=True, log_level="DEBUG")
    catalyst = FakeCatalyst(messages=[("task-1", {"message": _task_payload()})])
    reagent = Reagent(catalyst, residue=residue, auto_react=False)

    def fail(*args, **kwargs):
        raise RuntimeError(f"{phase} failed")

    if phase == "input":
        monkeypatch.setattr(reagent, "handle_incoming_data", fail)
    elif phase == "output":
        monkeypatch.setattr(reagent, "handle_outgoing_data", fail)
    elif phase == "checkpoint":
        send_update = reagent.outgoing_oxidizer_update

        def fail_checkpoint(update_type, *args, **kwargs):
            if update_type == "checkpoint":
                fail()
            return send_update(update_type, *args, **kwargs)

        monkeypatch.setattr(reagent, "outgoing_oxidizer_update", fail_checkpoint)
    elif phase == "acknowledgement":
        monkeypatch.setattr(reagent, "_acknowledge_messages", fail)

    @reagent.react()
    def process(data, context):
        if phase == "handler":
            fail()
        return []

    if phase == "handler":
        assert process() is None
    else:
        with pytest.raises(RuntimeError, match=f"{phase} failed"):
            process()

    events = residue.dump()
    assert missing_event not in [event["event"] for event in events]
    assert any(event["event"] == "REAGENT: Worker task failed" for event in events)
    assert events[-1]["event"] == "REAGENT: Sent update"
    assert events[-1]["update_type"] == "failed"
    assert catalyst.acks == []
    if phase in {"checkpoint", "acknowledgement"}:
        checkpoint_sent = any(event.get("update_type") == "checkpoint" for event in events)
        assert checkpoint_sent is (phase == "acknowledgement")


def test_reagent_scopes_residue_logs_to_current_task():
    sink = CaptureSink()
    residue = Residue()
    residue.add_sink(sink)
    catalyst = FakeCatalyst(messages=[("1-0", {"message": _task_payload()})])
    reagent = Reagent(catalyst, residue=residue, auto_react=False)

    @reagent.react()
    def process(data, context):
        reagent.residue.emit(reagent.residue.ash.INFO, "task-scoped log")
        return []

    process()
    event = next(event for event in sink.events if event["event"] == "task-scoped log")

    assert event["formula_id"] == "daily-users"
    assert event["run_id"] == "run-123"
    assert event["node_id"] == "daily-users.bronze.fetch_data"
    assert "formula_id" not in reagent.residue.context


def test_reagent_react_rejects_unknown_handler_mode():
    reagent = Reagent(FakeCatalyst(), auto_react=False)

    with pytest.raises(ValueError, match="handler_mode must be"):
        reagent.react(handler_mode="legacy")


def test_reagent_react_decorator_failure_path_does_not_ack():
    catalyst = FakeCatalyst(messages=[("1-0", {"message": _task_payload()})])
    residue = Residue()
    sink = CaptureSink()
    residue.add_sink(sink)
    reagent = Reagent(catalyst, residue=residue, auto_react=False)

    @reagent.react(handler_mode="task")
    def handler(task):
        raise RuntimeError("boom")

    assert handler() is None

    assert [message[1]["type"] for message in catalyst.writes] == ["started", "failed"]
    assert catalyst.writes[-1][1]["error_details"]["error_type"] == "RuntimeError"
    assert reagent.last_error["error_message"] == "boom"
    assert len(sink.events) == 1
    assert sink.events[0]["event"] == "REAGENT: Worker task failed"
    assert sink.events[0]["traceback"]
    assert catalyst.acks == []


def test_data_handler_fetches_and_selects_api_pages():
    handler = DataHandler()
    api = FakeAnvil({"data": {"items": [{"id": 1}]}, "next": "/page/2"})

    result = handler.incoming_data(
        {"method": "api", "endpoint": "/items", "path": "data.items", "paginator": "next"},
        api,
        context={"cursor": None},
    )

    assert isinstance(result, InputFetchResult)
    assert result.data == [{"id": 1}]
    assert result.is_final is False
    assert result.next_cursor == "/page/2"
    assert api.calls == [("GET", "/items", None)]


def test_api_trigger_pages_with_token_parameter_and_acks_after_last_page():
    class PagedApi:
        def __init__(self):
            self.responses = [
                {"bars": [{"c": 1}], "next_page_token": "page-2"},
                {"bars": [{"c": 2}], "next_page_token": None},
            ]
            self.calls = []

        def get(self, endpoint, params=None):
            self.calls.append((endpoint, params))
            return self.responses.pop(0)

        def post(self, endpoint, data=None):
            raise AssertionError("POST is not expected")

    api = PagedApi()
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"endpoint": "/v2/stocks/bars?symbols=AAPL&feed=iex"}})])
    handler = DataHandler(catalyst=catalyst)
    method = {
        "method": "api",
        "input_trigger": {"type": "stream", "attribute": "endpoint"},
        "params": {"timeframe": "1Day", "limit": 1000},
        "path": "$.bars",
        "paginator": "$.next_page_token",
        "paginator_parameter": "page_token",
    }
    context = {"input_ref": "alpaca.equity.requests"}

    first = handler.incoming_data(method, api, context=context)
    assert first.data == [{"c": 1}]
    assert first.next_cursor["next"] == "page-2"
    assert first.ack_msgs == []

    second = handler.incoming_data(method, api, context={**context, "cursor": first.next_cursor})
    assert second.data == [{"c": 2}]
    assert second.next_cursor is None
    assert second.ack_msgs == [("alpaca.equity.requests", "reagent-input-group", "1-0", "reagent")]
    assert api.calls == [
        ("/v2/stocks/bars?symbols=AAPL&feed=iex", {"timeframe": "1Day", "limit": 1000}),
        ("/v2/stocks/bars?symbols=AAPL&feed=iex", {"timeframe": "1Day", "limit": 1000, "page_token": "page-2"}),
    ]


def test_data_handler_without_api_paginator_marks_response_final():
    handler = DataHandler()
    api = FakeAnvil({"items": [{"id": 1}, {"id": 2}]})

    result = handler.incoming_data(
        {"method": "api", "endpoint": "/items"},
        api,
    )

    assert result.data == {"items": [{"id": 1}, {"id": 2}]}
    assert result.is_final is True
    assert result.next_cursor is None


def test_api_endpoint_trigger_pages_before_acking_trigger_message():
    catalyst = FakeCatalyst(messages=[("7-0", {"endpoint": "/v3/tickers/AAPL"})])
    handler = DataHandler(catalyst=catalyst)
    method = {
        "method": "api",
        "input_trigger": {"type": "stream", "attribute": "endpoint"},
        "path": "$.results",
        "paginator": "next_url",
    }
    context = {"input_ref": "bronze.ticker_list", "lattice_id": "lattice", "node_id": "node"}

    first = handler.incoming_data(
        method, FakeAnvil({"results": [{"id": 1}], "next_url": "https://api.test/page/2"}), context=context
    )
    assert first.data == [{"id": 1}]
    assert first.is_final is False
    assert first.ack_msgs == []
    assert first.next_cursor["next"] == "https://api.test/page/2"

    api = FakeAnvil({"results": [{"id": 2}]})
    second = handler.incoming_data(method, api, context={**context, "cursor": first.next_cursor})
    assert api.calls == [("GET", "https://api.test/page/2", None)]
    assert second.data == [{"id": 2}]
    assert second.is_final is False
    assert second.next_cursor is None
    assert second.ack_msgs == [("bronze.ticker_list", "lattice.node", "7-0", "reagent")]
    assert catalyst.reads == [("bronze.ticker_list", "lattice.node", "reagent", 1, 1000)]


def test_api_payload_trigger_posts_stream_record_and_empty_stream_is_final():
    catalyst = FakeCatalyst(batches={"requests": [[("1-0", {"message": {"ticker": "AAPL"}})], []]})
    handler = DataHandler(catalyst=catalyst)
    method = {
        "method": "api",
        "endpoint": "/search",
        "http_method": "POST",
        "input_trigger": {"type": "stream", "attribute": "payload"},
    }
    api = FakeAnvil({"ok": True})

    result = handler.incoming_data(method, api, context={"input_ref": "requests"})
    assert api.calls == [("POST", "/search", {"ticker": "AAPL"})]
    assert result.ack_msgs == [("requests", "reagent-input-group", "1-0", "reagent")]
    assert result.is_final is False

    done = handler.incoming_data(method, api, context={"input_ref": "requests"})
    assert done.is_final is True
    assert len(api.calls) == 1


@pytest.mark.parametrize(
    "trigger,http_method",
    [({"type": "kafka", "attribute": "endpoint"}, "GET"), ({"type": "stream", "attribute": "params"}, "GET"),
     ({"type": "stream", "attribute": "payload"}, "GET")],
)
def test_api_trigger_rejects_invalid_config(trigger, http_method):
    with pytest.raises(ValueError):
        DataHandler(catalyst=FakeCatalyst()).incoming_data(
            {"method": "api", "http_method": http_method, "input_trigger": trigger},
            FakeAnvil({}),
            context={"input_ref": "s"},
        )


def test_data_handler_reads_stream_batch_and_returns_deferred_acks():
    catalyst = FakeCatalyst(messages=[("1-0", {"id": 1}), ("2-0", {"id": 2})])
    handler = DataHandler(catalyst=catalyst)

    result = handler.incoming_data(
        {"method": "stream", "batch_size": 2, "block": 20},
        context={"input_ref": "upstream", "lattice_id": "lattice", "node_id": "consumer"},
    )

    assert result.data == [{"id": 1}, {"id": 2}]
    assert result.is_final is False
    assert result.ack_msgs == [
        ("upstream", "lattice.consumer", "1-0", "reagent"),
        ("upstream", "lattice.consumer", "2-0", "reagent"),
    ]
    assert catalyst.created_groups == [("upstream", "lattice.consumer")]
    assert catalyst.reads == [("upstream", "lattice.consumer", "reagent", 2, 20)]


def test_data_handler_writes_stream_records():
    catalyst = FakeCatalyst()
    handler = DataHandler(catalyst=catalyst)

    message_ids = handler.outgoing_data(
        {"method": "stream"},
        data=[{"id": 1}, {"id": 2}],
        context={"node_id": "producer"},
    )

    assert message_ids == ["1-0", "2-0"]
    assert catalyst.created_streams == ["producer"]
    assert [message for _, message in catalyst.writes] == [{"id": 1}, {"id": 2}]


def test_data_handler_writes_records_to_s3_tables():
    crucible = Crucible(S3TablesConnection(client=object()))
    adapter = FakeCrucibleAdapter()
    crucible.adapter = adapter
    handler = DataHandler()
    schema_document = {
        "templateVersion": 1,
        "name": "events",
        "type": "schema",
        "schema": {
            "version": 1,
            "columns": [{"name": "id", "type": "int64", "nullable": False}],
            "write": {"operation": "append"},
        },
    }

    count = handler.outgoing_data(
        {"method": "s3_tables", "table_bucket": "warehouse", "catalog": "local_catalog", "namespace": "analytics", "table": "events", "schema": "events", "schema_document": schema_document},
        crucible,
        [{"id": 1}, {"id": 2}],
    )

    assert count == 2
    assert adapter.inserts == [("warehouse", "local_catalog", "analytics", "events", [{"id": 1}, {"id": 2}])]


def test_s3_tables_output_forwards_assume_role_configuration(monkeypatch):
    from oxidizer import reagent as reagent_module

    captured = {}

    class FakeS3TablesConnection:
        def __init__(self, **options):
            captured.update(options)

    class FakeCrucible:
        def __init__(self, connection, residue=None):
            self.adapter = FakeCrucibleAdapter()

        def write(self, table_bucket, catalog, namespace, table, records, schema):
            return len(records)

        def close(self):
            pass

    monkeypatch.setattr(reagent_module, "S3TablesConnection", FakeS3TablesConnection)
    monkeypatch.setattr(reagent_module, "Crucible", FakeCrucible)
    schema_document = {"name": "events", "schema": {"columns": [], "write": {"operation": "append"}}}
    count = DataHandler().outgoing_data(
        {"method": "s3_tables", "table_bucket": "warehouse", "catalog": "local_catalog",
         "namespace": "analytics", "table": "events", "schema": "events", "schema_document": schema_document},
        {"type": "s3_tables", "aws_sso_profile": "source-profile",
         "role_arn": "arn:aws:iam::123456789012:role/target",
         "role_session_name": "presentation-test", "external_id": "customer-123"},
        data=[{"id": 1}],
    )

    assert count == 1
    assert captured == {
        "region_name": "us-east-1",
        "aws_profile_name": "source-profile",
        "access_key_id": None,
        "secret_access_key": None,
        "session_token": None,
        "role_arn": "arn:aws:iam::123456789012:role/target",
        "role_session_name": "presentation-test",
        "external_id": "customer-123",
    }


def test_reagent_react_passes_input_data_and_context_then_routes_output():
    task_payload = _task_payload()
    task_payload["node"]["inputs"] = [{
        "ref": "upstream",
        "alias": "orders",
        "methods": [{
            "method": "stream",
            "stream": "orders-input",
            "consumer_group": "worker-group",
            "batch_size": 2,
        }],
    }]
    task_payload["node"]["outputs"] = {"methods": [{"method": "stream", "stream": "orders-output"}]}
    catalyst = FakeCatalyst(responses={
        "oxidizer:reagent": [("task-1", {"message": task_payload})],
        "orders-input": [("input-1", {"message": {"order_id": 7}})],
    })
    reagent = Reagent(catalyst, auto_react=False)
    received = {}

    @reagent.react()
    def process(data, context):
        received["data"] = data
        received["context"] = context
        return [{"order_id": data["orders"][0]["order_id"], "processed": True}]

    result = process()

    assert result == [{"order_id": 7, "processed": True}]
    assert received["data"] == {"orders": [{"order_id": 7}]}
    assert received["context"] == {
        "lattice_id": "daily-users",
        "formula_id": "daily-users",
        "run_id": "run-123",
        "layer_id": "daily-users.bronze",
        "layer_name": "bronze",
        "node_id": "daily-users.bronze.fetch_data",
        "node_name": "fetch_data",
        "batch_index": 0,
    }
    assert ("orders-output", {"order_id": 7, "processed": True}) in catalyst.writes
    assert catalyst.writes[-1][0] == "oxidizer:controller"
    assert catalyst.acks == [
        ("orders-input", "worker-group", "input-1", "reagent"),
        ("oxidizer:reagent", "reagent-consumer-group", "task-1", "reagent"),
    ]


def test_reagent_react_uses_and_acks_dedicated_task_stream():
    catalyst = FakeCatalyst(responses={"custom:tasks": [("task-9", {"message": _task_payload()})]})
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react(dedicated_stream="custom:tasks")
    def process(data, context):
        return {"handled": True, "node_id": context["node_id"]}

    assert process() == {"handled": True, "node_id": "daily-users.bronze.fetch_data"}
    assert catalyst.reads[0][0] == "custom:tasks"
    assert catalyst.acks == [("custom:tasks", "reagent-consumer-group", "task-9", "reagent")]


def test_reagent_does_not_ack_inputs_when_output_fails():
    task_payload = _task_payload()
    task_payload["node"]["inputs"] = [{
        "ref": "upstream",
        "methods": [{"method": "stream", "stream": "orders-input", "batch_size": 1}],
    }]
    task_payload["node"]["outputs"] = {"methods": [{"method": "unsupported"}]}
    catalyst = FakeCatalyst(responses={
        "oxidizer:reagent": [("task-1", {"message": task_payload})],
        "orders-input": [("input-1", {"order_id": 7})],
    })
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react
    def process(data, context):
        return data

    with pytest.raises(ValueError, match="Unsupported output method"):
        process()

    assert not any(ack[2] == "input-1" for ack in catalyst.acks)
    assert catalyst.writes[-1][1]["type"] == ReagentUpdateType.FAILED.value


def test_reagent_does_not_ack_task_when_crucible_adapter_fails():
    adapter = FakeCrucibleAdapter(fail_insert=True)
    crucible = Crucible(S3TablesConnection(client=object()))
    crucible.adapter = adapter
    task_payload = _table_output_task(crucible)
    catalyst = FakeCatalyst(responses={"oxidizer:reagent": [("task-1", {"message": task_payload})]})
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return [{"id": 1}]

    with pytest.raises(RuntimeError, match="simulated adapter write failure"):
        process()

    assert adapter.table_checks == 0
    assert adapter.inserts == []
    assert catalyst.acks == []
    assert [message["type"] for stream, message in catalyst.writes if stream == "oxidizer:controller"] == [
        "started", "failed",
    ]


def test_reagent_retry_after_checkpoint_failure_can_duplicate_append():
    class FailOnceCheckpointCatalyst(FakeCatalyst):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fail_checkpoint = True

        def write_to_stream(self, topic_name, message):
            if (
                self.fail_checkpoint
                and topic_name == "oxidizer:controller"
                and message.get("type") == "checkpoint"
            ):
                self.fail_checkpoint = False
                raise RuntimeError("simulated checkpoint transport failure")
            return super().write_to_stream(topic_name, message)

    adapter = FakeCrucibleAdapter()
    crucible = Crucible(S3TablesConnection(client=object()))
    crucible.adapter = adapter
    task_payload = _table_output_task(crucible)
    task_messages = [("task-1", {"message": task_payload})]
    catalyst = FailOnceCheckpointCatalyst(batches={"oxidizer:reagent": [task_messages, task_messages]})
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return [{"id": 1}]

    with pytest.raises(RuntimeError, match="simulated checkpoint transport failure"):
        process()

    assert len(adapter.inserts) == 1
    assert catalyst.acks == []

    process()

    assert len(adapter.inserts) == 2
    assert adapter.inserts[0][-1] == adapter.inserts[1][-1] == [{"id": 1}]
    assert catalyst.acks == [("oxidizer:reagent", "reagent-consumer-group", "task-1", "reagent")]


def test_reagent_retry_after_checkpoint_before_ack_can_duplicate_append():
    class FailOnceAckCatalyst(FakeCatalyst):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fail_ack = True

        def ack_messages(self, topic_name, group_name, message_ids, consumer_name=None):
            if self.fail_ack:
                self.fail_ack = False
                self.operations.append(("ack_failed", topic_name, group_name, list(message_ids), consumer_name))
                raise RuntimeError("simulated acknowledgement failure")
            return super().ack_messages(topic_name, group_name, message_ids, consumer_name)

    adapter = FakeCrucibleAdapter()
    crucible = Crucible(S3TablesConnection(client=object()))
    crucible.adapter = adapter
    task_payload = _table_output_task(crucible)
    task_messages = [("task-1", {"message": task_payload})]
    catalyst = FailOnceAckCatalyst(batches={"oxidizer:reagent": [task_messages, task_messages]})
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return [{"id": 1}]

    with pytest.raises(RuntimeError, match="simulated acknowledgement failure"):
        process()

    checkpoint_index = next(
        index for index, (_, message) in enumerate(catalyst.writes)
        if message.get("type") == ReagentUpdateType.CHECKPOINT.value
    )
    assert catalyst.operations[checkpoint_index][0] == "write"
    assert any(operation[0] == "ack_failed" for operation in catalyst.operations)
    assert catalyst.acks == []
    assert len(adapter.inserts) == 1

    process()

    assert len(adapter.inserts) == 2
    assert adapter.inserts[0][-1] == adapter.inserts[1][-1] == [{"id": 1}]
    assert catalyst.acks == [
        ("oxidizer:reagent", "reagent-consumer-group", "task-1", "reagent")
    ]


def test_s3_static_input_pages_with_offset_cursor(monkeypatch):
    calls = []

    def read(connection, paths, **options):
        calls.append((connection, paths, options))
        return {"rows": [{"id": 1}], "is_final": False, "next_cursor": options["offset"] + 2}

    monkeypatch.setattr(Crucible, "read_s3_batch", read)
    method = {"method": "s3", "connection": "raw", "s3_path": "a.parquet", "order_by": "id", "batch_size": 2}

    result = DataHandler().incoming_data(method, {"type": "s3"}, context={"cursor": 4})

    assert result.next_cursor == 6 and result.is_final is False and result.ack_msgs == []
    assert calls == [({"type": "s3"}, "a.parquet", {
        "file_format": None, "order_by": "id", "batch_size": 2, "offset": 4, "residue": calls[0][2]["residue"],
    })]


def test_s3_triggered_input_pages_file_then_acks_and_finishes(monkeypatch):
    pages = [
        {"rows": [{"id": 1}], "is_final": False, "next_cursor": 1},
        {"rows": [{"id": 2}], "is_final": True, "next_cursor": None},
    ]
    calls = []

    def read(connection, paths, **options):
        calls.append((paths, options["offset"]))
        return pages.pop(0)

    monkeypatch.setattr(Crucible, "read_s3_batch", read)
    catalyst = FakeCatalyst(batches={"files": [[("9-0", {"message": {"s3_path": ["x.csv", "y.csv"]}})], []]})
    handler = DataHandler(catalyst=catalyst)
    method = {"method": "s3", "connection": "raw", "input_trigger": {"type": "stream"}, "batch_size": 1}
    context = {"input_ref": "files"}

    first = handler.incoming_data(method, {"type": "s3"}, context=context)
    assert first.ack_msgs == [] and first.next_cursor["next"] == 1
    second = handler.incoming_data(method, {"type": "s3"}, context={**context, "cursor": first.next_cursor})
    assert second.data == [{"id": 2}] and second.is_final is False and second.next_cursor is None
    assert second.ack_msgs == [("files", "reagent-input-group", "9-0", "reagent")]
    assert calls == [(["x.csv", "y.csv"], 0), (["x.csv", "y.csv"], 1)]
    assert handler.incoming_data(method, {"type": "s3"}, context=context).is_final is True


@pytest.mark.parametrize("method", [
    {"method": "s3"},
    {"method": "s3", "input_trigger": {"type": "kafka"}},
    {"method": "s3", "input_trigger": {"type": "stream", "attribute": "endpoint"}},
])
def test_s3_input_rejects_invalid_config(method):
    with pytest.raises(ValueError):
        DataHandler(catalyst=FakeCatalyst()).incoming_data(method, {"type": "s3"}, context={"input_ref": "s"})


def test_sql_parameters_trigger_pages_query_then_acks(monkeypatch):
    pages = [
        {"rows": [{"id": 100}], "is_final": False, "next_cursor": 1},
        {"rows": [{"id": 101}], "is_final": True, "next_cursor": None},
        {"rows": [{"id": 7}], "is_final": True, "next_cursor": None},
    ]
    calls = []

    def read(connections, query, **options):
        calls.append((options["parameters"], options["offset"]))
        return pages.pop(0)

    monkeypatch.setattr(Crucible, "read_sql_batch", read)
    catalyst = FakeCatalyst(batches={"ids": [
        [("1-0", {"message": {"parameters": [100, "open"]}})], [("2-0", {"message": {"parameters": 7}})], [],
    ]})
    handler = DataHandler(catalyst=catalyst)
    method = {
        "method": "sql", "catalogs": [{"connection": "lake", "catalog": "lake"}],
        "query": "SELECT id FROM lake.main.orders WHERE id >= ? AND status = ? ORDER BY id",
        "parameters": [0, "ignored"], "input_trigger": {"type": "stream", "attribute": "parameters"}, "batch_size": 1,
    }
    context = {"input_ref": "ids"}

    first = handler.incoming_data(method, {"lake": {}}, context=context)
    assert first.ack_msgs == [] and first.next_cursor["next"] == 1
    second = handler.incoming_data(method, {"lake": {}}, context={**context, "cursor": first.next_cursor})
    assert second.data == [{"id": 101}] and second.is_final is False and second.next_cursor is None
    assert second.ack_msgs == [("ids", "reagent-input-group", "1-0", "reagent")]
    third = handler.incoming_data(method, {"lake": {}}, context=context)
    assert third.ack_msgs == [("ids", "reagent-input-group", "2-0", "reagent")]
    assert handler.incoming_data(method, {"lake": {}}, context=context).is_final is True
    assert calls == [([100, "open"], 0), ([100, "open"], 1), ([7], 0)]


@pytest.mark.parametrize("trigger,message", [
    ({"type": "kafka"}, {"parameters": [1]}),
    ({"type": "stream", "attribute": "endpoint"}, {"parameters": [1]}),
    ({"type": "stream"}, {"order_id": 1}),
])
def test_sql_parameters_trigger_rejects_invalid_config(monkeypatch, trigger, message):
    monkeypatch.setattr(Crucible, "read_sql_batch", lambda *args, **kwargs: pytest.fail("should not query"))
    handler = DataHandler(catalyst=FakeCatalyst(messages=[("1-0", {"message": message})]))
    method = {"method": "sql", "catalogs": [{"connection": "lake", "catalog": "lake"}], "query": "SELECT 1", "input_trigger": trigger}
    with pytest.raises(ValueError):
        handler.incoming_data(method, {"lake": {}}, context={"input_ref": "ids"})


def test_sql_input_resumes_cursor_and_resolves_catalog_bindings(monkeypatch):
    calls = []

    def read(connections, query, **options):
        calls.append((connections, query, options))
        offset = options["offset"]
        return {"rows": [{"id": offset}], "is_final": offset == 2, "next_cursor": None if offset == 2 else 2}

    monkeypatch.setattr(Crucible, "read_sql_batch", read)
    payload = _task_payload()
    payload["connections"] = {"sales_lake": {"connection": {"type": "s3_tables"}}, "people_lake": {"connection": {"type": "s3_tables"}}}
    payload["node"]["inputs"] = [{"alias": "joined", "methods": [{
        "method": "sql", "query": "SELECT s.id FROM sales.main.events s", "batch_size": 2,
        "catalogs": [{"connection": "sales_lake", "catalog": "sales"}, {"connection": "people_lake", "catalog": "people"}],
    }]}]
    catalyst = FakeCatalyst(messages=[("task-1", {"message": payload})])
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return data["joined"]

    assert process() == [{"id": 0}]
    checkpoint = catalyst.writes[-1][1]["checkpoint_metadata"]
    assert checkpoint["batch_cursors"] == {"joined": 2}
    assert checkpoint["is_final"] is False
    payload["checkpoint_metadata"] = checkpoint
    assert process() == [{"id": 2}]
    checkpoint = catalyst.writes[-1][1]["checkpoint_metadata"]
    assert checkpoint["is_final"] is True
    assert checkpoint["total_records_so_far"] == 2
    assert list(calls[0][0]) == ["sales", "people"]
    assert [call[2]["offset"] for call in calls] == [0, 2]
    assert all(ack[0] == "oxidizer:reagent" for ack in catalyst.acks)


def test_sql_input_single_connection_uses_one_catalog_binding(monkeypatch):
    calls = []

    def read(connections, query, **options):
        calls.append(connections)
        return {"rows": [], "is_final": True, "next_cursor": None}

    monkeypatch.setattr(Crucible, "read_sql_batch", read)
    method = {"method": "sql", "query": "SELECT 1", "catalogs": [{"connection": "lake", "catalog": "warehouse"}]}
    handler = DataHandler()
    resolved = handler._task_connection(method, {"lake": {"type": "s3_tables"}})
    result = handler.incoming_data(method, resolved)
    assert calls == [{"warehouse": {"type": "s3_tables"}}]
    assert result.is_final is True
    assert result.ack_msgs == []


@pytest.mark.parametrize("bindings", [[], [{"catalog": "sales"}], ["sales"],
    [{"connection": "lake", "catalog": "sales"}, {"connection": "lake", "catalog": "sales"}],
    [{"connection": "unknown", "catalog": "sales"}],
])
def test_sql_input_rejects_invalid_catalog_bindings(bindings):
    with pytest.raises(ValueError):
        DataHandler._task_connection({"method": "sql", "catalogs": bindings}, {"lake": {}})


@pytest.mark.parametrize("options", [
    {}, {"connection": "lake"}, {"connection": "lake", "catalog": "warehouse"},
    {"catalogs": [{"connection": "lake", "catalog": "warehouse"}], "catalog": "other"},
    {"catalogs": [{"connection": "lake", "catalog": "warehouse"}], "connection": "lake"},
])
def test_sql_input_rejects_nonuniform_connection_config(options):
    method = {"method": "sql", "query": "SELECT 1", **options}
    with pytest.raises(ValueError, match="catalogs list"):
        DataHandler._task_connection(method, {"lake": {}})
    with pytest.raises(ValueError, match="catalogs list"):
        DataHandler().incoming_data(method, {})


def test_sql_input_failure_does_not_checkpoint_or_ack(monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("SQL read failed")

    monkeypatch.setattr(Crucible, "read_sql_batch", fail)
    payload = _task_payload()
    payload["connections"] = {"lake": {"type": "s3_tables"}}
    payload["node"]["inputs"] = [{"alias": "rows", "methods": [{"method": "sql", "catalogs": [{"connection": "lake", "catalog": "warehouse"}], "query": "SELECT 1"}]}]
    catalyst = FakeCatalyst(messages=[("task-1", {"message": payload})])
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return data["rows"]

    with pytest.raises(RuntimeError, match="SQL read failed"):
        process()
    assert catalyst.acks == []
    assert [message["type"] for _, message in catalyst.writes] == ["started", "failed"]


def test_reagent_reports_checkpoint_after_one_full_batch():
    task_payload = _task_payload()
    task_payload["node"]["inputs"] = [{
        "ref": "upstream",
        "alias": "orders",
        "methods": [{"method": "stream", "stream": "orders-input", "batch_size": 50}],
    }]
    task_payload["node"]["outputs"] = {"methods": [{"method": "stream", "stream": "orders-output"}]}
    task_payload["checkpoint_metadata"].update(records_processed=40, total_records_so_far=100)
    messages = [(f"{index}-0", {"message": {"order_id": index}}) for index in range(50)]
    catalyst = FakeCatalyst(
        responses={"oxidizer:reagent": [("task-1", {"message": task_payload})]},
        batches={"orders-input": [messages]},
    )
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        assert context["batch_index"] == 0
        return data["orders"]

    result = process()
    controller_updates = [message for stream, message in catalyst.writes if stream == "oxidizer:controller"]

    assert len(result) == 50
    assert [update["type"] for update in controller_updates] == ["started", "checkpoint"]
    checkpoint = controller_updates[-1]["checkpoint_metadata"]
    assert checkpoint["batch_index"] == 1
    assert checkpoint["records_processed"] == 50
    assert checkpoint["total_records_so_far"] == 150
    assert checkpoint["batch_final"] == {"orders": False}
    assert catalyst.acks[-1] == ("oxidizer:reagent", "reagent-consumer-group", "task-1", "reagent")


def test_reagent_checkpoints_batch_and_cumulative_late_arrival_counts(monkeypatch):
    task_payload = _task_payload()
    task_payload["checkpoint_metadata"]["late_arrivals_ignored"] = 3
    task_payload["checkpoint_metadata"]["total_late_arrivals_ignored"] = 4
    task_payload["node"]["outputs"] = {"methods": [
        {"method": "s3_tables", "ignored_count": 2},
        {"method": "s3_tables", "ignored_count": 1},
    ]}
    catalyst = FakeCatalyst(responses={"oxidizer:reagent": [("task-1", {"message": task_payload})]})
    reagent = Reagent(catalyst, auto_react=False)

    def fake_outgoing_data(method, connection, data, *, context=None, write_metrics=None):
        write_metrics["late_arrivals_ignored"] += method["ignored_count"]

    monkeypatch.setattr(reagent, "outgoing_data", fake_outgoing_data)

    @reagent.react()
    def process(data, context):
        return []

    process()
    checkpoint = [
        message for stream, message in catalyst.writes if stream == "oxidizer:controller"
    ][-1]["checkpoint_metadata"]

    assert "late_arrivals_ignored" not in checkpoint
    assert checkpoint["total_late_arrivals_ignored"] == 7


def test_reagent_accumulates_phase_runtimes_in_checkpoint(monkeypatch):
    from types import SimpleNamespace

    import oxidizer.reagent as reagent_module

    # Clock reads: preprocess start/end, function start/end, postprocess start/end.
    ticks = iter([10.0, 10.5, 11.0, 13.0, 13.0, 13.25])
    monkeypatch.setattr(reagent_module, "time", SimpleNamespace(perf_counter=lambda: next(ticks)))
    task_payload = _task_payload()
    task_payload["node"]["outputs"] = {"methods": [{"method": "stream", "stream": "orders-output"}]}
    task_payload["checkpoint_metadata"].update(
        accumulated_preprocess_runtime=1.0,
        accumulated_function_runtime=1.0,
        accumulated_postprocess_runtime=1.0,
    )
    catalyst = FakeCatalyst(responses={"oxidizer:reagent": [("task-1", {"message": task_payload})]})
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return [{"ok": True}]

    process()
    checkpoint = [message for stream, message in catalyst.writes if stream == "oxidizer:controller"][-1]["checkpoint_metadata"]

    assert checkpoint["accumulated_preprocess_runtime"] == 1.5
    assert checkpoint["accumulated_function_runtime"] == 3.0
    assert checkpoint["accumulated_postprocess_runtime"] == 1.25


def test_reagent_samples_each_phase_and_checkpoints_peak_bytes(monkeypatch):
    import oxidizer.reagent as reagent_module

    readings = iter([100, 120, 200, 210, 300, 320])
    monkeypatch.setattr(reagent_module, "_read_worker_memory_bytes", lambda: next(readings))
    monkeypatch.setattr(reagent_module._PhaseMemorySampler, "_SAMPLE_INTERVAL_SECONDS", 60.0)
    task_payload = _task_payload()
    task_payload["node"]["outputs"] = {"methods": [{"method": "stream", "stream": "orders-output"}]}
    task_payload["checkpoint_metadata"].update(
        peak_preprocess_memory_bytes=90,
        peak_function_memory_bytes=150,
        peak_postprocess_memory_bytes=250,
        accumulated_preprocess_memory=10.0,
        accumulated_function_memory=20.0,
        accumulated_postprocess_memory=30.0,
    )
    catalyst = FakeCatalyst(responses={"oxidizer:reagent": [("task-1", {"message": task_payload})]})
    reagent = Reagent(catalyst, auto_react=False)

    @reagent.react()
    def process(data, context):
        return [{"ok": True}]

    process()
    checkpoint = [message for stream, message in catalyst.writes if stream == "oxidizer:controller"][-1]["checkpoint_metadata"]

    assert checkpoint["peak_preprocess_memory_bytes"] == 120
    assert checkpoint["peak_function_memory_bytes"] == 210
    assert checkpoint["peak_postprocess_memory_bytes"] == 320
    assert "accumulated_preprocess_memory" not in checkpoint
    assert "accumulated_function_memory" not in checkpoint
    assert "accumulated_postprocess_memory" not in checkpoint


def test_reagent_keeps_largest_phase_memory_peak_across_batches():
    task = TaskMessage.from_dict(_task_payload())
    task.checkpoint_metadata.update(
        peak_preprocess_memory_bytes=400,
        peak_function_memory_bytes=200,
        peak_postprocess_memory_bytes=500,
        accumulated_preprocess_memory=10.0,
        accumulated_function_memory=20.0,
        accumulated_postprocess_memory=30.0,
    )

    checkpoint = Reagent._update_checkpoint(
        task,
        {},
        {},
        {},
        phase_memory_peaks={"preprocess": 300, "function": 250, "postprocess": 450},
    )

    assert checkpoint["peak_preprocess_memory_bytes"] == 400
    assert checkpoint["peak_function_memory_bytes"] == 250
    assert checkpoint["peak_postprocess_memory_bytes"] == 500
    assert not any(key.startswith("accumulated_") and key.endswith("_memory") for key in checkpoint)


def test_reagent_run_continues_after_cycle_errors_until_stopped():
    reagent = Reagent(FakeCatalyst(), auto_react=False)
    attempts = []

    def process():
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise RuntimeError("transient outage")
        if len(attempts) == 3:
            reagent.stop()

    reagent.run(process, error_backoff_seconds=0)

    assert len(attempts) == 3


def test_reagent_auto_react_starts_loop_when_handler_is_decorated():
    task_payload = _task_payload()
    catalyst = FakeCatalyst(batches={"oxidizer:reagent": [[("task-1", {"message": task_payload})]]})
    reagent = Reagent(catalyst)
    handled = []

    @reagent.react()
    def process(data, context):
        handled.append((data, context["node_id"]))
        reagent.stop()
        return []

    assert handled == [({}, task_payload["node_id"])]
    assert catalyst.acks[-1][2] == "task-1"
