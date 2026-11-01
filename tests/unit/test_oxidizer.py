import pytest

from oxidizer import Catalyst, Formula, Oxidizer, RedisConnection, RedisStreamLogSink, Residue, S3Connection


class FakeS3Client:
    pass


class FakeRedisClient:
    class exceptions:
        class ResponseError(Exception):
            pass

    def __init__(self):
        self.values = {}
        self.consumer_groups = []
        self.streams = {}

    def json(self):
        return FakeRedisJsonClient(self)

    def scan_iter(self, match):
        prefix = match.removesuffix("*")
        return [key for key in self.values if key.startswith(prefix)]

    def xgroup_create(self, topic_name, group_name, id="0", mkstream=True):
        self.consumer_groups.append((topic_name, group_name, id, mkstream))

    def exists(self, key):
        return key in self.streams

    def xadd(self, stream_name, data):
        self.streams.setdefault(stream_name, []).append(data)
        return f"{len(self.streams[stream_name])}-0"

    def xtrim(self, stream_name, maxlen=0):
        self.streams[stream_name] = self.streams.get(stream_name, [])[-maxlen:] if maxlen else []


class FakeRedisJsonClient:
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def set(self, key, path, data):
        self.redis_client.values[key] = data

    def get(self, key):
        return self.redis_client.values.get(key)

    def delete(self, key, path):
        self.redis_client.values.pop(key, None)


class FakeCatalyst:
    def __init__(self, messages=None):
        self.connection = object()
        self.catalyst = type("FakeCatalystBackend", (), {})()
        self.residue = None
        self.messages = messages or []
        self.created_groups = []
        self.created_streams = []
        self.reads = []
        self.writes = []
        self.acks = []
        self.expirations = []

    def set_ttl(self, topic_name, message_id, ttl):
        self.expirations.append((topic_name, message_id, ttl))

    def create_stream(self, topic_name):
        self.created_streams.append(topic_name)

    def create_consumer_group(self, topic_name, group_name):
        self.created_groups.append((topic_name, group_name))

    def read_from_stream(self, topic_name, group_name, consumer_name, count=1, block=1000):
        self.reads.append((topic_name, group_name, consumer_name, count, block))
        return self.messages

    def write_to_stream(self, topic_name, message):
        self.writes.append((topic_name, message))
        return f"{len(self.writes)}-0"

    def ack_message(self, topic_name, group_name, message_id, consumer_name=None):
        self.acks.append((topic_name, group_name, message_id, consumer_name))


def test_oxidizer_owns_shared_residue_without_auto_attaching_sinks(monkeypatch):
    monkeypatch.setenv("OXIDIZER_LOG_SINKS", "redis")
    formula = Formula(S3Connection(bucket="demo", client=FakeS3Client()))
    catalyst = Catalyst(RedisConnection(client=FakeRedisClient()))

    oxidizer = Oxidizer(formula, catalyst)

    assert formula.residue.context["component"] == "formula"
    assert formula.formula.residue is formula.residue
    assert catalyst.residue.context["component"] == "catalyst"
    assert catalyst.catalyst.residue is catalyst.residue
    assert formula.residue.sinks is catalyst.residue.sinks is oxidizer.residue.sinks
    assert oxidizer.residue.sinks == []


def test_oxidizer_attaches_explicit_sinks_from_constructor():
    formula = Formula(S3Connection(bucket="demo", client=FakeS3Client()))
    catalyst = Catalyst(RedisConnection(client=FakeRedisClient()))
    redis_sink = RedisStreamLogSink(connection=catalyst.connection)

    oxidizer = Oxidizer(formula, catalyst, sinks=[redis_sink])

    assert oxidizer.residue.sinks == [redis_sink]
    assert formula.residue.sinks is catalyst.residue.sinks is oxidizer.residue.sinks


def test_oxidizer_does_not_duplicate_explicit_connection_sink():
    formula = Formula(S3Connection(bucket="demo", client=FakeS3Client()))
    catalyst = Catalyst(RedisConnection(client=FakeRedisClient()))
    redis_sink = RedisStreamLogSink(connection=catalyst.connection)

    oxidizer = Oxidizer(formula, catalyst, sinks=[redis_sink, RedisStreamLogSink(connection=catalyst.connection)])

    assert oxidizer.residue.sinks == [redis_sink]


@pytest.mark.parametrize("wrap_workflow", [True, False])
@pytest.mark.parametrize("retention", [None, "1d"])
def test_oxidizer_reads_from_default_invocation_stream(wrap_workflow, retention):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    workflow_config = {
        "layers": [
            {
                "name": "bronze",
                "nodes": [
                    {
                        "name": "fetch_data",
                        "type": "scheduled",
                        "outputs": {"methods": [{"method": "stream", "retention": "12h"}]},
                        "inputs": [
                            {
                                "methods": [
                                    {"method": "api", "connection": "sample_api"},
                                    {"method": "stream"},
                                ]
                            }
                        ],
                    }
                ],
            }
        ]
    }
    formula.create_formula_config(
        "daily-users",
        {"workflow": workflow_config} if wrap_workflow else workflow_config,
    )
    formula.connection.client.values["oxidizer:connections:sample_api"] = {
        "connection": {"type": "api", "base_url": "https://example.test", "password": "initial-secret"},
        "name": "sample_api",
    }
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"formula_id": "daily-users", "run_id": "run-123"}})])
    residue = Residue(capture_events=True, log_level="DEBUG")
    oxidizer = Oxidizer(formula, catalyst, residue=residue, retention=retention)

    invocations = oxidizer.oxidize_invocations(consumer_name="worker-1", count=10, block=250)

    events = [event for event in residue.dump() if event["event"] in {
        "OXIDIZER: Preparing invocation", "OXIDIZER: Prepared invocation",
    }]
    assert [event["event"] for event in events] == [
        "OXIDIZER: Preparing invocation", "OXIDIZER: Prepared invocation",
    ]
    assert all(event["formula_id"] == "daily-users" and event["run_id"] == "run-123" for event in events)
    assert all(event["invocation_id"] == "1-0" for event in events)
    assert events[-1]["node_count"] == 1
    assert "initial-secret" not in str(events)
    assert invocations[0]["invocation_id"] == "1-0"
    assert invocations[0]["invocation"] == {"formula_id": "daily-users", "run_id": "run-123"}
    assert invocations[0]["run_id"] == "run-123"
    assert invocations[0]["formula_connections"] == {
        "sample_api": {
            "connection": {"type": "api", "base_url": "https://example.test", "password": "initial-secret"},
            "name": "sample_api",
        }
    }
    assert set(invocations[0]["formula_dag"]["nodes"]) == {"daily-users.bronze.fetch_data"}
    initial_state = formula.load_formula_state("daily-users", run_id="run-123")
    assert initial_state["status"] == "created"
    assert initial_state["started_at"] is None
    assert initial_state["completed_at"] is None
    assert initial_state["connection_names"] == ["sample_api"]
    assert "formula_connections" not in initial_state
    assert "initial-secret" not in str(initial_state)
    assert initial_state["dag"]["status"] == {
        "daily-users.bronze.fetch_data": "scheduled"
    }
    assert catalyst.created_groups == [
        ("oxidizer:invocations", "oxidizer-consumer-group"),
        ("oxidizer:controller", "oxidizer-consumer-group"),
    ]
    assert catalyst.created_streams == ["oxidizer:reagent"]
    assert catalyst.reads == [("oxidizer:invocations", "oxidizer-consumer-group", "worker-1", 10, 250)]
    assert catalyst.acks == [("oxidizer:invocations", "oxidizer-consumer-group", "1-0", "worker-1")]
    assert catalyst.expirations == ([] if retention is None else [("oxidizer:invocations", "1-0", 86400)])
    oxidizer.oxidize_formulas()
    assert catalyst.writes[-1][1]["node"]["outputs"]["methods"][0]["retention"] == "12h"


@pytest.mark.parametrize("stream", ["oxidizer:invocations", "oxidizer:invocations:priority"])
def test_invocation_acknowledgement_follows_durable_state(monkeypatch, stream):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_config("orders", {"workflow": {"layers": []}})
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"formula_id": "orders", "run_id": "run-1"}})])
    residue = Residue(capture_events=True, log_level="DEBUG")
    oxidizer = Oxidizer(formula, catalyst, residue=residue)
    acknowledge = catalyst.ack_message

    def acknowledge_after_save(topic_name, group_name, message_id, consumer_name=None):
        state = formula.load_formula_state("orders", "run-1")
        assert state["status"] == "created"
        assert state["invocation_id"] == message_id
        assert formula.list_active_formula_runs() == [("orders", "run-1")]
        acknowledge(topic_name, group_name, message_id, consumer_name)

    monkeypatch.setattr(catalyst, "ack_message", acknowledge_after_save)

    assert len(oxidizer.oxidize_invocations(consumer_name="controller-2", invocation_stream=stream)) == 1
    assert catalyst.acks == [(stream, "oxidizer-consumer-group", "1-0", "controller-2")]
    events = residue.dump()
    prepared = next(index for index, event in enumerate(events) if event["event"] == "OXIDIZER: Prepared invocation")
    acknowledged = next(index for index, event in enumerate(events) if event["event"] == "OXIDIZER: Acknowledged invocation")
    assert prepared < acknowledged
    assert events[acknowledged]["formula_id"] == "orders"
    assert events[acknowledged]["run_id"] == "run-1"
    assert events[acknowledged]["invocation_id"] == "1-0"


@pytest.mark.parametrize("failure_method", ["load_formula_config", "create_formula_state"])
def test_failed_invocation_preparation_is_not_acknowledged(monkeypatch, failure_method):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_config("orders", {"workflow": {"layers": []}})
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"formula_id": "orders", "run_id": "run-1"}})])
    oxidizer = Oxidizer(formula, catalyst)

    def fail(*args, **kwargs):
        raise RuntimeError("storage unavailable")

    monkeypatch.setattr(formula, failure_method, fail)

    with pytest.raises(RuntimeError, match="storage unavailable"):
        oxidizer.oxidize_invocations()

    assert catalyst.acks == []


def test_invocation_batch_acknowledges_only_successfully_prepared_entries(monkeypatch):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_config("orders", {"workflow": {"layers": []}})
    catalyst = FakeCatalyst(messages=[
        ("1-0", {"message": {"formula_id": "orders", "run_id": "run-1"}}),
        ("2-0", {"message": {"formula_id": "orders", "run_id": "run-2"}}),
    ])
    oxidizer = Oxidizer(formula, catalyst)
    save_state = formula.create_formula_state

    def fail_second_save(formula_id, state, run_id):
        if run_id == "run-2":
            raise RuntimeError("second save failed")
        return save_state(formula_id, state, run_id)

    monkeypatch.setattr(formula, "create_formula_state", fail_second_save)

    with pytest.raises(RuntimeError, match="second save failed"):
        oxidizer.oxidize_invocations(count=2)

    assert catalyst.acks == [("oxidizer:invocations", "oxidizer-consumer-group", "1-0", "oxidizer")]
    assert formula.load_formula_state("orders", "run-1")["status"] == "created"
    assert formula.load_formula_state("orders", "run-2") == {}


def test_invocation_acknowledgement_failure_keeps_saved_state(monkeypatch):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_config("orders", {"workflow": {"layers": []}})
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"formula_id": "orders", "run_id": "run-1"}})])
    residue = Residue(capture_events=True, log_level="DEBUG")
    oxidizer = Oxidizer(formula, catalyst, residue=residue)

    def fail(*args, **kwargs):
        raise RuntimeError("acknowledgement failed")

    monkeypatch.setattr(catalyst, "ack_message", fail)

    with pytest.raises(RuntimeError, match="acknowledgement failed"):
        oxidizer.oxidize_invocations()

    assert formula.load_formula_state("orders", "run-1")["invocation_id"] == "1-0"
    assert catalyst.acks == []
    assert not any(event["event"] == "OXIDIZER: Acknowledged invocation" for event in residue.dump())


def test_ignored_invocations_are_not_acknowledged():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {}})])
    oxidizer = Oxidizer(formula, catalyst)

    assert oxidizer.oxidize_invocations() == []
    assert catalyst.acks == []


def test_oxidizer_reads_from_custom_invocation_stream():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    catalyst = FakeCatalyst(messages=[])
    oxidizer = Oxidizer(formula, catalyst)

    invocations = oxidizer.oxidize_invocations(invocation_stream="oxidizer:invocations:priority")

    assert invocations == []
    assert catalyst.created_groups == [
        ("oxidizer:invocations", "oxidizer-consumer-group"),
        ("oxidizer:controller", "oxidizer-consumer-group"),
    ]
    assert catalyst.created_streams == ["oxidizer:reagent"]
    assert catalyst.reads == [("oxidizer:invocations:priority", "oxidizer-consumer-group", "oxidizer", 1, 1000)]


def test_oxidizer_resolves_and_persists_table_output_schema_snapshot():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    schema = {
        "templateVersion": 1,
        "name": "market_quotes",
        "type": "schema",
        "schema": {"version": 1, "columns": [], "write": {"operation": "append"}},
    }
    workflow = {
        "layers": [{
            "name": "market_data",
            "nodes": [{
                "name": "quote",
                "type": "scheduled",
                "outputs": {"methods": [{
                    "method": "s3_tables",
                    "connection": "local_lake",
                    "schema": "market_quotes",
                    "catalog": "local_catalog",
                    "namespace": "analytics",
                    "table": "events",
                }]},
            }],
        }],
    }
    formula.create_formula_config("quotes", {"workflow": workflow})
    formula.create_formula_schema("market_quotes", schema)
    formula.connection.client.values["oxidizer:connections:local_lake"] = {
        "connection": {"type": "rustfs_s3_tables"},
        "name": "local_lake",
    }
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"formula_id": "quotes", "run_id": "run-1"}})])
    oxidizer = Oxidizer(formula, catalyst)

    prepared = oxidizer.oxidize_invocations()

    node_id = "quotes.market_data.quote"
    output = prepared[0]["formula_dag"]["nodes"][node_id]["outputs"]["methods"][0]
    persisted = formula.load_formula_state("quotes", run_id="run-1")
    persisted_output = persisted["dag"]["nodes"][node_id]["outputs"]["methods"][0]
    assert output["schema"] == "market_quotes"
    assert output["schema_document"] == schema
    assert persisted_output["schema_document"] == schema


def test_oxidizer_rejects_table_output_without_named_schema():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_config("quotes", {"workflow": {"layers": [{
        "name": "market_data",
        "nodes": [{"name": "quote", "outputs": {"methods": [{"method": "s3_tables"}]}}],
    }]}})
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"formula_id": "quotes", "run_id": "run-1"}})])
    oxidizer = Oxidizer(formula, catalyst)

    with pytest.raises(ValueError, match="Every table output requires a named schema"):
        oxidizer.oxidize_invocations()

    assert formula.load_formula_state("quotes", run_id="run-1") == {}


def test_oxidizer_extracts_connection_names_from_nested_workflow_config():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(formula, catalyst)

    assert oxidizer._Oxidizer__extract_formula_connection_names(
        {
            "layers": [
                {
                    "nodes": [
                        {
                            "inputs": [
                                {
                                    "methods": [
                                        {"method": "api", "connection": "sample_api"},
                                        {"method": "api", "connection": "sample_api"},
                                        {"method": "api", "connection": "warehouse"},
                                        {"method": "sql", "catalogs": [
                                            {"connection": "warehouse", "catalog": "sales"},
                                            {"connection": "sample_api", "catalog": "people"},
                                        ]},
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ) == ["sample_api", "warehouse"]


def test_oxidizer_dispatches_ready_nodes_to_worker_stream():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.connection.client.values["oxidizer:connections:sample_api"] = {
        "connection": {"type": "api", "password": "current-secret"},
    }
    formula.create_formula_state(
        "daily-users",
        {
            "status": "created",
            "formula_connections": {"sample_api": {"connection": {"type": "api", "password": "old-secret"}}},
            "dag": {
                "nodes": {
                    "daily-users.bronze.fetch_data": {
                        "name": "fetch_data",
                        "layer": "bronze",
                        "checkpoint_metadata": {"node_id": "daily-users.bronze.fetch_data"},
                    },
                    "daily-users.silver.transform_data": {
                        "name": "transform_data",
                        "layer": "silver",
                        "checkpoint_metadata": {"node_id": "daily-users.silver.transform_data"},
                    },
                },
                "edges": {"daily-users.bronze.fetch_data": ["daily-users.silver.transform_data"]},
                "reverse_edges": {"daily-users.silver.transform_data": ["daily-users.bronze.fetch_data"]},
                "status": {
                    "daily-users.bronze.fetch_data": "scheduled",
                    "daily-users.silver.transform_data": "pending",
                },
            },
        },
        run_id="run-123",
    )
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(formula, catalyst)

    dispatched = oxidizer.oxidize_formulas()

    assert dispatched == {("daily-users", "run-123"): ["daily-users.bronze.fetch_data"]}
    assert catalyst.writes[0][0] == "oxidizer:reagent"
    assert catalyst.writes[0][1]["type"] == "start_node"
    assert catalyst.writes[0][1]["formula_id"] == "daily-users"
    assert catalyst.writes[0][1]["run_id"] == "run-123"
    assert catalyst.writes[0][1]["node_id"] == "daily-users.bronze.fetch_data"
    assert catalyst.writes[0][1]["connections"] == {"sample_api": {"connection": {"type": "api", "password": "current-secret"}}}
    stored_state = formula.load_formula_state("daily-users", run_id="run-123")
    assert stored_state["connection_names"] == ["sample_api"]
    assert "formula_connections" not in stored_state
    assert "secret" not in str(stored_state)
    assert formula.load_formula_state("daily-users", run_id="run-123")["dag"]["status"] == {
        "daily-users.bronze.fetch_data": "dispatched",
        "daily-users.silver.transform_data": "pending",
    }


@pytest.mark.parametrize("retention", [None, "5m"])
def test_oxidizer_dispatches_node_to_dedicated_task_stream(retention):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_state(
        "daily-users",
        {
            "status": "created",
            "connection_names": [],
            "dag": {
                "nodes": {
                    "daily-users.bronze.fetch_data": {"name": "fetch_data", "layer": "bronze"},
                    "daily-users.bronze.train_model": {
                        "name": "train_model",
                        "layer": "bronze",
                        "task_stream": "oxidizer:reagent:gpu",
                    },
                },
                "edges": {},
                "reverse_edges": {},
                "status": {
                    "daily-users.bronze.fetch_data": "ready",
                    "daily-users.bronze.train_model": "ready",
                },
            },
        },
        run_id="run-123",
    )
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(formula, catalyst, retention=retention)

    dispatched = oxidizer.oxidize_formulas()

    assert dispatched == {("daily-users", "run-123"): [
        "daily-users.bronze.fetch_data", "daily-users.bronze.train_model",
    ]}
    writes_by_node = {message["node_id"]: topic_name for topic_name, message in catalyst.writes}
    assert writes_by_node == {
        "daily-users.bronze.fetch_data": "oxidizer:reagent",
        "daily-users.bronze.train_model": "oxidizer:reagent:gpu",
    }
    assert catalyst.expirations == ([] if retention is None else [
        ("oxidizer:reagent", "1-0", 300), ("oxidizer:reagent:gpu", "2-0", 300),
    ])
    assert formula.load_formula_state("daily-users", run_id="run-123")["dag"]["status"] == {
        "daily-users.bronze.fetch_data": "dispatched",
        "daily-users.bronze.train_model": "dispatched",
    }


def test_oxidizer_does_not_dispatch_nodes_with_unmet_dependencies():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_state(
        "daily-users",
        {
            "status": "created",
            "formula_connections": {},
            "dag": {
                "nodes": {
                    "daily-users.bronze.fetch_data": {"name": "fetch_data", "layer": "bronze"},
                    "daily-users.silver.transform_data": {"name": "transform_data", "layer": "silver"},
                },
                "edges": {"daily-users.bronze.fetch_data": ["daily-users.silver.transform_data"]},
                "reverse_edges": {"daily-users.silver.transform_data": ["daily-users.bronze.fetch_data"]},
                "status": {
                    "daily-users.bronze.fetch_data": "running",
                    "daily-users.silver.transform_data": "pending",
                },
            },
        },
        run_id="run-123",
    )
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(formula, catalyst)

    dispatched = oxidizer.oxidize_formulas()

    assert dispatched == {("daily-users", "run-123"): []}
    assert catalyst.writes == []
    assert formula.load_formula_state("daily-users", run_id="run-123")["dag"]["status"]["daily-users.silver.transform_data"] == "pending"


@pytest.mark.parametrize("legacy", [False, True])
def test_oxidizer_resolves_rotated_credentials_without_persisting_them(legacy):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    node_id = "daily-users.bronze.fetch_data"
    references = {"formula_connections": {"sample_api": {"password": "embedded-secret"}}} if legacy else {"connection_names": ["sample_api"]}
    formula.create_formula_state("daily-users", {
        "status": "pending",
        **references,
        "dag": {
            "nodes": {node_id: {"name": "fetch_data", "layer": "bronze"}},
            "edges": {}, "reverse_edges": {}, "status": {node_id: "ready"},
        },
    }, run_id="run-123")
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(formula, catalyst)

    for batch_index, password in enumerate(("first-secret", "rotated-secret"), start=1):
        formula.connection.client.values["oxidizer:connections:sample_api"] = {"connection": {"type": "api", "password": password}}
        oxidizer.oxidize_formulas()
        assert catalyst.writes[-1][1]["connections"]["sample_api"]["connection"]["password"] == password
        if legacy:
            state = formula.load_formula_state("daily-users", run_id="run-123")
            state.pop("connection_names")
            state["formula_connections"] = {"sample_api": {"password": "embedded-secret"}}
            formula.update_formula_state("daily-users", state, run_id="run-123")
        catalyst.messages = [(f"{batch_index}-0", {"message": {
            "type": "checkpoint", "formula_id": "daily-users", "run_id": "run-123", "node_id": node_id,
            "checkpoint_metadata": {"is_final": False, "batch_index": batch_index},
        }})]
        oxidizer.oxidize_reagent_updates()
        state = formula.load_formula_state("daily-users", run_id="run-123")
        assert state["connection_names"] == ["sample_api"]
        assert "formula_connections" not in state
        assert "secret" not in str(state)


def test_oxidizer_missing_connection_does_not_publish_task_or_change_state():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    node_id = "daily-users.bronze.fetch_data"
    state = {
        "status": "pending", "connection_names": ["missing_api"],
        "dag": {
            "nodes": {node_id: {"name": "fetch_data", "layer": "bronze"}},
            "edges": {}, "reverse_edges": {}, "status": {node_id: "ready"},
        },
    }
    formula.create_formula_state("daily-users", state, run_id="run-123")
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(formula, catalyst)

    with pytest.raises(ValueError, match="Connection not found: missing_api"):
        oxidizer.oxidize_formulas()
    assert catalyst.writes == []
    assert formula.load_formula_state("daily-users", run_id="run-123") == state


@pytest.mark.parametrize("retention", [None, "12h"])
def test_oxidizer_processes_valid_reagent_update_and_acks_message(retention):
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_state(
        "daily-users",
        {
            "status": "dispatched",
            "dag": {
                "nodes": {
                    "daily-users.bronze.fetch_data": {
                        "name": "fetch_data",
                        "layer": "bronze",
                    }
                },
                "edges": {},
                "reverse_edges": {},
                "status": {"daily-users.bronze.fetch_data": "dispatched"},
            },
        },
        run_id="run-123",
    )
    catalyst = FakeCatalyst(
        messages=[
            (
                "1-0",
                {
                    "message": {
                        "type": "started",
                        "formula_id": "daily-users",
                        "run_id": "run-123",
                        "node_id": "daily-users.bronze.fetch_data",
                    }
                },
            )
        ]
    )
    residue = Residue(capture_events=True, log_level="DEBUG")
    oxidizer = Oxidizer(formula, catalyst, residue=residue, retention=retention)

    processed = oxidizer.oxidize_reagent_updates(consumer_name="controller-1")

    event = next(event for event in residue.dump() if event["event"] == "OXIDIZER: Processed reagent update")
    assert event["formula_id"] == "daily-users"
    assert event["run_id"] == "run-123"
    assert event["node_id"] == "daily-users.bronze.fetch_data"
    assert event["update_id"] == "1-0"
    assert event["update_type"] == "started"
    assert event["node_status"] == event["run_status"] == "running"

    assert processed == [
        {
            "type": "started",
            "formula_id": "daily-users",
            "run_id": "run-123",
            "node_id": "daily-users.bronze.fetch_data",
            "checkpoint_metadata": None,
            "error_details": None,
            "heartbeat": None,
        }
    ]
    assert formula.load_formula_state("daily-users", run_id="run-123")["dag"]["status"] == {
        "daily-users.bronze.fetch_data": "running"
    }
    assert formula.load_formula_state("daily-users", run_id="run-123")["status"] == "running"
    started_state = formula.load_formula_state("daily-users", run_id="run-123")
    assert started_state["started_at"].endswith("Z")
    assert started_state["completed_at"] is None
    assert catalyst.reads == [("oxidizer:controller", "oxidizer-consumer-group", "controller-1", 1, 1000)]
    assert catalyst.acks == [("oxidizer:controller", "oxidizer-consumer-group", "1-0", "controller-1")]
    assert catalyst.expirations == ([] if retention is None else [("oxidizer:controller", "1-0", 43200)])


def test_oxidizer_empty_reads_do_not_refresh_retention():
    catalyst = FakeCatalyst()
    oxidizer = Oxidizer(Formula(RedisConnection(client=FakeRedisClient())), catalyst, retention="1h")
    oxidizer.oxidize_invocations(block=1)
    oxidizer.oxidize_reagent_updates(block=1)
    assert catalyst.expirations == []


def test_oxidizer_rejects_invalid_retention_before_creating_streams():
    catalyst = FakeCatalyst()
    with pytest.raises(ValueError, match="retention"):
        Oxidizer(Formula(RedisConnection(client=FakeRedisClient())), catalyst, retention="invalid")
    assert catalyst.created_streams == []
    assert catalyst.created_groups == []


def test_oxidizer_retries_update_when_state_persistence_fails_before_ack(monkeypatch):
    from copy import deepcopy

    formula = Formula(RedisConnection(client=FakeRedisClient()))
    node_id = "daily-users.bronze.fetch_data"
    formula.create_formula_state(
        "daily-users",
        {
            "status": "dispatched",
            "dag": {
                "nodes": {node_id: {"name": "fetch_data", "layer": "bronze"}},
                "edges": {},
                "reverse_edges": {},
                "status": {node_id: "dispatched"},
            },
        },
        run_id="run-123",
    )
    update = {
        "type": "checkpoint",
        "formula_id": "daily-users",
        "run_id": "run-123",
        "node_id": node_id,
        "checkpoint_metadata": {"node_id": node_id, "is_final": True},
    }
    catalyst = FakeCatalyst(messages=[("checkpoint-1", {"message": update})])
    oxidizer = Oxidizer(formula, catalyst)
    load_state = formula.load_formula_state
    monkeypatch.setattr(
        formula,
        "load_formula_state",
        lambda *args, **kwargs: deepcopy(load_state(*args, **kwargs)),
    )
    persist = formula.update_formula_state
    failed_once = False

    def fail_once(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise RuntimeError("simulated state persistence failure")
        return persist(*args, **kwargs)

    monkeypatch.setattr(formula, "update_formula_state", fail_once)

    assert oxidizer.oxidize_reagent_updates() == []
    assert catalyst.acks == []
    assert formula.load_formula_state("daily-users", run_id="run-123")["dag"]["status"][node_id] == "dispatched"

    processed = oxidizer.oxidize_reagent_updates()

    assert len(processed) == 1
    assert formula.load_formula_state("daily-users", run_id="run-123")["dag"]["status"][node_id] == "success"
    assert catalyst.acks == [
        ("oxidizer:controller", "oxidizer-consumer-group", "checkpoint-1", "oxidizer")
    ]


def test_oxidizer_persists_checkpoint_and_redispatches_with_metadata():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    node_id = "daily-users.bronze.fetch_data"
    formula.create_formula_state(
        "daily-users",
        {
            "status": "running",
            "dag": {
                "nodes": {node_id: {"name": "fetch_data", "layer": "bronze"}},
                "edges": {},
                "reverse_edges": {},
                "status": {node_id: "running"},
            },
        },
        run_id="run-123",
    )
    checkpoint = {
        "node_id": node_id,
        "batch_index": 1,
        "batch_cursors": {"orders": None},
        "batch_final": {"orders": False},
        "records_processed": 50,
        "total_records_so_far": 50,
        "is_final": False,
    }
    catalyst = FakeCatalyst(messages=[("checkpoint-1", {"message": {
        "type": "checkpoint",
        "formula_id": "daily-users",
        "run_id": "run-123",
        "node_id": node_id,
        "checkpoint_metadata": checkpoint,
    }})])
    oxidizer = Oxidizer(formula, catalyst)

    oxidizer.oxidize_reagent_updates()
    state = formula.load_formula_state("daily-users", run_id="run-123")
    assert state["dag"]["status"][node_id] == "ready"
    assert state["dag"]["nodes"][node_id]["checkpoint_metadata"] == checkpoint
    dispatched = oxidizer.oxidize_formulas()
    state = formula.load_formula_state("daily-users", run_id="run-123")

    assert state["dag"]["status"][node_id] == "dispatched"
    assert state["dag"]["nodes"][node_id]["checkpoint_metadata"] == checkpoint
    assert dispatched == {("daily-users", "run-123"): [node_id]}
    task = catalyst.writes[-1][1]
    assert task["node_id"] == node_id
    assert task["checkpoint_metadata"] == checkpoint


def test_oxidizer_marks_run_success_after_final_checkpoint():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_state(
        "daily-users",
        {
            "status": "dispatched",
            "dag": {
                "nodes": {
                    "daily-users.bronze.fetch_data": {"name": "fetch_data"},
                    "daily-users.silver.transform_data": {"name": "transform_data"},
                },
                "edges": {},
                "reverse_edges": {},
                "status": {
                    "daily-users.bronze.fetch_data": "success",
                    "daily-users.silver.transform_data": "running",
                },
            },
        },
        run_id="run-123",
    )
    catalyst = FakeCatalyst(messages=[(
        "2-0",
        {"message": {
            "type": "checkpoint",
            "formula_id": "daily-users",
            "run_id": "run-123",
            "node_id": "daily-users.silver.transform_data",
            "checkpoint_metadata": {
                "batch_index": 1,
                "records_processed": 10,
                "is_final": True,
            },
        }},
    )])
    oxidizer = Oxidizer(formula, catalyst)

    oxidizer.oxidize_reagent_updates()

    state = formula.load_formula_state("daily-users", run_id="run-123")
    assert state["status"] == "success"
    assert formula.list_active_formula_runs() == []
    assert state["started_at"].endswith("Z")
    assert state["completed_at"].endswith("Z")
    assert state["dag"]["status"] == {
        "daily-users.bronze.fetch_data": "success",
        "daily-users.silver.transform_data": "success",
    }
    assert state["dag"]["nodes"]["daily-users.silver.transform_data"]["checkpoint_metadata"]["is_final"] is True


def test_oxidizer_sets_completed_at_when_run_fails():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    node_id = "daily-users.extract.fetch"
    formula.create_formula_state(
        "daily-users",
        {
            "status": "running",
            "started_at": "2026-10-05T12:00:00Z",
            "dag": {"nodes": {node_id: {}}, "status": {node_id: "running"}},
        },
        run_id="run-123",
    )
    catalyst = FakeCatalyst(messages=[("3-0", {"message": {
        "type": "failed",
        "formula_id": "daily-users",
        "run_id": "run-123",
        "node_id": node_id,
        "error_details": {"error_type": "RuntimeError"},
    }})])
    Oxidizer(formula, catalyst).oxidize_reagent_updates()
    state = formula.load_formula_state("daily-users", run_id="run-123")
    assert state["status"] == "failed"
    assert formula.list_active_formula_runs() == []
    assert state["started_at"] == "2026-10-05T12:00:00Z"
    assert state["completed_at"].endswith("Z")


def test_oxidizer_logs_malformed_reagent_update_without_ack():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"type": "started"}})])
    oxidizer = Oxidizer(formula, catalyst)

    processed = oxidizer.oxidize_reagent_updates()

    assert processed == []
    assert catalyst.acks == []


def test_archived_failed_run_ignores_late_updates_and_repairs_stale_marker():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    node_id = "orders.source.fetch"
    state = {"status": "failed", "dag": {
        "nodes": {node_id: {}}, "status": {node_id: "ready"}, "reverse_edges": {},
    }}
    formula.create_formula_state("orders", state, "run-1")
    formula.formula._write_active_marker("orders", "run-1")
    catalyst = FakeCatalyst(messages=[("late-1", {"message": {
        "type": "started", "formula_id": "orders", "run_id": "run-1", "node_id": node_id,
    }})])
    controller = Oxidizer(formula, catalyst)
    assert controller.oxidize_formulas() == {("orders", "run-1"): []}
    assert formula.list_active_formula_runs() == []
    controller.oxidize_reagent_updates()
    assert formula.load_formula_state("orders", "run-1") == state
    assert catalyst.writes == []
    assert len(catalyst.acks) == 1

def test_oxidizer_run_continues_after_tick_errors_until_stopped(monkeypatch):
    oxidizer = Oxidizer(Formula(S3Connection(bucket="demo", client=FakeS3Client())), FakeCatalyst())
    ticks = []

    def oxidize_once(count=1, block=1000):
        ticks.append((count, block))
        if len(ticks) == 1:
            raise RuntimeError("transient outage")
        if len(ticks) == 3:
            oxidizer.stop()

    monkeypatch.setattr(oxidizer, "oxidize_once", oxidize_once)

    oxidizer.oxidize(count=5, block=10, error_backoff_seconds=0)

    assert ticks == [(5, 10)] * 3


def test_oxidizer_loop_exits_cleanly_on_keyboard_interrupt(monkeypatch):
    oxidizer = Oxidizer(Formula(S3Connection(bucket="demo", client=FakeS3Client())), FakeCatalyst())

    def oxidize_once(count=1, block=1000):
        raise KeyboardInterrupt

    monkeypatch.setattr(oxidizer, "oxidize_once", oxidize_once)

    oxidizer.oxidize()


def test_oxidizer_shutdown_invocation_stops_run_after_current_tick():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    catalyst = FakeCatalyst(messages=[("1-0", {"message": {"type": "shutdown"}})])
    oxidizer = Oxidizer(formula, catalyst)

    oxidizer.oxidize(block=0)

    assert formula.load_formula_state("shutdown", run_id="1-0") == {}
    assert [read[0] for read in catalyst.reads] == ["oxidizer:invocations", "oxidizer:controller"]
