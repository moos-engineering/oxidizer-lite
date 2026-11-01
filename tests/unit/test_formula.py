from io import BytesIO

import pytest

from oxidizer import Formula, NodeStatus, RedisConnection, S3Connection


class FakeS3Client:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def list_objects_v2(self, Bucket, Prefix, **kwargs):
        contents = [
            {"Key": key}
            for bucket, key in self.objects
            if bucket == Bucket and key.startswith(Prefix)
        ]
        return {"Contents": contents}

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)


class FakeRedisJsonClient:
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def set(self, key, path, data):
        if path == "$":
            self.redis_client.values[key] = data
            return

        current_value = self.redis_client.values[key]
        parts = path.removeprefix("$.").split(".")
        for part in parts[:-1]:
            current_value = current_value[part]
        current_value[parts[-1]] = data

    def get(self, key):
        return self.redis_client.values.get(key)

    def delete(self, key, path):
        self.redis_client.values.pop(key, None)


class FakeRedisClient:
    def __init__(self):
        self.values = {}

    def json(self):
        return FakeRedisJsonClient(self)

    def scan_iter(self, match):
        prefix = match.removesuffix("*")
        return [key for key in self.values if key.startswith(prefix)]


@pytest.mark.parametrize("code", ["NoSuchKey", "NotFound", "404"])
def test_s3_missing_state_returns_empty_mapping(code):
    from botocore.exceptions import ClientError

    class MissingStateClient(FakeS3Client):
        def get_object(self, Bucket, Key):
            raise ClientError({"Error": {"Code": code, "Message": "Not found"}}, "GetObject")

    formula = Formula(S3Connection(bucket="demo", client=MissingStateClient()))

    assert formula.load_formula_state("orders", "not-created-yet") == {}


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchBucket", "ServiceUnavailable"])
def test_s3_state_storage_errors_are_not_hidden(code):
    from botocore.exceptions import ClientError

    class FailingStateClient(FakeS3Client):
        def get_object(self, Bucket, Key):
            raise ClientError({"Error": {"Code": code, "Message": "Storage error"}}, "GetObject")

    formula = Formula(S3Connection(bucket="demo", client=FailingStateClient()))

    with pytest.raises(ClientError):
        formula.load_formula_state("orders", "run-1")


def test_formula_uses_redis_backend_with_injected_client():
    connection = RedisConnection(client=FakeRedisClient())

    formula = Formula(connection)

    assert formula.list_formula_configs() == []


@pytest.fixture(params=["s3", "redis"])
def indexed_formula(request):
    if request.param == "s3":
        return Formula(S3Connection(bucket="demo", prefix="formulae", client=FakeS3Client()))
    return Formula(RedisConnection(client=FakeRedisClient()))


@pytest.mark.parametrize("status", ["success", "failed", "cancelled", "timed_out", "skipped"])
def test_terminal_runs_leave_active_index_but_keep_history(indexed_formula, status):
    formula = indexed_formula
    formula.create_formula_state("orders", {"status": "running"}, run_id="run-1")
    assert formula.list_active_formula_runs() == [("orders", "run-1")]
    formula.update_formula_state("orders", {"status": status}, run_id="run-1")
    assert formula.list_active_formula_runs() == []
    assert formula.list_formula_runs() == [("orders", "run-1")]
    assert formula.load_formula_state("orders", "run-1") == {"status": status}


def test_markers_reconcile_legacy_missing_stale_and_orphan_entries(indexed_formula):
    formula = indexed_formula
    formula.create_formula_state("orders", {"status": "running"}, "active")
    formula.create_formula_state("orders", {"status": "failed"}, "archived")
    formula.formula._delete_active_marker("orders", "active")
    formula.formula._write_active_marker("orders", "archived")
    formula.formula._write_active_marker("orders", "missing")
    assert formula.reconcile_active_formula_runs() == [("orders", "active")]
    formula.expire_formula_state("orders", "active")
    assert formula.list_active_formula_runs() == []


def test_partial_redis_update_uses_persisted_status_for_marker():
    formula = Formula(RedisConnection(client=FakeRedisClient()))
    formula.create_formula_state("orders", {"status": "running"}, "run-1")
    formula.update_formula_state("orders", {"status": "failed"}, "run-1", path="$.status")
    assert formula.list_active_formula_runs() == []
    formula.update_formula_state("orders", {"checkpoint": {}}, "run-1", path="$.checkpoint")
    assert formula.list_active_formula_runs() == []


def test_s3_state_and_marker_listings_follow_continuation_tokens():
    class PaginatedClient(FakeS3Client):
        def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
            keys = sorted(key for bucket, key in self.objects if bucket == Bucket and key.startswith(Prefix))
            offset = int(ContinuationToken or 0)
            response = {"Contents": [{"Key": key} for key in keys[offset:offset + 1]],
                        "IsTruncated": offset + 1 < len(keys)}
            if response["IsTruncated"]:
                response["NextContinuationToken"] = str(offset + 1)
            return response

    formula = Formula(S3Connection(bucket="demo", client=PaginatedClient()))
    for run_id in ("one", "two", "three"):
        formula.create_formula_state("orders", {"status": "running"}, run_id)
    assert len(formula.list_formula_runs()) == len(formula.list_active_formula_runs()) == 3


def test_failed_state_write_keeps_active_marker(indexed_formula, monkeypatch):
    formula = indexed_formula
    formula.create_formula_state("orders", {"status": "running"}, "run-1")

    def fail(*args, **kwargs):
        raise RuntimeError("storage unavailable")

    client = formula.connection.client
    if formula.formula.backend == "s3":
        monkeypatch.setattr(client, "put_object", fail)
    else:
        monkeypatch.setattr(client, "json", fail)
    with pytest.raises(RuntimeError, match="storage unavailable"):
        formula.update_formula_state("orders", {"status": "success"}, "run-1")
    assert formula.list_active_formula_runs() == [("orders", "run-1")]


def test_api_lists_active_and_archived_states_without_marker_reads(indexed_formula, monkeypatch):
    from oxidizer.utils.topology import Topology
    from starlette.testclient import TestClient

    formula = indexed_formula
    formula.create_formula_config("orders", {"layers": []})
    for run_id, status in (("one", "running"), ("two", "success"), ("three", "failed")):
        formula.create_formula_state("orders", {"status": status}, run_id)

    def fail():
        pytest.fail("History must not depend on the active marker index")

    monkeypatch.setattr(formula, "list_active_formula_runs", fail)
    topology = Topology(formula, catalyst=None)
    topology.register_api_endpoints()
    client = TestClient(topology.app)
    response = client.get("/api/formulas/orders/runs")
    assert response.status_code == 200
    assert {run["status"] for run in response.json()["runs"]} == {"running", "success", "failed"}


def test_formula_uses_redis_json_backend_with_injected_client():
    client = FakeRedisClient()
    connection = RedisConnection(client=client)
    formula = Formula(connection)

    config = {"layers": [{"name": "extract", "nodes": [{"name": "users"}]}]}
    connection_config = {"type": "postgres", "name": "warehouse"}
    state = {"status": "created"}

    formula.create_formula_config("daily-users", config)
    formula.create_formula_connection("warehouse", connection_config)
    formula.create_formula_state("daily-users", state, run_id="run-123")

    assert formula.load_formula_config("daily-users") == config
    assert formula.load_formula_connection("warehouse") == connection_config
    assert formula.list_formula_connections() == ["warehouse"]
    assert formula.load_formula_state("daily-users", run_id="run-123") == state
    assert formula.list_formula_configs() == ["daily-users"]
    assert formula.list_formula_states() == ["daily-users:run-123"]
    assert formula.list_active_formula_runs() == [("daily-users", "run-123")]

    formula.update_formula_state("daily-users", {"status": "updated"}, run_id="run-123")
    assert formula.load_formula_state("daily-users", run_id="run-123") == {"status": "updated"}

    formula.update_formula_state(
        "daily-users",
        {"status": "patched", "checkpoint": {"records_processed": 10}},
        run_id="run-123",
        path="$.checkpoint",
    )
    assert formula.load_formula_state("daily-users", run_id="run-123") == {
        "status": "updated",
        "checkpoint": {"records_processed": 10},
    }

    formula.expire_formula_state("daily-users", run_id="run-123")
    assert formula.list_formula_states() == []


def test_formula_persists_named_schema_documents_in_redisjson():
    client = FakeRedisClient()
    formula = Formula(RedisConnection(client=client))
    schema = {"templateVersion": 1, "name": "market_quotes", "type": "schema", "schema": {"version": 1}}

    formula.create_formula_schema("market_quotes", schema)

    assert formula.load_formula_schema("market_quotes") == schema
    assert formula.list_formula_schemas() == ["market_quotes"]
    assert client.values["oxidizer:schemas:market_quotes"] == schema


def test_formula_persists_named_schema_documents_in_s3_yaml():
    client = FakeS3Client()
    connection = S3Connection(bucket="demo", prefix="formulae", client=client)
    formula = Formula(connection)
    schema = {"templateVersion": 1, "name": "market_quotes", "type": "schema", "schema": {"version": 1}}

    formula.create_formula_schema("market_quotes", schema)

    assert formula.load_formula_schema("market_quotes") == schema
    assert formula.list_formula_schemas() == ["market_quotes"]
    assert ("demo", "formulae/schemas/market_quotes.yml") in client.objects


def test_formula_uses_redis_json_backend_with_run_scoped_state():
    client = FakeRedisClient()
    connection = RedisConnection(client=client)
    formula = Formula(connection)

    formula.create_formula_state("daily-users", {"status": "created"}, run_id="run-123")

    assert formula.load_formula_state("daily-users", run_id="run-123") == {"status": "created"}
    assert client.values["oxidizer:states:daily-users:run-123"] == {"status": "created"}

    formula.update_formula_state("daily-users", {"status": "updated"}, run_id="run-123")
    assert formula.load_formula_state("daily-users", run_id="run-123") == {"status": "updated"}

    formula.expire_formula_state("daily-users", run_id="run-123")
    assert formula.load_formula_state("daily-users", run_id="run-123") == {}


def test_formula_uses_s3_backend_with_injected_client():
    client = FakeS3Client()
    connection = S3Connection(bucket="demo", prefix="formulae", client=client)
    formula = Formula(connection)

    config = {"layers": [{"name": "extract", "nodes": [{"name": "users"}]}]}
    connection_config = {"type": "postgres", "name": "warehouse"}
    state = {"status": "created"}

    formula.create_formula_config("daily-users", config)
    formula.create_formula_connection("warehouse", connection_config)
    formula.create_formula_state("daily-users", state, run_id="run-123")

    assert formula.load_formula_config("daily-users") == config
    assert formula.load_formula_connection("warehouse") == connection_config
    assert formula.list_formula_connections() == ["warehouse"]
    assert formula.load_formula_state("daily-users", run_id="run-123") == state
    assert formula.list_formula_configs() == ["daily-users"]
    assert formula.list_formula_states() == ["daily-users/run-123"]
    assert formula.list_active_formula_runs() == [("daily-users", "run-123")]
    assert ("demo", "formulae/connections/warehouse.yml") in client.objects

    formula.update_formula_state("daily-users", {"status": "updated"}, run_id="run-123", path="$.status")
    assert formula.load_formula_state("daily-users", run_id="run-123") == {"status": "updated"}

    formula.expire_formula_state("daily-users", run_id="run-123")
    assert formula.list_formula_states() == []


def test_formula_uses_s3_backend_with_run_scoped_state():
    client = FakeS3Client()
    connection = S3Connection(bucket="demo", prefix="formulae", client=client)
    formula = Formula(connection)

    formula.create_formula_state("daily-users", {"status": "created"}, run_id="run-123")

    assert formula.load_formula_state("daily-users", run_id="run-123") == {"status": "created"}
    assert ("demo", "formulae/states/daily-users/run-123.yml") in client.objects

    formula.update_formula_state("daily-users", {"status": "updated"}, run_id="run-123")
    assert formula.load_formula_state("daily-users", run_id="run-123") == {"status": "updated"}

    formula.expire_formula_state("daily-users", run_id="run-123")
    assert ("demo", "formulae/states/daily-users/run-123.yml") not in client.objects


def test_formula_saves_and_loads_analysis_summary_in_redis_and_s3():
    # Test Redis backend
    redis_client = FakeRedisClient()
    redis_formula = Formula(RedisConnection(client=redis_client))
    redis_formula.create_formula_state("orders", {"status": "failed"}, run_id="run-1")

    analysis_payload = {"formula_id": "orders", "run_id": "run-1", "analysis": "Root cause: extraction failure"}
    redis_formula.save_formula_analysis("orders", "run-1", analysis_payload)

    loaded_redis = redis_formula.load_formula_analysis("orders", "run-1")
    assert loaded_redis["analysis"] == "Root cause: extraction failure"
    assert "run-1" in redis_formula.list_formula_analyses("orders")[0]

    # Test S3 backend
    s3_client = FakeS3Client()
    s3_formula = Formula(S3Connection(bucket="bucket", prefix="prefix", client=s3_client))
    s3_formula.create_formula_state("orders", {"status": "failed"}, run_id="run-1")

    s3_formula.save_formula_analysis("orders", "run-1", analysis_payload)
    loaded_s3 = s3_formula.load_formula_analysis("orders", "run-1")
    assert loaded_s3["analysis"] == "Root cause: extraction failure"
    assert "run-1" in s3_formula.list_formula_analyses("orders")[0]


def test_formula_dag_builds_nodes_edges_and_statuses():
    connection = RedisConnection(client=object())
    formula = Formula(connection)
    config = {
        "layers": [
            {
                "name": "extract",
                "nodes": [
                    {"name": "users", "type": "scheduled"},
                ],
            },
            {
                "name": "load",
                "nodes": [
                    {
                        "name": "warehouse",
                        "inputs": [{"ref": "demo.extract.users"}],
                    },
                ],
            },
        ],
    }

    dag = formula.dag("demo", config)

    assert set(dag["nodes"]) == {"demo.extract.users", "demo.load.warehouse"}
    assert dag["edges"] == {"demo.extract.users": ["demo.load.warehouse"]}
    assert dag["reverse_edges"] == {"demo.load.warehouse": ["demo.extract.users"]}
    assert dag["status"]["demo.extract.users"] is NodeStatus.SCHEDULED
    assert dag["status"]["demo.load.warehouse"] is NodeStatus.PENDING
    assert dag["nodes"]["demo.extract.users"]["checkpoint_metadata"]["node_id"] == "demo.extract.users"

    

def test_formula_dag_qualifies_layer_and_node_ids_and_rejects_dotted_names():
    import pytest

    formula = Formula(RedisConnection(client=FakeRedisClient()))
    dag = formula.dag("weather", {"layers": [
        {"name": "source", "nodes": [{"name": "fetch", "type": "scheduled"}]},
        {"name": "history", "nodes": [{"name": "track", "inputs": [{"ref": "weather.source.fetch"}]}]},
    ]})

    node = dag["nodes"]["weather.history.track"]
    assert (node["layer_id"], node["layer_name"], node["node_name"]) == ("weather.history", "history", "track")
    assert "layer" not in node and "name" not in node
    assert dag["reverse_edges"] == {"weather.history.track": ["weather.source.fetch"]}

    with pytest.raises(ValueError, match="layer name"):
        formula.dag("weather", {"layers": [{"name": "bad.layer", "nodes": []}]})
    with pytest.raises(ValueError, match="formula_id"):
        formula.dag("bad.formula", {"layers": []})
