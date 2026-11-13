import json
from pathlib import Path

import pytest
import yaml

from oxidizer import Anvil, Crucible, DataHandler, Formula, S3Connection


ROOT = Path(__file__).parents[2] / "examples/quickstart"


@pytest.mark.parametrize("backend", ["redis", "kafka"])
def test_quickstart_bronze_silver_workflow_and_schemas(backend):
    runtime = yaml.safe_load((ROOT / f"runtime.{backend}.yml").read_text())
    workflow = yaml.safe_load((ROOT / runtime["quickstart_seed"]["workflow_file"]).read_text())
    assert workflow["name"] == f"quickstart_observations_{backend}"
    dag = Formula(S3Connection(bucket="unused", client=object())).dag(workflow["name"], workflow["workflow"])
    source_id = f"{workflow['name']}.bronze.raw_observations"
    sql_id = f"{workflow['name']}.silver.read_observations"
    history_id = f"{workflow['name']}.silver.transform_observation_history"
    assert set(dag["nodes"]) == {source_id, sql_id, history_id}
    assert dag["reverse_edges"][sql_id] == [source_id]
    assert dag["reverse_edges"][history_id] == [sql_id]
    assert [method["method"] for method in dag["nodes"][source_id]["outputs"]["methods"]] == ["s3_tables"]
    sql_method = dag["nodes"][sql_id]["inputs"][0]["methods"][0]
    assert sql_method["sql_name"] == "quickstart_observations"
    assert sql_method["catalogs"] == [{"connection": "quickstart_lake", "catalog": "local_catalog"}]
    assert dag["nodes"][sql_id]["outputs"]["methods"][0]["method"] == "stream"
    assert dag["nodes"][history_id]["inputs"][0]["methods"][0]["method"] == "stream"
    schemas = {
        document["name"]: document
        for document in (yaml.safe_load(path.read_text()) for path in (ROOT / "schemas").glob("*.yml"))
    }
    for node in dag["nodes"].values():
        for method in node["outputs"]["methods"]:
            if method["method"] == "s3_tables":
                assert method["connection"] == "quickstart_lake"
                Crucible._schema_context(schemas[method["schema"]])
    if backend == "kafka":
        assert runtime["catalyst"]["consumer_config"]["enable.auto.commit"] is False
        assert runtime["controller"]["kafka_group_id"] != runtime["worker"]["kafka_group_id"]


@pytest.mark.parametrize("backend", ["redis", "kafka"])
def test_quickstart_named_sql_pages_feed_json_stream_to_history(backend, monkeypatch):
    pytest.importorskip("duckdb")
    from examples.quickstart.custom import quickstart_router

    workflow = yaml.safe_load((ROOT / f"workflows/workflow.{backend}.yml").read_text())
    dag = Formula(S3Connection(bucket="unused", client=object())).dag(workflow["name"], workflow["workflow"])
    sql_id = f"{workflow['name']}.silver.read_observations"
    sql_node = dag["nodes"][sql_id]
    method = dict(sql_node["inputs"][0]["methods"][0])
    method["query"] = (ROOT / f"sql/{method['sql_name']}.sql").read_text()
    method["batch_size"] = 2
    connection = yaml.safe_load((ROOT / "connections/local-lake.yml").read_text())

    def attach(crucible, bucket, catalog):
        engine = crucible.adapter.engine
        engine.execute(f"ATTACH ':memory:' AS {catalog}")
        engine.execute(f"CREATE SCHEMA {catalog}.analytics")
        engine.execute(f"""
            CREATE TABLE {catalog}.analytics.quickstart_observation_upsert AS
            SELECT 'entity-1' AS entity_id,
                   TIMESTAMPTZ '2026-01-01 00:00:00+00' + range * INTERVAL '1 hour' AS observed_at,
                   TIMESTAMPTZ '2026-10-01 00:00:00+00' AS source_revision,
                   1.5::DOUBLE AS metric_value, 'nominal' AS status, range AS reading
            FROM range(3)
        """)

    monkeypatch.setattr(Crucible, "attach", attach)

    class MemoryCatalyst:
        def __init__(self):
            self.messages = []

        def create_stream(self, stream):
            assert stream == sql_id

        def write_to_stream(self, stream, record):
            self.messages.append(json.loads(json.dumps(record)))
            return f"{len(self.messages)}-0"

        def set_ttl(self, stream, message_id, ttl):
            assert stream == sql_id and ttl == 180

        def create_consumer_group(self, stream, group):
            assert stream == sql_id

        def read_from_stream(self, stream, group, consumer, count, block):
            return [(f"{index + 1}-0", {"message": record}) for index, record in enumerate(self.messages[:count])]

    catalyst = MemoryCatalyst()
    handler = DataHandler(catalyst=catalyst)
    catalogs = handler._task_connection(method, {"quickstart_lake": connection})
    first = handler.incoming_data(method, catalogs)
    second = handler.incoming_data(method, catalogs, context={"cursor": first.next_cursor})
    assert not first.is_final and second.is_final
    assert [len(first.data), len(second.data)] == [2, 1]
    for page in (first.data, second.data):
        rows = quickstart_router(page, {"node_name": "read_observations", "node_id": sql_id})
        handler.outgoing_data(sql_node["outputs"]["methods"][0], data=rows, context={"node_id": sql_id})
    history_id = f"{workflow['name']}.silver.transform_observation_history"
    history_method = dag["nodes"][history_id]["inputs"][0]["methods"][0]
    history_rows = handler.incoming_data(history_method, context={"input_ref": sql_id}).data
    assert [row["reading"] for row in history_rows] == [0, 1, 2]
    assert all(row["observed_at"].endswith("Z") and row["source_revision"].endswith("Z") for row in history_rows)
    assert quickstart_router(history_rows, {"node_name": "transform_observation_history"}) == history_rows


@pytest.mark.parametrize("backend", ["redis", "kafka"])
def test_quickstart_compose_uses_split_images_and_disposable_storage(backend):
    compose = yaml.safe_load((ROOT / f"compose.{backend}.yml").read_text())
    broker = "redis" if backend == "redis" else "redpanda"
    assert compose["name"] == "oxidizer-quickstart"
    assert not compose.get("volumes")
    expected_services = {"rustfs", broker, "seed", "api", "controller", "worker"}
    if backend == "redis":
        expected_services.add("special-worker")
    assert set(compose["services"]) == expected_services
    for name in (broker, "rustfs"):
        assert not compose["services"][name].get("volumes")
    if backend == "redis":
        assert compose["services"]["redis"]["command"] == ["redis-server", "--appendonly", "no", "--save", ""]
    runtime_services = ["seed", "controller", "worker"]
    if backend == "redis":
        runtime_services.append("special-worker")
    for name in (*runtime_services, "api"):
        service = compose["services"][name]
        assert service["image"] == f"ghcr.io/moos-engineering/oxidizer-lite:{'latest-api' if name == 'api' else 'latest'}"
        assert service["pull_policy"] == "always"
        assert "platform" not in service
        assert "build" not in service
        assert service["command"][service["command"].index("--config") + 1] == f"/example/runtime.{backend}.yml"
        assert len(service["volumes"]) == 1
    for name in ("api", "controller", "worker"):
        assert compose["services"][name]["depends_on"][broker]["condition"] == "service_healthy"
    if backend == "redis":
        special_worker = compose["services"]["special-worker"]
        assert special_worker["command"][-2:] == ["--worker", "special-worker"]
        assert special_worker["depends_on"]["redis"]["condition"] == "service_healthy"


def test_quickstart_parquet_fixture_is_reproducible(tmp_path):
    pq = pytest.importorskip("pyarrow.parquet")
    from examples.quickstart.parquet_generator import write_observations

    first, second = tmp_path / "first.parquet", tmp_path / "second.parquet"
    assert write_observations(first, size=4, entities=3) == 12
    write_observations(second, size=4, entities=3)
    assert first.read_bytes() == second.read_bytes()
    table = pq.read_table(first)
    assert str(table.schema.field("observed_at").type) == "timestamp[us, tz=UTC]"
    keys = [(row["entity_id"], row["observed_at"]) for row in table.to_pylist()]
    assert keys == sorted(keys)


def test_quickstart_api_params_workflow_fetches_and_flattens_city_forecasts():
    from fastapi import FastAPI, Request
    from starlette.testclient import TestClient

    from examples.quickstart.custom import quickstart_router

    workflow = yaml.safe_load((ROOT / "workflows/workflow.api-params.yml").read_text())
    connection = yaml.safe_load((ROOT / "connections/open-meteo.yml").read_text())
    dag = Formula(S3Connection(bucket="unused", client=object())).dag(workflow["name"], workflow["workflow"])
    source_id = "quickstart_api_params.source.weather_requests"
    target_id = "quickstart_api_params.weather.hourly_forecasts"
    assert set(dag["nodes"]) == {source_id, target_id}
    assert dag["reverse_edges"][target_id] == [source_id]
    method = dag["nodes"][target_id]["inputs"][0]["methods"][0]
    assert method["connection"] == connection["name"]

    class MemoryCatalyst:
        def __init__(self):
            self.streams = {}

        def create_stream(self, stream):
            self.streams.setdefault(stream, [])

        def write_to_stream(self, stream, record):
            self.streams[stream].append(record)
            return f"{len(self.streams[stream])}-0"

        def create_consumer_group(self, stream, group):
            pass

        def read_from_stream(self, stream, group, consumer, count, block):
            index = len(requests) - len(pending)
            return [(f"{index + 1}-0", {"message": pending.pop(0)})] if pending else []

    catalyst = MemoryCatalyst()
    handler = DataHandler(catalyst=catalyst)
    requests = quickstart_router(None, {"node_name": "weather_requests", "node_id": source_id})
    assert requests == [
        {"params": {"latitude": 40.7128, "longitude": -74.0060, "hourly": "temperature_2m", "forecast_days": 1, "timezone": "UTC"}},
        {"params": {"latitude": 51.5074, "longitude": -0.1278, "hourly": "temperature_2m", "forecast_days": 1, "timezone": "UTC"}},
    ]
    handler.outgoing_data(dag["nodes"][source_id]["outputs"]["methods"][0], data=requests, context={"node_id": source_id})
    pending = list(requests)
    app = FastAPI()
    calls = []

    @app.get("/v1/forecast")
    async def forecast(request: Request):
        calls.append(dict(request.query_params))
        return {
            "latitude": float(request.query_params["latitude"]),
            "longitude": float(request.query_params["longitude"]),
            "timezone": "GMT",
            "hourly_units": {"temperature_2m": "C"},
            "hourly": {
                "time": [f"2026-11-07T{hour:02d}:00" for hour in range(24)],
                "temperature_2m": [10 + hour / 10 for hour in range(24)],
            },
        }

    with TestClient(app) as client:
        class Session:
            headers = {}

            def get(self, url, params=None, *, allow_redirects):
                return client.get(url, params=params, follow_redirects=allow_redirects)

        api = Anvil(connection["connection"], session=Session())
        for message_id in ("1-0", "2-0"):
            result = handler.incoming_data(method, api, context={"input_ref": source_id})
            assert result.is_final is False
            assert result.ack_msgs == [(source_id, "reagent-input-group", message_id, "reagent")]
            forecasts = quickstart_router(result.data, {"node_name": "hourly_forecasts", "node_id": target_id})
            handler.outgoing_data(
                dag["nodes"][target_id]["outputs"]["methods"][0], data=forecasts, context={"node_id": target_id},
            )
        drained = handler.incoming_data(method, api, context={"input_ref": source_id})
        assert drained.is_final is True
        assert quickstart_router(drained.data, {"node_name": "hourly_forecasts", "node_id": target_id}) == []

    assert calls == [
        {key: str(value) for key, value in record["params"].items()}
        for record in requests
    ]
    rows = catalyst.streams[target_id]
    assert len(rows) == 48
    assert rows == [
        {
            "latitude": record["params"]["latitude"],
            "longitude": record["params"]["longitude"],
            "forecast_at": f"2026-11-07T{hour:02d}:00",
            "timezone": "GMT",
            "temperature_2m": 10 + hour / 10,
            "temperature_unit": "C",
        }
        for record in requests
        for hour in range(24)
    ]


@pytest.mark.parametrize("data", [
    [{"invalid": True}], None, "invalid", {}, {"hourly": {}},
    {"latitude": 1, "longitude": 2, "timezone": "GMT", "hourly_units": {"temperature_2m": "C"},
     "hourly": {"time": ["2026-11-07T00:00"], "temperature_2m": []}},
])
def test_quickstart_hourly_forecasts_rejects_invalid_response(data):
    from examples.quickstart.custom import hourly_forecasts

    with pytest.raises(ValueError, match="hourly_forecasts"):
        hourly_forecasts(data, {})


@pytest.mark.parametrize("backend", ["redis", "kafka"])
def test_quickstart_seed_publishes_api_params_workflow_and_connection(backend, monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT))
    from examples.quickstart import seed

    documents = []
    sql_objects = []

    class Storage:
        def list_buckets(self):
            return {}

        def head_bucket(self, **kwargs):
            return {}

        def put_object(self, **kwargs):
            if kwargs["Key"].endswith(".sql"):
                sql_objects.append(kwargs)
            else:
                documents.append((kwargs["Key"], yaml.safe_load(kwargs["Body"])))

        def upload_file(self, *args):
            assert args == (str(tmp_path / "observations.parquet"), "quickstart-data", "quickstart/observations.parquet")

    monkeypatch.setattr(seed, "_document", lambda filename: yaml.safe_load((ROOT / filename).read_text()))
    monkeypatch.setattr(seed, "ROOT", tmp_path)
    monkeypatch.setattr(seed.boto3, "client", lambda *args, **kwargs: Storage())
    monkeypatch.setattr(seed, "_enable_tables", lambda *args: None)
    (tmp_path / "observations.parquet").write_bytes(b"fixture")
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql/quickstart_observations.sql").write_text(
        (ROOT / "sql/quickstart_observations.sql").read_text()
    )
    seed.main(ROOT / f"runtime.{backend}.yml")
    assert {document["name"] for _, document in documents} == {
        f"quickstart_observations_{backend}", "quickstart_api_params", "quickstart_weather_api",
        "quickstart_files", "quickstart_lake", "quickstart_observation_upsert", "quickstart_observation_history",
    }
    assert (
        "quickstart/formulas/quickstart_api_params.yml",
        yaml.safe_load((ROOT / "workflows/workflow.api-params.yml").read_text()),
    ) in documents
    assert sql_objects == [{
        "Bucket": "quickstart-configs",
        "Key": "quickstart/sql/quickstart_observations.sql",
        "Body": (ROOT / "sql/quickstart_observations.sql").read_bytes(),
        "ContentType": "application/sql",
    }]