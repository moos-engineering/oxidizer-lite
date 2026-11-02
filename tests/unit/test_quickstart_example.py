from pathlib import Path

import pytest
import yaml

from oxidizer import Crucible, Formula, S3Connection


ROOT = Path(__file__).parents[2] / "examples/quickstart"


@pytest.mark.parametrize("backend", ["redis", "kafka"])
def test_quickstart_bronze_silver_workflow_and_schemas(backend):
    runtime = yaml.safe_load((ROOT / f"runtime.{backend}.yml").read_text())
    workflow = yaml.safe_load((ROOT / runtime["quickstart_seed"]["workflow_file"]).read_text())
    assert workflow["name"] == f"quickstart_observations_{backend}"
    dag = Formula(S3Connection(bucket="unused", client=object())).dag(workflow["name"], workflow["workflow"])
    source_id = f"{workflow['name']}.bronze.raw_observations"
    history_id = f"{workflow['name']}.silver.transform_observation_history"
    assert set(dag["nodes"]) == {source_id, history_id}
    assert dag["reverse_edges"][history_id] == [source_id]
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