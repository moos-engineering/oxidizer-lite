import asyncio
import json
import uuid

import pytest
import yaml
from starlette.testclient import TestClient

from oxidizer.utils.topology import Topology


class FakeFormula:
    def __init__(self):
        self.list_calls = 0
        self.configs = {
            "daily-users": {
                "name": "daily-users",
                "workflow": {"layers": []},
                "outputs": [{"methods": [{"method": "api", "connection": "users_api"}]}],
            },
        }
        self.states = ["daily-users/run-123", "orders/run-456"]
        self.state_pairs = [("daily-users", "run-123"), ("orders", "run-456")]
        self.active_pairs = [("daily-users", "run-123")]
        self.uploads = {}
        self.connections = {
            "rustfs_lake": {"connection": {"type": "s3_tables", "provider": "rustfs", "secret_access_key": "private"}},
            "aws_lake": {"connection": {"type": "s3_tables", "provider": "aws"}},
            "users_api": {"connection": {"type": "api"}},
            "object_storage": {"connection": {"type": "s3"}},
            "missing": {},
        }
        self.run_state = {
            "status": "running",
            "started_at": "2026-10-05T12:00:00Z",
            "completed_at": None,
            "connection_names": ["users_api"],
            "dag": {
                "status": {
                    "daily-users.extract": "running",
                    "daily-users.transform": "pending",
                },
                "nodes": {
                    "daily-users.extract": {"checkpoint_metadata": {"batch_index": 2}},
                    "daily-users.transform": {},
                },
                "edges": {"daily-users.extract": ["daily-users.transform"]},
                "reverse_edges": {"daily-users.transform": ["daily-users.extract"]},
            },
        }

    def list_formula_configs(self):
        self.list_calls += 1
        return ["daily-users", "orders"]

    def load_formula_config(self, formula_id):
        return self.configs.get(formula_id, {})

    def create_formula_config(self, name, config):
        self.uploads[("workflow", name)] = config

    def create_formula_connection(self, name, config):
        self.uploads[("connection", name)] = config

    def create_formula_schema(self, name, config):
        self.uploads[("schema", name)] = config

    def list_formula_connections(self):
        return list(self.connections)

    def load_formula_connection(self, name):
        return self.connections[name]

    def list_formula_states(self):
        return self.states

    def list_formula_runs(self):
        return self.state_pairs

    def list_active_formula_runs(self):
        return self.active_pairs

    def load_formula_state(self, formula_id, run_id):
        return self.run_state if (formula_id, run_id) == ("daily-users", "run-123") else {}


class FakeCatalyst:
    def __init__(self):
        self.range_calls = []
        self.writes = []
        self.health_calls = []
        self.entries = [
            ("3-0", {"event": {"formula_id": "daily-users", "run_id": "run-123", "event": "worker failed", "api_token": "[REDACTED]"}}),
            ("2-0", {"event": {"formula_id": "orders", "run_id": "run-456", "event": "other formula"}}),
            ("1-0", {"event": {"formula_id": "daily-users", "run_id": "run-122", "event": "older run"}}),
        ]

    def read_stream_range(self, stream_name, count=10000):
        self.range_calls.append((stream_name, count))
        return self.entries[:count]

    def write_to_stream(self, stream_name, message):
        self.writes.append((stream_name, message))
        return f"{len(self.writes)}-0"

    def get_transport_health(self, stream_name, group_name=None):
        self.health_calls.append((stream_name, group_name))
        return {"backend": "redis", "stream": stream_name, "length": 3, "groups": []}


def test_topology_health_and_formula_tools_and_routes():
    from fastapi import FastAPI

    formula = FakeFormula()
    catalyst = FakeCatalyst()
    topology = Topology(formula, catalyst)

    assert topology.register_api_endpoints() is topology.app
    assert topology.register_api_endpoints() is topology.app
    assert topology.register_mcp_tools() is topology.mcp
    assert topology.register_mcp_tools() is topology.mcp
    assert isinstance(topology.app, FastAPI)
    assert any(route.path == "/mcp" for route in topology.app.routes)
    assert "/api/health" in topology.app.openapi()["paths"]

    client = TestClient(topology.app)
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "oxidizer-topology"}
    assert client.get("/api/formulas").json() == {"formulas": ["daily-users", "orders"]}
    assert client.get("/api/formulas/missing/runs").status_code == 404
    assert client.get("/api/formulas/daily-users/runs").json() == {
        "formula_id": "daily-users",
        "runs": [{
            "formula_id": "daily-users",
            "run_id": "run-123",
            "status": "running",
            "started_at": "2026-10-05T12:00:00Z",
            "completed_at": None,
        }],
    }
    state_response = client.get("/api/formulas/daily-users/runs/run-123")
    assert state_response.status_code == 200
    assert state_response.json()["state"] == formula.run_state
    assert topology.get_formula_state("daily-users", "run-123") is formula.run_state
    assert client.get("/api/formulas/daily-users/runs/missing").status_code == 404
    node_response = client.get("/api/formulas/daily-users/runs/run-123/nodes/daily-users.extract")
    assert node_response.status_code == 200
    assert node_response.json() == {
        "formula_id": "daily-users",
        "run_id": "run-123",
        "node_id": "daily-users.extract",
        "status": "running",
        "node": {"checkpoint_metadata": {"batch_index": 2}},
        "parents": [],
        "children": ["daily-users.transform"],
    }
    assert client.get("/api/formulas/daily-users/runs/run-123/nodes/missing").status_code == 404
    logs_response = client.get("/api/formulas/daily-users/logs?run_id=run-123&limit=1")
    assert logs_response.status_code == 200
    assert logs_response.json() == {
        "formula_id": "daily-users",
        "run_id": "run-123",
        "returned": 1,
        "logs": [{
            "formula_id": "daily-users",
            "run_id": "run-123",
            "event": "worker failed",
            "api_token": "[REDACTED]",
            "message_id": "3-0",
            "stream": "oxidizer:logs:daily-users",
        }],
    }

    tools = asyncio.run(topology.mcp._list_tools() if hasattr(topology.mcp, "_list_tools") else topology.mcp.list_tools())
    assert {tool.name for tool in tools} == {
        "list_formulas", "get_formula", "list_formula_states", "get_formula_state", "get_node_state", "upload_config", "list_formula_logs",
        "upload_config", "list_connections", "list_schemas", "query", "list_formula_runs", "list_active_formula_runs", "get_transport_health", "search_docs", "read_doc", "get_docs_search_status",
    }
    assert topology.list_formulas() == ["daily-users", "orders"]
    assert formula.list_calls == 4
    assert topology.get_formula("daily-users") == {
        "name": "daily-users",
        "workflow": {"layers": []},
        "outputs": [{"methods": [{"method": "api", "connection": "users_api"}]}],
    }
    assert topology.list_formula_states() == ["daily-users/run-123", "orders/run-456"]
    assert topology.list_formula_runs("daily-users") == [
        {
            "formula_id": "daily-users",
            "run_id": "run-123",
            "status": "running",
            "started_at": "2026-10-05T12:00:00Z",
            "completed_at": None,
        },
    ]
    assert topology.get_node_state("daily-users", "run-123", "daily-users.transform")["parents"] == [
        "daily-users.extract",
    ]
    assert topology.list_formula_logs("daily-users", run_id="run-123", limit=1)["returned"] == 1
    assert catalyst.range_calls == [("oxidizer:logs:daily-users", topology.MAX_LOG_SCAN)] * 2
    assert topology.get_formula_state("daily-users", "run-123") == {
        "status": "running",
        "started_at": "2026-10-05T12:00:00Z",
        "completed_at": None,
        "connection_names": ["users_api"],
        "dag": {
            "status": {
                "daily-users.extract": "running",
                "daily-users.transform": "pending",
            },
            "nodes": {
                "daily-users.extract": {"checkpoint_metadata": {"batch_index": 2}},
                "daily-users.transform": {},
            },
            "edges": {"daily-users.extract": ["daily-users.transform"]},
            "reverse_edges": {"daily-users.transform": ["daily-users.extract"]},
        },
    }


def test_fastapi_mount_serves_mcp_protocol_with_lifespan():
    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_api_endpoints()
    topology.register_mcp_tools()

    with TestClient(topology.app) as client:
        response = client.post(
            "/mcp",
            headers={"accept": "application/json, text/event-stream"},
            json={
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26", "capabilities": {},
                    "clientInfo": {"name": "topology-test", "version": "1.0.0"},
                },
            },
        )

    assert response.status_code == 200, response.text


def test_topology_run_uses_fastapi_and_rejects_stdio(monkeypatch):
    topology = Topology(FakeFormula(), FakeCatalyst())
    calls = []

    monkeypatch.setattr("uvicorn.run", lambda app, **kwargs: calls.append((app, kwargs)))
    monkeypatch.setattr(topology.mcp, "run", lambda **kwargs: calls.append((topology.mcp, kwargs)))

    topology.run(transport="http", host="127.0.0.1", port=8100)
    assert calls[-1] == (topology.app, {"host": "127.0.0.1", "port": 8100})
    assert any(route.path == "/mcp" for route in topology.app.routes)

    with pytest.raises(ValueError, match="Streamable HTTP"):
        topology.run(transport="stdio")


@pytest.mark.parametrize("terminal", ["success", "failed", "cancelled", "timed_out", "skipped"])
def test_active_run_listing_uses_index_and_skips_stale_markers(monkeypatch, terminal):
    formula, catalyst = FakeFormula(), FakeCatalyst()
    formula.active_pairs = [
        ("daily-users", "run-b"), ("daily-users", "run-a"),
        ("daily-users", "terminal"), ("daily-users", "missing"),
        ("orders", "other-run"),
    ]
    states = {
        "run-a": {"status": "paused", "started_at": "2026-10-05T12:00:00Z"},
        "run-b": {"status": "pending"},
        "terminal": {"status": terminal},
    }
    loaded = []

    def load(formula_id, run_id):
        loaded.append((formula_id, run_id))
        return states.get(run_id, {})

    def forbidden_history_scan():
        pytest.fail("Active listing must not scan run history")

    monkeypatch.setattr(formula, "load_formula_state", load)
    monkeypatch.setattr(formula, "list_formula_runs", forbidden_history_scan)
    topology = Topology(formula, catalyst)
    topology.register_api_endpoints()
    topology.register_mcp_tools()
    expected = [
        {"formula_id": "daily-users", "run_id": "run-a", "status": "paused", "started_at": "2026-10-05T12:00:00Z", "completed_at": None},
        {"formula_id": "daily-users", "run_id": "run-b", "status": "pending", "started_at": None, "completed_at": None},
    ]
    assert topology.list_active_formula_runs("daily-users") == expected
    client = TestClient(topology.app)
    response = client.get("/api/formulas/daily-users/runs/active")
    assert response.status_code == 200
    assert response.json() == {"formula_id": "daily-users", "runs": expected}
    result = asyncio.run(topology.mcp.call_tool("list_active_formula_runs", {"formula_id": "daily-users"}))
    assert json.loads(result.content[0].text) == expected
    assert all(formula_id == "daily-users" for formula_id, _ in loaded)
    assert client.get("/api/formulas/missing/runs/active").status_code == 404
    formula.active_pairs = []
    assert client.get("/api/formulas/daily-users/runs/active").json()["runs"] == []
    assert catalyst.writes == []
    assert formula.uploads == {}
    with pytest.raises(ValueError, match="formula_id"):
        topology.list_active_formula_runs(" ")


def test_active_run_backend_failure_is_sanitized(monkeypatch):
    from fastmcp.exceptions import ToolError

    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_api_endpoints()
    topology.register_mcp_tools()

    def fail():
        raise RuntimeError("private credentials")

    monkeypatch.setattr(topology.formula, "list_active_formula_runs", fail)
    response = TestClient(topology.app).get("/api/formulas/daily-users/runs/active")
    assert response.status_code == 503
    assert response.json() == {"error": "Unable to list active runs"}
    with pytest.raises(ToolError) as failure:
        asyncio.run(topology.mcp.call_tool("list_active_formula_runs", {"formula_id": "daily-users"}))
    assert "private" not in str(failure.value)


def test_topology_mcp_run_listing_and_transport_health_are_read_only():
    formula, catalyst = FakeFormula(), FakeCatalyst()
    topology = Topology(formula, catalyst)
    topology.register_api_endpoints()
    topology.register_mcp_tools()
    client = TestClient(topology.app)
    result = asyncio.run(topology.mcp.call_tool("list_formula_runs", {"formula_id": "daily-users"}))
    assert json.loads(result.content[0].text) == topology.list_formula_runs("daily-users")
    result = asyncio.run(topology.mcp.call_tool("get_transport_health", {
        "stream_name": "oxidizer:reagent", "group_name": "workers",
    }))
    expected = {"backend": "redis", "stream": "oxidizer:reagent", "length": 3, "groups": []}
    assert json.loads(result.content[0].text) == expected
    response = client.get("/api/transport/health", params={"stream_name": "oxidizer:reagent", "group_name": "workers"})
    assert response.status_code == 200
    assert response.json() == expected
    assert catalyst.health_calls == [("oxidizer:reagent", "workers")] * 2
    assert catalyst.writes == []
    assert formula.uploads == {}
    assert client.get("/api/transport/health").status_code == 400
    assert client.get("/api/transport/health", params={"stream_name": "tasks", "group_name": ""}).status_code == 400
    tools = asyncio.run(topology.mcp._list_tools() if hasattr(topology.mcp, "_list_tools") else topology.mcp.list_tools())
    for tool in tools:
        if tool.name in {"list_formula_runs", "list_active_formula_runs", "get_transport_health"}:
            assert tool.annotations.model_dump(by_alias=True)["readOnlyHint"] is True


@pytest.mark.parametrize("error,status", [
    (LookupError("private credentials"), 404),
    (ValueError("private credentials"), 400),
    (RuntimeError("private credentials"), 503),
])
def test_transport_health_errors_do_not_expose_backend_details(monkeypatch, error, status):
    from fastmcp.exceptions import ToolError

    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_api_endpoints()
    topology.register_mcp_tools()

    def fail(*args):
        raise error

    monkeypatch.setattr(topology.catalyst, "get_transport_health", fail)
    client = TestClient(topology.app)
    response = client.get("/api/transport/health", params={"stream_name": "tasks"})
    assert response.status_code == status
    assert "private" not in response.text
    with pytest.raises(ToolError) as failure:
        asyncio.run(topology.mcp.call_tool("get_transport_health", {"stream_name": "tasks"}))
    assert "private" not in str(failure.value)


def test_topology_documentation_tools_are_read_only(monkeypatch, tmp_path, index_documentation):
    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_mcp_tools()
    root = tmp_path / "docs"
    root.mkdir()
    (root / "guide.md").write_text("# Guide\n\n## Pending Work\n\nInspect Redis pending entries without acknowledgment.\n")
    monkeypatch.setattr(topology.documentation, "_root", lambda: (root, "test"))
    index_documentation(topology.documentation)

    result = asyncio.run(topology.mcp.call_tool("search_docs", {"query": "Redis pending", "limit": 1}))
    found = json.loads(result.content[0].text)
    assert found["results"][0]["path"] == "guide.md"
    assert found["results"][0]["section"] == "Pending Work"
    result = asyncio.run(topology.mcp.call_tool("read_doc", {"path": "guide.md", "section": "Pending Work"}))
    document = json.loads(result.content[0].text)
    assert document["source"] == "test"
    assert "Inspect Redis pending" in document["content"]
    assert topology.catalyst.writes == []
    assert topology.formula.uploads == {}
    tools = asyncio.run(topology.mcp._list_tools() if hasattr(topology.mcp, "_list_tools") else topology.mcp.list_tools())
    for tool in tools:
        if tool.name in {"search_docs", "read_doc", "get_docs_search_status"}:
            assert tool.annotations.model_dump(by_alias=True)["readOnlyHint"] is True


def test_documentation_search_readiness_matches_rest_and_mcp(tmp_path):
    from fastmcp.exceptions import ToolError

    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.documentation.index_path = tmp_path / "missing"
    topology.register_api_endpoints()
    topology.register_mcp_tools()
    client = TestClient(topology.app)
    response = client.get("/api/docs/status")
    assert response.status_code == 503
    result = asyncio.run(topology.mcp.call_tool("get_docs_search_status", {}))
    assert json.loads(result.content[0].text) == response.json()
    assert response.json()["error"]["code"] == "documentation_search_unavailable"
    with pytest.raises(ToolError, match="documentation_search_unavailable"):
        asyncio.run(topology.mcp.call_tool("search_docs", {"query": "pending"}))
    assert client.get("/api/health").status_code == 200


def test_topology_lists_only_s3_tables_connection_names():
    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_api_endpoints()
    topology.register_mcp_tools()
    client = TestClient(topology.app)

    assert topology.list_connections() == ["aws_lake", "rustfs_lake"]
    response = client.get("/api/connections")
    assert response.status_code == 200
    assert response.json() == {"connections": ["aws_lake", "rustfs_lake"]}
    assert "private" not in response.text
    result = asyncio.run(topology.mcp.call_tool("list_connections", {}))
    assert json.loads(result.content[0].text) == ["aws_lake", "rustfs_lake"]

    topology.formula.connections = {}
    assert client.get("/api/connections").json() == {"connections": []}


def test_topology_connection_listing_filters_types():
    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_api_endpoints()
    client = TestClient(topology.app)
    for kind in ("redis", "kafka", "redpanda", "msk", "confluent"):
        topology.formula.connections[kind] = {"connection": {"type": kind, "password": "private"}}
    response = client.get("/api/connections", params=[("type", "redis"), ("type", "kafka")])
    assert response.status_code == 200
    assert response.json() == {"connections": ["kafka", "redis"]}
    assert "private" not in response.text
    assert client.get("/api/connections?type=s3_tables").json() == {"connections": ["aws_lake", "rustfs_lake"]}
    assert client.get("/api/connections?type=api").json() == {"connections": ["users_api"]}
    assert client.get("/api/connections?type=unknown").json() == {"connections": []}
    assert client.get("/api/connections?type=stream").json() == {"connections": []}
    topology.formula.connections["stream_events"] = {"connection": {"type": "stream"}}
    assert client.get("/api/connections?type=stream").json() == {"connections": ["stream_events"]}
    assert client.get("/api/connections?type=").status_code == 400
    assert client.get("/api/connections?type=%20").status_code == 400
    response = client.get("/api/connections", params=[("type", kind) for kind in
                                                    ("redis", "kafka", "redpanda", "msk", "confluent")])
    assert response.json() == {"connections": ["confluent", "kafka", "msk", "redis", "redpanda"]}


def test_topology_connection_listing_failure_does_not_expose_credentials(monkeypatch):
    topology = Topology(FakeFormula(), FakeCatalyst())
    topology.register_api_endpoints()
    client = TestClient(topology.app)

    def fail():
        raise RuntimeError("private credentials")

    monkeypatch.setattr(topology.formula, "list_formula_connections", fail)
    response = client.get("/api/connections")
    assert response.status_code == 503
    assert response.json() == {"error": "Unable to list connections"}


@pytest.fixture
def catalog_topology(monkeypatch):
    import duckdb
    import oxidizer.utils.topology as topology_module
    from oxidizer import Crucible, S3TablesConnection

    engines = []
    connect = duckdb.connect

    def make_engine():
        engine = connect()
        engine.execute("SET TimeZone = 'UTC'")
        engine.execute("ATTACH ':memory:' AS local_catalog")
        engine.execute("CREATE SCHEMA local_catalog.analytics")
        engine.execute("CREATE TABLE local_catalog.analytics.events AS SELECT 1 AS id UNION ALL SELECT 2")
        engines.append(engine)
        return engine

    def make_crucible(connection, *, engine):
        crucible = Crucible(connection, engine=engine)
        crucible.adapter.attach = lambda *_args: None
        return crucible

    monkeypatch.setattr(duckdb, "connect", make_engine)
    monkeypatch.setattr(topology_module, "S3TablesConnection", lambda **_kwargs: S3TablesConnection(client=object()))
    monkeypatch.setattr(topology_module, "Crucible", make_crucible)
    formula = FakeFormula()
    formula.connections["aws_lake"]["connection"]["aws_s3_table_bucket"] = "warehouse"
    topology = Topology(formula, FakeCatalyst())
    topology.register_api_endpoints()
    topology.register_mcp_tools()
    return topology, engines


def test_topology_discovers_catalog_through_crucible(catalog_topology):
    topology, engines = catalog_topology
    client = TestClient(topology.app)

    response = client.get("/api/connections/aws_lake/schemas")
    assert response.status_code == 200
    result = response.json()
    assert result["connection_name"] == "aws_lake"
    assert result["namespaces"] == result["schemas"]
    assert "analytics" in result["namespaces"]
    assert {"namespace": "analytics", "table": "events", "type": "BASE TABLE"} in result["tables"]
    tool_result = asyncio.run(topology.mcp.call_tool("list_schemas", {"connection_name": "aws_lake"}))
    assert json.loads(tool_result.content[0].text) == result
    for engine in engines:
        with pytest.raises(Exception, match="closed"):
            engine.execute("SELECT 1")


def test_topology_queries_catalog_through_crucible(catalog_topology):
    topology, engines = catalog_topology
    client = TestClient(topology.app)
    arguments = {"sql": "SELECT id FROM local_catalog.analytics.events ORDER BY id", "max_rows": 1}

    response = client.post("/api/connections/aws_lake/query", json=arguments)
    assert response.status_code == 200
    result = response.json()
    assert result["columns"] == ["id"]
    assert result["rows"] == [{"id": 1}]
    assert result["row_count"] == 1
    assert result["truncated"] is True
    tool_result = asyncio.run(topology.mcp.call_tool("query", {"connection_name": "aws_lake", **arguments}))
    assert json.loads(tool_result.content[0].text) == result
    for engine in engines:
        with pytest.raises(Exception, match="closed"):
            engine.execute("SELECT 1")


def test_topology_query_serializes_typed_results(catalog_topology):
    topology, _engines = catalog_topology
    result = topology.query("aws_lake", "SELECT DATE '2026-10-16' AS day, 1.25::DECIMAL(4,2) AS amount, 'abc'::BLOB AS data, TIMESTAMPTZ '2026-10-16 00:00:00+00' AS observed_at")
    assert result["rows"] == [{"day": "2026-10-16", "amount": "1.25", "data": "YWJj", "observed_at": "2026-10-16T00:00:00+00:00"}]


def test_topology_catalog_routes_validate_inputs(catalog_topology):
    topology, _engines = catalog_topology
    client = TestClient(topology.app)
    assert client.get("/api/connections/unknown/schemas").status_code == 404
    assert client.get("/api/connections/users_api/schemas").status_code == 400
    assert client.get("/api/connections/rustfs_lake/schemas").status_code == 400
    assert client.post("/api/connections/aws_lake/query", content="invalid").status_code == 400
    assert client.post("/api/connections/aws_lake/query", json=[]).status_code == 400
    for payload in ({}, {"sql": "DELETE FROM local_catalog.analytics.events"},
                    {"sql": "SELECT 1", "max_rows": 1001}, {"sql": "SELECT 1", "max_rows": True}):
        assert client.post("/api/connections/aws_lake/query", json=payload).status_code == 400
    assert client.post("/api/connections/unknown/query", json={"sql": "SELECT 1"}).status_code == 404


def test_topology_invocation_route_enqueues_formula_with_generated_run_id():
    formula = FakeFormula()
    catalyst = FakeCatalyst()
    topology = Topology(formula, catalyst)
    topology.register_api_endpoints()
    client = TestClient(topology.app)

    response = client.post("/api/invocations", json={"formula_id": "daily-users"})

    assert response.status_code == 202
    result = response.json()
    assert result["formula_id"] == "daily-users"
    assert str(uuid.UUID(result["run_id"])) == result["run_id"]
    assert result["invocation_id"] == "1-0"
    assert catalyst.writes == [(
        "oxidizer:invocations",
        {"formula_id": "daily-users", "run_id": result["run_id"]},
    )]
    assert client.post("/api/invocations", json={}).status_code == 400
    assert client.post("/api/invocations", json={"formula_id": "missing"}).status_code == 404
    assert client.post("/api/invocations", content="not-json").status_code == 400

    catalyst.write_to_stream = lambda *_args: (_ for _ in ()).throw(RuntimeError("redis details"))
    failed = client.post("/api/invocations", json={"formula_id": "daily-users"})
    assert failed.status_code == 503
    assert failed.json() == {"error": "Unable to enqueue formula invocation"}


def test_topology_requires_optional_mcp_dependency(monkeypatch):
    import oxidizer.utils.topology as topology_module

    monkeypatch.setattr(topology_module, "FastMCP", None)
    with pytest.raises(ImportError, match=r"oxidizer-lite\[mcp\]"):
        Topology(FakeFormula(), FakeCatalyst())


def test_topology_reads_catalyst_message_field_logs():
    catalyst = FakeCatalyst()
    catalyst.entries = [
        ("4-0", {"message": {
            "formula_id": "daily-users",
            "run_id": "run-123",
            "event": "worker checkpoint",
            "component": "reagent",
            "api_token": "[REDACTED]",
        }}),
        ("5-0", {"event": None, "message": '{"formula_id":"daily-users","run_id":"run-123","event":"serialized checkpoint"}'}),
    ]
    topology = Topology(FakeFormula(), catalyst)

    assert topology.list_formula_logs("daily-users", run_id="run-123")["logs"] == [{
        "formula_id": "daily-users",
        "run_id": "run-123",
        "event": "worker checkpoint",
        "component": "reagent",
        "api_token": "[REDACTED]",
        "message_id": "4-0",
        "stream": "oxidizer:logs:daily-users",
    }, {
        "formula_id": "daily-users",
        "run_id": "run-123",
        "event": "serialized checkpoint",
        "message_id": "5-0",
        "stream": "oxidizer:logs:daily-users",
    }]


@pytest.mark.parametrize(
    ("config_type", "name"),
    [("workflow", "daily_users"), ("connection", "users_api"), ("schema", "users")],
)
def test_topology_upload_config_dispatches_by_type(config_type, name):
    formula = FakeFormula()
    topology = Topology(formula, FakeCatalyst())
    config = {"templateVersion": 1, "name": name, "type": config_type}
    if config_type == "workflow":
        config["workflow"] = {"layers": []}
    elif config_type == "connection":
        config["connection"] = {"type": "api", "base_url": "https://example.test"}
    else:
        config["schema"] = {"version": 1}

    assert topology.upload_config(yaml.safe_dump(config)) == {
        "type": config_type, "name": name, "stored": True,
    }
    assert formula.uploads == {(config_type, name): config}


def test_topology_upload_config_rejects_invalid_or_unsupported_documents():
    formula = FakeFormula()
    topology = Topology(formula, FakeCatalyst())
    for content in (
        "not: [valid",
        "- not-a-mapping",
        yaml.safe_dump({"templateVersion": 2, "name": "users", "type": "schema"}),
        yaml.safe_dump({"templateVersion": 1, "name": "../users", "type": "schema"}),
        yaml.safe_dump({"templateVersion": 1, "name": "users", "type": "unknown"}),
    ):
        with pytest.raises(ValueError):
            topology.upload_config(content)
    assert formula.uploads == {}


def test_topology_config_upload_endpoint_stores_and_validates_yaml():
    formula = FakeFormula()
    topology = Topology(formula, FakeCatalyst())
    topology.register_api_endpoints()
    client = TestClient(topology.app)
    config = {
        "templateVersion": 1, "name": "users_api", "type": "connection",
        "connection": {"type": "api", "base_url": "https://example.test"},
    }

    response = client.post("/api/configs", json={"content": yaml.safe_dump(config)})
    assert response.status_code == 201
    assert response.json() == {"type": "connection", "name": "users_api", "stored": True}
    assert formula.uploads == {("connection", "users_api"): config}

    invalid = client.post("/api/configs", json={"content": "type: invalid"})
    assert invalid.status_code == 400
    assert formula.uploads == {("connection", "users_api"): config}
