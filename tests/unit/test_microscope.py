import asyncio
import json

from oxidizer import Formula, Microscope, OllamaConnection, RedisConnection, Residue


class FakeRedisJson:
    def __init__(self, values):
        self.values = values

    def get(self, key):
        return self.values.get(key)

    def set(self, key, path, value):
        self.values[key] = value


class FakeRedisClient:
    def __init__(self):
        self.values = {
            "oxidizer:formulas:orders": {
                "workflow": {"layers": [], "api_token": "secret-formula-token"},
            },
            "oxidizer:states:orders:run-123": {
                "status": "failed",
                "dag": {
                    "status": {"orders.extract": "failed"},
                    "nodes": {
                        "orders.extract": {
                            "error_details": {
                                "error_type": "RuntimeError",
                                "auth_token": "secret-state-token",
                            },
                            "checkpoint_metadata": {"records_processed": 10},
                        }
                    },
                    "edges": {},
                    "reverse_edges": {},
                },
            },
        }
        self.logs = []
        self.logs_by_stream = {}
        self.log_batch = []
        self.groups = []
        self.reads = []
        self.acks = []

    def json(self):
        return FakeRedisJson(self.values)

    def xrevrange(self, stream_name, count=1000):
        assert stream_name in ("oxidizer:logs", "logs-a", "logs-b")
        entries = self.logs_by_stream.get(stream_name, self.logs)
        return list(reversed(entries[-count:]))

    def xgroup_create(self, stream, group, id="$", mkstream=True):
        self.groups.append((stream, group, id, mkstream))

    def xreadgroup(self, group, consumer, streams, count=10, block=1000):
        self.reads.append((group, consumer, streams, count, block))
        if all(stream_id == "0" for stream_id in streams.values()):
            return []
        topic_name = next(iter(streams))
        topic_key = topic_name.decode("utf-8") if isinstance(topic_name, bytes) else str(topic_name)
        if topic_key in self.logs_by_stream:
            entries = self.logs_by_stream[topic_key]
            return [(topic_name, entries)] if entries else []
        return self.log_batch

    def xack(self, stream, group, message_id):
        self.acks.append((stream, group, message_id))
        return 1


def test_microscope_analyzes_run_with_only_matching_redacted_evidence():
    redis_client = FakeRedisClient()
    redis_client.logs = [
        ("1-0", {"message": json.dumps({"run_id": "other-run", "event": "ignore"})}),
        ("2-0", {"message": json.dumps({"run_id": "run-123", "component": "reagent", "event": "worker failed"})}),
        ("3-0", {"event": json.dumps({"run_id": "run-123", "event": "sink event", "auth_token": "secret-log-token"})}),
    ]
    trace = Residue()
    formula = Formula(RedisConnection(client=redis_client), residue=trace)
    captured_evidence = []

    def analysis_runner(evidence):
        captured_evidence.append(evidence)
        return "Evidence points to a failed node."

    microscope = Microscope(
        formula,
        OllamaConnection(client=object()),
        RedisConnection(client=redis_client),
        model="local-model",
        topology_mcp="http://localhost:8000/mcp",
        log_stream="oxidizer:logs",
        analysis_runner=analysis_runner,
        residue=trace,
    )

    result = asyncio.run(microscope.analyze_run("orders", "run-123"))

    assert result == {
        "formula_id": "orders",
        "run_id": "run-123",
        "model": "local-model",
        "log_count": 2,
        "analysis": "Evidence points to a failed node.",
    }
    evidence = captured_evidence[0]
    assert evidence["run"]["status"] == "failed"
    assert evidence["run"]["nodes"]["orders.extract"]["error_details"]["error_type"] == "RuntimeError"
    assert evidence["workflow"]["api_token"] == "[REDACTED]"
    assert evidence["run"]["nodes"]["orders.extract"]["error_details"]["auth_token"] == "[REDACTED]"
    assert evidence["logs"][0]["message_id"] == "2-0"
    assert evidence["logs"][1]["auth_token"] == "[REDACTED]"
    evidence_json = json.dumps(evidence)
    assert "secret-formula-token" not in evidence_json
    assert "secret-state-token" not in evidence_json


def test_microscope_requires_existing_run():
    redis_client = FakeRedisClient()
    formula = Formula(RedisConnection(client=redis_client))
    microscope = Microscope(
        formula,
        OllamaConnection(client=object()),
        RedisConnection(client=redis_client),
        model="local-model",
        topology_mcp="http://localhost:8000/mcp",
        log_stream="oxidizer:logs",
        analysis_runner=lambda evidence: "unused",
    )

    try:
        asyncio.run(microscope.analyze_run("orders", "missing-run"))
    except ValueError as exc:
        assert "Run not found" in str(exc)
    else:
        raise AssertionError("Missing run should be rejected before calling Ollama")


def test_microscope_does_not_emit_lifecycle_events_to_parent_sink():
    class RecordingSink:
        def __init__(self):
            self.events = []

        def emit(self, event):
            self.events.append(event)

    sink = RecordingSink()
    redis_client = FakeRedisClient()
    formula = Formula(RedisConnection(client=redis_client))
    trace = Residue(sinks=[sink])
    microscope = Microscope(
        formula,
        OllamaConnection(client=object()),
        RedisConnection(client=redis_client),
        model="local-model",
        topology_mcp="http://localhost:8000/mcp",
        log_stream="oxidizer:logs",
        analysis_runner=lambda evidence: "analysis",
        residue=trace,
    )

    asyncio.run(microscope.analyze_run("orders", "run-123"))

    assert sink.events == []


def test_microscope_consumes_multiple_trigger_streams_and_acks_after_analysis():
    redis_client = FakeRedisClient()
    redis_client.log_batch = [
        (b"logs-a", [(b"1-0", {b"message": json.dumps({
            "formula_id": "orders", "run_id": "run-123", "level": "error", "event": "node failed",
        }).encode()})]),
        (b"logs-b", [(b"2-0", {b"message": json.dumps({
            "formula_id": "orders", "run_id": "run-123", "level": "info", "event": "worker heartbeat",
        }).encode()})]),
    ]
    redis_client.logs_by_stream = {
        "logs-a": [(b"1-0", {b"message": json.dumps({
            "formula_id": "orders", "run_id": "run-123", "level": "error", "event": "node failed",
        }).encode()})],
        "logs-b": [(b"2-0", {b"message": json.dumps({
            "formula_id": "orders", "run_id": "run-123", "level": "info", "event": "worker heartbeat",
        }).encode()})],
    }
    formula = Formula(RedisConnection(client=redis_client))
    analyses = []
    microscope = Microscope(
        formula,
        OllamaConnection(client=object()),
        RedisConnection(client=redis_client),
        model="local-model",
        topology_mcp="http://localhost:8000/mcp",
        log_streams=["logs-a", "logs-b"],
        analysis_runner=lambda evidence: analyses.append(evidence) or "analysis",
    )

    results = asyncio.run(microscope.run_once(count=5, block=1))

    assert len(analyses) == 1
    assert results[0]["trigger_stream"] == "logs-a"
    assert redis_client.groups == [
        ("logs-a", "oxidizer:microscope", "$", True),
        ("logs-b", "oxidizer:microscope", "$", True),
    ]
    assert redis_client.reads == [
        ("oxidizer:microscope", "microscope", {"logs-a": "0"}, 5, 0),
        ("oxidizer:microscope", "microscope", {"logs-a": ">"}, 5, 1),
        ("oxidizer:microscope", "microscope", {"logs-b": "0"}, 5, 0),
        ("oxidizer:microscope", "microscope", {"logs-b": ">"}, 5, 1),
    ]
    assert redis_client.acks == [
        ("logs-a", "oxidizer:microscope", "1-0"),
        ("logs-b", "oxidizer:microscope", "2-0"),
    ]
    # Verify the findings were stored in Formula / Redis cache
    saved = formula.load_formula_analysis("orders", "run-123")
    assert saved["analysis"] == "analysis"
    assert saved["formula_id"] == "orders"
    assert saved["run_id"] == "run-123"


def test_microscope_hits_api_for_log_streams_and_saves_to_formula(monkeypatch):
    from oxidizer import Catalyst

    redis_client = FakeRedisClient()
    redis_client.logs_by_stream = {
        "api-stream-1": [(b"10-0", {b"message": json.dumps({
            "formula_id": "orders", "run_id": "run-123", "level": "error", "event": "api-stream failure",
        }).encode()})],
    }
    formula = Formula(RedisConnection(client=redis_client))
    catalyst = Catalyst(RedisConnection(client=redis_client))

    # Mock urllib.request.urlopen to simulate the API endpoint returning available streams
    class FakeHttpResponse:
        status = 200

        def read(self):
            return json.dumps({"streams": ["api-stream-1"]}).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    requested_urls = []

    def fake_urlopen(req, timeout=3):
        url = req.full_url if hasattr(req, "full_url") else req
        requested_urls.append(url)
        return FakeHttpResponse()

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    microscope = Microscope(
        formula,
        OllamaConnection(client=object()),
        catalyst=catalyst,
        model="local-model",
        topology_mcp="http://localhost:8000/mcp",
        analysis_runner=lambda evidence: "Identified failure from API-discovered stream.",
    )

    results = asyncio.run(microscope.run_once(count=5, block=1))

    assert requested_urls == ["http://localhost:8000/api/logs/streams"]
    assert len(results) == 1
    assert results[0]["trigger_stream"] == "api-stream-1"
    assert results[0]["analysis"] == "Identified failure from API-discovered stream."

    # Verify findings saved in Formula (Redis cache)
    saved = formula.load_formula_analysis("orders", "run-123")
    assert saved["formula_id"] == "orders"
    assert saved["run_id"] == "run-123"
    assert saved["analysis"] == "Identified failure from API-discovered stream."