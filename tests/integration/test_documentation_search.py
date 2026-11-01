import asyncio
import json

import pytest

from oxidizer.utils.chisel import Chisel


pytestmark = pytest.mark.integration


def test_bundled_semantic_search_without_network_or_writes(monkeypatch):
    if not (Chisel.DEFAULT_INDEX_PATH / "manifest.json").is_file():
        pytest.skip("Run inside the API image with bundled semantic assets")

    import socket
    from starlette.testclient import TestClient
    from oxidizer.utils.topology import Topology

    def deny_network(*args, **kwargs):
        raise AssertionError("Semantic documentation lookup must not access the network")

    monkeypatch.setattr(socket, "create_connection", deny_network)
    topology = Topology(object(), object())
    assert topology.get_docs_search_status()["ready"] is True
    topology.documentation._records = []
    result = topology.search_docs("How can I stop passwords appearing in logs?", limit=5)
    assert any(entry["path"] == "reference/logging-and-sinks.md" for entry in result["results"])
    assert result["source"] == "package"
    assert result["mode"] == "hybrid"
    assert result["version"] is not None
    assert topology.read_doc("reference/logging-and-sinks.md")["content"]
    topology.register_api_endpoints()
    topology.register_mcp_tools()
    client = TestClient(topology.app)
    assert client.get("/api/docs/status").status_code == 200

    async def check_mcp():
        status = await topology.mcp.call_tool("get_docs_search_status", {})
        assert json.loads(status.content[0].text)["ready"] is True
        response = await topology.mcp.call_tool("search_docs", {"query": "worker crashed after writing output", "limit": 3})
        assert len(json.loads(response.content[0].text)["results"]) == 3

    asyncio.run(check_mcp())