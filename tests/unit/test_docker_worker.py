from oxidizer import RedisConnection

from docker.utils import helpers


def test_load_config_reads_yaml_sections(tmp_path):
	path = tmp_path / "worker.yml"
	path.write_text("catalyst:\n  url: redis://redis:6379/0\nworker:\n  log_level: DEBUG\n")

	assert helpers.load_config(str(path)) == {
		"catalyst": {"url": "redis://redis:6379/0"},
		"worker": {"log_level": "DEBUG"},
	}


def test_load_custom_router_from_file(tmp_path):
	path = tmp_path / "custom.py"
	path.write_text("def sample_router(data, context):\n    return {'data': data, 'node_id': context['node_id']}\n")
	router = helpers.load_custom_router(str(path), "sample_router")

	assert router([1, 2], {"node_id": "orders"}) == {"data": [1, 2], "node_id": "orders"}


def test_redis_connection_uses_tls_from_rediss_url():
	pool = RedisConnection(url="rediss://redis.example.test:6379/2").client.connection_pool

	assert pool.connection_class.__name__ == "SSLConnection"
	assert "ssl" not in pool.connection_kwargs
	assert "ssl_cert_reqs" not in pool.connection_kwargs


def test_mcp_extra_groups_api_search_and_microscope_dependencies():
	from pathlib import Path

	import tomllib
	from packaging.requirements import Requirement

	root = Path(__file__).parents[2]
	project = tomllib.loads((root / "pyproject.toml").read_text())
	extras = project["project"]["optional-dependencies"]
	assert "search" not in extras
	assert "microscope" not in extras
	assert {Requirement(requirement).name for requirement in extras["mcp"]} == {
		"fastapi", "fastmcp", "uvicorn", "lancedb", "fastembed", "langchain",
		"langchain-openai", "langchain-ollama", "ollama",
	}
