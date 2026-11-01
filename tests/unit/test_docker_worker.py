import pytest

from oxidizer import RedisConnection, Residue

from docker.utils import helpers


@pytest.mark.parametrize("setting", [{}, {"persist_logs": False}, {"persist_logs": True}])
def test_catalyst_log_sink_requires_explicit_opt_in(setting):
	class Client:
		def __init__(self):
			self.events = []

		def xadd(self, stream, fields, **kwargs):
			self.events.append((stream, fields))
			return "1-0"

	client = Client()
	config = {"provider": "redis", "client": client, **setting}
	original = dict(config)
	residue = Residue()
	connection = helpers.create_catalyst_connection(config, residue)
	assert config == original
	assert connection.client is client
	assert len(residue.sinks) == int(setting.get("persist_logs", False))
	if residue.sinks:
		assert residue.sinks[0].connection is connection
	residue.bind(component_name="worker").emit(residue.ash.INFO, "test event")
	assert len(client.events) == int(setting.get("persist_logs", False))


@pytest.mark.parametrize("value", ["true", "false", 1, None, {}])
def test_catalyst_rejects_invalid_log_persistence_before_connecting(value, monkeypatch):
	def unexpected_connection(**kwargs):
		pytest.fail("Invalid configuration must fail before connecting")

	class Redis:
		supported_providers = RedisConnection.supported_providers

		def __new__(cls, **kwargs):
			return unexpected_connection(**kwargs)

	monkeypatch.setattr(helpers, "RedisConnection", Redis)
	with pytest.raises(ValueError, match="YAML boolean"):
		helpers.create_catalyst_connection({"persist_logs": value}, Residue())


def test_kafka_log_persistence_is_rejected():
	with pytest.raises(ValueError, match="requires a Redis"):
		helpers.create_catalyst_connection({"provider": "kafka", "persist_logs": True}, Residue())


def test_kafka_accepts_disabled_log_persistence_without_passing_it_to_driver(monkeypatch):
	from oxidizer import KafkaConnection

	configured = []

	class Kafka:
		supported_providers = KafkaConnection.supported_providers

		def __init__(self, **kwargs):
			configured.append(kwargs)

	monkeypatch.setattr(helpers, "KafkaConnection", Kafka)
	residue = Residue()
	helpers.create_catalyst_connection(
		{"provider": "kafka", "persist_logs": False}, residue, group_id="workers",
	)
	assert "persist_logs" not in configured[0]
	assert configured[0]["group_id"] == "workers"
	assert configured[0]["create_consumer"] is True
	assert residue.sinks == []


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


def test_mcp_extra_groups_only_api_and_search_dependencies():
	from pathlib import Path

	import tomllib
	from packaging.requirements import Requirement

	root = Path(__file__).parents[2]
	project = tomllib.loads((root / "pyproject.toml").read_text())
	extras = project["project"]["optional-dependencies"]
	assert "search" not in extras
	assert "microscope" not in extras
	assert {Requirement(requirement).name for requirement in extras["mcp"]} == {
		"fastapi", "fastmcp", "uvicorn", "lancedb", "fastembed",
	}
	for requirements in extras.values():
		assert not any(
			Requirement(requirement).name == "ollama"
			or Requirement(requirement).name.startswith("langchain")
			for requirement in requirements
		)
