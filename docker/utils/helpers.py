"""Config loading, provider connections, and helpers shared by Docker processes."""

import importlib.util
from typing import Any

import yaml

from oxidizer import KafkaConnection, RedisConnection, RedisStreamLogSink, Residue, S3Connection


def load_config(config_path: str) -> dict[str, object]:
	"""Load a YAML mapping from a process configuration file."""
	with open(config_path, "r") as config_file:
		config = yaml.safe_load(config_file)
	if not isinstance(config, dict):
		raise ValueError("Configuration file must contain a YAML mapping")
	return config


def load_custom_router(file_path: str | None, router_name: str | None):
	"""Load a worker router function from a mounted Python file."""
	if file_path is None and router_name is None:
		return None
	if not file_path or not router_name:
		raise ValueError("custom_functions_file and custom_functions_router must be configured together")

	spec = importlib.util.spec_from_file_location("oxidizer_custom_functions", file_path)
	if spec is None or spec.loader is None:
		raise ImportError(f"Cannot load custom functions from: {file_path}")
	module = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(module)
	router = getattr(module, router_name, None)
	if not callable(router):
		raise ValueError(f"Custom router '{router_name}' is not callable in {file_path}")
	return router


def create_catalyst_connection(
	config: dict[str, Any], residue: Residue, *, group_id: str | None = None
) -> RedisConnection | KafkaConnection:
	provider = config.get("provider", "redis")
	if provider in RedisConnection.supported_providers:
		connection = RedisConnection(**config, residue=residue)
		residue.add_sink(RedisStreamLogSink(connection=connection))
	elif provider in KafkaConnection.supported_providers:
		kafka_config = dict(config)
		kafka_config["create_consumer"] = True
		if group_id:
			kafka_config["group_id"] = group_id
		connection = KafkaConnection(**kafka_config, residue=residue)
	else:
		raise ValueError(f"Unsupported Catalyst provider: {provider}")
	return connection


def create_formula_connection(config: dict[str, Any], residue: Residue) -> S3Connection | RedisConnection:
	provider = config.get("provider", "s3")
	if provider == "s3":
		s3_config = {key: value for key, value in config.items() if key != "provider"}
		return S3Connection(**s3_config, residue=residue)
	if provider == "redis":
		redis_config = {key: value for key, value in config.items() if key != "provider"}
		return RedisConnection(**redis_config, residue=residue)
	raise ValueError(f"Unsupported Formula provider: {provider}")


def convert_data_types(data: dict, context: dict) -> dict:
    """Convert the data types of the input data based on the context."""
    # Implement your data type conversion logic here
    return data