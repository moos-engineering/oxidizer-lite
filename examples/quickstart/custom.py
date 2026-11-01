"""Quickstart transforms and the local RustFS metadata-log workaround."""

from datetime import datetime, timezone
from functools import wraps

from oxidizer.utils.crucible import DuckDBCrucible


def _configure_local_iceberg():
    attach_rustfs = DuckDBCrucible._attach_rustfs

    @wraps(attach_rustfs)
    def attach(self, warehouse, catalog, engine):
        if warehouse == "quickstart-tables":
            engine.execute("SET iceberg_use_metadata_log = false")
        return attach_rustfs(self, warehouse, catalog, engine)

    DuckDBCrucible._attach_rustfs = attach


_configure_local_iceberg()


def raw_observations(data, context):
    if not isinstance(data, list):
        raise ValueError("raw_observations expects a list of records")
    records = []
    for record in data:
        normalized = dict(record)
        for column in ("observed_at", "source_revision"):
            value = normalized[column]
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
            if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
                raise ValueError(f"{column} must be a timezone-aware timestamp")
            normalized[column] = timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        records.append(normalized)
    return records


def transform_observation_history(data, context):
    if not isinstance(data, list):
        raise ValueError("transform_observation_history expects a list of records")
    return data


NODE_ROUTES = {
    "raw_observations": raw_observations,
    "transform_observation_history": transform_observation_history,
}


def quickstart_router(data, context):
    route = NODE_ROUTES.get(context.get("node_name"))
    if route is None:
        raise ValueError(f"Unknown quickstart node: {context.get('node_id')}")
    return route(data, context)