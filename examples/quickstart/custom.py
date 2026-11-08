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


def weather_requests(data, context):
    return [
        {"params": {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m",
            "forecast_days": 1,
            "timezone": "UTC",
        }}
        for latitude, longitude in ((40.7128, -74.0060), (51.5074, -0.1278))
    ]


def hourly_forecasts(data, context):
    if isinstance(data, list) and not data:
        return []
    if not isinstance(data, dict):
        raise ValueError("hourly_forecasts expects an API response object")
    hourly = data.get("hourly")
    units = data.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(units, dict) or "temperature_2m" not in units:
        raise ValueError("hourly_forecasts requires hourly forecast data and temperature units")
    if any(key not in data for key in ("latitude", "longitude", "timezone")):
        raise ValueError("hourly_forecasts requires coordinates and timezone")
    times, temperatures = hourly.get("time"), hourly.get("temperature_2m")
    if not isinstance(times, list) or not isinstance(temperatures, list) or len(times) != len(temperatures):
        raise ValueError("hourly_forecasts requires matching time and temperature arrays")
    return [
        {
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "forecast_at": timestamp,
            "timezone": data["timezone"],
            "temperature_2m": temperature,
            "temperature_unit": units["temperature_2m"],
        }
        for timestamp, temperature in zip(times, temperatures, strict=True)
    ]


NODE_ROUTES = {
    "raw_observations": raw_observations,
    "transform_observation_history": transform_observation_history,
    "weather_requests": weather_requests,
    "hourly_forecasts": hourly_forecasts,
}


def quickstart_router(data, context):
    route = NODE_ROUTES.get(context.get("node_name"))
    if route is None:
        raise ValueError(f"Unknown quickstart node: {context.get('node_id')}")
    return route(data, context)