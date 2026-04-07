from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.reagent import Reagent

catalyst = CatalystConnection(host="localhost", port=6379, db=0)

reagent = Reagent(catalyst)

# Coordinate-to-city lookup for the 18 cities in scduck2.yml
CITY_LOOKUP = {
    (40.71, -74.01): "New York",
    (34.05, -118.24): "Los Angeles",
    (41.88, -87.63): "Chicago",
    (37.77, -122.42): "San Francisco",
    (47.61, -122.33): "Seattle",
    (25.76, -80.19): "Miami",
    (39.74, -104.99): "Denver",
    (42.36, -71.06): "Boston",
    (29.76, -95.37): "Houston",
    (33.45, -112.07): "Phoenix",
    (32.78, -96.80): "Dallas",
    (38.91, -77.04): "Washington DC",
    (39.95, -75.16): "Philadelphia",
    (36.17, -115.14): "Las Vegas",
    (35.23, -80.84): "Charlotte",
    (30.27, -97.74): "Austin",
    (35.47, -97.52): "Oklahoma City",
    (33.75, -84.39): "Atlanta",
}

def _closest_city(lat, lon):
    """Match API response coordinates to our known cities by nearest distance."""
    best = None
    best_dist = float("inf")
    for (clat, clon), name in CITY_LOOKUP.items():
        dist = (lat - clat) ** 2 + (lon - clon) ** 2
        if dist < best_dist:
            best_dist = dist
            best = name
    return best


@reagent.react()
def process(data: dict, context: dict):
    node_id = context.get("node_id", "")

    # Bronze: flatten Open-Meteo nested response into flat rows for SQL insert
    if node_id == "bronze.fetch_weather":
        raw = data["fetch_weather"]
        # API returns a list of location objects, each with nested .current
        rows = []
        for entry in raw:
            lat = entry.get("latitude")
            lon = entry.get("longitude")
            current = entry.get("current", {})
            rows.append({
                "city": _closest_city(lat, lon),
                "latitude": lat,
                "longitude": lon,
                "temperature": current.get("temperature_2m"),
                "humidity": current.get("relative_humidity_2m"),
                "wind_speed": current.get("wind_speed_10m"),
                "pressure": current.get("pressure_msl"),
                "weather_code": current.get("weather_code"),
                "observation_time": current.get("time"),
            })
        return rows

    # Silver: pass through as-is (SCD Type 2 merge handled by the framework)
    if node_id == "silver.weather_scd2":
        return data["fetch_weather"]

    # Gold: pass through current records to stream
    if node_id == "gold.publish_weather":
        return data["weather_scd2"]

    return []
