# SCD Type 2 Weather Pipeline

A real-world example demonstrating Slowly Changing Dimension Type 2 (SCD2) tracking with weather data from 18 US cities via the Open-Meteo API.

## Prerequisites

- Redis running locally (port 6379)
- AWS SSO configured for Glue Data Catalog access
- AWS S3 bucket for Iceberg tables and config storage

## Configuration

### 1. Update AWS Settings

Edit `scd2.yml` and update the Glue connection:

```yaml
connections:
  - name: glue_data_lake
    type: glue_catalog
    aws_sso_profile: your-sso-profile      # ← Your AWS SSO profile
    aws_region: us-west-2                   # ← Your region
    aws_account_id: 123456789012            # ← Your AWS account ID
    aws_s3_bucket: your-lakehouse-bucket    # ← Your S3 bucket for Iceberg
```

### 2. Update Controller Settings

Edit `controller.py` to use your AWS SSO profile:

```python
crucible_connection = CrucibleConnection(
    s3_bucket="your-config-bucket",         # ← S3 bucket for configs
    sso_role_name="your-sso-profile"        # ← Your AWS SSO profile
)
```

### 3. AWS SSO Login

Ensure you're logged in before running:

```bash
aws sso login --profile your-sso-profile
```

## Running the Example

### 1. Start Redis

```bash
# Using Docker:
docker run -d --name redis -p 6379:6379 redis/redis-stack:latest

# Or verify it's running:
redis-cli ping
```

### 2. Start the Controller

In **Terminal 1**:
```bash
python3 controller.py
```

The controller reads configs from S3, compiles the DAG, and dispatches tasks.

### 3. Start the API Server

In **Terminal 2**:
```bash
python3 api.py
```

The API server runs on `http://localhost:8000` and provides MCP endpoints.

### 4. Invoke the Topology

In **Terminal 3**:
```bash
python3 invoke.py
```

This triggers the `scd2` topology to start processing.

### 5. Start Workers

In **Terminal 4** (and additional terminals for each node):
```bash
python3 worker.py
```

Workers process tasks from the streams. **Re-run the worker command after each node completes** to process the next node in the DAG:

```
bronze.fetch_weather → silver.weather_scd2 → gold.publish_weather
```

## Pipeline Flow

```
Open-Meteo API (18 US cities)
         │
         ▼
┌────────────────────┐
│ bronze.fetch_weather │  ← Fetch current weather for all cities
└─────────┬──────────┘
          │ (INSERT into weather_bronze)
          ▼
┌────────────────────┐
│ silver.weather_scd2 │  ← SCD Type 2 merge into weather_silver
└─────────┬──────────┘
          │ (Tracks changes in temperature, humidity, wind_speed, pressure, weather_code)
          ▼
┌────────────────────────┐
│ gold.publish_weather   │  ← Stream current records for downstream consumers
└────────────────────────┘
```

## SCD Type 2 Behavior

The `silver.weather_scd2` node tracks historical changes:

| Column | Description |
|--------|-------------|
| `begin_date` | When this version became active |
| `end_date` | When this version was superseded (NULL if current) |
| `is_current` | TRUE for the active record |

When weather data changes for a city:
1. The existing current record gets `end_date` set and `is_current = FALSE`
2. A new record is inserted with `begin_date = NOW()`, `end_date = NULL`, `is_current = TRUE`

## Cities Tracked

| City | Coordinates |
|------|-------------|
| New York | 40.71, -74.01 |
| Los Angeles | 34.05, -118.24 |
| Chicago | 41.88, -87.63 |
| San Francisco | 37.77, -122.42 |
| Seattle | 47.61, -122.33 |
| Miami | 25.76, -80.19 |
| Denver | 39.74, -104.99 |
| Boston | 42.36, -71.06 |
| Houston | 29.76, -95.37 |
| Phoenix | 33.45, -112.07 |
| Dallas | 32.78, -96.80 |
| Washington DC | 38.91, -77.04 |
| Philadelphia | 39.95, -75.16 |
| Las Vegas | 36.17, -115.14 |
| Charlotte | 35.23, -80.84 |
| Austin | 30.27, -97.74 |
| Oklahoma City | 35.47, -97.52 |
| Atlanta | 33.75, -84.39 |

## Querying SCD Type 2 Data

```sql
-- Current weather for all cities
SELECT * FROM oxidizer.weather_silver WHERE is_current = TRUE;

-- Historical changes for a specific city
SELECT * FROM oxidizer.weather_silver 
WHERE city = 'New York' 
ORDER BY begin_date;

-- Weather at a specific point in time
SELECT * FROM oxidizer.weather_silver 
WHERE begin_date <= TIMESTAMP '2026-04-01 12:00:00'
  AND (end_date IS NULL OR end_date > TIMESTAMP '2026-04-01 12:00:00');
```

## Troubleshooting

- **Controller exits immediately**: Check Redis connection and S3 bucket permissions
- **Worker Idle**: Ensure the topology was invoked via `invoke.py`
- **S3/Glue Errors**: Verify AWS SSO login (`aws sso login --profile your-profile`)
- **SCD2 not detecting changes**: The pipeline only tracks changes in `tracked_columns`
