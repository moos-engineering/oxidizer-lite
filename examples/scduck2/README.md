# SCD Type 2 Weather Pipeline with DuckLake

A real-world example demonstrating Slowly Changing Dimension Type 2 (SCD2) tracking with weather data from 18 US cities via the Open-Meteo API, using **DuckLake** as the lakehouse backend.

## What is DuckLake?

DuckLake is a lightweight lakehouse architecture that combines:
- **DuckDB** for fast analytical queries
- **PostgreSQL** as the metadata catalog
- **S3/MinIO** for object storage

This provides Iceberg-like capabilities without the complexity of AWS Glue or Spark.

## Prerequisites

- Redis running locally (port 6379)
- MinIO running locally (port 9000)
- PostgreSQL running locally (port 5432)

## Quick Start with Docker

### 1. Start Infrastructure

```bash
# Start all services
docker compose up -d

# Verify services are running
docker compose ps
```

Or start services individually:

```bash
# Redis
docker run -d --name redis -p 6379:6379 redis/redis-stack:latest

# MinIO
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"

# PostgreSQL
docker run -d --name postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=ducklake \
  postgres:16
```

### 2. Create MinIO Buckets

Access MinIO Console at http://localhost:9001 (login: minioadmin/minioadmin) and create:
- `oxidizer-configs` - for topology configurations
- `data-lake` - for DuckLake data storage

Or via CLI:

```bash
# Install mc (MinIO Client) if needed
brew install minio/stable/mc

# Configure mc
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create buckets
mc mb local/oxidizer-configs
mc mb local/data-lake
```

### 3. Initialize DuckLake Catalog

Connect to PostgreSQL and create the DuckLake database:

```bash
psql -h localhost -U postgres -c "CREATE DATABASE ducklake;"
```

## Configuration

The `scduck2.yml` topology uses these connections:

```yaml
connections:
  - name: ducklake
    type: ducklake
    s3_endpoint: localhost:9000
    s3_access_key_id: minioadmin
    s3_secret_access_key: minioadmin
    s3_region: us-east-1
    s3_use_ssl: false
    s3_bucket: data-lake
    s3_prefix: lakehouse
    postgres_host: localhost
    postgres_port: 5432
    postgres_user: postgres
    postgres_password: postgres
    postgres_db: ducklake
```

## Running the Example

### 1. Start the Controller

In **Terminal 1**:
```bash
python3 controller.py
```

The controller reads configs from MinIO, compiles the DAG, and dispatches tasks.

### 2. Start the API Server

In **Terminal 2**:
```bash
python3 api.py
```

The API server runs on `http://localhost:8000` and provides MCP endpoints.

### 3. Invoke the Topology

In **Terminal 3**:
```bash
python3 invoke.py
```

This triggers the `scduck2` topology to start processing.

### 4. Start Workers

In **Terminal 4**:
```bash
python3 worker.py
```

Workers process tasks from the streams. Re-run after each node completes:
```
bronze.fetch_weather → silver.weather_scd2 → gold.publish_weather
```

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SCD2 DuckLake Pipeline                            │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────┐      ┌─────────────────┐      ┌──────────────────┐
  │ Open-Meteo  │──────│  Bronze Layer   │──────│  Silver Layer    │
  │    API      │      │  (Raw Ingest)   │      │  (SCD2 Merge)    │
  └─────────────┘      └─────────────────┘      └──────────────────┘
        │                      │                        │
   18 US Cities         weather_bronze           weather_silver
   Weather Data         (DuckLake Table)         (DuckLake Table)
                                                        │
                                                        ▼
                                               ┌──────────────────┐
                                               │   Gold Layer     │
                                               │ (Current State)  │
                                               └──────────────────┘
                                                        │
                                                  Redis Stream
                                               weather_current
```

## SCD Type 2 Behavior

The silver layer performs SCD2 merge operations:

1. **New Records**: Inserted with `is_current=true`, `valid_from=now()`, `valid_to=null`
2. **Changed Records**: 
   - Existing record updated: `is_current=false`, `valid_to=now()`
   - New record inserted: `is_current=true`, `valid_from=now()`
3. **Unchanged Records**: No action taken

Track columns monitored for changes:
- `temperature`
- `humidity`
- `wind_speed`
- `pressure`
- `weather_code`

## Querying DuckLake

You can query the DuckLake tables directly using DuckDB:

```python
import duckdb

# Connect to DuckLake
conn = duckdb.connect()
conn.execute("""
    INSTALL ducklake FROM community;
    LOAD ducklake;
""")

# Attach the DuckLake catalog
conn.execute("""
    ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake user=postgres password=postgres' 
    AS lakehouse (DATA_PATH 's3://data-lake/lakehouse');
""")

# Query current weather
conn.execute("SELECT * FROM lakehouse.oxidizer.weather_silver WHERE is_current = true").fetchdf()

# View history for a specific city
conn.execute("""
    SELECT city, temperature, valid_from, valid_to, is_current 
    FROM lakehouse.oxidizer.weather_silver 
    WHERE city = 'New York' 
    ORDER BY valid_from
""").fetchdf()
```

## Comparing to AWS Glue (scd2 example)

| Feature | scd2 (Glue) | scduck2 (DuckLake) |
|---------|-------------|-------------------|
| Metadata Catalog | AWS Glue Data Catalog | PostgreSQL |
| Object Storage | AWS S3 | MinIO (S3-compatible) |
| Query Engine | AWS Athena / Spark | DuckDB |
| Authentication | AWS SSO | Access Keys |
| Cost | Pay-per-use | Free (self-hosted) |
| Setup Complexity | AWS account required | Docker only |

## Troubleshooting

### MinIO Connection Issues

```bash
# Check MinIO is running
curl http://localhost:9000/minio/health/live

# Verify buckets exist
mc ls local/
```

### PostgreSQL Connection Issues

```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify ducklake database exists
psql -h localhost -U postgres -c "\l"
```

### Redis Connection Issues

```bash
redis-cli ping
# Should return: PONG
```

## Files

| File | Description |
|------|-------------|
| `scduck2.yml` | Topology configuration with DuckLake connection |
| `controller.py` | DAG compiler and task dispatcher |
| `api.py` | MCP API server |
| `worker.py` | Data transformation worker |
| `invoke.py` | Topology invocation script |
