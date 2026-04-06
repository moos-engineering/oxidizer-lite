# Skeleton Example

A minimal ETL pipeline that fetches posts from JSONPlaceholder API, transforms them through Redis streams, and loads into Iceberg via AWS Glue.

## Prerequisites

- Redis running locally (port 6379)
- MinIO running locally (ports 9000/9001) — for config storage
- AWS SSO configured — for Iceberg/Glue output

## Configuration

### 1. Update AWS Settings

Edit `sample.yml` and update the Glue connection:

```yaml
connections:
  - name: glue_data_lake
    type: glue_catalog
    aws_sso_profile: your-sso-profile      # ← Your AWS SSO profile
    aws_region: us-west-2                   # ← Your region
    aws_account_id: 123456789012            # ← Your AWS account ID
    aws_s3_bucket: your-lakehouse-bucket    # ← Your S3 bucket for Iceberg
```

### 2. Update Controller Settings (if using AWS S3 instead of MinIO)

Edit `controller.py` to switch from MinIO to AWS S3:

```python
# For MinIO (local development):
crucible_connection = CrucibleConnection(
    s3_bucket="oxidizer-configs",
    s3_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin"
)

# For AWS S3:
crucible_connection = CrucibleConnection(
    s3_bucket="your-config-bucket",
    sso_role_name="your-sso-profile"
)
```

## Running the Example

### 1. Start Redis

```bash
# Using Docker:
docker run -d --name redis -p 6379:6379 redis/redis-stack:latest

# Or verify it's running:
redis-cli ping
```

### 2. Start MinIO (if using local storage)

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create the config bucket via MinIO Console (http://localhost:9001) or CLI:
```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/oxidizer-configs
```

### 3. Start the Controller

In **Terminal 1**:
```bash
python3 controller.py
```

The controller reads configs from S3/MinIO, compiles the DAG, and dispatches tasks.

### 4. Start the API Server

In **Terminal 2**:
```bash
python3 api.py
```

The API server runs on `http://localhost:8000` and provides MCP endpoints.

### 5. Invoke the Topology

In **Terminal 3**:
```bash
python3 invoke.py
```

This triggers the `sample` topology to start processing.

### 6. Start Workers

In **Terminal 4** (and additional terminals for each node):
```bash
python3 worker.py
```

Workers process tasks from the streams. **Re-run the worker command after each node completes** to process the next node in the DAG:

```
bronze.fetch_data → silver.transform_data → gold.load_data → gold.load_data_2
```

## Pipeline Flow

```
JSONPlaceholder API (/posts)
         │
         ▼
┌───────────────────┐
│ bronze.fetch_data │  ← Fetch 100 posts from API
└────────┬──────────┘
         │ (Redis Stream)
         ▼
┌───────────────────────┐
│ silver.transform_data │  ← Pass through transformation
└────────┬──────────────┘
         │ (Redis Stream)
         ▼
┌─────────────────┐
│ gold.load_data  │  ← INSERT into Iceberg table
└────────┬────────┘
         │ (SQL read)
         ▼
┌──────────────────┐
│ gold.load_data_2 │  ← Read from Iceberg
└──────────────────┘
```

## Troubleshooting

- **Controller exits immediately**: Check Redis connection and S3/MinIO bucket exists
- **Worker Idle**: Ensure the topology was invoked via `invoke.py`
- **S3/Glue Errors**: Verify AWS SSO login (`aws sso login --profile your-profile`)
