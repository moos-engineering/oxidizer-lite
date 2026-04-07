# Oxidizer-Lite

A distributed, event-driven DAG orchestration engine built on Redis Streams. Oxidizer-Lite coordinates data pipelines using a controller-worker pattern — configs define the topology, Redis handles dispatch, and workers scale horizontally.

## Installation

```bash
pip install oxidizer-lite
```

Or install from source:

```bash
git clone https://github.com/moos-engineering/oxidizer-lite.git
cd oxidizer-lite
pip install -e .
```

## Quick Start

**1. Start infrastructure:**

```bash
# Redis (with JSON module)
docker run -d --name redis -p 6379:6379 redis/redis-stack:latest

# MinIO (S3-compatible storage)
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

**2. Create a worker:**

```python
from oxidizer_lite import Reagent, CatalystConnection

catalyst = CatalystConnection(host="localhost", port=6379, db=0)
reagent = Reagent(catalyst)

@reagent.react()
def process(data: dict, context: dict):
    # Your processing logic here
    return data.get("input", [])
```

**3. Run the controller:**

```python
from oxidizer_lite import Oxidizer, CatalystConnection, CrucibleConnection

catalyst = CatalystConnection(host="localhost", port=6379, db=0)
crucible = CrucibleConnection(
    s3_bucket="oxidizer-configs",
    s3_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin"
)

oxidizer = Oxidizer(catalyst, crucible)
oxidizer.oxidize()
```

**4. Start the API server:**

```python
from oxidizer_lite import Microscope, Catalyst, Crucible, CatalystConnection, CrucibleConnection

crucible = Crucible(CrucibleConnection(...))
catalyst = Catalyst(CatalystConnection(...))
microscope = Microscope(crucible=crucible, catalyst=catalyst)
microscope.run(host="0.0.0.0", port=8000)
```

## Architecture

```
YAML Config (S3)
      │
      ▼
┌──────────────┐   Redis Streams    ┌─────────────┐
│  Controller  │◄──────────────────►│  Worker(s)  │
│  (Oxidizer)  │   task dispatch /  │  (Reagent)  │
└──────┬───────┘   checkpoints      └─────┬───────┘
       │                                  │
       ▼                                  ▼
┌──────────────┐                    ┌─────────────┐
│    Redis     │                    │ SQL / API / │
│  State Cache │                    │  Streams    │
└──────────────┘                    └─────────────┘
```

**Controller** reads invocations, compiles the DAG, dispatches ready nodes, and processes worker updates. **Workers** fetch inputs, execute user functions, write outputs, and report checkpoints — all through Redis Streams.

## Project Structure

```
oxidizer-lite/
├── oxidizer_lite/          # Core package
│   ├── oxidizer.py         # DAG controller
│   ├── reagent.py          # Worker decorator
│   ├── catalyst.py         # Redis client
│   ├── anvil.py            # SQL/API engines
│   ├── crucible.py         # S3 storage
│   ├── microscope.py       # REST API + MCP
│   ├── lattice.py          # Config parser
│   ├── topology.py         # DAG compiler
│   ├── phase.py            # Data models
│   ├── incubation.py       # Scheduler
│   └── residue.py          # Logging
├── examples/
│   ├── skeleton/           # Minimal starter
│   └── scd2/               # SCD Type 2 pipeline
├── tests/
├── pyproject.toml
└── LICENSE
```

## Components

| Component | Module | Role |
|-----------|--------|------|
| **Oxidizer** | `oxidizer.py` | Core orchestrator — topology lifecycle, node dispatch, checkpoint processing |
| **Reagent** | `reagent.py` | Distributed worker — input fetching, user function execution, output handling |
| **Catalyst** | `catalyst.py` | Redis cache and streams — consumer groups, JSON state, TTL management |
| **Anvil** | `anvil.py` | SQL engine (DuckDB, Iceberg) and REST API engine for data I/O |
| **Crucible** | `crucible.py` | S3-compatible object storage — configs, artifacts, multi-auth (IAM, SSO, keys) |
| **Lattice** | `lattice.py` | YAML config parsing, validation, and S3 persistence |
| **Topology** | `topology.py` | DAG compiler — multi-layer nodes, edges, dependency resolution |
| **Phase** | `phase.py` | Dataclass models — task messages, I/O methods, checkpoints, errors |
| **Microscope** | `microscope.py` | REST API + MCP server — invoke topologies, query logs, manage configs |
| **Incubation** | `incubation.py` | Cron-based scheduler for scheduled nodes |
| **Residue** | `residue.py` | Structured logging with Redis persistence |

## Features

### I/O Methods

| Method | Input | Output |
|--------|-------|--------|
| **Streams** | Redis streams with batching, windowing, consumer groups | Write records to downstream streams |
| **SQL** | SELECT queries with pagination, SCD Type 2 support | INSERT, MERGE, UPDATE with auto-schema creation |
| **API** | HTTP GET with auth (bearer token, API key) | HTTP POST with JSON payload |

### DAG Orchestration
- **YAML-driven topology** — multi-layer DAG with dependency resolution
- **15 node lifecycle states** — PENDING, READY, DISPATCHED, RUNNING, LIVE, PAUSED, SUCCESS, FAILED, RETRYING, SKIPPED, etc.
- **Control signals** — pause, resume, and shutdown topologies during execution
- **Live streaming nodes** — run indefinitely until explicitly stopped

### Batch Processing
- **Stateful checkpoints** — cursor tracking, batch index, accumulated runtimes and memory per stage
- **Resumable execution** — workers resume from the last checkpoint on failure or restart
- **Auto-pagination** — SQL and stream inputs paginate automatically based on batch size

### Monitoring & Control
- **REST API** — health checks, topology invocation, log queries
- **MCP Server** (FastMCP) — AI-friendly tools for topology management
- **Structured logging** — per-component logs persisted to Redis with TTL
- **Performance profiling** — runtime and memory tracking per processing stage

### Storage
- **Redis** — topology state, stream coordination, JSON cache, log persistence
- **S3-compatible** — configs and artifacts via MinIO or AWS S3
- **Iceberg** — via AWS Glue Data Catalog with DuckDB

## Examples

See the [examples/](examples/) folder for complete working pipelines:

- **[skeleton/](examples/skeleton/)** — Minimal starter template (API → Streams → SQL)
- **[scd2/](examples/scd2/)** — SCD Type 2 pipeline with Open-Meteo weather API (18 US cities)

## API Usage

Invoke topologies via the MCP client:

```python
import asyncio
from fastmcp import Client

async def main():
    async with Client("http://localhost:8000/mcp") as client:
        # List available operations
        tools = await client.list_tools()
        
        # Invoke a topology
        result = await client.call_tool("invoke_topology", {"lattice_id": "scd2"})
        print(result)

asyncio.run(main())
```



## TODO:
[ ] Add Pause / Resume Functionality  
[ ] Scheduling - This will come with Rust Integration  
[ ] Node Retry Logic (How to Test)
[ ] Data Retreival Fallback Updates  
[ ] Improve Memory Metric Capture (Maybe Switch to Rust? Maybe update CheckpointMetadata?)
[ ] Add Default Override to Config - If Defaults in Config global, layer, node level - override the in code defaults
[ ] Ensure TTL on All Necessary keys / streams / etc. 


## Ideas: 
- Ack Node Logs (Reduce Existing TTL)
- Add Lineage View to Lattice Page in UI




## License

MIT License — see [LICENSE](LICENSE) for details.