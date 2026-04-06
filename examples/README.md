# Oxidizer-Lite Examples

Example pipelines demonstrating Oxidizer-Lite capabilities.

## Examples

| Example | Description | Data Source |
|---------|-------------|-------------|
| [skeleton/](skeleton/) | Minimal starter template | JSONPlaceholder API |
| [scd2/](scd2/) | SCD Type 2 historical tracking | Open-Meteo weather (18 US cities) |

Each example includes detailed setup instructions in its README.

## Quick Start

```bash
cd examples/skeleton   # or scd2
```

Then follow the README in each folder for:
1. Configuring AWS/MinIO credentials
2. Starting Redis
3. Running controller, API, and workers

## Prerequisites

- **Redis** — for streams and state
- **MinIO or S3** — for YAML config storage
- **AWS Glue Catalog** (optional) — for Iceberg table output
