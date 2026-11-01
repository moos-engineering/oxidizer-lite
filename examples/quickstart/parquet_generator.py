"""Generate a reproducible million-row Parquet fixture for the quickstart."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import random


def write_observations(path: Path, *, size: int = 100_000, entities: int = 10) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if size < 1 or entities < 1 or size * entities > 10_000_000:
        raise ValueError("Fixture size must be between 1 and 10,000,000 records")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    revision = datetime(2026, 10, 1, tzinfo=timezone.utc)
    schema = pa.schema([
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_revision", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("metric_value", pa.float64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("reading", pa.int64(), nullable=False),
    ])
    with pq.ParquetWriter(path, schema, compression="zstd") as writer:
        for entity_index in range(entities):
            rng = random.Random(42 + entity_index)
            records = [
                {
                    "entity_id": f"entity-{entity_index:06d}",
                    "observed_at": start + timedelta(hours=time_index),
                    "source_revision": revision,
                    "metric_value": round(rng.uniform(-1000, 1000), 4),
                    "status": ("nominal", "warning", "critical")[time_index % 3],
                    "reading": time_index,
                }
                for time_index in range(size)
            ]
            writer.write_table(pa.Table.from_pylist(records, schema=schema), row_group_size=65_536)
    return size * entities


if __name__ == "__main__":
    output = Path(__file__).with_name("observations.parquet")
    print(f"Wrote {write_observations(output):,} records to {output}")