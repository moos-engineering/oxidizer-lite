"""Public package interface for Oxidizer."""

from .core.catalyst import Catalyst, KafkaCatalyst, RedisCatalyst
from .core.formula import Formula, NodeStatus, RedisFormula, S3Formula
from .core.lattice import (
    IcebergRestConnection,
    KafkaConnection,
    Lattice,
    OllamaConnection,
    RedisConnection,
    S3Connection,
    S3TablesConnection,
)
from .oxidizer import Oxidizer
from .reagent import DataHandler, Reagent
from .utils.phase import CheckpointMetadata, InputFetchResult, ReagentUpdateMessage, ReagentUpdateType, TaskMessage, WorkerTaskType
from .core.residue import Ash, RedisStreamLogSink, Residue, configured_residue_sink_names, log_stream_name
from .utils.crucible import Crucible, DuckDBCrucible, SparkCrucible
from .utils.anvil import Anvil
from .utils.microscope import Microscope

__all__ = [
    "Ash",
    "Anvil",
    "Catalyst",
    "CheckpointMetadata",
    "Crucible",
    "DataHandler",
    "DuckDBCrucible",
    "Formula",
    "IcebergRestConnection",
    "InputFetchResult",
    "KafkaCatalyst",
    "KafkaConnection",
    "Lattice",
    "Microscope",
    "NodeStatus",
    "OllamaConnection",
    "Oxidizer",
    "RedisCatalyst",
    "RedisConnection",
    "RedisFormula",
    "RedisStreamLogSink",
    "Reagent",
    "ReagentUpdateMessage",
    "ReagentUpdateType",
    "Residue",
    "S3Connection",
    "S3Formula",
    "S3TablesConnection",
    "SparkCrucible",
    "TaskMessage",
    "WorkerTaskType",
    "configured_residue_sink_names",
    "log_stream_name",
]
