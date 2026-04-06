"""
Oxidizer-Lite: A distributed, event-driven DAG orchestration engine built on Redis Streams.
"""

__version__ = "0.1.0"

# Core components
from oxidizer_lite.oxidizer import Oxidizer
from oxidizer_lite.reagent import Reagent
from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.crucible import Crucible, CrucibleConnection
from oxidizer_lite.microscope import Microscope
from oxidizer_lite.anvil import SQLEngine, APIEngine
from oxidizer_lite.lattice import Lattice
from oxidizer_lite.topology import Topology
from oxidizer_lite.residue import Residue
from oxidizer_lite.incubation import Incubation

# Data models
from oxidizer_lite.phase import (
    GlueCatalogConnection,
    DuckLakeConnection,
    APIConnection,
    TaskMessage,
    NodeConfiguration,
    ErrorDetails,
    OutputSQLMethod,
    InputSQLMethod,
    InputStreamMethod,
    InputAPIMethod,
    OutputStreamMethod,
    OutputAPIMethod,
)

__all__ = [
    # Version
    "__version__",
    # Core
    "Oxidizer",
    "Reagent",
    "Catalyst",
    "CatalystConnection",
    "Crucible",
    "CrucibleConnection",
    "Microscope",
    "SQLEngine",
    "APIEngine",
    "Lattice",
    "Topology",
    "Residue",
    "Incubation",
    # Data models
    "GlueCatalogConnection",
    "DuckLakeConnection",
    "APIConnection",
    "TaskMessage",
    "NodeConfiguration",
    "ErrorDetails",
    "OutputSQLMethod",
    "InputSQLMethod",
    "InputStreamMethod",
    "InputAPIMethod",
    "OutputStreamMethod",
    "OutputAPIMethod",
]
