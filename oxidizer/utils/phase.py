import json
import uuid
from typing import Any, List
from dataclasses import dataclass, asdict, field
from enum import Enum



@dataclass
class CheckpointMetadata:
    """Serializable progress snapshot carried with a node task.

    ``batch_index`` identifies the next batch, ``batch_cursors`` resumes
    paginated inputs, and ``batch_final`` prevents exhausted inputs from being
    fetched again. ``is_final`` summarizes whether every configured input is
    exhausted. ``records_processed`` counts input rows in this checkpoint's
    batch; ``total_records_so_far`` accumulates input rows across the node run.
    Accumulated runtimes are seconds summed across batches: preprocess covers reading the task and
    inputs, function covers the wrapped handler, and postprocess covers output
    routing. Peak memory fields are worker-cgroup bytes, maxed across batches
    for the corresponding phase.
    """

    node_id: str
    batch_methods: dict = None
    batch_index: int = 0
    batch_cursors: dict = None
    batch_final: dict = field(default_factory=dict)
    records_processed: int = 0
    total_records_so_far: int = 0
    total_late_arrivals_ignored: int = 0
    is_final: bool = False
    accumulated_preprocess_runtime: float = 0.0
    accumulated_function_runtime: float = 0.0
    accumulated_postprocess_runtime: float = 0.0
    peak_preprocess_memory_bytes: int | None = None
    peak_function_memory_bytes: int | None = None
    peak_postprocess_memory_bytes: int | None = None

    def to_dict(self):
        """Return all checkpoint fields as a plain dictionary.

        Returns:
            A recursively converted dictionary suitable for message/state
            serialization.
        """
        return asdict(self)


class WorkerTaskType(Enum):
    """Commands sent from Oxidizer to a Reagent worker.

    ``START_NODE`` starts or resumes node execution. ``CHECKPOINT_NODE`` and
    ``CANCEL_NODE`` reserve explicit worker-control commands; the current
    one-shot worker path primarily consumes start-node tasks.
    """

    START_NODE = "start_node"
    CHECKPOINT_NODE = "checkpoint_node"
    CANCEL_NODE = "cancel_node"


class ReagentUpdateType(Enum):
    """Lifecycle events sent from Reagent to the Oxidizer controller.

    Final completion is represented by ``CHECKPOINT`` with
    ``checkpoint_metadata.is_final`` set, not by a separate success update.
    """

    STARTED = "started"
    CHECKPOINT = "checkpoint"
    FAILED = "failed"
    HEARTBEAT = "heartbeat"


@dataclass
class TaskMessage:
    """Controller-to-worker task envelope for one node execution.

    The node mapping contains its configuration, ``connections`` contains the
    connection configs referenced by the workflow, and checkpoint metadata
    resumes a prior batch. The envelope carries configuration and execution
    context, not computed handler output.
    """

    type: str
    formula_id: str
    run_id: str
    node_id: str
    layer_id: str
    node: dict
    connections: dict = field(default_factory=dict)
    checkpoint_metadata: dict | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "TaskMessage":
        """Build a task envelope from its serialized mapping.

        Args:
            data: Mapping with required task identity and node fields, plus
                optional connections and checkpoint metadata.

        Returns:
            A TaskMessage. Missing required keys raise ``KeyError``.
        """
        return cls(
            type=data["type"],
            formula_id=data["formula_id"],
            run_id=data["run_id"],
            node_id=data["node_id"],
            layer_id=data["layer_id"],
            node=data["node"],
            connections=data.get("connections", {}),
            checkpoint_metadata=data.get("checkpoint_metadata"),
        )

    def to_dict(self):
        """Serialize the task and nested dataclass values to dictionaries."""
        return asdict(self)


@dataclass
class ReagentUpdateMessage:
    """Worker-to-controller lifecycle update for a Formula node.

    Updates carry status, checkpoint, error, or heartbeat data. They do not
    carry computed output; Reagent routes that directly through configured
    output methods before sending the update.
    """

    type: str
    formula_id: str
    run_id: str
    node_id: str
    checkpoint_metadata: dict | None = None
    error_details: dict | None = None
    heartbeat: dict | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ReagentUpdateMessage":
        """Build an update from its serialized mapping.

        Args:
            data: Mapping with required update identity/type and optional
                checkpoint, error, or heartbeat fields.

        Returns:
            A ReagentUpdateMessage. Unknown keys are ignored; missing required
            keys raise ``KeyError``.
        """
        return cls(
            type=data["type"],
            formula_id=data["formula_id"],
            run_id=data["run_id"],
            node_id=data["node_id"],
            checkpoint_metadata=data.get("checkpoint_metadata"),
            error_details=data.get("error_details"),
            heartbeat=data.get("heartbeat"),
        )

    def to_dict(self):
        """Serialize the update fields for the controller stream."""
        return asdict(self)


@dataclass
class InputFetchResult:
    """Result of one input-method fetch, including deferred commit metadata.

    ``is_final`` indicates whether the source is exhausted. Stream handlers
    place acknowledgement tuples in ``ack_msgs`` rather than acknowledging
    immediately, allowing Reagent to commit source messages only after output
    routing succeeds. ``next_cursor`` carries pagination state to the next
    batch when the input method supports cursors.
    """

    data: Any
    is_final: bool
    ack_msgs: list[tuple] = field(default_factory=list)
    next_cursor: Any = None