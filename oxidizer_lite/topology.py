# DAG
import json
from enum import Enum
from collections import defaultdict, deque 
from typing import Dict, List

from oxidizer_lite.residue import Residue, Ash
from oxidizer_lite.phase import CheckpointMetadata

class EnumEncoder(json.JSONEncoder):
    """Custom JSON encoder that serializes Enum members by their value."""

    def default(self, obj):
        """Returns the Enum value for Enum instances, otherwise delegates to the parent encoder."""
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)

class NodeStatus(Enum):
    """
    Possible phases for DAG nodes in the Oxidizer distributed system.
    """
    SCHEDULED        = "scheduled"        # time condition satisfied; may also require upstream deps (configurable)
    PENDING          = "pending"          # waiting for upstream deps or external trigger (API, file arrival, etc.)
    READY            = "ready"            # all deps satisfied; eligible to dispatch
    LOCKED           = "locked"           # waiting to acquire a cross-run node lock (FIFO queue per lock key)
    DISPATCHED       = "dispatched"       # task message published to stream; awaiting worker ACK
    RUNNING          = "running"          # worker ACK received; processing — includes mid-batch checkpoint loops
    LIVE             = "live"             # streaming node running its consume loop (runs indefinitely until stopped)
    PAUSED           = "paused"           # live node manually paused; resumes from last durable cursor
    PAUSED_UPSTREAM  = "paused_upstream"  # live node paused because an upstream dep failed; auto-resumes on recovery
    SUCCESS          = "success"          # completed successfully; releases lock and unblocks downstream nodes
    FAILED           = "failed"           # processing failed; retains last checkpoint cursor for resume
    RETRYING         = "retrying"         # backing off before re-entering READY; tracks attempt count
    SKIPPED          = "skipped"          # upstream failed/skipped and this node is configured to skip on failure
    CANCELLED        = "cancelled"        # externally cancelled by operator or API; not the same as SKIPPED
    TIMED_OUT        = "timed_out"        # exceeded deadline; distinct from FAILED for operational alerting


class WorkerMessageType(Enum):
    """
    Message types flowing between workers and the controller.
    Distinct from NodeStatus — these are transport signals, not lifecycle states.
    """
    STARTED          = "started"          # worker picked up task; controller transitions node to RUNNING
    CHECKPOINT       = "checkpoint"       # batch N complete; carries cursor so controller can re-dispatch next batch
    # SUCCESS          = "success"          # final batch done (is_final=True); controller transitions node to SUCCESS
    FAILED           = "failed"           # unrecoverable error; carries last known cursor and attempt number
    HEARTBEAT        = "heartbeat"        # periodic liveness ping for long-running batches; resets DISPATCHED timeout
    # PAUSE_ACK        = "pause_ack"        # worker confirms it has drained and committed before halting
    # RESUME_ACK       = "resume_ack"       # worker confirms it has resumed from the given cursor


class WorkerTaskType(Enum):
    """
    Types of tasks that can be dispatched to workers.
    """
    START_NODE     = "start_node"     # perform the work of a specific DAG node
    CHECKPOINT_NODE    = "checkpoint_node"  # perform a checkpoint for a long-running node


class Topology(Residue):
    """
    Represents the DAG topology of nodes and their dependencies.
    """
    def __init__(self): 
        """Initializes the Topology with empty node, edge, status, and results registries."""
        super().__init__(component_name="topology")

        self.nodes: Dict[str, dict] = {}       # "config.layer.table" -> table config
        self.edges: Dict[str, List[str]] = defaultdict(list)  # parent -> [children]
        self.reverse_edges: Dict[str, List[str]] = defaultdict(list)  # child -> [parents]
        self.status: Dict[str, NodeStatus] = {}  # "config.layer.table" -> NodeStatus
        self.results: Dict[str, str] = {}  # "config.layer.table" -> output artifact path
    

    def dag(self, config: dict):
        """
        Updates the DAG structure based on the lattice configuration.

        Args:
            config (dict): The topology configuration dictionary loaded from the lattice configuration file, including layers, nodes, and their dependencies.

        Returns:
            dict: A dictionary representing the DAG structure with nodes, edges, reverse_edges, and status.
        """
        #0 - Config Details 
        layers = config.get("layers", [])

        #1 - Register Nodes  
        base_checkpoint_metadata = CheckpointMetadata(
            batch_methods=None,
            batch_index=0,
            batch_cursors=None,
            accumulated_preprocess_runtime=0.0,
            accumulated_function_runtime=0.0,
            accumulated_postprocess_runtime=0.0,
            accumulated_preprocess_memory=0.0,
            accumulated_function_memory=0.0,
            accumulated_postprocess_memory=0.0,
            is_final=False
        )

        for layer in layers:
            layer_name = layer["name"]
            self.residue(self.ash.DEBUG, f"TOPOLOGY: Processing layer '{layer_name}' with {len(layer.get('nodes', []))} nodes.")
            for node in layer.get("nodes", []):
                node_id = f"{layer_name}.{node.get('name')}" 
                self.residue(self.ash.DEBUG, f"TOPOLOGY: Registering node '{node_id}' with initial checkpoint metadata: {base_checkpoint_metadata.to_dict()}.")
                self.nodes[node_id] = node
                self.nodes[node_id]["layer"] = layer_name
                self.nodes[node_id]["checkpoint_metadata"] = base_checkpoint_metadata.to_dict()
                node_type = node.get("type", None)
                if node_type == "scheduled":
                    self.status[node_id] = NodeStatus.SCHEDULED  
                    self.residue(self.ash.DEBUG, f"TOPOLOGY: Registered scheduled node '{node_id}' with initial status SCHEDULED.")
                else:
                    self.status[node_id] = NodeStatus.PENDING 
                    self.residue(self.ash.DEBUG, f"TOPOLOGY: Registered node '{node_id}' with initial status PENDING.")

        #2 - Register Edges
        for layer in layers:
            layer_name = layer["name"]
            for node in layer.get("nodes", []):
                node_id = f"{layer_name}.{node.get('name')}"
                self.residue(self.ash.DEBUG, f"TOPOLOGY: Processing node edges '{node_id}' with {len(node.get('inputs', []))} inputs.")
                inputs = node.get("inputs", [])
                for source in inputs:
                    ref = source.get("ref")
                    if ref:
                        # Edge: ref (parent) -> node_id (child)
                        self.edges[ref].append(node_id)
                        self.reverse_edges[node_id].append(ref)
                        self.residue(self.ash.DEBUG, f"TOPOLOGY: Registered edge from '{ref}' to '{node_id}'.")

        topology = { 
            "nodes": self.nodes,
            "edges": self.edges,
            "reverse_edges": self.reverse_edges,
            "status": self.status
        }
        return topology

        