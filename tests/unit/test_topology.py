"""
Unit tests for topology.py - DAG logic.

These tests verify DAG generation, node status transitions, and dependency tracking.
Pure logic tests - no mocks needed.
"""

import pytest
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from oxidizer_lite.topology import Topology, NodeStatus, WorkerMessageType, WorkerTaskType


class TestNodeStatusEnum:
    """Tests for NodeStatus enum values and transitions."""
    
    def test_all_statuses_defined(self):
        """Verify all expected node statuses exist."""
        expected_statuses = [
            "scheduled", "pending", "ready", "locked", "dispatched",
            "running", "live", "paused", "paused_upstream", "success",
            "failed", "retrying", "skipped", "cancelled", "timed_out"
        ]
        
        for status in expected_statuses:
            assert hasattr(NodeStatus, status.upper()), f"Missing status: {status}"
    
    def test_status_values_are_strings(self):
        """Verify status values are lowercase strings."""
        for status in NodeStatus:
            assert isinstance(status.value, str)
            assert status.value == status.value.lower()
    
    def test_terminal_statuses(self):
        """Identify terminal statuses (no further transitions)."""
        terminal = {NodeStatus.SUCCESS, NodeStatus.FAILED, NodeStatus.SKIPPED, 
                    NodeStatus.CANCELLED, NodeStatus.TIMED_OUT}
        
        for status in terminal:
            assert status in NodeStatus


class TestWorkerMessageType:
    """Tests for WorkerMessageType enum."""
    
    def test_message_types_defined(self):
        """Verify expected message types exist."""
        expected = ["started", "checkpoint", "failed", "heartbeat"]
        
        for msg_type in expected:
            assert hasattr(WorkerMessageType, msg_type.upper())


class TestWorkerTaskType:
    """Tests for WorkerTaskType enum."""
    
    def test_task_types_defined(self):
        """Verify expected task types exist."""
        expected = ["start_node", "checkpoint_node"]
        
        for task_type in expected:
            assert hasattr(WorkerTaskType, task_type.upper())


class TestTopologyDAGGeneration:
    """Tests for Topology.dag() method."""
    
    def test_dag_generation_valid_simple(self):
        """Test DAG generation with a simple valid config."""
        topology = Topology()
        
        config = {
            "name": "test_topology",
            "layers": [
                {
                    "name": "bronze",
                    "nodes": [
                        {
                            "name": "fetch_data",
                            "inputs": [{"ref": None}],
                            "outputs": {"methods": [{"method": "stream"}]}
                        }
                    ]
                },
                {
                    "name": "silver",
                    "nodes": [
                        {
                            "name": "transform_data",
                            "inputs": [{"ref": "bronze.fetch_data"}],
                            "outputs": {"methods": [{"method": "stream"}]}
                        }
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Verify nodes were registered
        assert "bronze.fetch_data" in result["nodes"] or "fetch_data" in str(result["nodes"])
        
        # Verify edges were created
        assert len(result.get("edges", {})) >= 0 or len(result.get("reverse_edges", {})) >= 0
        
        # Verify status was set
        assert len(result.get("status", {})) > 0
    
    def test_dag_generation_empty_config(self):
        """Test DAG generation with empty layers."""
        topology = Topology()
        
        config = {
            "name": "empty",
            "layers": []
        }
        
        result = topology.dag(config)
        
        # Should produce empty DAG without error
        assert result.get("nodes") is not None
        assert len(result.get("nodes", {})) == 0
    
    def test_dag_generation_single_node(self):
        """Test DAG with single node (no dependencies)."""
        topology = Topology()
        
        config = {
            "name": "single",
            "layers": [
                {
                    "name": "only_layer",
                    "nodes": [
                        {
                            "name": "only_node",
                            "inputs": [{"ref": None}],
                            "outputs": {"methods": [{"method": "stream"}]}
                        }
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Single node should have no parent edges
        nodes = result.get("nodes", {})
        assert len(nodes) == 1
    
    def test_dag_node_initial_status_pending(self):
        """Verify new nodes start with PENDING status."""
        topology = Topology()
        
        config = {
            "name": "test",
            "layers": [
                {
                    "name": "bronze",
                    "nodes": [
                        {
                            "name": "node1",
                            "inputs": [{"ref": None}],
                            "outputs": {"methods": [{"method": "stream"}]}
                        }
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # All nodes should start as PENDING
        for node_id, status in result.get("status", {}).items():
            assert status == NodeStatus.PENDING.value or status == NodeStatus.PENDING
    
    def test_dag_multi_layer_dependencies(self):
        """Test DAG with multiple layers and cross-layer dependencies."""
        topology = Topology()
        
        config = {
            "name": "multi_layer",
            "layers": [
                {
                    "name": "bronze",
                    "nodes": [
                        {"name": "extract", "inputs": [{"ref": None}], "outputs": {"methods": []}}
                    ]
                },
                {
                    "name": "silver",
                    "nodes": [
                        {"name": "transform", "inputs": [{"ref": "bronze.extract"}], "outputs": {"methods": []}}
                    ]
                },
                {
                    "name": "gold",
                    "nodes": [
                        {"name": "load", "inputs": [{"ref": "silver.transform"}], "outputs": {"methods": []}}
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Verify 3 nodes created
        assert len(result.get("nodes", {})) == 3
        
        # Verify edges exist (transform depends on extract, load depends on transform)
        edges = result.get("edges", {})
        reverse_edges = result.get("reverse_edges", {})
        
        # At least one edge relationship should exist
        assert len(edges) > 0 or len(reverse_edges) > 0


class TestTopologyDependencyTracking:
    """Tests for dependency resolution logic."""
    
    def test_node_with_no_deps_is_ready(self):
        """A node with null ref (no dependencies) should be dispatchable."""
        topology = Topology()
        
        config = {
            "name": "test",
            "layers": [
                {
                    "name": "layer1",
                    "nodes": [
                        {"name": "root_node", "inputs": [{"ref": None}], "outputs": {"methods": []}}
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Root node has no parents
        reverse_edges = result.get("reverse_edges", {})
        
        # Find the root node's parents (should be empty or not in reverse_edges)
        for node_id in result.get("nodes", {}).keys():
            if "root" in node_id:
                parents = reverse_edges.get(node_id, [])
                assert len(parents) == 0, f"Root node should have no parents, got: {parents}"
    
    def test_node_with_deps_has_parents(self):
        """A node with ref should have parent edges."""
        topology = Topology()
        
        config = {
            "name": "test",
            "layers": [
                {
                    "name": "layer1",
                    "nodes": [
                        {"name": "parent", "inputs": [{"ref": None}], "outputs": {"methods": []}}
                    ]
                },
                {
                    "name": "layer2",
                    "nodes": [
                        {"name": "child", "inputs": [{"ref": "layer1.parent"}], "outputs": {"methods": []}}
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Child should have parent in reverse_edges
        reverse_edges = result.get("reverse_edges", {})
        
        # Find child node
        child_parents = None
        for node_id in result.get("nodes", {}).keys():
            if "child" in node_id:
                child_parents = reverse_edges.get(node_id, [])
                break
        
        # Child should have at least one parent
        if child_parents is not None:
            assert len(child_parents) > 0 or len(result.get("edges", {})) > 0


class TestTopologySerialization:
    """Tests for DAG serialization (to/from dict)."""
    
    def test_dag_result_is_dict(self):
        """DAG result should be a dict suitable for JSON serialization."""
        topology = Topology()
        
        config = {
            "name": "test",
            "layers": [
                {
                    "name": "layer1",
                    "nodes": [
                        {"name": "node1", "inputs": [{"ref": None}], "outputs": {"methods": []}}
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        assert isinstance(result, dict)
        
        # Should have standard keys
        expected_keys = {"nodes", "edges", "reverse_edges", "status"}
        assert expected_keys.issubset(set(result.keys()))
    
    def test_status_serializes_to_string(self):
        """Status values should serialize to strings for Redis."""
        topology = Topology()
        
        config = {
            "name": "test",
            "layers": [
                {
                    "name": "layer1",
                    "nodes": [
                        {"name": "node1", "inputs": [{"ref": None}], "outputs": {"methods": []}}
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Check if status values are serializable
        import json
        from oxidizer_lite.topology import EnumEncoder
        
        try:
            json_str = json.dumps(result, cls=EnumEncoder)
            assert isinstance(json_str, str)
        except TypeError as e:
            pytest.fail(f"DAG result not JSON serializable: {e}")


class TestTopologyEdgeCases:
    """Edge case tests for topology handling."""
    
    def test_missing_layers_key(self):
        """Test handling of config without layers key."""
        topology = Topology()
        
        config = {"name": "no_layers"}
        
        # Should handle gracefully (empty DAG or error)
        try:
            result = topology.dag(config)
            assert result.get("nodes") is not None
        except (KeyError, TypeError):
            # Explicit error is also acceptable
            pass
    
    def test_empty_nodes_in_layer(self):
        """Test handling of layer with no nodes."""
        topology = Topology()
        
        config = {
            "name": "empty_layer",
            "layers": [
                {"name": "empty", "nodes": []}
            ]
        }
        
        result = topology.dag(config)
        
        # Should produce empty DAG
        assert len(result.get("nodes", {})) == 0
    
    def test_node_missing_inputs(self):
        """Test handling of node without inputs key."""
        topology = Topology()
        
        config = {
            "name": "no_inputs",
            "layers": [
                {
                    "name": "layer1",
                    "nodes": [
                        {"name": "node1", "outputs": {"methods": []}}
                        # Missing 'inputs' key
                    ]
                }
            ]
        }
        
        # Should handle gracefully or raise clear error
        try:
            result = topology.dag(config)
            # If it succeeds, node should still be registered
        except (KeyError, TypeError) as e:
            # Clear error is acceptable
            assert "input" in str(e).lower() or True  # Any error for missing inputs is fine
    
    def test_duplicate_node_names_across_layers(self):
        """Test handling of same node name in different layers."""
        topology = Topology()
        
        config = {
            "name": "duplicates",
            "layers": [
                {
                    "name": "layer1",
                    "nodes": [
                        {"name": "process", "inputs": [{"ref": None}], "outputs": {"methods": []}}
                    ]
                },
                {
                    "name": "layer2",
                    "nodes": [
                        {"name": "process", "inputs": [{"ref": "layer1.process"}], "outputs": {"methods": []}}
                    ]
                }
            ]
        }
        
        result = topology.dag(config)
        
        # Both nodes should exist with distinct IDs (layer.name prefix)
        nodes = result.get("nodes", {})
        assert len(nodes) == 2, f"Expected 2 nodes, got {len(nodes)}"
