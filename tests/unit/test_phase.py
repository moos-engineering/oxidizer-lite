"""
Unit tests for phase.py - Data structure serialization.

These tests verify that dataclasses serialize/deserialize correctly.
Critical for message passing between controller and workers.
"""

import pytest
import sys
import json
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from oxidizer_lite.phase import (
    InputStreamMethod,
    InputSQLMethod,
    InputAPIMethod,
    OutputStreamMethod,
    OutputSQLMethod,
    OutputAPIMethod,
    CheckpointMetadata,
    SchemaField,
    OxidizerDefaults,
    DEFAULTS,
)


class TestInputMethods:
    """Tests for input method dataclasses."""
    
    def test_input_stream_method_defaults(self):
        """InputStreamMethod should have sensible defaults."""
        method = InputStreamMethod(method="stream")
        
        assert method.method == "stream"
        assert method.batch_size == DEFAULTS.DEFAULT_BATCH_SIZE
        assert method.window == DEFAULTS.DEFAULT_WINDOW_SIZE
        assert method.block == DEFAULTS.DEFAULT_BLOCKING_TIMEOUT
    
    def test_input_stream_method_to_dict(self):
        """InputStreamMethod.to_dict() should return all fields."""
        method = InputStreamMethod(
            method="stream",
            batch_size=500,
            window=120
        )
        
        result = method.to_dict()
        
        assert isinstance(result, dict)
        assert result["method"] == "stream"
        assert result["batch_size"] == 500
        assert result["window"] == 120
    
    def test_input_sql_method_required_fields(self):
        """InputSQLMethod requires connection, sql_type, database, table."""
        method = InputSQLMethod(
            method="sql",
            connection={"name": "test_conn"},
            sql_type="select",
            database="mydb",
            table="mytable"
        )
        
        assert method.database == "mydb"
        assert method.table == "mytable"
        assert method.sql_type == "select"
    
    def test_input_sql_method_to_dict(self):
        """InputSQLMethod.to_dict() should include all fields."""
        method = InputSQLMethod(
            method="sql",
            connection={"name": "conn1"},
            sql_type="select",
            database="db",
            table="tbl",
            batch_size=100,
            columns=["col1", "col2"],
            filters={"col1": {"operator": ">", "value": 10}},
            limit=1000
        )
        
        result = method.to_dict()
        
        assert result["columns"] == ["col1", "col2"]
        assert result["limit"] == 1000
        assert result["filters"] is not None
    
    def test_input_api_method_to_dict(self):
        """InputAPIMethod.to_dict() should include endpoint and http_method."""
        method = InputAPIMethod(
            method="api",
            connection={"base_url": "https://api.example.com"},
            endpoint="/users",
            http_method="GET"
        )
        
        result = method.to_dict()
        
        assert result["endpoint"] == "/users"
        assert result["http_method"] == "GET"


class TestOutputMethods:
    """Tests for output method dataclasses."""
    
    def test_output_stream_method_defaults(self):
        """OutputStreamMethod should allow minimal initialization."""
        method = OutputStreamMethod(method="stream")
        
        assert method.method == "stream"
        assert method.connection is None
        assert method.stream_name is None
    
    def test_output_sql_method_scd_fields(self):
        """OutputSQLMethod should support SCD Type 2 configuration."""
        method = OutputSQLMethod(
            method="sql",
            connection={"name": "conn"},
            database="warehouse",
            table="dim_customer",
            sql_type="scd2",
            primary_key=["customer_id"],
            begin_date_col="valid_from",
            end_date_col="valid_to",
            is_current_col="is_current",
            tracked_columns=["name", "email", "address"]
        )
        
        result = method.to_dict()
        
        assert result["sql_type"] == "scd2"
        assert result["primary_key"] == ["customer_id"]
        assert result["tracked_columns"] == ["name", "email", "address"]
    
    def test_output_api_method_to_dict(self):
        """OutputAPIMethod.to_dict() should include all fields."""
        method = OutputAPIMethod(
            method="api",
            connection={"base_url": "https://api.example.com"},
            endpoint="/webhook",
            http_method="POST",
            payload_template={"event": "data_ready"}
        )
        
        result = method.to_dict()
        
        assert result["http_method"] == "POST"
        assert result["payload_template"] == {"event": "data_ready"}


class TestCheckpointMetadata:
    """Tests for CheckpointMetadata dataclass."""
    
    def test_checkpoint_defaults(self):
        """CheckpointMetadata should have sensible defaults."""
        meta = CheckpointMetadata()
        
        assert meta.batch_index == 0
        assert meta.is_final is False
        assert meta.records_processed == 0
        assert meta.accumulated_function_runtime == 0.0
    
    def test_checkpoint_accumulation(self):
        """Checkpoint should accumulate runtime/memory metrics."""
        meta = CheckpointMetadata(
            batch_index=5,
            records_processed=500,
            accumulated_function_runtime=10.5,
            accumulated_function_memory=256.0
        )
        
        assert meta.batch_index == 5
        assert meta.records_processed == 500
        assert meta.accumulated_function_runtime == 10.5
    
    def test_checkpoint_is_final_flag(self):
        """is_final flag indicates batch completion."""
        meta = CheckpointMetadata(is_final=True)
        
        assert meta.is_final is True
    
    def test_checkpoint_batch_cursors(self):
        """batch_cursors should store per-input cursor positions."""
        cursors = {
            "input1": "1234567890-0",
            "input2": "1234567891-5"
        }
        
        meta = CheckpointMetadata(batch_cursors=cursors)
        
        assert meta.batch_cursors == cursors
        assert meta.batch_cursors["input1"] == "1234567890-0"


class TestSchemaField:
    """Tests for SchemaField dataclass."""
    
    def test_schema_field_minimal(self):
        """SchemaField with just name and type."""
        field = SchemaField(name="user_id", type="int")
        
        assert field.name == "user_id"
        assert field.type == "int"
        assert field.description is None
    
    def test_schema_field_with_alias(self):
        """SchemaField with alias for column renaming."""
        field = SchemaField(
            name="original_name",
            type="string",
            alias="new_name"
        )
        
        assert field.alias == "new_name"
    
    def test_schema_field_with_expr(self):
        """SchemaField with expression for computed columns."""
        field = SchemaField(
            name="full_name",
            type="string",
            expr="CONCAT(first_name, ' ', last_name)"
        )
        
        assert field.expr == "CONCAT(first_name, ' ', last_name)"
    
    def test_schema_field_to_dict(self):
        """SchemaField.to_dict() should include all fields."""
        field = SchemaField(
            name="amount",
            type="decimal",
            description="Transaction amount in USD",
            alias="transaction_amount"
        )
        
        result = field.to_dict()
        
        assert result["name"] == "amount"
        assert result["description"] == "Transaction amount in USD"


class TestSerializationRoundtrip:
    """Tests for JSON serialization roundtrip."""
    
    def test_input_stream_method_json_roundtrip(self):
        """InputStreamMethod should survive JSON serialization."""
        original = InputStreamMethod(
            method="stream",
            batch_size=250,
            window=60
        )
        
        # Serialize
        json_str = json.dumps(original.to_dict())
        
        # Deserialize
        data = json.loads(json_str)
        restored = InputStreamMethod(**data)
        
        assert restored.method == original.method
        assert restored.batch_size == original.batch_size
    
    def test_checkpoint_metadata_json_roundtrip(self):
        """CheckpointMetadata should survive JSON serialization."""
        original = CheckpointMetadata(
            batch_index=10,
            is_final=False,
            records_processed=1000,
            accumulated_function_runtime=5.5
        )
        
        # Serialize (need to convert dataclass to dict first)
        from dataclasses import asdict
        json_str = json.dumps(asdict(original))
        
        # Deserialize
        data = json.loads(json_str)
        restored = CheckpointMetadata(**data)
        
        assert restored.batch_index == original.batch_index
        assert restored.is_final == original.is_final
        assert restored.accumulated_function_runtime == original.accumulated_function_runtime
    
    def test_nested_structures_serialize(self):
        """Complex nested structures should serialize correctly."""
        method = InputSQLMethod(
            method="sql",
            connection={"name": "conn", "config": {"host": "localhost", "port": 5432}},
            sql_type="select",
            database="analytics",
            table="events",
            columns=[{"name": "event_id"}, {"name": "timestamp"}],
            filters={"timestamp": {"operator": ">", "value": "2024-01-01"}}
        )
        
        # Should not raise
        json_str = json.dumps(method.to_dict())
        data = json.loads(json_str)
        
        assert data["connection"]["config"]["host"] == "localhost"


class TestOxidizerDefaults:
    """Tests for OxidizerDefaults configuration."""
    
    def test_defaults_exist(self):
        """DEFAULTS singleton should be accessible."""
        assert DEFAULTS is not None
        assert isinstance(DEFAULTS, OxidizerDefaults)
    
    def test_default_values_reasonable(self):
        """Default values should be reasonable for production use."""
        assert DEFAULTS.DEFAULT_BATCH_SIZE > 0
        assert DEFAULTS.DEFAULT_API_TIMEOUT > 0
        assert DEFAULTS.MAX_RETRY_ATTEMPTS >= 1
        assert DEFAULTS.LOG_TTL > 0
    
    def test_redis_defaults(self):
        """Redis defaults should be standard values."""
        assert DEFAULTS.REDIS_HOST == "localhost"
        assert DEFAULTS.REDIS_PORT == 6379
        assert DEFAULTS.REDIS_DB == 0
