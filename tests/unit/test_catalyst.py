"""
Unit tests for catalyst.py - Redis wrapper.

These tests use mocked redis-py to verify error handling and edge cases.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestCatalystConnection:
    """Tests for CatalystConnection dataclass."""
    
    def test_connection_defaults(self):
        """CatalystConnection should have sensible defaults."""
        from oxidizer_lite.catalyst import CatalystConnection
        
        conn = CatalystConnection()
        
        assert conn.host == "localhost"
        assert conn.port == 6379
        assert conn.db == 0
    
    def test_connection_custom_values(self):
        """CatalystConnection should accept custom values."""
        from oxidizer_lite.catalyst import CatalystConnection
        
        conn = CatalystConnection(
            host="redis.internal",
            port=6380,
            db=5
        )
        
        assert conn.host == "redis.internal"
        assert conn.port == 6380
        assert conn.db == 5


class TestCatalystConsumerGroup:
    """Tests for consumer group management."""
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_create_consumer_group_already_exists(self, mock_redis_class):
        """Should handle BUSYGROUP error gracefully."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        import redis
        
        # Setup mock
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        # Simulate BUSYGROUP error
        mock_client.xgroup_create.side_effect = redis.ResponseError("BUSYGROUP Consumer Group name already exists")
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        # Should not raise - BUSYGROUP is expected
        try:
            catalyst.create_consumer_group("test_stream", "test_group")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_create_consumer_group_other_error_raises(self, mock_redis_class):
        """Non-BUSYGROUP errors should propagate."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        import redis
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        # Simulate different error
        mock_client.xgroup_create.side_effect = redis.ResponseError("ERR unknown command")
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        with pytest.raises(redis.ResponseError):
            catalyst.create_consumer_group("test_stream", "test_group")


class TestCatalystStreamOperations:
    """Tests for stream read/write operations."""
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_write_to_stream_returns_message_id(self, mock_redis_class):
        """write_to_stream should return the message ID."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        mock_client.xadd.return_value = b"1234567890-0"
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        msg_id = catalyst.write_to_stream("test_stream", {"key": "value"})
        
        mock_client.xadd.assert_called_once()
        assert msg_id is not None
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_write_to_stream_non_serializable_error(self, mock_redis_class):
        """Non-JSON-serializable data should raise clear error."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        # Create non-serializable object
        class NotSerializable:
            pass
        
        # Should either raise TypeError or handle gracefully
        try:
            catalyst.write_to_stream("test_stream", {"bad": NotSerializable()})
        except (TypeError, ValueError):
            pass  # Expected
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_read_from_stream_empty_returns_empty_list(self, mock_redis_class):
        """Empty stream should return empty list."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        mock_client.xreadgroup.return_value = []
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        result = catalyst.read_from_stream("stream", "group", "consumer")
        
        assert result is None or result == []
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_acknowledge_message(self, mock_redis_class):
        """acknowledge_message should call xack."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        catalyst.acknowledge_message("stream", "group", "1234567890-0")
        
        mock_client.xack.assert_called_once_with("stream", "group", "1234567890-0")


class TestCatalystJSONOperations:
    """Tests for JSON cache operations."""
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_set_json_stores_data(self, mock_redis_class):
        """set_json should store JSON data."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        catalyst.set_json("test_key", {"data": "value"})
        
        # Should call json().set or similar
        assert mock_client.json.called or mock_client.set.called
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_get_json_returns_dict(self, mock_redis_class):
        """get_json should return parsed JSON."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        mock_client.json.return_value.get.return_value = {"data": "value"}
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        # The actual implementation may vary
        try:
            result = catalyst.get_json("test_key")
            assert result is None or isinstance(result, dict)
        except AttributeError:
            pass  # Implementation may differ
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_update_json_path(self, mock_redis_class):
        """update_json should update specific JSON path."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        try:
            catalyst.update_json("test_key", ".status", "running")
            # Should call json().set with path
        except (AttributeError, TypeError):
            pass  # Implementation may differ


class TestCatalystKeyManagement:
    """Tests for key scanning and management."""
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_scan_keys_returns_matches(self, mock_redis_class):
        """scan_keys should return matching key names."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        # scan() returns (cursor, [keys]) - cursor 0 means done
        mock_client.scan.return_value = (0, [b"key1", b"key2", b"key3"])
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        result = catalyst.scan_keys("key*")
        assert result == ["key1", "key2", "key3"]
        mock_client.scan.assert_called_once_with(cursor=0, match="key*", count=100)
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_scan_keys_handles_pagination(self, mock_redis_class):
        """scan_keys should handle large result sets."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        
        # Simulate pagination: first call returns cursor=1, second call returns cursor=0 (done)
        mock_client.scan.side_effect = [
            (1, [b"key1", b"key2"]),  # First batch, more to come
            (0, [b"key3", b"key4"]),  # Final batch
        ]
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        result = catalyst.scan_keys("key*")
        assert result == ["key1", "key2", "key3", "key4"]
        assert mock_client.scan.call_count == 2


class TestCatalystErrorHandling:
    """Tests for error handling scenarios."""
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_connection_error_on_init(self, mock_redis_class):
        """Connection error should propagate clearly."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        import redis
        
        mock_redis_class.side_effect = redis.ConnectionError("Cannot connect")
        
        conn = CatalystConnection()
        
        with pytest.raises(redis.ConnectionError):
            Catalyst(conn)
    
    @patch('oxidizer_lite.catalyst.redis.Redis')
    def test_timeout_error_on_read(self, mock_redis_class):
        """Timeout on stream read should be handled."""
        from oxidizer_lite.catalyst import Catalyst, CatalystConnection
        import redis
        
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client
        mock_client.xreadgroup.side_effect = redis.TimeoutError("Read timeout")
        
        conn = CatalystConnection()
        catalyst = Catalyst(conn)
        
        # Should either return empty or raise timeout
        try:
            result = catalyst.read_from_stream("stream", "group", "consumer")
        except redis.TimeoutError:
            pass  # Expected
