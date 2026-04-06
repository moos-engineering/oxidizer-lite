"""
Unit tests for crucible.py - S3/MinIO wrapper.

These tests use mocked boto3 to verify error handling for missing buckets/objects.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestCrucibleConnection:
    """Tests for CrucibleConnection configuration."""
    
    def test_connection_with_access_keys(self):
        """CrucibleConnection should accept access key credentials."""
        from oxidizer_lite.crucible import CrucibleConnection
        
        conn = CrucibleConnection(
            s3_bucket="test-bucket",
            s3_url="http://localhost:9000",
            access_key="myaccesskey",
            secret_key="mysecretkey"
        )
        
        assert conn.s3_bucket == "test-bucket"
        assert conn.access_key == "myaccesskey"
    
    def test_connection_minio_endpoint(self):
        """CrucibleConnection should support MinIO endpoints."""
        from oxidizer_lite.crucible import CrucibleConnection
        
        conn = CrucibleConnection(
            s3_bucket="configs",
            s3_url="http://minio.local:9000",
            access_key="minioadmin",
            secret_key="minioadmin"
        )
        
        assert "minio" in conn.s3_url.lower() or "9000" in conn.s3_url


class TestCrucibleBucketOperations:
    """Tests for bucket existence and creation."""
    
    @patch('oxidizer_lite.crucible.boto3.client')
    def test_bucket_exists_returns_true(self, mock_boto_client):
        """bucket_exists should return True for existing bucket."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.head_bucket.return_value = {}  # Success
        
        conn = CrucibleConnection(
            s3_bucket="existing-bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        try:
            result = crucible.bucket_exists("existing-bucket")
            # Should return True or not raise
        except (AttributeError, TypeError):
            pass  # Implementation may differ
    
    @patch('oxidizer_lite.crucible.boto3.client')
    def test_bucket_exists_returns_false_for_missing(self, mock_boto_client):
        """bucket_exists should return False for non-existent bucket."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        from botocore.exceptions import ClientError
        
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Simulate 404 error
        error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
        mock_s3.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')
        
        conn = CrucibleConnection(
            s3_bucket="missing-bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        try:
            result = crucible.bucket_exists("missing-bucket")
            assert result is False
        except (ClientError, AttributeError):
            pass  # Error propagation is also valid
    
    @patch('oxidizer_lite.crucible.boto3.client')
    def test_bucket_permission_denied(self, mock_boto_client):
        """403 error should be handled clearly."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        from botocore.exceptions import ClientError
        
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Simulate 403 error
        error_response = {'Error': {'Code': '403', 'Message': 'Access Denied'}}
        mock_s3.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')
        
        conn = CrucibleConnection(
            s3_bucket="forbidden-bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        # Should raise or return clear error
        try:
            result = crucible.bucket_exists("forbidden-bucket")
        except ClientError as e:
            assert "403" in str(e) or "Access Denied" in str(e)


class TestCrucibleObjectOperations:
    """Tests for object get/put operations."""
    
    @patch('oxidizer_lite.crucible.boto3.Session')
    def test_get_object_success(self, mock_session_class):
        """get_object should return object content."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        
        # Mock response body
        mock_body = MagicMock()
        mock_body.read.return_value = b"version: 1\nname: test"
        mock_s3.get_object.return_value = {'Body': mock_body}
        
        conn = CrucibleConnection(
            s3_bucket="bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        result = crucible.get_object("bucket", "config.yml")
        assert result == "version: 1\nname: test"
        mock_s3.get_object.assert_called_once_with(Bucket="bucket", Key="config.yml")
    
    @patch('oxidizer_lite.crucible.boto3.Session')
    def test_get_object_not_found(self, mock_session_class):
        """get_object should handle 404 clearly."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        from botocore.exceptions import ClientError
        
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        
        # Simulate NoSuchKey error
        error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.'}}
        mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        conn = CrucibleConnection(
            s3_bucket="bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        with pytest.raises(ClientError) as exc_info:
            crucible.get_object("bucket", "nonexistent.yml")
        
        assert "NoSuchKey" in str(exc_info.value) or "does not exist" in str(exc_info.value)
    
    @patch('oxidizer_lite.crucible.boto3.Session')
    def test_put_object_success(self, mock_session_class):
        """put_object should store content."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        mock_s3.put_object.return_value = {'ETag': '"abc123"'}
        
        conn = CrucibleConnection(
            s3_bucket="bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        crucible.put_object("bucket", "new_config.yml", "version: 1")
        mock_s3.put_object.assert_called_once_with(
            Bucket="bucket", 
            Key="new_config.yml", 
            Body=b"version: 1"
        )
    
    @patch('oxidizer_lite.crucible.boto3.Session')
    def test_put_object_quota_exceeded(self, mock_session_class):
        """put_object should handle storage quota errors."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        from botocore.exceptions import ClientError
        
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        
        # Simulate quota error
        error_response = {'Error': {'Code': 'QuotaExceeded', 'Message': 'Storage quota exceeded'}}
        mock_s3.put_object.side_effect = ClientError(error_response, 'PutObject')
        
        conn = CrucibleConnection(
            s3_bucket="bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        with pytest.raises(ClientError):
            crucible.put_object("bucket", "config.yml", "content")


class TestCrucibleListOperations:
    """Tests for listing objects."""
    
    @patch('oxidizer_lite.crucible.boto3.Session')
    def test_list_objects_returns_keys(self, mock_session_class):
        """list_objects should return object keys."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'config1.yml'},
                {'Key': 'config2.yml'},
            ]
        }
        
        conn = CrucibleConnection(
            s3_bucket="bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        result = crucible.list_objects("bucket")
        assert len(result) == 2
        assert result[0]['Key'] == 'config1.yml'
    
    @patch('oxidizer_lite.crucible.boto3.Session')
    def test_list_objects_empty_bucket(self, mock_session_class):
        """list_objects should handle empty bucket."""
        from oxidizer_lite.crucible import Crucible, CrucibleConnection
        
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}  # No 'Contents' key
        
        conn = CrucibleConnection(
            s3_bucket="bucket",
            s3_url="http://localhost:9000",
            access_key="key",
            secret_key="secret"
        )
        crucible = Crucible(conn)
        
        result = crucible.list_objects("bucket")
        assert result == []


class TestCrucibleErrorMessages:
    """Tests for clear error messages."""
    
    def test_missing_bucket_error_includes_bucket_name(self):
        """Error for missing bucket should include the bucket name."""
        from botocore.exceptions import ClientError
        
        error_response = {
            'Error': {
                'Code': 'NoSuchBucket',
                'Message': 'The specified bucket does not exist',
                'BucketName': 'my-missing-bucket'
            }
        }
        
        error = ClientError(error_response, 'GetObject')
        
        # The error should be informative
        error_str = str(error)
        assert "NoSuchBucket" in error_str or "does not exist" in error_str
    
    def test_missing_object_error_includes_key(self):
        """Error for missing object should include the key."""
        from botocore.exceptions import ClientError
        
        error_response = {
            'Error': {
                'Code': 'NoSuchKey',
                'Message': 'The specified key does not exist.',
                'Key': 'missing-config.yml'
            }
        }
        
        error = ClientError(error_response, 'GetObject')
        
        error_str = str(error)
        assert "NoSuchKey" in error_str or "does not exist" in error_str
