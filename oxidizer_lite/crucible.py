# S3
import boto3
from botocore.exceptions import TokenRetrievalError, ClientError

from oxidizer_lite.residue import Residue, Ash


class CrucibleConnection:
    def __init__(self, s3_bucket, s3_url="", access_key=None, secret_key=None, session_token=None, role_arn=None, sso_role_name=None):
        """
        Initializes the S3 connection configuration.
        
        Args:
            s3_bucket (str): The name of the S3 bucket.
            s3_url (str): The S3 endpoint URL for custom/local S3-compatible services.
            access_key (str | None): The AWS access key ID.
            secret_key (str | None): The AWS secret access key.
            session_token (str | None): The AWS session token for temporary credentials.
            role_arn (str | None): The ARN of the IAM role to assume.
            sso_role_name (str | None): The SSO role name for authentication.
        """
        self.s3_bucket = s3_bucket
        self.s3_url = s3_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.role_arn = role_arn
        self.sso_role_name = sso_role_name


class Crucible(Residue):
    def __init__(self, credentials: CrucibleConnection, region_name='us-east-1'):
        """
        Initializes the Crucible S3 client with the provided credentials and region.
        
        Args:
            credentials (CrucibleConnection): The S3 connection configuration.
            region_name (str): The AWS region name for the S3 client.
        """
        super().__init__(component_name="crucible")
        self.credentials = credentials
        self.bucket = self.credentials.s3_bucket
        self.region_name = region_name
        self.aws_session = self.create_aws_session()
        if self.credentials.s3_url:
            self.s3_client = self.aws_session.client('s3', endpoint_url=self.credentials.s3_url)
        else:
            self.s3_client = self.aws_session.client('s3')
        self.residue(self.ash.DEBUG, f"CRUCIBLE: Initialized with region '{self.region_name}' and S3 URL '{self.credentials.s3_url}'.")

    def create_aws_session(self):
        """
        Create a boto3 session based on the provided credentials. Supports direct access keys, role assumption, and SSO.
        Returns:
            boto3.Session: An authenticated boto3 session for interacting with AWS services.
        """
        if self.credentials.role_arn:
            sts_client = boto3.client('sts', region_name=self.region_name)
            assumed_role = sts_client.assume_role(
                RoleArn=self.credentials.role_arn,      
                RoleSessionName='CrucibleSession'
            )
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Assumed role '{self.credentials.role_arn}' for AWS session.")
            return boto3.Session(
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken'],
                region_name=self.region_name
            )
        elif self.credentials.sso_role_name:
            # Use boto3 to assume SSO role and create session
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Using SSO role '{self.credentials.sso_role_name}' for AWS session.")
            return boto3.Session(profile_name=self.credentials.sso_role_name)
        else:
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Using direct access keys for AWS session.")
            return boto3.Session(
                aws_access_key_id=self.credentials.access_key,
                aws_secret_access_key=self.credentials.secret_key,
                aws_session_token=self.credentials.session_token,
                region_name=self.region_name
            )


    def bucket_exists(self, bucket_name):
        """
        Check if an S3 bucket exists.
        Args:
            bucket_name: str
        Returns:
            bool: True if the bucket exists, False otherwise.
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Bucket '{bucket_name}' exists.")
            return True
        except self.s3_client.exceptions.NoSuchBucket:
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Bucket '{bucket_name}' does not exist.")
            return False
        except Exception as e:
            self.residue(self.ash.ERROR, f"CRUCIBLE: Error checking bucket existence for '{bucket_name}'.", error=str(e))
            return False

    def create_bucket(self, bucket_name):
        """
        Create a new S3 bucket if it does not already exist.
        Args:
            bucket_name: str
        Returns:
            None
        """
        if bucket_name not in [bucket['Name'] for bucket in self.s3_client.list_buckets().get('Buckets', [])]:
            self.s3_client.create_bucket(Bucket=bucket_name)
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Created bucket '{bucket_name}'.")
        else:
            self.residue(self.ash.DEBUG, f"CRUCIBLE: Bucket '{bucket_name}' already exists.")

    def list_objects(self, bucket_name, prefix=None):
        """
        List objects in an S3 bucket with an optional prefix filter.
        Args:
            bucket_name: str
            prefix: str | None (optional prefix to filter objects)
        Returns:
            list: A list of objects in the bucket, each as a dictionary of object metadata.
        """
        objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix) if prefix else self.s3_client.list_objects_v2(Bucket=bucket_name)
        self.residue(self.ash.DEBUG, f"CRUCIBLE: Listed objects in bucket '{bucket_name}' with prefix '{prefix}'.")
        return objects.get('Contents', [])

    def get_object(self, bucket_name, object_key):
        """
        Get an object from an S3 bucket.
        Args:
            bucket_name: str
            object_key: str
        Returns:
            str: The content of the object as a string.
        """
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read() 
        self.residue(self.ash.DEBUG, f"CRUCIBLE: Retrieved object '{object_key}' from bucket '{bucket_name}'.")
        return obj.decode('utf-8')  # Assuming the object is a text file. Adjust decoding as needed for different file types.

    def put_object(self, bucket_name, object_key, data):
        """
        Put an object into an S3 bucket.
        Args:
            bucket_name: str
            object_key: str
            data: str | bytes
        Returns:
            None
        """
        data = data.encode('utf-8') if isinstance(data, str) else data  # Ensure data is bytes
        self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data)
        self.residue(self.ash.DEBUG, f"CRUCIBLE: Put object '{object_key}' into bucket '{bucket_name}'.")

    def delete_object(self, bucket_name, object_key):
        """
        Delete an object from an S3 bucket.
        Args:
            bucket_name: str
            object_key: str
        Returns:
            None
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        self.residue(self.ash.DEBUG, f"CRUCIBLE: Deleted object '{object_key}' from bucket '{bucket_name}'.")
