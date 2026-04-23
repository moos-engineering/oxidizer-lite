import json
import uuid
import time
import socket
from typing import List
from dataclasses import dataclass, asdict, field



@dataclass
class OxidizerDefaults:
    """
    Default configuration values for Oxidizer components.
    """
    # S3 Defaults
    S3_REGION: str = "us-east-1"
    S3_ENDPOINT: str = "https://s3.amazonaws.com"
    S3_USE_SSL: bool = True
    # Redis Defaults
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    # Logging Defaults
    LOG_TTL: int = 7 * 24 * 3600  # 7 days in seconds for log retention in Redis
    # Batch Processing Defaults
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_BATCH_METHOD: str = "stream"  # or "sql" or "api"
    DEFAULT_WINDOW_SIZE: int = 60  # seconds for stream batch window
    DEFAULT_BLOCKING_TIMEOUT: int = 30  # seconds to wait for data before considering input complete
    # API Defaults
    DEFAULT_API_TIMEOUT: int = 30  # seconds
    DEFAULT_API_RETRIES: int = 3
    # Topology Execution Defaults
    NODE_STATUS_POLL_INTERVAL: int = 5  # seconds between polling for node status
    MAX_RETRY_ATTEMPTS: int = 3  # max retries for failed nodes

DEFAULTS = OxidizerDefaults()


@dataclass
class InputStreamMethod:
    method: str
    batch_size: int = DEFAULTS.DEFAULT_BATCH_SIZE
    window: int = DEFAULTS.DEFAULT_WINDOW_SIZE  # seconds for stream batch window
    block: int = DEFAULTS.DEFAULT_BLOCKING_TIMEOUT  # seconds to wait for data before considering input complete
    connection: dict = None # UPDATE WITH DIFFERENT CONNECTION TYPES
    stream_name: str = None # DEFAULTS TO BUILT / MANAGED STREAMS

    def to_dict(self):
        """
        Converts the InputStreamMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class InputSQSMethod:
    method: str
    batch_size: int = DEFAULTS.DEFAULT_BATCH_SIZE
    wait_time: int = DEFAULTS.DEFAULT_BLOCKING_TIMEOUT  # seconds to wait for messages before considering input complete
    connection: dict = None # UPDATE WITH DIFFERENT CONNECTION TYPES
    queue_name: str = None 

    def to_dict(self):
        """
        Converts the InputSQSMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class InputSQLMethod:
    method: str
    connection: dict # UPDATE WITH DIFFERENT CONNECTION TYPES  
    sql_type: str
    database: str
    table: str
    batch_size: int = DEFAULTS.DEFAULT_BATCH_SIZE
    query: str = None
    columns: List[str] = None
    filters: dict = None
    sort_by: List[str] = None
    limit: int = None

    def to_dict(self):
        """
        Converts the InputSQLMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class InputAPIMethod:
    method: str
    connection: dict # UPDATE WITH DIFFERENT CONNECTION TYPES
    endpoint: str
    http_method: str
    payload_template: dict = None

    def to_dict(self):
        """
        Converts the InputAPIMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)


@dataclass
class SchemaField:
    name: str
    type: str
    description: str = None
    alias: str = None
    expr: str = None

    def to_dict(self):
        """
        Converts the SchemaField to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class Schema:
    fields: List[SchemaField]

    def to_dict(self):
        """
        Converts the Schema to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)



@dataclass
class OutputStreamMethod:
    method: str
    connection: dict = None # UPDATE WITH DIFFERENT CONNECTION TYPES 
    stream_name: str = None # DEFAULTS TO BUILT / MANAGED STREAMS

    def to_dict(self):
        """
        Converts the OutputStreamMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class OutputSQLMethod:
    method: str
    connection: dict # UPDATE WITH DIFFERENT CONNECTION TYPES 
    database: str
    table: str
    sql_type: str = "insert"
    primary_key: list = None
    begin_date_col: str = None
    end_date_col: str = None
    is_current_col: str = "is_current"
    tracked_columns: list = None
    timestamp_expr: str = None

    def to_dict(self):
        """
        Converts the OutputSQLMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class OutputAPIMethod:
    method: str
    connection: dict # UPDATE WITH DIFFERENT CONNECTION TYPES 
    endpoint: str
    http_method: str
    payload_template: dict = None

    def to_dict(self):
        """
        Converts the OutputAPIMethod to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)




@dataclass
class CheckpointMetadata:
    batch_methods: dict = None
    batch_index: int = 0
    batch_cursors: dict = None
    records_processed: int = 0
    total_records_so_far: int = 0
    is_final: bool = False
    accumulated_preprocess_runtime: float = 0.0
    accumulated_function_runtime: float = 0.0
    accumulated_postprocess_runtime: float = 0.0
    accumulated_preprocess_memory: float = 0.0
    accumulated_function_memory: float = 0.0
    accumulated_postprocess_memory: float = 0.0

    def to_dict(self):
        """
        Converts the CheckpointMetadata to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class ErrorDetails:
    error_type: str = None
    error_message: str = None
    stack_trace: str = None
    additional_info: dict = None

    # Export as JSON
    def to_dict(self):
        """
        Converts the ErrorDetails to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class InputConfiguration:
    ref: str
    alias: str
    methods: List[InputStreamMethod | InputSQLMethod | InputAPIMethod] = field(default_factory=list)


@dataclass
class NodeConfiguration:
    name: str
    layer: str
    description: str = None
    type: str = "batch" # live | batch | scheduled (DEFAULTS TO BATCH)
    schedule: str = None # ONLY APPLICABLE IF TYPE IS SCHEDULED
    enabled: bool = True
    on_failure: str = "stop" # continue | stop | retry
    inputs: List[InputConfiguration] = field(default_factory=list)
    schema: List[SchemaField] = field(default_factory=list)
    outputs: List[OutputStreamMethod | OutputSQLMethod | OutputAPIMethod] = field(default_factory=list)
    checkpoint_metadata: CheckpointMetadata = field(default_factory=CheckpointMetadata)
    error_details: ErrorDetails = None

    def to_dict(self):
        """
        Converts the NodeConfiguration to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)


@dataclass 
class GlueCatalogConnection:
    name: str
    aws_account_id: str
    aws_s3_bucket: str
    type: str = "glue_catalog"
    aws_region: str = None
    aws_role_arn: str = None
    aws_sso_profile: str = None
    aws_access_key_id: str = None
    aws_secret_access_key: str = None
    aws_session_token: str = None 

    def to_dict(self):
        """
        Converts the GlueCatalogConnection to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass 
class DuckLakeConnection:
    name: str
    s3_endpoint: str 
    s3_bucket: str
    s3_prefix: str
    s3_region: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_use_ssl: bool
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str
    type: str = "ducklake"

    def to_dict(self):
        """
        Converts the DuckLakeConnection to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass 
class APIAuthentication:
    auth_type: str
    credentials: dict

    def to_dict(self):
        """
        Converts the APIAuthentication to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

@dataclass
class APIConnection:
    name: str
    type: str 
    base_url: str
    authentication: APIAuthentication = None

    def to_dict(self):
        """
        Converts the APIConnection to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)


@dataclass 
class SQSConnection:
    name: str
    aws_sso_profile: str
    aws_region: str
    aws_account_id: str
    queue_name: str
    type: str = "sqs"

    def to_dict(self):
        """
        Converts the SQSConnection to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)




@dataclass
class TaskMessage:
    type: str
    lattice_id: str
    run_id: str
    layer_id: str
    node_id: str
    worker_id: str  = "worker-" + socket.gethostname() + "-" + str(uuid.uuid4())
    timestamp: float = time.time()
    node_configuration: NodeConfiguration = None
    connections: List[APIConnection | GlueCatalogConnection | DuckLakeConnection | SQSConnection] = field(default_factory=list)


    # Export as JSON
    def to_dict(self):
        """
        Converts the TaskMessage to a dictionary.
        
        Returns:
            dict: A dictionary representation of the dataclass.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        """
        Constructs a TaskMessage from a dictionary, deserializing nested dataclass objects.
        
        Args:
            data (dict): The dictionary containing TaskMessage fields and nested configuration data.
        
        Returns:
            TaskMessage: A fully constructed TaskMessage instance with nested dataclass objects.
        """
        if data.get("node_configuration"):
            # INPUTS 
            if data["node_configuration"].get("inputs"):
                inputs = []
                for input in data["node_configuration"]["inputs"]:
                    if input.get("methods"):
                        input_methods = []
                        for method in input["methods"]:
                            method_type = method.get("method") 
                            if method_type == "stream":
                                input_methods.append(InputStreamMethod(**method))
                            elif method_type == "sql":
                                input_methods.append(InputSQLMethod(**method))
                            elif method_type == "api":
                                input_methods.append(InputAPIMethod(**method))
                        input["methods"] = input_methods
                    inputs.append(InputConfiguration(**input))
                data["node_configuration"]["inputs"] = inputs

            # SCHEMA 
            if data["node_configuration"].get("schema"):
                data["node_configuration"]["schema"] = [SchemaField(**field) for field in data["node_configuration"]["schema"]]
            
            # OUTPUTS
            if data["node_configuration"].get("outputs"):
                if "methods" in data["node_configuration"]["outputs"]:
                    output_methods = []
                    for method in data["node_configuration"]["outputs"]["methods"]:
                        method_type = method.get("method") 
                        if method_type == "stream":
                            output_methods.append(OutputStreamMethod(**method))
                        elif method_type == "sql":
                            output_methods.append(OutputSQLMethod(**method))
                        elif method_type == "api":
                            output_methods.append(OutputAPIMethod(**method))
                    data["node_configuration"]["outputs"]["methods"] = output_methods


            # CHECKPOINT METADATA
            if data["node_configuration"].get("checkpoint_metadata"):
                data["node_configuration"]["checkpoint_metadata"] = CheckpointMetadata(**data["node_configuration"]["checkpoint_metadata"])
            
            
            # ERROR DETAILS
            if data["node_configuration"].get("error_details"):
                data["node_configuration"]["error_details"] = ErrorDetails(**data["node_configuration"]["error_details"])

            # NODE CONFIGURATION W/ NESTED OBJECTS (FROM ABOVE)
            data["node_configuration"] = NodeConfiguration(**data["node_configuration"])
        return cls(**data)


















