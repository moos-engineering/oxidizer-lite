# WORKER 

import json
import uuid
import time
import socket
from functools import wraps
from memory_profiler import memory_usage

from oxidizer_lite.anvil import APIEngine, SQLEngine
from oxidizer_lite.residue import Residue
from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.topology import WorkerMessageType, WorkerTaskType
from oxidizer_lite.phase import GlueCatalogConnection, TaskMessage, NodeConfiguration, ErrorDetails, OutputSQLMethod, InputSQLMethod, InputStreamMethod, InputAPIMethod, OutputStreamMethod, OutputAPIMethod, APIConnection, DuckLakeConnection, ErrorDetails


class Reagent(Residue):
    def __init__(self, catalyst: CatalystConnection):
        """
        Initializes the Reagent worker with a Catalyst connection, streams, and consumer groups.
        
        Args:
            catalyst (CatalystConnection): The Redis connection configuration for the Catalyst cache engine.
        """
        super().__init__(component_name="reagent")

        self.oxidizer_ascii_art()

        self.catalyst = Catalyst(catalyst)  

        # Oxidizer Streams and Consumer Group Names
        self.oxidizer_consumer_group = "worker-group" 
        self.oxidizer_consumer_name = "worker-" + socket.gethostname() + "-" + str(uuid.uuid4()) 
        self.worker_stream = "oxidizer:streams:worker"
        self.controller_stream = "oxidizer:streams:controller" 
        self.catalyst.create_consumer_group(self.worker_stream, self.oxidizer_consumer_group) 


    # General Task Handlers / Helpers
    def _handle_task_message(self, msg_id: str, task: TaskMessage):
        """
        Handles a task message from the worker stream by processing the task based on its type and acknowledging the message.
        
        Args:
            msg_id (str): The ID of the message from the worker stream to acknowledge after processing.
            task (TaskMessage): The task message containing details about the task to be processed.
        """
        # Task Message Handler 
        task_type = task.type
        lattice_id = task.lattice_id
        run_id = task.run_id
        node_id = task.node_id
        
        if task_type in [WorkerTaskType.START_NODE.value, WorkerTaskType.CHECKPOINT_NODE.value]: 
            self._acknowledge_dispatched_task_msg(msg_id, task) 
        else:   
            self.residue(self.ash.ERROR, "Received unknown worker task type", task=task, lattice_id=lattice_id, run_id=run_id, node_id=node_id) 

    def _acknowledge_dispatched_task_msg(self, msg_id: str, task: TaskMessage):
        """
        Acknowledges a dispatched task message from the worker stream and sends a STARTED update to the controller.
        
        Args:
            msg_id (str): The ID of the message from the worker stream to acknowledge.
            task (TaskMessage): The task message containing details about the task that was processed.
        """
        # Write the Worker Message to Controller Stream (WorkerMessageType.STARTED)
        task.type = WorkerMessageType.STARTED.value
        self.catalyst.write_to_stream(self.controller_stream, task.to_dict()) 
        
        # ??? Should the ack be here? Or agt the end of processing
        self.catalyst.acknowledge_message(self.worker_stream, self.oxidizer_consumer_group, msg_id) 

    def _checkpoint_task_msg(self, task: TaskMessage):
        """
        Sends a checkpoint update to the controller stream for a long-running node.
        
        Args:
            task (TaskMessage): The task message containing checkpoint metadata for the node.
        """
        task.type = WorkerMessageType.CHECKPOINT.value 
        
        # Write Checkpoint Message to Controller Stream (WorkerMessageType.CHECKPOINT)
        self.catalyst.write_to_stream(self.controller_stream, task.to_dict()) 

        
    # FUTURE: Fix - Change error_details from dict to ErrorDetails - This will require the use of the function and the lines before it to be slightly refactored
    def _failed_task_msg(self, oxidizer_task: TaskMessage, error_details: ErrorDetails):
        """
        Sends a failure update to the controller stream with error details and checkpoint metadata.
        
        Args:
            oxidizer_task (TaskMessage): The task message containing details about the failed task.
            error_details (ErrorDetails): The error details including error type, message, and stack trace.
        """
        # Write Checkpoint Message to Controller Stream (WorkerMessageType.CHECKPOINT)
        oxidizer_task.type = WorkerMessageType.FAILED.value
        oxidizer_task.node_configuration.error_details = error_details

        self.catalyst.write_to_stream(self.controller_stream, oxidizer_task.to_dict()) 


    def _lattice_connections_lookup(self, connections: list[APIConnection | GlueCatalogConnection | DuckLakeConnection]):
        """
        Creates a lookup dictionary for lattice connections by name.
        
        Args:
            connections (list): A list of lattice connection dicts from the topology configuration.
        
        Returns:
            dict: A dictionary mapping connection names to their typed connection objects.
        """
        connections_dict = {}
        for connection in connections:
            name = connection.get("name")
            type = connection.get("type")
            if type == "glue_catalog":
                connection = GlueCatalogConnection(**connection)
            elif type == "ducklake":
                connection = DuckLakeConnection(**connection)
            elif type == "api":
                connection = APIConnection(**connection)
            else:
                self.residue(self.ash.ERROR, f"Unknown connection type '{type}' for connection '{name}' in lattice configuration.", connection=connection)
                continue
            connections_dict[name] = connection
        return connections_dict

    def _update_checkpoint_metadata(self, task: TaskMessage, input_final:dict, input_methods:dict):
        """
        Updates the checkpoint metadata based on input completion status and methods used.
        
        Args:
            task (TaskMessage): The task message containing the node configuration and checkpoint metadata to update in place.
            input_final (dict): A dictionary mapping input identifiers to booleans indicating whether each input has completed.
            input_methods (dict): A dictionary mapping input identifiers to their retrieval method type strings.
        
        Returns:
            TaskMessage: The updated task message with modified checkpoint metadata.
        """
        # Update Is Final Checkpoint Logic Based on If Batching is Complete
        if all(input_final.values()):  
            task.node_configuration.checkpoint_metadata.is_final = True
            task.node_configuration.checkpoint_metadata.batch_methods = input_methods 
        else:  
            task.node_configuration.checkpoint_metadata.is_final = False
            task.node_configuration.checkpoint_metadata.batch_methods = input_methods
            # FUTURE: Fix How to Handle Partial + Full Batches
            print("ADD LATER - BUT THIS SHOULD FAIL... OR WE NEED HANDLE IT SPECIAL or ADJUST BATCH SIZES = THIS IS FOR MULTIPLE INPUTS")
        return task

    # Input / Output Handlers
    def _handle_incoming_stream(self, lattice_id: str, node_id: str, input_ref: str, method: InputStreamMethod):
        """
        Handles incoming data for a node from a Redis stream.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration.
            node_id (str): The identifier of the node being processed.
            input_ref (str): The reference identifier for the input dependency.
            method (InputStreamMethod): The stream method details including batch size, block time, and windowing.
        
        Returns:
            tuple: A tuple of (data, future_ack_msgs) where data is a list of records and future_ack_msgs is a list of (stream, group, msg_id) tuples to acknowledge after processing.
        """
        self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_ref} using stream retrieval strategy", **method.to_dict()) 
        stream = f"oxidizer:data:{input_ref}"
        window = method.window
        consumer_group = f"{lattice_id}.{node_id}"
        # DO WE NEED TO INCLUDE LATTICE IN THE STREAM NAME?
        # UPDATE TO HANDLE THE WINDOW AND THE BATCH AND BLOCK ACCORDINGLY
        self.catalyst.create_consumer_group(stream, consumer_group) 
        raw_data = self.catalyst.read_from_stream(stream, consumer_group, self.oxidizer_consumer_name, count=method.batch_size, block=method.block)
        future_ack_msgs = []
        data = []
        for msg_id, msg in raw_data:
            future_ack_msgs.append((stream, consumer_group, msg_id)) 
            data.append(msg)
        return data, future_ack_msgs
    
    def _handle_outgoing_stream(self, node_id: str, data: list):
        """
        Handles outgoing data for a node by writing records to a Redis stream.
        
        Args:
            node_id (str): The identifier of the node, used to derive the output stream name.
            data (list): A list of data records to write to the output stream.
        """
        # DO WE NEED TO INCLUDE LATTICE IN THE STREAM NAME?
        stream = f"oxidizer:data:{node_id}"
        self.catalyst.create_stream(stream) 
        for msg in data:
            self.catalyst.write_to_stream(stream, msg)
        return 

    def _handle_incoming_sql(self, method: InputSQLMethod, connections_lookup: dict[str, APIConnection | DuckLakeConnection], batch_idx: int, batch_size: int):
        """
        Handles incoming data for a node by executing a SQL query and returning the results.
        
        Args:
            method (InputSQLMethod): The SQL method details including connection, database, table, and query parameters.
            connections_lookup (dict): A dictionary mapping connection names to their typed connection objects.
            batch_idx (int): The zero-based batch index for paginated query execution.
            batch_size (int): The number of rows per batch.
        
        Returns:
            list: A list of data records returned from the SQL query.
        """
        self.residue(self.ash.INFO, f"PRE PROCESS: Processing Incoming Data using SQL retrieval strategy", database=method.database, table=method.table, sql_type=method.sql_type, batch_size=method.batch_size) 
        self.residue(self.ash.DEBUG, f"SQL method filters", filters=method.filters, columns=method.columns)
        
        # SQL Connection Details
        method_connection = method.connection
        connection = connections_lookup.get(method_connection) 

        # SQL Catalog
        catalog = connection.name  

        # SQL Raw Query - Optional
        query = method.query

        # SQL Structured Query Details - Optional
        database = method.database
        table = method.table
        sql_type = method.sql_type
        columns = method.columns
        filters = method.filters
        sort_by = method.sort_by
        limit = method.limit
        
        engine_type = connection.type 
        sql_engine = SQLEngine(engine_type, connection)

        # Create Query String Based on SQL Type
        if query is None: 
            if sql_type == "select":
                query = sql_engine.select_query_str(database, table, columns=columns, where=filters, order_by=sort_by, limit=limit)
                
        # Query Execution
        data = sql_engine.execute_batch_query(query, batch_idx=batch_idx, batch_size=batch_size)
        sql_engine.close()
        return data

    def _handle_outgoing_sql(self, method: OutputSQLMethod, connections_lookup: dict[str, APIConnection | GlueCatalogConnection | DuckLakeConnection], schema: dict, data: list, table_description=None):
        """
        Handles outgoing data for a node by writing records to a SQL database.
        
        Args:
            method (OutputSQLMethod): The SQL output method details including connection, database, table, and SQL type.
            connections_lookup (dict): A dictionary mapping connection names to their typed connection objects.
            schema (dict): The schema definition for the target table columns.
            data (list): A list of data records to write to the SQL database.
            table_description (str | None): An optional description to attach as a table comment.
        """
        self.residue(self.ash.INFO, f"Handling outgoing SQL with method details", **method.to_dict()) 
        
        # Connection Details
        method_connection = method.connection
        connection = connections_lookup.get(method_connection) 

        # SQL Engine
        engine_name = connection.name 
        engine_type = connection.type 
        sql_engine = SQLEngine(engine_type, connection)

        # Database, Table Names and SQL Type
        database = method.database
        table = method.table
        sql_type = method.sql_type
        
        # Create Database if Not Exists
        if not sql_engine.check_database_exists(database):
            sql_engine.create_database(database)

        # Create Table if Not Exists
        if not sql_engine.check_table_exists(database, table):
            sql_engine.create_table(database, table, schema, table_description=table_description)
        
        # FUTURE CHECK SCHEMA DRIFT AND HANDLE ACCORDINGLY (E.G. ALTER TABLE, ETC.)
        

        # Write Data Based on SQL Type 
        if data is not None and len(data) > 0:
            # Load incoming data into a temporary staging table
            staging_table = "staging"
            sql_engine.load_staging_table(data, staging_table)

            if sql_type == "insert":
                sql = sql_engine.insert_query_str(database, table, staging_table)
                total = sql_engine.exectute_single_query(sql)
                self.residue(self.ash.INFO, f"Inserted {total} records into {database}.{table} in {engine_name}", total=total, database=database, table=table, engine_name=engine_name) 
            if sql_type == "scd_type_2":
                columns = list(data[0].keys())
                scd_kwargs = dict(
                    database=database, table_name=table, source_table=staging_table, columns=columns,
                    primary_key=method.primary_key,
                    begin_date_col=method.begin_date_col,
                    end_date_col=method.end_date_col,
                    is_current_col=method.is_current_col,
                    tracked_columns=method.tracked_columns,
                    **({"timestamp_expr": method.timestamp_expr} if method.timestamp_expr else {})
                )
                # Both methods now return a list of queries to handle intra-batch duplicates
                # Use UPDATE+INSERT for Iceberg (no MERGE support), MERGE for DuckLake
                if sql_engine.catalog_type == "glue_catalog":
                    try: 
                        queries = sql_engine.scd_type2_update_insert_query_strs(**scd_kwargs)
                        sql_engine.exectute_single_query("BEGIN TRANSACTION") # Start a Transaction
                        for q in queries:
                            sql_engine.exectute_single_query(q)
                        sql_engine.exectute_single_query("COMMIT")  # Commit the transaction
                        self.residue(self.ash.INFO, f"SCD Type 2 UPDATE+INSERT completed for {database}.{table} in {engine_name}", database=database, table=table, engine_name=engine_name)
                    except Exception as e:
                        sql_engine.exectute_single_query("ROLLBACK")  # Rollback the transaction on error
                        self.residue(self.ash.ERROR, f"Error during SCD Type 2 UPDATE+INSERT for {database}.{table} in {engine_name}", error=str(e), database=database, table=table, engine_name=engine_name) 
                else:
                    try:
                        queries = sql_engine.scd_type2_query_str(**scd_kwargs)
                        sql_engine.exectute_single_query("BEGIN TRANSACTION") # Start a Transaction
                        for q in queries:
                            sql_engine.exectute_single_query(q)
                        sql_engine.exectute_single_query("COMMIT")  # Commit the transaction
                        self.residue(self.ash.INFO, f"SCD Type 2 merge completed for {database}.{table} in {engine_name}", database=database, table=table, engine_name=engine_name)
                    except Exception as e:
                        sql_engine.exectute_single_query("ROLLBACK")  # Rollback the transaction on error
                        self.residue(self.ash.ERROR, f"Error during SCD Type 2 merge for {database}.{table} in {engine_name}", error=str(e), database=database, table=table, engine_name=engine_name)
                        
            # Cleanup Staging Table
            sql_engine.drop_staging_table(staging_table)

        sql_engine.close()
        return 


    def _handle_incoming_api(self, method: InputAPIMethod, connections_lookup: dict[str, APIConnection | DuckLakeConnection]):
        """
        Handles incoming data for a node by making an API call and returning the response.
        
        Args:
            method (InputAPIMethod): The API method details including connection, endpoint, HTTP method, and payload template.
            connections_lookup (dict): A dictionary mapping connection names to their typed connection objects.
        
        Returns:
            dict | list: The data returned from the API call.
        """
        self.residue(self.ash.INFO, "Handling incoming API method for input dependency", **method.to_dict()) 
        
        # Connection Details
        method_connection = method.connection
        connection = connections_lookup.get(method_connection) 
        
        # API Call Details
        endpoint = method.endpoint
        request_method = method.http_method
        # FUTURE: Fix - This needs updates
        data_selector = method.payload_template 
        
        # API Engine
        api = APIEngine(connection.to_dict()) 
        
        # Make API Call Based on Request Method
        if request_method == "GET":
            response = api.get(endpoint)

        # FUTURE: Fix - Add data_selector Logic
        if data_selector:
            data = response.get(data_selector, None) 
        else:                            
            data = response

        return data

    def _handle_outgoing_api(self, method: OutputAPIMethod, connections, data):
        """
        Handles outgoing data for a node by making an API call to send the processed results.
        
        Args:
            method (OutputAPIMethod): The API output method details including connection, endpoint, and HTTP method.
            connections (dict): A dictionary mapping connection names to their connection detail dicts.
            data (dict | list): The data to send as part of the API request body.
        
        Returns:
            dict | list: The response returned from the API call.
        """
        self.residue(self.ash.INFO, "Handling outgoing API method for output dependency", method=method.to_dict()) 
        
        # Connection Details
        method_connection = method.connection
        connection = connections.get(method_connection, {}) # Get the connection details for the API
        
        # API Call Details
        endpoint = method.endpoint
        request_method = method.http_method
        
        # API Engine
        api = APIEngine(connection) # Initialize the API engine with the connection details provided
        
        # Make API Call Based on Request Method
        if request_method == "POST":
            response = api.post(endpoint, {"data": data})
        return response 
    


    def react(self, dedicated_stream: str = None):
        """
        Decorator factory that wraps a user function with task stream pre/post processing.
        
        Args:
            dedicated_stream (str | None): An optional dedicated stream name to read tasks from. Defaults to the worker stream.
        
        Returns:
            callable: A decorator that wraps the user function with input fetching, execution, output handling, and checkpointing.
        """
        # FUTURE: Fix - Add Wrapper to Protect from using key streams (i.e. controller / worker streams)
        stream = dedicated_stream or self.worker_stream

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                
                ##############################################################################
                ## PRE PROCESSING AND SETUP
                preprocess_start = time.time()
                preprocess_memory_start = memory_usage(-1, interval=.01, timeout=1)
                try:
                    # Read from the worker task stream
                    tasks = self.catalyst.read_from_stream(stream, self.oxidizer_consumer_group, self.oxidizer_consumer_name, count=1)
                    if not tasks:
                        return None

                    # Get Message and Task Details
                    msg_id, task = tasks[0] 
                    node_configuration = NodeConfiguration(**task["node_configuration"])
                    
                    oxidizer_task = TaskMessage.from_dict(task)
                    self._handle_task_message(msg_id, oxidizer_task)
                    
                    # Lattice Details
                    lattice_id = oxidizer_task.lattice_id
                    connections = oxidizer_task.connections
                    connections_dict = self._lattice_connections_lookup(connections)
                    
                    # Node ID and Node Configuration Details
                    node_id = oxidizer_task.node_id
                    
                    description = oxidizer_task.node_configuration.description
                    inputs = oxidizer_task.node_configuration.inputs
                    schema = oxidizer_task.node_configuration.schema
                    outputs = oxidizer_task.node_configuration.outputs
                    checkpoint_metadata = oxidizer_task.node_configuration.checkpoint_metadata

                except Exception as e:
                    self.residue(self.ash.CRITICAL, "PRE PROCESS: Error Occurred during pre-processing setup", error=str(e), task=oxidizer_task, node_id=oxidizer_task.node_id, run_id=oxidizer_task.run_id, layer_id=oxidizer_task.layer_id, lattice_id=oxidizer_task.lattice_id) 
                    error_details = ErrorDetails(
                        error_type=type(e).__name__,
                        error_message=str(e)
                    )
                    self._failed_task_msg(oxidizer_task, error_details=error_details) 
                    return None
                ##############################################################################


                ##############################################################################
                # INPUT DATA FETCHING LOGIC
                try:
                    input_data = {} 
                    input_final = {}
                    input_batch_methods = {}
                    input_data_ack_msgs = [] 
                    
                    for input in inputs:
                        input_ref = input.ref
                        input_alias = input.alias or input_ref
                        input_name = input_alias
                        input_methods = input.methods 
    
                        for method in input_methods:
                            method_type = method.method
                            if method_type == "stream":
                                self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using stream retrieval strategy", method=method.to_dict()) 
                                input_batch_methods[input_name] = "stream"
                                batch_size = method.batch_size 
                                data, input_data_ack_msgs = self._handle_incoming_stream(lattice_id, node_id, input_ref, method) 
                                input_data[input_name] = data
                                if len(data) < batch_size:
                                    input_final[input_name] = True 
                                else:
                                    input_final[input_name] = False
                                break
                            elif method_type == "sql":
                                self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using SQL retrieval strategy with method details", database=method.database, table=method.table, sql_type=method.sql_type, batch_size=method.batch_size) 
                                input_batch_methods[input_name] = "sql" 
                                batch_idx = checkpoint_metadata.batch_index 
                                batch_size = method.batch_size
                                data = self._handle_incoming_sql(method, connections_dict, batch_idx, batch_size)
                                input_data[input_name] = data
                                if len(data) < batch_size:
                                    input_final[input_name] = True
                                else:
                                    input_final[input_name] = False
                                break
                            elif method_type == "api":
                                self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using API retrieval strategy", endpoint=method.endpoint, api_method=method.http_method) 
                                input_batch_methods[input_name] = "api"
                                data = self._handle_incoming_api(method, connections_dict)
                                input_data[input_name] = data
                                input_final[input_name] = True # THIS IS UNTIL WE IMPLEMENT A CURSOR / PAGINATION
                                break
                            else:
                                self.residue(self.ash.WARNING, f"Unknown retrieval method type: {method_type} for input dependency {input_name}. Skipping this method.")
                                raise ValueError(f"Unknown retrieval method type: {method_type}")
                        oxidizer_task = self._update_checkpoint_metadata(oxidizer_task, input_final, input_batch_methods)
                except Exception as e:
                    self.residue(self.ash.CRITICAL, "Error occurred during fetching of input data for the task, such as reading from input streams, executing SQL queries, or making API calls based on the retrieval strategies defined for the inputs in the task message", error=str(e), task=task, node_id=oxidizer_task.node_id, run_id=oxidizer_task.run_id, layer_id=oxidizer_task.layer_id, lattice_id=oxidizer_task.lattice_id)           
                    error_details = ErrorDetails(
                        error_type=type(e).__name__,
                        error_message=str(e)
                    )
                    self._failed_task_msg(oxidizer_task, error_details=error_details) 
                    return None
                

                # UPDATE
                preprocess_end = time.time()
                oxidizer_task.node_configuration.checkpoint_metadata.accumulated_preprocess_runtime = checkpoint_metadata.accumulated_preprocess_runtime + (preprocess_end - preprocess_start)
                preprocess_memory_end = memory_usage(-1, interval=.01, timeout=1)
                oxidizer_task.node_configuration.checkpoint_metadata.accumulated_preprocess_memory = checkpoint_metadata.accumulated_preprocess_memory + (max(preprocess_memory_end) - max(preprocess_memory_end))
                ##############################################################################
                




                
                ##############################################################################
                ## CUSTOM FUNCTION LOGIC GOES HERE
                function_start = time.time()
                function_memory_start = memory_usage(-1, interval=.01, timeout=1)
                
                try:
                    # context = {
                    #     "task": task,
                    #     "checkpoint_metadata": checkpoint_metadata,
                    #     "worker_id": self.oxidizer_consumer_name,
                    # }

                    context = {
                        "lattice_id": oxidizer_task.lattice_id,
                        "run_id": oxidizer_task.run_id,
                        "layer_id": oxidizer_task.layer_id,
                        "node_id": oxidizer_task.node_id
                    }
                    result = func(input_data, context)
                    
                    # Convert to Json
                    if isinstance(result, (str, int, float, bool)):
                        result = json.loads(result) 
                    
                    

                except Exception as e:
                    self.residue(self.ash.ERROR, "Error occurred during execution of the user function for the task", error=str(e), node_id=oxidizer_task.node_id, run_id=oxidizer_task.run_id, layer_id=oxidizer_task.layer_id, lattice_id=oxidizer_task.lattice_id) 
                    error_details = ErrorDetails(
                        error_type=type(e).__name__,
                        error_message=str(e)
                    )
                    self._failed_task_msg(oxidizer_task, error_details=error_details) 
                    raise e 
                
                function_end = time.time()
                oxidizer_task.node_configuration.checkpoint_metadata.accumulated_function_runtime = checkpoint_metadata.accumulated_function_runtime + (function_end - function_start)
                function_memory_end = memory_usage(-1, interval=.01, timeout=1)
                oxidizer_task.node_configuration.checkpoint_metadata.accumulated_function_memory = checkpoint_metadata.accumulated_function_memory + (max(function_memory_end) - max(function_memory_start))
                ##############################################################################
                
                
                
                
                
                
                ##############################################################################
                # POST PROCESS ACK INPUT DATA MESSAGES
                postprocess_start = time.time()
                postprocess_memory_start = memory_usage(-1, interval=.01, timeout=1)

                try:
                    for ack_msg in input_data_ack_msgs:
                        self.catalyst.acknowledge_message(*ack_msg) 
                except Exception as e:
                    self.residue(self.ash.CRITICAL, "Error occurred while acknowledging messages from input streams after processing input data for the task", error=str(e), task=task, node_id=oxidizer_task.node_id, run_id=oxidizer_task.run_id, layer_id=oxidizer_task.layer_id, lattice_id=oxidizer_task.lattice_id) 
                ##############################################################################

                ##############################################################################
                # POST PROCESS DOWNSTREAM DATA
                try:
                    for method in outputs.get("methods", []):
                        method_type = method.method
                        if method_type == "stream":
                            self.residue(self.ash.INFO, f"POST PROCESS: Sending Data Downstream using stream strategy", lattice_id=oxidizer_task.lattice_id, run_id=oxidizer_task.run_id, node_id=oxidizer_task.node_id, **method.to_dict()) 
                            self._handle_outgoing_stream(oxidizer_task.node_id, result) 
                        elif method_type == "sql":
                            self.residue(self.ash.INFO, f"POST PROCESS: Sending Data Downstream using SQL strategy", lattice_id=oxidizer_task.lattice_id, run_id=oxidizer_task.run_id, node_id=oxidizer_task.node_id, **method.to_dict()) 
                            self._handle_outgoing_sql(method, connections_dict, schema, result, table_description=description) 
                        elif method_type == "api":
                            self.residue(self.ash.INFO, f"POST PROCESS: Sending Data Downstream using API strategy", lattice_id=oxidizer_task.lattice_id, run_id=oxidizer_task.run_id, node_id=oxidizer_task.node_id, **method.to_dict()) 
                            self._handle_outgoing_api(method, connections_dict, result) 
                        else:
                            self.residue(self.ash.WARNING, f"POST PROCESS: Unknown Output Method Type: {method_type}. Skipping this method.", lattice_id=oxidizer_task.lattice_id, run_id=oxidizer_task.run_id, node_id=oxidizer_task.node_id, **method.to_dict()) 
                            raise ValueError(f"Unknown output method type: {method_type}")

                    # POST PROCESS CHECKPOINT MESSAGE TO CONTROLLER STREAM
                    postprocess_end = time.time()
                    oxidizer_task.node_configuration.checkpoint_metadata.accumulated_postprocess_runtime = checkpoint_metadata.accumulated_postprocess_runtime + (postprocess_end - postprocess_start)
                    postprocess_memory_end = memory_usage(-1, interval=.01, timeout=1)
                    oxidizer_task.node_configuration.checkpoint_metadata.accumulated_postprocess_memory = checkpoint_metadata.accumulated_postprocess_memory + (max(postprocess_memory_end) - max(postprocess_memory_start))
                    self._checkpoint_task_msg(oxidizer_task)
                except Exception as e:
                    self.residue(self.ash.CRITICAL, "POST PROCESS: Error Occurred while handling post-processing tasks", error_type=type(e).__name__, error=str(e), task=oxidizer_task, node_id=oxidizer_task.node_id, run_id=oxidizer_task.run_id, layer_id=oxidizer_task.layer_id, lattice_id=oxidizer_task.lattice_id)          
                    postprocess_end = time.time()
                    oxidizer_task.node_configuration.checkpoint_metadata.accumulated_postprocess_runtime = checkpoint_metadata.accumulated_postprocess_runtime + (postprocess_end - postprocess_start)
                    postprocess_memory_end = memory_usage(-1, interval=.01, timeout=1)
                    oxidizer_task.node_configuration.checkpoint_metadata.accumulated_postprocess_memory = checkpoint_metadata.accumulated_postprocess_memory + (max(postprocess_memory_end) - max(postprocess_memory_start))
                    
                    error_details = ErrorDetails(
                        error_type=type(e).__name__,
                        error_message=str(e)
                    )
                    self._failed_task_msg(oxidizer_task, error_details=error_details) 
                ##############################################################################
                


                return result

            return wrapper

        def auto_run_decorator(func):
            wrapped = decorator(func)
            wrapped()
            return wrapped

        return auto_run_decorator

            
