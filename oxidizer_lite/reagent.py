# WORKER 

import json
import uuid
import time
import socket
from functools import wraps
from contextlib import contextmanager
from memory_profiler import memory_usage

from oxidizer_lite.oxidizer import OxidizerKeys
from oxidizer_lite.anvil import APIEngine, SQLEngine, SQSEngine, StreamEngine
from oxidizer_lite.residue import Residue
from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.topology import WorkerMessageType, WorkerTaskType
from oxidizer_lite.phase import CheckpointMetadata, GlueCatalogConnection, TaskMessage, NodeConfiguration, ErrorDetails, OutputSQLMethod, InputSQLMethod, InputStreamMethod, InputAPIMethod, OutputStreamMethod, OutputAPIMethod, OutputsConfiguration, APIConnection, DuckLakeConnection, ErrorDetails, SQSConnection, InputSQSMethod, InputFetchContext, InputFetchResult


class ReagentTaskHandler(Residue):
    def __init__(self):
        """
        Initializes the ReagentTaskHandler worker which is responsible for handling task messages from the worker stream.

        """
        super().__init__(component_name="reagent_task_handler")
        self.controller_stream = OxidizerKeys.CONTROLLER_STREAM

    def send_checkpoint_task_msg(self, catalyst: Catalyst, task: TaskMessage):
        """
        Sends a checkpoint update to the controller stream for a long-running node.
        
        Args:
            catalyst (Catalyst): The catalyst instance used to write to the stream.
            task (TaskMessage): The task message containing checkpoint metadata for the node.
        """
        task.type = WorkerMessageType.CHECKPOINT.value 
        
        # Write Checkpoint Message to Controller Stream (WorkerMessageType.CHECKPOINT)
        catalyst.write_to_stream(self.controller_stream, task.to_dict()) 

    def send_failed_task_msg(self, catalyst: Catalyst, task: TaskMessage, error_details: ErrorDetails):
        """
        Sends a failure update to the controller stream with error details and checkpoint metadata.
        
        Args:
            catalyst (Catalyst): The catalyst instance used to write to the stream.
            task (TaskMessage): The task message containing details about the failed task.
            error_details (ErrorDetails): The error details including error type, message, and stack trace.
        """
        # Write Checkpoint Message to Controller Stream (WorkerMessageType.CHECKPOINT)
        task.type = WorkerMessageType.FAILED.value
        task.node_configuration.error_details = error_details

        catalyst.write_to_stream(self.controller_stream, task.to_dict()) 

    def update_checkpoint_metadata(self, task: TaskMessage, input_final:dict, input_methods:dict, input_cursors: dict = None):
        """
        Updates the checkpoint metadata based on input completion status and methods used.
        
        Args:
            task (TaskMessage): The task message containing the node configuration and checkpoint metadata to update in place.
            input_final (dict): A dictionary mapping input identifiers to booleans indicating whether each input has completed.
            input_methods (dict): A dictionary mapping input identifiers to their retrieval method type strings.
            input_cursors (dict): A dictionary mapping input identifiers to their current cursor or pagination state.
        Returns:
            TaskMessage: The updated task message with modified checkpoint metadata.
        """
        meta = task.node_configuration.checkpoint_metadata
        meta.is_final = all(input_final.values())
        meta.batch_methods = input_methods
        meta.batch_cursors = input_cursors
        # FUTURE: Handle partial batches — when only some inputs are exhausted,
        #         is_final=False may still need special cursor/batch size adjustment
        return task

class ReagentInputHandler(Residue):
    def __init__(self, catalyst: Catalyst):
        """
        Initializes the ReagentInputHandler worker which is responsible for handling incoming data for a node.

        """
        super().__init__(component_name="reagent_input_handler")
        self.oxidizer_consumer_name = "worker-" + socket.gethostname() + "-" + str(uuid.uuid4())
        self.stream_engine = StreamEngine(catalyst, self.oxidizer_consumer_name)

    
    def handle_incoming_stream(self, method: InputStreamMethod, connection=None, context: InputFetchContext = None) -> InputFetchResult:
        """
        Handles incoming data for a node from a Redis stream.
        
        Args:
            method (InputStreamMethod): The stream method details including batch size, block time, and windowing.
            connection: Unused; present for interface uniformity with other handlers.
            context (InputFetchContext): Per-call routing context providing lattice_id, node_id, and input_ref.
        
        Returns:
            InputFetchResult: Unified result containing data, is_final flag, and ack_msgs.
        """
        self.residue(self.ash.INFO, f"Fetching data from upstream dependency {context.input_ref} using stream retrieval strategy", **method.to_dict())
        stream_key = OxidizerKeys.data_stream(context.input_ref)
        consumer_group = f"{context.lattice_id}.{context.node_id}"
        # DO WE NEED TO INCLUDE LATTICE IN THE STREAM NAME?
        # UPDATE TO HANDLE THE WINDOW AND THE BATCH AND BLOCK ACCORDINGLY
        data, ack_msgs = self.stream_engine.read(stream_key, consumer_group, method.batch_size, method.block)
        is_final = len(data) < method.batch_size
        return InputFetchResult(data=data, is_final=is_final, ack_msgs=ack_msgs)
    
    def handle_incoming_sqs(self, method: InputSQSMethod, connection: SQSConnection, context: InputFetchContext) -> InputFetchResult:
        """
        Handles incoming data for a node from an SQS queue.
        
        Args:
            method (InputSQSMethod): The SQS method details including connection, batch size, and wait time.
            connection (SQSConnection): The SQS connection object.
            context (InputFetchContext): Per-call context; not used directly by this handler.
        Returns:
            InputFetchResult: Unified result containing data, is_final flag, and ack_msgs (receipt handles).
        """
        self.residue(self.ash.INFO, f"Fetching data from upstream dependency using SQS retrieval strategy", **method.to_dict())
        sqs_engine = SQSEngine(connection)
        raw_messages = sqs_engine.receive_messages(method.batch_size, method.wait_time)
        data = []
        ack_msgs = []
        for msg in raw_messages:
            data.append(msg.body)
            ack_msgs.append(msg.receipt_handle)
        is_final = len(data) < method.batch_size
        return InputFetchResult(data=data, is_final=is_final, ack_msgs=ack_msgs)

    def handle_incoming_sql(self, method: InputSQLMethod, connection: GlueCatalogConnection | DuckLakeConnection, context: InputFetchContext) -> InputFetchResult:
        """
        Handles incoming data for a node by executing a SQL query and returning the results.
        
        Args:
            method (InputSQLMethod): The SQL method details including connection, database, table, and query parameters.
            connection (GlueCatalogConnection | DuckLakeConnection): The SQL connection object.
            context (InputFetchContext): Per-call context; provides batch_index for paginated queries.
        Returns:
            InputFetchResult: Unified result containing data and is_final flag.
        """
        self.residue(self.ash.INFO, f"PRE PROCESS: Processing Incoming Data using SQL retrieval strategy", database=method.database, table=method.table, sql_type=method.sql_type, batch_size=method.batch_size) 
        self.residue(self.ash.DEBUG, f"SQL method filters", filters=method.filters, columns=method.columns)
        
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
        data = sql_engine.execute_batch_query(query, batch_idx=context.batch_index, batch_size=method.batch_size)
        sql_engine.close()
        is_final = len(data) < method.batch_size
        return InputFetchResult(data=data, is_final=is_final)

    def handle_incoming_api(self, method: InputAPIMethod, connection: APIConnection, context: InputFetchContext) -> InputFetchResult:
        """
        Handles incoming data for a node by making an API call and returning the response.
        
        Args:
            method (InputAPIMethod): The API method details including connection, endpoint, HTTP method, and payload template.
            connection (APIConnection): The API connection object.
            context (InputFetchContext): Per-call context; provides cursor, trigger_data, and trigger_attribute.
        Returns:
            InputFetchResult: Unified result containing data, is_final flag, and next_cursor.
        """
        self.residue(self.ash.INFO, "Handling incoming API method for input dependency", **method.to_dict())
        cursor = context.cursor
        trigger_attribute = context.trigger_attribute
        trigger_data = context.trigger_data

        # API Call Details
        endpoint = method.endpoint
        request_method = method.http_method
        data_selector = method.path
        paginator = method.paginator
        next_cursor = None

        # API Engine
        api = APIEngine(connection.to_dict())

        # Build URL for API Call
        if cursor is not None:
            url = cursor
        elif endpoint is not None:
            url = f"{api.base_url}{endpoint}"
        elif trigger_attribute is not None and trigger_data is not None:
            endpoint = trigger_data.get(trigger_attribute)
            url = f"{api.base_url}{endpoint}"
        else:
            self.residue(self.ash.WARNING, "No valid endpoint or cursor provided for API call in incoming API method", method=method.method, endpoint=endpoint, cursor=cursor, trigger_attribute=trigger_attribute, trigger_data=trigger_data)
            raise ValueError("No valid endpoint or cursor provided for API call")
        
        # Make API Call Based on Request Method
        if request_method == "GET":
            data = api.get(url)
        elif request_method == "POST":
            payload = {}
            data = api.post(url, payload)

        
        # API Post Processing for Pagination and Data Selection
        if paginator is not None:
            if paginator in data:
                next_cursor = data[paginator]

        if data_selector is not None:
            keys = data_selector.lstrip("$.").split(".")
            for key in keys:
                data = data.get(key, {})

        is_final = next_cursor is None
        return InputFetchResult(data=data, is_final=is_final, next_cursor=next_cursor)

class ReagentOutputHandler(Residue):
    def __init__(self):
        """
        Initializes the ReagentOutputHandler worker which is responsible for handling outgoing data for a node.

        """
        super().__init__(component_name="reagent_output_handler")

    def handle_outgoing_api(self, method: OutputAPIMethod, connection: APIConnection, data: list):
        """
        Handles outgoing data for a node by posting records to an API endpoint.

        Args:
            method (OutputAPIMethod): The API output method details including endpoint and HTTP method.
            connection (APIConnection): The API connection object.
            data (list): A list of data records to post to the API.
        """
        # FUTURE: Implement API output handler
        pass

    def handle_outgoing_stream(self, catalyst: Catalyst, node_id: str, data: list):
        """
        Handles outgoing data for a node by writing records to a Redis stream.
        
        Args:
            node_id (str): The identifier of the node, used to derive the output stream name.
            data (list): A list of data records to write to the output stream.
        """
        # DO WE NEED TO INCLUDE LATTICE IN THE STREAM NAME? 
        stream = OxidizerKeys.data_stream(node_id)
        catalyst.create_stream(stream) 
        for msg in data:
            catalyst.write_to_stream(stream, msg)
        return

    def handle_outgoing_sql(self, method: OutputSQLMethod, connection: GlueCatalogConnection | DuckLakeConnection, schema: dict, data: list, table_description=None):
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






class Reagent(Residue):
    def __init__(self, catalyst: CatalystConnection, auto_react: bool = False):
        """
        Initializes the Reagent worker with a Catalyst connection, streams, and consumer groups.
        
        Args:
            catalyst (CatalystConnection): The Redis connection configuration for the Catalyst cache engine.
            auto_react (bool): Whether the Reagent should automatically react to tasks.
        """
        super().__init__(component_name="reagent")

        self.oxidizer_ascii_art()

        # Catalyst Setup
        self.catalyst = Catalyst(catalyst)

        # Reagent Handlers
        self.task_handler = ReagentTaskHandler()
        self.input_handler = ReagentInputHandler(self.catalyst)
        self.output_handler = ReagentOutputHandler()
        self.auto_react = auto_react

        # Oxidizer Streams and Consumer Group Names
        self.oxidizer_consumer_group = "worker-group" 
        self.oxidizer_consumer_name = "worker-" + socket.gethostname() + "-" + str(uuid.uuid4()) 
        
        self.worker_stream = OxidizerKeys.WORKER_STREAM # "oxidizer:streams:worker"
        self.controller_stream = OxidizerKeys.CONTROLLER_STREAM # "oxidizer:streams:controller"
        self.catalyst.create_consumer_group(self.worker_stream, self.oxidizer_consumer_group) 


    # Connections Lookup Dictionary
    def lattice_connections_lookup_dict(self, connections: list[APIConnection | GlueCatalogConnection | DuckLakeConnection | SQSConnection]):
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
            elif type == "sqs":
                connection = SQSConnection(**connection)
            else:
                self.residue(self.ash.ERROR, f"Unknown connection type '{type}' for connection '{name}' in lattice configuration.", connection=connection)
                continue
            connections_dict[name] = connection
        return connections_dict

    # Incoming / Outgoing Task Handlers
    def handle_incoming_task(self, stream: str):
        try:
            # Read from the worker task stream
            tasks = self.catalyst.read_from_stream(stream, self.oxidizer_consumer_group, self.oxidizer_consumer_name, count=1)
            if not tasks:
                return None, None
            
            # Get Message and Task Details
            msg_id, task = tasks[0] 
            oxidizer_task = TaskMessage.from_dict(task)
            oxidizer_task.type = WorkerMessageType.STARTED.value
            self.catalyst.write_to_stream(self.controller_stream, oxidizer_task.to_dict()) 
            return oxidizer_task, msg_id
        except Exception as e:
            self.residue(self.ash.CRITICAL, "PRE PROCESS: Error Occurred during pre-processing setup", error=str(e), task=oxidizer_task, node_id=oxidizer_task.node_id, run_id=oxidizer_task.run_id, layer_id=oxidizer_task.layer_id, lattice_id=oxidizer_task.lattice_id) 
            return None, None
    
    def handle_outgoing_task(self, task_msg_id: str, task: TaskMessage, input_data_ack_msgs: list):
        try:
            # Acknowledge the worker data messages after processing is complete
            for ack_msg in input_data_ack_msgs:
                self.catalyst.acknowledge_message(*ack_msg) 

            # Update Checkpoint Metadata and Send Checkpoint Update to Controller
            self.task_handler.send_checkpoint_task_msg(self.catalyst, task)

            # Acknowledge the worker task message after processing is complete
            self.catalyst.acknowledge_message(self.worker_stream, self.oxidizer_consumer_group, task_msg_id) 
        except Exception as e:
            self.residue(self.ash.CRITICAL, "POST PROCESS: Error Occurred during post-processing cleanup", error=str(e), task=task, node_id=task.node_id, run_id=task.run_id, layer_id=task.layer_id, lattice_id=task.lattice_id)
            raise e 


    # Incoming / Outgoing Data Handlers
    def handle_incoming_data(self, task: TaskMessage):
        """
        Handles incoming data for a node based on the specified retrieval method type.
        
        Args:
            task (TaskMessage): The task message containing the node configuration and connections.
        Returns:
            The data fetched based on the input method type, along with any relevant metadata for checkpointing.
        """
        lattice_id = task.lattice_id
        layer_id = task.layer_id
        node_id = task.node_id
        inputs = task.node_configuration.inputs
        checkpoint_metadata = task.node_configuration.checkpoint_metadata
        connections = task.connections
        connections_dict = self.lattice_connections_lookup_dict(connections)
        try:
            input_data = {} 
            input_final = {}
            input_cursors = {}
            input_batch_methods = {}
            input_data_ack_msgs = []
            
            for input in inputs:
                input_ref = input.ref
                input_name = input.alias or input_ref
                input_methods = input.methods 

                for method in input_methods:
                    method_type = method.method
                    method_connection = connections_dict.get(method.connection)

                    # API trigger pre-processing: fetch stream record for dynamic endpoint construction.
                    # This is orchestration logic and runs before context is built.
                    trigger_data = None
                    trigger_attribute = None
                    if method_type == "api" and method.input_trigger is not None:
                        checkpoint_cursor = checkpoint_metadata.batch_cursors.get(input_name) if checkpoint_metadata.batch_cursors else None
                        if checkpoint_cursor is None:  # Only use input trigger for the initial API call, not for subsequent paginated calls
                            trigger = method.input_trigger
                            trigger_attribute = trigger.attribute
                            if trigger.type == "stream":
                                stream_method = InputStreamMethod(method=trigger.type, batch_size=1)
                                trigger_context = InputFetchContext(lattice_id=lattice_id, node_id=node_id, input_ref=input_ref)
                                trigger_result = self.input_handler.handle_incoming_stream(stream_method, context=trigger_context)
                                trigger_data = trigger_result.data[0] if trigger_result.data else None
                                input_data_ack_msgs.extend(trigger_result.ack_msgs)

                    # Build per-fetch context
                    context = InputFetchContext(
                        lattice_id=lattice_id,
                        node_id=node_id,
                        input_ref=input_ref,
                        batch_index=checkpoint_metadata.batch_index,
                        cursor=checkpoint_metadata.batch_cursors.get(input_name) if checkpoint_metadata.batch_cursors else None,
                        trigger_data=trigger_data,
                        trigger_attribute=trigger_attribute,
                    )

                    if method_type == "stream":
                        self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using stream retrieval strategy", method=method.to_dict())
                        input_batch_methods[input_name] = "stream"
                        result = self.input_handler.handle_incoming_stream(method, context=context)
                    elif method_type == "sql":
                        self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using SQL retrieval strategy with method details", database=method.database, table=method.table, sql_type=method.sql_type, batch_size=method.batch_size)
                        input_batch_methods[input_name] = "sql"
                        result = self.input_handler.handle_incoming_sql(method, method_connection, context)
                    elif method_type == "api":
                        self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using API retrieval strategy", endpoint=method.endpoint, api_method=method.http_method)
                        input_batch_methods[input_name] = "api"
                        result = self.input_handler.handle_incoming_api(method, method_connection, context)
                    elif method_type == "sqs":
                        self.residue(self.ash.INFO, f"Fetching data from upstream dependency {input_name} using SQS retrieval strategy", method=method.to_dict())
                        input_batch_methods[input_name] = "sqs"
                        result = self.input_handler.handle_incoming_sqs(method, method_connection, context)
                    else:
                        self.residue(self.ash.WARNING, f"Unknown retrieval method type: {method_type} for input dependency {input_name}. Skipping this method.")
                        raise ValueError(f"Unknown retrieval method type: {method_type}")

                    input_data[input_name] = result.data
                    input_final[input_name] = result.is_final
                    input_cursors[input_name] = result.next_cursor
                    input_data_ack_msgs.extend(result.ack_msgs)
                    break

                task = self.task_handler.update_checkpoint_metadata(task, input_final, input_batch_methods, input_cursors)
                return input_data, input_data_ack_msgs
        
        except Exception as e:
            self.residue(self.ash.CRITICAL, f"Error occurred during fetching of input data for {node_id}", error=str(e), task=task, node_id=node_id, run_id=task.run_id, layer_id=layer_id, lattice_id=lattice_id)
            raise e

    def handle_outgoing_data(self, task: TaskMessage, data: dict):
        """
        Handles outgoing data for a node based on the specified output method type.
        
        Args:
            data: The data to be outputted, which can be in various formats depending on the node's processing logic.
            schema: The schema definition for the output data, used for SQL outputs.
            outputs: A list of output details which can be of type OutputStreamMethod or OutputSQLMethod.
            connections_dict: A dictionary mapping connection names to their typed connection objects.
        """
        lattice_id = task.lattice_id
        layer_id = task.layer_id
        node_id = task.node_id
        schema = task.node_configuration.schema
        outputs = task.node_configuration.outputs
        connections = task.connections
        connections_dict = self.lattice_connections_lookup_dict(connections)
        try:
            output_methods = outputs.methods
            for method in output_methods:
                method_type = method.method
                method_connection = connections_dict.get(method.connection)

                if method_type == "stream":
                    self.residue(self.ash.INFO, f"Writing data to downstream dependency {node_id} using stream output strategy", method=method.method) 
                    self.output_handler.handle_outgoing_stream(self.catalyst, node_id, data)
                elif method_type == "sql":
                    self.residue(self.ash.INFO, f"Writing data to downstream dependency {node_id} using SQL output strategy with method details", database=method.database, table=method.table, sql_type=method.sql_type) 
                    self.output_handler.handle_outgoing_sql(method, method_connection, schema, data, table_description=task.node_configuration.description)
                elif method_type == "api":
                    self.residue(self.ash.INFO, f"Writing data to downstream dependency {node_id} using API output strategy", endpoint=method.endpoint, api_method=method.http_method) 
                    self.output_handler.handle_outgoing_api(method, method_connection, data)
                    
                else:
                    self.residue(self.ash.WARNING, f"Unknown output method type: {method_type} for output dependency {node_id}. Skipping this method.")
                    raise ValueError(f"Unknown output method type: {method_type}")
        except Exception as e:
            self.residue(self.ash.CRITICAL, f"Error occurred during handling of outgoing data for {node_id}", error=str(e), task=task, node_id=node_id, run_id=task.run_id, layer_id=layer_id, lattice_id=lattice_id)
            raise e


    @contextmanager
    def _timed_phase(self, task: TaskMessage, phase: str):
        start = time.time()
        mem_start = memory_usage(-1, interval=.01, timeout=1)
        yield
        elapsed = time.time() - start
        mem_delta = max(memory_usage(-1, interval=.01, timeout=1)) - max(mem_start)
        meta = task.node_configuration.checkpoint_metadata
        setattr(meta, f"accumulated_{phase}_runtime",
                getattr(meta, f"accumulated_{phase}_runtime") + elapsed)
        setattr(meta, f"accumulated_{phase}_memory",
                getattr(meta, f"accumulated_{phase}_memory") + mem_delta)

    # Main Reagent Decorator
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
                task, msg_id = self.handle_incoming_task(stream)
                if task is None:
                    return None
                ##############################################################################

                ##############################################################################
                ## TASK DETAILS 
                lattice_id = task.lattice_id
                layer_id = task.layer_id
                node_id = task.node_id
                ##############################################################################

                ##############################################################################
                ## INPUT DATA FETCHING LOGIC (TIMED)
                with self._timed_phase(task, "preprocess"):
                    try:
                        input_data, input_data_ack_msgs = self.handle_incoming_data(task)
                    except Exception as e:
                        self.residue(self.ash.CRITICAL, "Error occurred during fetching of input data for the task", error=str(e), task=task, node_id=task.node_id, run_id=task.run_id, layer_id=task.layer_id, lattice_id=task.lattice_id) 
                        error_details = ErrorDetails(error_type=type(e).__name__, error_message=str(e))
                        self.task_handler.send_failed_task_msg(self.catalyst, task, error_details=error_details)
                        raise e
                ##############################################################################
                



                ##############################################################################
                ## CUSTOM FUNCTION LOGIC GOES HERE (TIMED)
                with self._timed_phase(task, "function"):
                    self.residue(self.ash.INFO, f"Executing user defined function for {layer_id}.{node_id}", node_id=node_id, run_id=task.run_id, layer_id=layer_id, lattice_id=lattice_id)
                    try:
                        context = {
                            "lattice_id": task.lattice_id,
                            "run_id": task.run_id,
                            "layer_id": task.layer_id,
                            "node_id": task.node_id
                        }
                        result = func(input_data, context)
                        
                        # Convert to JSON
                        if isinstance(result, (str, int, float, bool)):
                            result = json.loads(result) 

                    except Exception as e:
                        self.residue(self.ash.ERROR, "Error occurred during execution of the user function for the task", error=str(e), node_id=task.node_id, run_id=task.run_id, layer_id=task.layer_id, lattice_id=task.lattice_id) 
                        error_details = ErrorDetails(
                            error_type=type(e).__name__,
                            error_message=str(e)
                        )
                        self.task_handler.send_failed_task_msg(self.catalyst, task, error_details=error_details) 
                        raise e 
                ##############################################################################
                
                




                
                ##############################################################################
                ## POST PROCESS DOWNSTREAM DATA (TIMED)
                with self._timed_phase(task, "postprocess"):
                    try:
                        self.handle_outgoing_data(task, result)
                    except Exception as e:
                        self.residue(self.ash.CRITICAL, "Error occurred while handling outgoing data for the task", error=str(e), task=task, node_id=task.node_id, run_id=task.run_id, layer_id=task.layer_id, lattice_id=task.lattice_id) 
                        error_details = ErrorDetails(error_type=type(e).__name__, error_message=str(e))
                        self.task_handler.send_failed_task_msg(self.catalyst, task, error_details=error_details)
                        raise e

                ##############################################################################

                ##############################################################################
                ## POST PROCESS HANDLE OUTGOING TASK (E.G. ACK TASK MESSAGE, SEND CHECKPOINT UPDATE, ETC.)
                try:
                    self.handle_outgoing_task(msg_id, task, input_data_ack_msgs)
                except Exception as e:
                    self.residue(self.ash.CRITICAL, "Error occurred while handling outgoing task", error=str(e), task=task, node_id=task.node_id, run_id=task.run_id, layer_id=task.layer_id, lattice_id=task.lattice_id) 
                    error_details = ErrorDetails(error_type=type(e).__name__, error_message=str(e))
                    self.task_handler.send_failed_task_msg(self.catalyst, task, error_details=error_details)
                    raise e
                ##############################################################################
                
                return result
            return wrapper

        def auto_run_decorator(func):
            wrapped = decorator(func)
            wrapped()
            return wrapped
                
        return auto_run_decorator

            
