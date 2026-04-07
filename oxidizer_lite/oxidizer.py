# CONTROLLER

from datetime import datetime
import uuid
import json

from oxidizer_lite.catalyst import Catalyst, CatalystConnection, ResponseError, RedisConnectionError
from oxidizer_lite.topology import NodeStatus, Topology, EnumEncoder, WorkerMessageType, WorkerTaskType
from oxidizer_lite.crucible import Crucible, CrucibleConnection, TokenRetrievalError, ClientError
from oxidizer_lite.lattice import Lattice
from oxidizer_lite.residue import Residue
from oxidizer_lite.phase import TaskMessage, NodeConfiguration, CheckpointMetadata, ErrorDetails


sample_invocation_payload = {   
    "type": "invoke_topology",
    "lattice": "sample"
}



class Oxidizer(Residue):
    def __init__(self, catalyst: CatalystConnection, crucible: CrucibleConnection): 
        """
        Initializes the Oxidizer controller with cache and storage engines, streams, and consumer groups.
        
        Args:
            catalyst (CatalystConnection): The Redis connection configuration for the Catalyst cache engine.
            crucible (CrucibleConnection): The S3 connection configuration for the Crucible storage engine.
        """
        super().__init__(component_name="oxidizer")
        # Startup Console Art
        self.oxidizer_ascii_art()

        # Oxidizer Cache Engine
        self.catalyst = Catalyst(catalyst)

        # Oxidizer Storage Engine
        self.crucible = Crucible(crucible)

        # Oxidizer Topology Engine 
        self.topology = Topology()

        # Oxidizer Lattice Engine
        self.lattice = Lattice(crucible=self.crucible)

        # Oxidizer Running State
        self.running = True

        # Oxidizer Lattice Cache Key
        self.lattice_cache_key = "oxidizer:lattice" # + lattice_id -> "oxidizer:lattice:lattice_id" -> cached lattice configuration that the controller can read from and write to as it executes the topology. 

        # Oxidizer Topology Run State Key Prefix
        self.topology_run_state_prefix = "oxidizer:topology:active" # + lattice + run_id -> "oxidizer:topology:state:lattice:run_id" -> live dag representation of the topology with node statuses and results that the controller can read from and write to as they execute the topology. This allows for real-time updates and state management of the topology execution.
        self.topology_archive_state_prefix = "oxidizer:topology:archive" # + lattice + run_id -> "oxidizer:topology:archive:lattice:run_id" -> archived state of the topology execution after completion for historical reference and debugging. This allows the controller to move completed topology states from the active namespace to the archive namespace in Catalyst to keep the active namespace clean and focused on currently running topologies, while still retaining access to historical execution data for troubleshooting and analysis.

        # Oxidizer Streams and Consumer Group Names
        self.oxidizer_consumer_group = "oxidizer-group" # This is the consumer group that the manager will use to read messages from the worker stream. Each worker should have its own consumer group to read from, but this ensures the stream exists before we start writing to it.
        self.oxidizer_consumer_name = "controller" # This is the consumer name that the manager will use to read messages from the worker stream. Each worker should have its own consumer name to read from, but this ensures the stream exists before we start writing to it.
        self.worker_stream = "oxidizer:streams:worker"
        self.controller_stream = "oxidizer:streams:controller"
        self.invocation_stream = "oxidizer:streams:invocations"


        # self.crucible_bucket = "oxidizer-test"
        self.crucible_bucket = self.crucible.bucket
        self.residue(self.ash.INFO, "Crucible Bucket", crucible_bucket=self.crucible_bucket)

        # Create Oxidizer Streams and Consumer Groups
        try:
            self.catalyst.create_consumer_group(self.worker_stream, self.oxidizer_consumer_group) # THiS IS TO BE USED BY THE WORKER TO READ CONTROLLER MESSAGES, NOT FOR THE WORKERS TO READ FROM. WORKERS SHOULD HAVE THEIR OWN CONSUMER GROUPS TO READ FROM. THIS IS JUST TO ENSURE THE STREAM EXISTS BEFORE WE START WRITING TO IT.
            self.catalyst.create_consumer_group(self.controller_stream, self.oxidizer_consumer_group) # THIS IS TO BE USED BY THE CONTROLLER TO READ WORKER MESSAGES
            self.catalyst.create_consumer_group(self.invocation_stream, self.oxidizer_consumer_group) # THIS IS TO BE USED BY THE CONTROLLER TO READ INVOCATION MESSAGES FROM THE API LAYER
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to create consumer groups at startup - will retry on first read", error=str(e))


    def load_lattice(self, object_key):
        """
        Loads a lattice configuration from S3 using the Crucible and returns it as a dictionary.
        
        Args:
            object_key (str): The S3 object key for the lattice configuration file (e.g., "sample.yml").
        
        Returns:
            dict | None: The loaded lattice configuration, or None if S3 is unavailable.
        """
        lattice_file_name = object_key + ".yml" if not object_key.endswith(".yml") else object_key
        try:
            lattice = self.lattice.load_config(self.crucible_bucket, lattice_file_name)
            return lattice
        except TokenRetrievalError:
            self.residue(self.ash.ERROR, "AWS SSO token expired. Run: aws sso login --profile <your-profile>")
            return None
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchBucket':
                self.residue(self.ash.ERROR, f"Bucket '{self.crucible_bucket}' does not exist")
            elif error_code == 'NoSuchKey':
                self.residue(self.ash.ERROR, f"Lattice config '{lattice_file_name}' not found in bucket")
            else:
                self.residue(self.ash.ERROR, "S3 error loading lattice", error=str(e))
            return None

    def save_lattice(self, bucket_name, object_key, config):
        """
        Saves a lattice configuration to S3 using the Crucible.
        
        Args:
            bucket_name (str): The name of the S3 bucket to save the lattice configuration to.
            object_key (str): The S3 object key for the lattice configuration file (e.g., "sample.yml").
            config (dict): The lattice configuration to save.
        """
        self.lattice.save_config(bucket_name, object_key, config)

    def cache_lattice(self, lattice_id, config):
        """
        Caches a lattice configuration in Catalyst for quick retrieval during topology generation and execution.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            config (dict): The lattice configuration to cache.
        """
        key = f"{self.lattice_cache_key}:{lattice_id}" # Use lattice name as part of the cache key to allow caching multiple lattice configurations if needed. This way, if the same lattice configuration is used for multiple invocations, it can be quickly retrieved from the cache without having to read from S3 each time, reducing latency and S3 read costs.
        try:
            self.catalyst.set_json(key, config) # Cache the lattice configuration in Catalyst so that it can be quickly retrieved for topology generation without having to read from S3 each time. This is especially beneficial if the same lattice configuration is used for multiple invocations, as it reduces latency and S3 read costs by avoiding repeated reads of the same lattice config from S3 for each invocation. The cache can have a TTL set to ensure it doesn't grow indefinitely and stale data is eventually cleared out.
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to cache lattice configuration", error=str(e), lattice_id=lattice_id)

    def get_cached_lattice(self, lattice_id: str):
        """
        Retrieves a cached lattice configuration from Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration to retrieve (e.g., "sample").
        
        Returns:
            dict | None: The cached lattice configuration, or None if not found in the cache.
        """
        key = f"{self.lattice_cache_key}:{lattice_id}"
        try:
            config = self.catalyst.get_json(key)
            return config
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to get cached lattice", error=str(e), lattice_id=lattice_id)
            return None


    def generate_topology(self, config):
        """
        Generates a topology DAG from a lattice configuration.
        
        Args:
            config (dict): The lattice configuration to generate the topology from.
        
        Returns:
            dict: The generated topology DAG.
        """
        return self.topology.dag(config)
    
    def cache_topology_state(self, lattice_id, run_id, state):
        """
        Caches the topology execution state in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
            state (dict): The topology execution state to cache.
        """
        state = json.loads(json.dumps(state, cls=EnumEncoder)) # Serialize the topology state to JSON format using the custom EnumEncoder to handle any Enum values in the state. This allows us to store complex data structures that may include Enums in the Catalyst state store as JSON strings, and then deserialize them back into their original form when reading from the state store. This is important for maintaining the integrity of the topology state, especially if it includes node statuses that are represented as Enums.
        key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        try:
            self.catalyst.set_json(key, state) # Cache the topology execution state in Catalyst so that it can be updated in real-time as worker updates come in and the controller can read the latest state to make decisions on which nodes to dispatch next based on their dependencies and statuses. This allows for stateful execution of the topology and real-time tracking of node statuses and results.
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to cache topology state", error=str(e), lattice_id=lattice_id, run_id=run_id)

    def get_cached_topology_state(self, lattice_id, run_id):
        """
        Retrieves the cached topology execution state from Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
        
        Returns:
            dict | None: The cached topology execution state, or None if not found in the cache.
        """
        key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        self.residue(self.ash.DEBUG, "Topology state key", key=key)
        try:
            state = self.catalyst.get_json(key)
            return state
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to get cached topology state", error=str(e), lattice_id=lattice_id, run_id=run_id)
            return None
    
    def mark_topology_started_timestamp(self, lattice_id, run_id):
        """
        Marks the topology execution with a started timestamp in the cached state in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
        """
        key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        try:
            self.catalyst.update_json(key, '.started_timestamp', datetime.utcnow().isoformat()) # Mark the topology execution with a started timestamp in the cached topology state in Catalyst. This allows for tracking when the topology execution started and can be useful for historical reference, debugging, and analysis of execution times and performance.
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to mark topology started timestamp", error=str(e), lattice_id=lattice_id, run_id=run_id)
    
    def mark_topology_completed_timestamp(self, lattice_id, run_id):
        """
        Marks the topology execution with a completed timestamp in the cached state in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
        """
        key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        try:
            self.catalyst.update_json(key, '.completed_timestamp', datetime.utcnow().isoformat()) # Mark the topology execution with a completed timestamp in the cached topology state in Catalyst. This allows for tracking when the topology execution finished and can be useful for historical reference, debugging, and analysis of execution times and performance.
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to mark topology completed timestamp", error=str(e), lattice_id=lattice_id, run_id=run_id)

    def get_lattice_connections(self, lattice_id):
        """
        Retrieves the connections (edges) of a lattice from the cached configuration in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
        
        Returns:
            list | None: The connections of the lattice, or None if not found in the cache.
        """
        key = f"{self.lattice_cache_key}:{lattice_id}"
        try:
            connections = self.catalyst.get_json(key, path=".connections")
            return connections
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to get lattice connections", error=str(e), lattice_id=lattice_id)
            return None

    def check_node_exist(self, lattice_id, run_id, node_id):
        """
        Checks if a node exists in the cached topology state in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
            node_id (str): The identifier of the node to check (e.g., "layer.node").
        
        Returns:
            bool: True if the node exists in the cached topology state, False otherwise.
        """
        state = self.get_cached_topology_state(lattice_id, run_id)
        if not state:
            return False
        exists = node_id in state["nodes"]
        return exists

    # Check Node Status
    def check_node_status(self, node_id, topology_state):
        """
        Checks the status of a node in the topology state.
        
        Args:
            node_id (str): The identifier of the node to check (e.g., "layer.node").
            topology_state (dict): The current state of the topology.
        
        Returns:
            str | None: The status of the node, or None if not found in the topology state.
        """
        status = topology_state["status"].get(node_id)
        return status
    
    # Get the Dependenceis met of a Node Based on the Current State of the Topology Execution (E.G. CHECK IF ALL DEPENDENCIES HAVE STATUS COMPLETED)
    def check_node_dependencies(self, lattice_id, run_id, node_id):
        """
        Checks if all dependencies of a node have been met based on the current topology execution state.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
            node_id (str): The identifier of the node to check dependencies for (e.g., "layer.node").
        
        Returns:
            bool: True if all dependencies have been met, False otherwise. Returns False if state is missing.
        """
        state = self.get_cached_topology_state(lattice_id, run_id)
        if not state:
            return False  # Can't check dependencies if state is missing
        parents = state["reverse_edges"].get(node_id, [])  
        
        return all(state["status"].get(p) in [NodeStatus.SUCCESS.value, NodeStatus.LIVE.value] for p in parents)
    
    # set node status in topology state in catalyst using update_json
    def update_node_status(self, lattice_id, run_id, node_id, status: NodeStatus):
        """
        Updates the status of a node in the cached topology state in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
            node_id (str): The identifier of the node to update (e.g., "layer.node").
            status (NodeStatus): The new status to set for the node (e.g., NodeStatus.SUCCESS).
        """
        key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        # xx = ["status: user.name"]
        # Use bracket notation for node_id to handle dots in key names
        try:
            self.catalyst.update_json(key, f'.status["{node_id}"]', status.value) # Update the status of a node in the cached topology state in Catalyst. This allows the controller to keep track of the current status of each node in the topology execution and make informed decisions on which nodes to dispatch next based on their dependencies and statuses.
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to update node status", error=str(e), lattice_id=lattice_id, run_id=run_id, node_id=node_id)
        
    # Update Node Checkpoint
    def update_node_checkpoint(self, lattice_id, run_id, node_id, worker_msg: TaskMessage):
        """
        Updates the checkpoint metadata and error details of a node in the cached topology state in Catalyst.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
            node_id (str): The identifier of the node to update (e.g., "layer.node").
            worker_msg (TaskMessage): The worker message containing checkpoint metadata and optional error details.
        """
        key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        checkpoint_metadata = worker_msg.node_configuration.checkpoint_metadata.to_dict()
        try:
            self.catalyst.update_json(key, f'.nodes["{node_id}"].checkpoint_metadata', checkpoint_metadata) # Update the checkpoint metadata of a node in the cached topology state in Catalyst. This allows the controller to keep track of the progress of long-running nodes that process data in batches and require checkpointing to manage their state between batches. The checkpoint metadata can include information such as the last processed cursor, batch index, number of records processed, total records processed so far, and whether the checkpoint is final, which can help the controller make informed decisions on when to dispatch the next batch of work to the worker based on the progress of the current batch and the overall status of the node.
            
            error_details = worker_msg.node_configuration.error_details
            if error_details:
                self.catalyst.update_json(key, f'.nodes["{node_id}"].error_details', error_details.to_dict()) # Update the error details of a node in the cached topology state in Catalyst. This allows the controller to keep track of any errors that occurred during the execution of a node and make informed decisions on how to handle failures, retries, or other error recovery mechanisms.
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to update node checkpoint", error=str(e), lattice_id=lattice_id, run_id=run_id, node_id=node_id)

    # Archive Completed / Skipped / Failed Topology State in Catalyst for Historical Reference and Debugging
    def archive_topology_state(self, lattice_id, run_id):
        """
        Archives the topology execution state in Catalyst for historical reference and debugging.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
        """
        active_key = f"{self.topology_run_state_prefix}:{lattice_id}:{run_id}"
        archive_key = f"{self.topology_archive_state_prefix}:{lattice_id}:{run_id}"
        try:
            self.catalyst.rename_key(active_key, archive_key)
        except (ResponseError, RedisConnectionError) as e:
            self.residue(self.ash.WARNING, "Failed to archive topology state", error=str(e), lattice_id=lattice_id, run_id=run_id)

    def topology_complete(self, lattice_id, run_id):
        """
        Checks if the topology execution is complete by verifying all node statuses.
        
        Args:
            lattice_id (str): The identifier of the lattice configuration (e.g., "sample").
            run_id (str): The identifier of the topology run (e.g., "run_1").
        
        Returns:
            bool: True if all nodes are SUCCESS or SKIPPED, False otherwise. Returns False if state is missing.
        """
        state = self.get_cached_topology_state(lattice_id, run_id)
        if not state:
            self.residue(self.ash.WARNING, "Topology state missing - cannot check completion", lattice_id=lattice_id, run_id=run_id)
            return False  # State was flushed/missing, skip this topology
        return all(status in [NodeStatus.SUCCESS.value, NodeStatus.SKIPPED.value] for status in state["status"].values()) # The topology is considered complete when all nodes have a status of either SUCCESS or SKIPPED. This means that all nodes have either completed successfully or were skipped due to upstream failures, and there are no nodes left in a pending or running state. This is a simple way to determine if the entire topology execution has finished and the controller can perform any necessary cleanup or archival of the topology state.


    def oxidize(self):
        """
        Main loop for the Oxidizer controller to read invocation messages, manage topology state, and process worker updates.
        """
        while self.running: # FUTURE REPLACE WITH SHUTDOWN SIGNAL HANDLER VIA INVOCATION STREAM (WAIT FOR ALL JOBS TO FINISH)
            lattice_id = None
            run_id = None

            # 1 - Read from Invocation Stream for New Jobs or Commands
            try:
                invocations = self.catalyst.read_from_stream(self.invocation_stream, self.oxidizer_consumer_group, self.oxidizer_consumer_name, count=1) # Read invocation messages from the invocation stream to get new jobs or commands for the controller to execute. This is how the API layer can communicate with the controller to trigger new topology executions or send control commands like shutdown signals. Each message should contain the necessary information for the controller to process the command, such as the type of command (e.g., invoke_topology, shutdown), and any relevant data (e.g., lattice configuration name for invoking a topology).
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    self.residue(self.ash.WARNING, "Consumer group missing, recreating...", stream=self.invocation_stream)
                    self.catalyst.create_consumer_group(self.invocation_stream, self.oxidizer_consumer_group)
                    continue
                else: 
                    self.residue(self.ash.ERROR, "Error reading from invocation stream", error=str(e))
                    continue
            if invocations:
                msg_id, invocation = invocations[0] 
                self.residue(self.ash.INFO, "Received INVOCATION", invocation=invocation)

                invoke_id = invocation["invoke_id"]                
                run_id = uuid.uuid4() # Generate a unique run ID for this invocation to track the topology execution state in Catalyst.


                # IF / ELSE LOGIC BASED ON INVOCATION TYPE (NEW JOB, SHUTDOWN SIGNAL, ETC.)
                # FOR NEW JOB: LOAD LATTICE CONFIG, GENERATE TOPOLOGY, DISPATCH INITIAL NODES TO WORKERS, ETC.
                if invocation['type'] == 'invoke_topology':  
                    lattice_id = invocation['lattice']

                    # Check if Lattice Exists in Cache
                    lattice = self.get_cached_lattice(lattice_id) # Check Cache First
                    

                    # If Lattice is not in Cache, Load from S3 and Cache Lattice Configuration
                    load_lattice_error = None
                    if not lattice: 
                        lattice = self.load_lattice(lattice_id)
                        if lattice:
                            self.cache_lattice(lattice_id, lattice) # Cache So We Dont Have to Keep Reading. TTL Should be set and then it should check first and then load and then cache
                        else:
                            load_lattice_error = "Failed to load lattice configuration from S3"
                    
                    # Validate Lattice Configuration Before Generating Topology
                    # FUTURE - This needs updating - Move into the above?
                    # if not self.validate_lattice(lattice):
                    #     self.residue(self.ash.ERROR, "Invalid lattice configuration", lattice_id=lattice_id)
                    #     load_lattice_error = "Invalid lattice configuration"
                    #     continue # Skip processing this invocation and move on to the next one if the lattice configuration is invalid, since we can't proceed with an invalid lattice config.




                    # Add Invocation to Run State in Catalyst for Reference by Microscope API and Frontend
                    invocation_details = {
                        "invoke_id": invoke_id,
                        "lattice_id": lattice_id,
                        "run_id": f"{run_id}", 
                        "invoke_error": load_lattice_error
                    }
                    # FUTURE: Fix - Make this a sub function in oxidizer ?
                    self.catalyst.set_json(f"oxidizer:invocation:{invoke_id}", invocation_details) 
                    self.catalyst.set_ttl(f"oxidizer:invocation:{invoke_id}", 60 * 15) 


                    # If Valid Invocation, Generate Topology and Cache Initial State in Catalyst for Controller to Manage During Execution
                    if not load_lattice_error:
                    # Generate Topology from Lattice Config 
                        topology = self.generate_topology(lattice) 
                        topology['lattice_id'] = lattice_id # Add lattice name to topology state for reference
                        topology['run_id'] = f"{run_id}" # Add run ID to topology state for reference

                        # CACHE TOPOLOGY STATE IN CATALYST FOR STATEFUL EXECUTION 
                        self.cache_topology_state(lattice_id, run_id, topology)

                    

                if invocation['type'] == 'pause_topology':
                    lattice_id = invocation['lattice_id'] # REQUIRED 
                    run_id = invocation['run_id'] # REQUIRED
                    node_id = invocation.get('node_id') # OPTIONAL - IF SPECIFIED, PAUSE FROM THIS NODE AND DOWNSTREAM NODES; IF NOT SPECIFIED, PAUSE ENTIRE TOPOLOGY
                    self.residue(self.ash.INFO, "Received PAUSE TOPOLOGY signal. Pausing topology execution gracefully after current batch completes.", lattice_id=lattice_id, run_id=run_id)
                    # FUTURE IMPLEMENTATION: UPDATE TOPOLOGY STATE IN CATALYST TO REFLECT PAUSED STATUS, AND WORKERS CHECK THIS STATUS BEFORE DISPATCHING NEW BATCHES TO KNOW WHETHER TO PAUSE OR NOT.

                if invocation['type'] == 'resume_topology':
                    lattice_id = invocation['lattice_id'] # REQUIRED 
                    run_id = invocation['run_id'] # REQUIRED
                    node_id = invocation.get('node_id') # OPTIONAL - IF SPECIFIED, RESUME FROM THIS NODE AND DOWNSTREAM NODES; IF NOT SPECIFIED, RESUME ENTIRE TOPOLOGY
                    self.residue(self.ash.INFO, "Received RESUME TOPOLOGY signal. Resuming topology execution.", lattice_id=lattice_id, run_id=run_id)
                    # FUTURE IMPLEMENTATION: UPDATE TOPOLOGY STATE IN CATALYST TO REFLECT RESUMED STATUS, AND WORKERS CHECK THIS STATUS BEFORE DISPATCHING NEW BATCHES TO KNOW WHETHER TO RESUME OR NOT.


                if invocation['type'] == 'shutdown':
                    self.residue(self.ash.INFO, "Received shutdown signal. Shutting down Oxidizer gracefully after completing current jobs.")
                    self.running = False
            
            else:
                self.residue(self.ash.INFO, "No messages in INVOCATION stream. Waiting...")




            # 2 - Read from Controller Stream for Worker Updates (WorkerMessageType) - UPDATE TOPOLOGY STATE IN CATALYST BASED ON WORKER UPDATE (E.G. NODE STATUS CHANGES, RESULTS, ETC.)
            try:
                worker_updates = self.catalyst.read_from_stream(self.controller_stream, self.oxidizer_consumer_group, self.oxidizer_consumer_name)
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    self.residue(self.ash.WARNING, "Consumer group missing, recreating...", stream=self.controller_stream)
                    self.catalyst.create_consumer_group(self.controller_stream, self.oxidizer_consumer_group)
                    continue
                else: 
                    self.residue(self.ash.ERROR, "Error reading from controller stream", error=str(e))
                    continue
            
            if worker_updates:

                # Message ID and Worker Update Details
                msg_id, worker_update = worker_updates[0]
                worker_msg = TaskMessage.from_dict(worker_update)

                # Lattice ID, Run ID, Node ID from Worker Update for Reference and State Management in Catalyst
                worker_update_type = worker_msg.type
                lattice_id = worker_msg.lattice_id
                run_id = worker_msg.run_id
                layer_id = worker_msg.layer_id
                node_id = worker_msg.node_id
                node_configuration = worker_msg.node_configuration

                # Lattice Connections for Reference in Processing Worker Update and Making Decisions on Next Nodes to Dispatch
                connections = self.get_lattice_connections(lattice_id) # Get the connections of the lattice from the cached lattice configuration in Catalyst. This allows the controller to understand the dependencies between nodes in the topology and make informed decisions on which nodes to dispatch next based on the current state of execution and which dependencies have been met.
                
                # Log Received Worker Update for Debugging and Transparency
                self.residue(self.ash.INFO, "Received WORKER UPDATE", worker_update=worker_update, lattice_id=lattice_id, run_id=worker_msg.run_id, node_id=worker_msg.node_id)

                # FUTURE - Fix - Is this even needed? Does the above Task message have everything?
                # Prep Node + Task Message Details for Processing Worker Update
                worker_msg = TaskMessage(
                    type=worker_msg.type,
                    lattice_id=worker_msg.lattice_id,
                    run_id=worker_msg.run_id,
                    layer_id=worker_msg.layer_id,
                    node_id=worker_msg.node_id,
                    node_configuration=worker_msg.node_configuration,
                    connections=connections
                )

                # Process Worker Update Based on Its Message Type (E.G. STARTED, CHECKPOINT, SUCCESS, FAILED, HEARTBEAT, PAUSE_ACK, RESUME_ACK)
                if worker_msg.type == WorkerMessageType.STARTED.value: 
                    # get node_configuration and see if LIVE node...
                    node_type = node_configuration.type
                    if node_type == "live":
                        self.mark_topology_started_timestamp(lattice_id, run_id) # Mark the topology execution with a started timestamp in the cached topology state in Catalyst when the first worker reports that it has started processing a node. This allows for tracking when the topology execution started and can be useful for historical reference, debugging, and analysis of execution times and performance.
                        self.update_node_status(lattice_id, run_id, node_id, NodeStatus.LIVE)
                        self.residue(self.ash.INFO, "Updated node status to LIVE", lattice_id=lattice_id, run_id=run_id, node_id=node_id) 
                    else:
                        self.mark_topology_started_timestamp(lattice_id, run_id) # Mark the topology execution with a started timestamp in the cached topology state in Catalyst when the first worker reports that it has started processing a node. This allows for tracking when the topology execution started and can be useful for historical reference, debugging, and analysis of execution times and performance.
                        self.update_node_status(lattice_id, run_id, node_id, NodeStatus.RUNNING)
                        self.residue(self.ash.INFO, "Updated node status to RUNNING", lattice_id=lattice_id, run_id=run_id, node_id=node_id) 

                elif worker_msg.type == WorkerMessageType.CHECKPOINT.value:
                    if worker_msg.node_configuration.checkpoint_metadata.is_final and worker_msg.node_configuration.type != "live":
                        self.update_node_checkpoint(lattice_id, run_id, node_id, worker_msg)
                        self.update_node_status(lattice_id, run_id, node_id, NodeStatus.SUCCESS)
                        self.residue(self.ash.INFO, "Updated node status to SUCCESS", lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                    else:
                        worker_msg.node_configuration.checkpoint_metadata.batch_index += 1 
                        self.residue(self.ash.INFO, "Updated node checkpoint metadata in topology state", lattice_id=lattice_id, run_id=run_id, node_id=node_id, checkpoint_metadata=worker_msg.node_configuration.checkpoint_metadata)
                        # Write Checkpoint Task Message to Worker Stream
                        worker_msg.type = WorkerTaskType.CHECKPOINT_NODE.value
                        try:
                            self.catalyst.write_to_stream(self.worker_stream, worker_msg.to_dict()) # Write a checkpoint task message to the worker stream to acknowledge the checkpoint and provide any updated checkpoint metadata. This allows the worker to know that the checkpoint was received and processed by the controller, and it can use the updated checkpoint metadata to manage its state for the next batch of work. The controller can also use this opportunity to update the node status in the topology state in Catalyst if needed (e.g., if the checkpoint indicates that the node is still running but has made progress, we can keep it as RUNNING, but if the checkpoint indicates that the node has completed its work, we can update it to SUCCESS).
                        except (ResponseError, RedisConnectionError) as e:
                            self.residue(self.ash.WARNING, "Failed to write checkpoint to worker stream", error=str(e), lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                        
                        self.update_node_checkpoint(lattice_id, run_id, node_id, worker_msg) 
                        self.update_node_status(lattice_id, run_id, node_id, NodeStatus.DISPATCHED)
                        self.residue(self.ash.INFO, "Updated node status to DISPATCHED", lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                
                # elif worker_msg.type == WorkerMessageType.SUCCESS.value:
                #     # This should never happend - checkpoint w/ is_final=true should be the only way for a node to report success 
                #     pass
                
                elif worker_msg.type == WorkerMessageType.FAILED.value:
                    # Handle failed acknowledgment - update node status to FAILED and record error details for debugging and potential retries
                    self.update_node_checkpoint(lattice_id, run_id, node_id, worker_msg)
                    # FUTURE - Add Retry Logic Here Based on Error Details and Retry Policies (E.G. MAX RETRIES, BACKOFF STRATEGIES, ETC.)
                    self.update_node_status(lattice_id, run_id, node_id, NodeStatus.FAILED)
                    self.residue(self.ash.INFO, "Updated node status to FAILED", lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                
                elif worker_msg.type == WorkerMessageType.HEARTBEAT.value:
                    # Handle heartbeat - update last seen timestamp for worker to monitor liveness and reset any dispatch timeouts
                    pass
                
                # elif worker_msg.type == WorkerMessageType.PAUSE_ACK.value:
                #     # Handle pause acknowledgment - update node status to PAUSED and ensure worker has committed any in-flight work before halting
                #     pass
                
                # elif worker_msg.type == WorkerMessageType.RESUME_ACK.value:
                #     # Handle resume acknowledgment - update node status to RUNNING and ensure worker has resumed processing from the correct cursor
                #     pass
            else:
                self.residue(self.ash.INFO, "No messages in CONTROLLER stream. Waiting...")

            # 3 - Send Messages to Workers via Worker Stream for Nodes that are Ready to be Executed Based on Topology State in Catalyst (E.G. DISPATCH NODES WITH ALL DEPENDENCIES COMPLETED)
            self.residue(self.ash.DEBUG, "Checking for nodes ready to dispatch...")
            try:
                active_topologies = self.catalyst.scan_keys(f"{self.topology_run_state_prefix}:*")
            except (ResponseError, RedisConnectionError) as e:
                self.residue(self.ash.WARNING, "Failed to scan for active topologies", error=str(e))
                active_topologies = []
            for topology_key in active_topologies:
                self.residue(self.ash.INFO, "Active topology", topology_key=topology_key)
                
                lattice_id = topology_key.split(":")[-2] # Assuming the key format is "oxidizer:topology:active:lattice_id:run_id"
                connections = self.get_lattice_connections(lattice_id)
                
                run_id = topology_key.split(":")[-1]
                
                topology_state = self.get_cached_topology_state(lattice_id, run_id)
                if not topology_state:
                    self.residue(self.ash.WARNING, "Topology state missing, skipping", lattice_id=lattice_id, run_id=run_id)
                    continue
                nodes = topology_state["nodes"]
                statuses = topology_state["status"]
                # iterate through each status and check if node is pending and if dependencies are met, then dispatch to worker stream and update status to running
                for node_id, status in statuses.items():
                    if status == NodeStatus.PENDING.value and self.check_node_dependencies(lattice_id, run_id, node_id):
                        # Dispatch to worker stream

                        node_details = nodes[node_id]
                        node_layer = node_details["layer"]

                        task_msg = TaskMessage(
                            type=WorkerTaskType.START_NODE.value,
                            lattice_id=lattice_id,
                            connections=connections, 
                            run_id=run_id,
                            layer_id=node_layer,
                            node_id=node_id,
                            node_configuration=NodeConfiguration(**node_details).to_dict()
                        )
                        try:
                            self.catalyst.write_to_stream(self.worker_stream, task_msg.to_dict())
                            self.residue(self.ash.INFO, "Dispatched node to worker stream", lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                            self.update_node_status(lattice_id, run_id, node_id, NodeStatus.DISPATCHED)
                            self.residue(self.ash.INFO, "Updated node status to DISPATCHED", lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                        except (ResponseError, RedisConnectionError) as e:
                            self.residue(self.ash.WARNING, "Failed to dispatch node to worker stream", error=str(e), lattice_id=lattice_id, run_id=run_id, node_id=node_id)   
                
                # Check if All Nodes Are Complete and Archive Topology State if So
                self.residue(self.ash.DEBUG, "Checking if topology execution is complete...", lattice_id=lattice_id, run_id=run_id)
                if self.topology_complete(lattice_id, run_id):
                    self.residue(self.ash.INFO, "Topology execution complete. Archiving topology state.", lattice_id=lattice_id, run_id=run_id, node_id=node_id)
                    # Add Completion Timestamp to Topology State Before Archiving
                    self.mark_topology_completed_timestamp(lattice_id, run_id)
                    self.archive_topology_state(lattice_id, run_id)