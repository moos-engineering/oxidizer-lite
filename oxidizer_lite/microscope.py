# API 

import json
from importlib import resources
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, Response
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from fastmcp import FastMCP
import yaml

from oxidizer_lite.catalyst import Catalyst
from oxidizer_lite.crucible import Crucible
from oxidizer_lite.residue import Residue
from oxidizer_lite.anvil import SQLEngine, APIEngine
from oxidizer_lite.phase import GlueCatalogConnection, DuckLakeConnection

# FastMCP
class Microscope(Residue):
    def __init__(self, catalyst: Catalyst, crucible: Crucible, enable_anvil: bool = False):
        """
        Initializes the Microscope API and MCP server.
        
        Args:
            catalyst (Catalyst): The Redis client for stream and cache operations.
            crucible (Crucible): The S3 client for object storage operations.
            enable_anvil (bool): Whether to enable the Anvil SQL/API engine endpoints.
        """
        super().__init__()
        self.catalyst = catalyst
        self.crucible = crucible
        self.invocation_stream = "oxidizer:streams:invocations" # This is the stream that the API layer will write invocation messages to for the controller to read from.
        self.enable_anvil = enable_anvil
        self.mcp = FastMCP("Microscope")
        self.cors_middleware = [
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )
        ]
        self._register_oxidizer_api_routes()
        self._register_oxidizer_mcp_tools()
        self._register_anvil_mcp_tools()




    def _register_oxidizer_api_routes(self):
        """
        Register API routes for the Microscope.
        """

        @self.mcp.custom_route("/", methods=["GET"])
        async def root(request: Request) -> HTMLResponse:
            """
            Serve the main UI page for the Microscope.
            Returns:
                HTMLResponse: The HTML content of the UI page.
            """
            ui_file = resources.files("oxidizer_lite").joinpath("ui.html")
            html = ui_file.read_text(encoding="utf-8")
            return HTMLResponse(html)

        @self.mcp.custom_route("/health", methods=["GET"])
        async def health(request: Request) -> JSONResponse:
            """
            Health check endpoint for the Microscope.
            Returns:
                JSONResponse: A JSON response indicating the health status of the service.
            """
            return JSONResponse({"status": "ok", "service": "microscope"}) 

    def _register_oxidizer_mcp_tools(self):
        """
        Register MCP tools for interacting with the Catalyst and Crucible.
        """

        @self.mcp.tool("invoke_topology")
        def invoke_topology_tool(invoke_id: str, lattice_id: str):
            """
            Invokes a topology execution by writing an invocation message to the Catalyst stream.
            
            Args:
                lattice_id (str): The identifier of the lattice (topology) to invoke.
            """
            self.catalyst.write_to_stream(self.invocation_stream, {"type": "invoke_topology", "invoke_id": invoke_id, "lattice": lattice_id})
            return 
        
        @self.mcp.tool("get_invoke_status")
        def get_invoke_status_tool(invoke_id: str):
            """
            Retrieves the status of a topology invocation.
            
            Args:
                invoke_id (str): The identifier of the invocation to check the status for.
            
            Returns:
                dict: A dictionary containing the current status of the invocation, such as "pending", "running", "completed", or "failed".
            """
            # FUTURE: Implement logic to track and retrieve the status of topology invocations based on messages in the Catalyst stream or state stored in Redis.
            invoke_key = f"oxidizer:invocation:{invoke_id}"
            invoke_status = self.catalyst.get_json(invoke_key)
            return invoke_status
        

        @self.mcp.tool("get_topology_runs")
        def get_topology_runs_tool(lattice_id: str = None):
            """
            Retrieves topology runs for a specific lattice, returning both archived and active runs with status summaries.
            
            Args:
                lattice_id (str | None): The identifier of the lattice to get runs for. If None, returns runs for all lattices.
            
            Returns:
                list: A list of run summary objects with lattice_id, run_id, state (active/archive),
                      started_timestamp, completed_timestamp, and node_statuses.
            """

            archive_pattern = "oxidizer:topology:archive:*"
            if lattice_id:
                archive_pattern = f"oxidizer:topology:archive:{lattice_id}:*"
            archive_keys = self.catalyst.scan_keys(archive_pattern) 

            active_pattern = "oxidizer:topology:active:*"
            if lattice_id:
                active_pattern = f"oxidizer:topology:active:{lattice_id}:*"
            active_keys = self.catalyst.scan_keys(active_pattern)

            runs = []
            for key in active_keys:
                parts = key.split(":")
                run_data = self.catalyst.get_json(key)
                summary = {
                    "lattice_id": parts[3],
                    "run_id": parts[4],
                    "state": "active",
                    "started_timestamp": run_data.get("started_timestamp") if run_data else None,
                    "completed_timestamp": run_data.get("completed_timestamp") if run_data else None,
                    "node_statuses": run_data.get("status", {}) if run_data else {},
                }
                runs.append(summary)

            for key in archive_keys:
                parts = key.split(":")
                run_data = self.catalyst.get_json(key)
                summary = {
                    "lattice_id": parts[3],
                    "run_id": parts[4],
                    "state": "archive",
                    "started_timestamp": run_data.get("started_timestamp") if run_data else None,
                    "completed_timestamp": run_data.get("completed_timestamp") if run_data else None,
                    "node_statuses": run_data.get("status", {}) if run_data else {},
                }
                runs.append(summary)

            return runs

        @self.mcp.tool("get_topology_run")
        def get_topology_run_tool(lattice_id: str, run_id: str):
            """
            Retrieves a single topology run including its lattice config and node statuses.

            Args:
                lattice_id (str): The lattice identifier.
                run_id (str): The run identifier.

            Returns:
                dict: Run data with lattice config, node_statuses, timestamps.
            """
            # Try active first, then archive
            run_data = None
            state = None
            for s in ("active", "archive"):
                key = f"oxidizer:topology:{s}:{lattice_id}:{run_id}"
                run_data = self.catalyst.get_json(key)
                if run_data is not None:
                    state = s
                    break

            if run_data is None:
                return {"error": f"Run '{run_id}' not found for lattice '{lattice_id}'"}

            # Get the lattice config for DAG structure
            lattice_key = f"oxidizer:lattice:{lattice_id}"
            lattice_data = self.catalyst.get_json(lattice_key)

            return {
                "lattice_id": lattice_id,
                "run_id": run_id,
                "state": state,
                "started_timestamp": run_data.get("started_timestamp"),
                "completed_timestamp": run_data.get("completed_timestamp"),
                "node_statuses": run_data.get("status", {}),
                "lattice": lattice_data,
            }

        @self.mcp.tool("list_logs")
        def list_logs_tool(lattice_id: str = None, run_id: str = None, node_id: str = None, level: str = None, since: float = None):
            """
            Lists logs matching the given filters. Returns log keys with metadata and content.
            
            Args:
                lattice_id (str | None): The lattice identifier filter.
                run_id (str | None): The run identifier filter.
                node_id (str | None): The node identifier filter.
                level (str | None): The log level filter (e.g. INFO, ERROR, WARNING).
                since (float | None): Only return log entries with timestamp > since (epoch seconds).
            
            Returns:
                list: Log entries with key metadata and content.
            """
            pattern = f"oxidizer:logs:{lattice_id or '*'}:{run_id or '*'}:{node_id or '*'}:{level or '*'}"
            keys = self.catalyst.scan_keys(pattern)
            results = []
            for key in sorted(keys):
                parts = key.split(":")
                # oxidizer:logs:<lattice_id>:<run_id>:<node_id>:<level>
                log_data = self.catalyst.get_json(key)
                logs = log_data if log_data else []
                if since is not None:
                    logs = [l for l in logs if (l.get("timestamp") or 0) > since]
                    if not logs:
                        continue
                entry = {
                    "key": key,
                    "lattice_id": parts[2] if len(parts) > 2 else None,
                    "run_id": parts[3] if len(parts) > 3 else None,
                    "node_id": parts[4] if len(parts) > 4 else None,
                    "level": parts[5] if len(parts) > 5 else None,
                    "logs": logs,
                }
                results.append(entry)
            return results
        

        @self.mcp.tool("list_lattices")
        def list_lattices_tool():
            """
            Lists all available lattices with summary information.
            
            Returns:
                list: A list of lattice summary objects containing lattice_id, version, name,
                      layer_count, node_count, and connection_count.
            """
            lattice_keys = self.catalyst.scan_keys("oxidizer:lattice:*")
            summaries = []
            for key in lattice_keys:
                lattice_id = key.split(":")[-1]
                lattice_data = self.catalyst.get_json(key)
                if lattice_data is None:
                    summaries.append({"lattice_id": lattice_id})
                    continue
                layers = lattice_data.get("layers", [])
                node_count = sum(len(layer.get("nodes", [])) for layer in layers)
                summaries.append({
                    "lattice_id": lattice_id,
                    "name": lattice_data.get("name", lattice_id),
                    "version": lattice_data.get("version"),
                    "layer_count": len(layers),
                    "node_count": node_count,
                    "connection_count": len(lattice_data.get("connections", [])),
                })
            return summaries

        @self.mcp.tool("get_lattice")
        def get_lattice_tool(lattice_id: str):
            """
            Retrieves the full lattice configuration by ID.
            
            Args:
                lattice_id (str): The identifier of the lattice.
            
            Returns:
                dict: The full lattice configuration including layers, nodes, and connections.
            """
            lattice_key = f"oxidizer:lattice:{lattice_id}"
            lattice_data = self.catalyst.get_json(lattice_key)
            if lattice_data is None:
                return {"error": f"Lattice '{lattice_id}' not found"}
            lattice_data["lattice_id"] = lattice_id
            return lattice_data

        @self.mcp.tool("lattice_connections")
        def lattice_connections_tool(lattice_id: str, connection_name: str = None):
            """
            Lists all connections for a lattice, or retrieves a specific connection by name.
            
            Args:
                lattice_id (str): The identifier of the lattice.
                connection_name (str | None): The name of the specific connection to retrieve. If None, returns all connections.
            
            Returns:
                dict: A dictionary of lattice connections, or a single connection if connection_name is specified.
            """
            lattice_key = f"oxidizer:lattice:{lattice_id}"
            lattice_data = self.catalyst.get_json(lattice_key)
            connection_list = lattice_data.get("connections", [])
            connections = {}
            for connection in connection_list:
                connection_name = connection.get("name")
                connections[connection_name] = connection
            if connection_name:
                return connections.get(connection_name, {})
            return connections
        
        @self.mcp.tool("upload_lattice")
        def upload_lattice_tool(bucket: str, lattice_id: str, lattice_data: dict):
            """
            Uploads a lattice configuration to the Crucible as YAML.
            
            Args:
                bucket (str): The name of the S3 bucket to upload the lattice to.
                lattice_id (str): The identifier of the lattice.
                lattice_data (dict): The configuration data for the lattice.
            """
            # Convert lattice_data to yaml
            yaml_data = yaml.dump(lattice_data)
            self.crucible.upload_lattice(bucket, lattice_id, yaml_data)
            return
        
        @self.mcp.tool("cache_lattice")
        def cache_lattice_tool(lattice_id: str, lattice_data: dict):
            """
            Caches a lattice configuration in the Catalyst for faster access.
            
            Args:
                lattice_id (str): The identifier of the lattice.
                lattice_data (dict): The configuration data for the lattice.
            """
            # FUTURE: ANY WAY TO REUSE WHAT OXIDIZER / CONTROLLER IS DOING
            key = f"oxidizer:lattice:{lattice_id}"
            self.catalyst.set_json(key, lattice_data)
            return
        
        @self.mcp.tool("validate_lattice")
        def validate_lattice_tool(lattice_data: dict):
            """
            Validates a lattice configuration against the expected schema.
            
            Args:
                lattice_data (dict): The configuration data for the lattice.
            
            Returns:
                dict: A dictionary containing validation results, including any errors or warnings.
            """
            # FUTURE: Implement validation logic based on the expected schema for the lattice configuration
            validation_results = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }
            return validation_results
        
        
        @self.mcp.tool("upload_lattice_file")
        def upload_lattice_file_tool(bucket: str, lattice_id: str, file_path: str):
            """
            Uploads a lattice configuration file to the Crucible.
            
            Args:
                bucket (str): The name of the S3 bucket to upload the lattice file to.
                lattice_id (str): The identifier of the lattice.
                file_path (str): The path to the lattice file.
            """
            self.crucible.upload_lattice_file(bucket, lattice_id, file_path)
            return
        
        @self.mcp.tool("get_lattice_metrics")
        def get_lattice_metrics_tool(lattice_id: str):
            """
            Gets aggregated metrics for a lattice by scanning all archived and active runs.
            Returns run counts, success/failure rates, duration stats, and per-node performance.

            Args:
                lattice_id (str): The identifier of the lattice.

            Returns:
                dict: Aggregated metrics including summary stats, run history, and node performance.
            """
            archive_keys = self.catalyst.scan_keys(f"oxidizer:topology:archive:{lattice_id}:*")
            active_keys = self.catalyst.scan_keys(f"oxidizer:topology:active:{lattice_id}:*")

            all_keys = [(k, "archive") for k in archive_keys] + [(k, "active") for k in active_keys]

            total_runs = len(all_keys)
            success_count = 0
            failed_count = 0
            running_count = 0
            durations = []
            run_history = []
            node_perf = {}  # node_id -> accumulated stats

            for key, state in all_keys:
                parts = key.split(":")
                run_id = parts[4] if len(parts) > 4 else "unknown"
                run_data = self.catalyst.get_json(key)
                if not run_data:
                    continue

                statuses = run_data.get("status", {})
                started = run_data.get("started_timestamp")
                completed = run_data.get("completed_timestamp")

                # Derive run status from node statuses
                status_values = list(statuses.values())
                if any(s == "failed" for s in status_values):
                    run_status = "failed"
                    failed_count += 1
                elif state == "active":
                    run_status = "running"
                    running_count += 1
                elif all(s == "success" for s in status_values) and status_values:
                    run_status = "success"
                    success_count += 1
                else:
                    run_status = "unknown"

                # Duration
                duration = None
                if started and completed:
                    try:
                        from datetime import datetime
                        t_start = datetime.fromisoformat(started)
                        t_end = datetime.fromisoformat(completed)
                        duration = (t_end - t_start).total_seconds()
                        durations.append(duration)
                    except Exception:
                        pass

                run_history.append({
                    "run_id": run_id,
                    "state": state,
                    "status": run_status,
                    "started": started,
                    "completed": completed,
                    "duration": duration,
                    "node_count": len(statuses),
                })

                # Per-node performance from checkpoint_metadata
                nodes = run_data.get("nodes", {})
                for node_id, node_data in nodes.items():
                    if not isinstance(node_data, dict):
                        continue
                    cp = node_data.get("checkpoint_metadata")
                    if not cp or not isinstance(cp, dict):
                        continue

                    if node_id not in node_perf:
                        node_perf[node_id] = {
                            "node_id": node_id,
                            "layer": node_data.get("layer", ""),
                            "type": node_data.get("type", "batch"),
                            "run_count": 0,
                            "total_preprocess_runtime": 0.0,
                            "total_function_runtime": 0.0,
                            "total_postprocess_runtime": 0.0,
                            "total_preprocess_memory": 0.0,
                            "total_function_memory": 0.0,
                            "total_postprocess_memory": 0.0,
                            "total_records": 0,
                            "total_batches": 0,
                        }

                    np = node_perf[node_id]
                    np["run_count"] += 1
                    np["total_preprocess_runtime"] += cp.get("accumulated_preprocess_runtime", 0) or 0
                    np["total_function_runtime"] += cp.get("accumulated_function_runtime", 0) or 0
                    np["total_postprocess_runtime"] += cp.get("accumulated_postprocess_runtime", 0) or 0
                    np["total_preprocess_memory"] += cp.get("accumulated_preprocess_memory", 0) or 0
                    np["total_function_memory"] += cp.get("accumulated_function_memory", 0) or 0
                    np["total_postprocess_memory"] += cp.get("accumulated_postprocess_memory", 0) or 0
                    np["total_records"] += cp.get("total_records_so_far", 0) or 0
                    np["total_batches"] += cp.get("batch_index", 0) or 0

            # Compute averages for node performance
            node_performance = []
            for np in node_perf.values():
                rc = np["run_count"] or 1
                node_performance.append({
                    **np,
                    "avg_preprocess_runtime": round(np["total_preprocess_runtime"] / rc, 3),
                    "avg_function_runtime": round(np["total_function_runtime"] / rc, 3),
                    "avg_postprocess_runtime": round(np["total_postprocess_runtime"] / rc, 3),
                    "avg_total_runtime": round((np["total_preprocess_runtime"] + np["total_function_runtime"] + np["total_postprocess_runtime"]) / rc, 3),
                    "avg_preprocess_memory": round(np["total_preprocess_memory"] / rc, 3),
                    "avg_function_memory": round(np["total_function_memory"] / rc, 3),
                    "avg_postprocess_memory": round(np["total_postprocess_memory"] / rc, 3),
                    "avg_total_memory": round((np["total_preprocess_memory"] + np["total_function_memory"] + np["total_postprocess_memory"]) / rc, 3),
                })

            # Sort run history by started (newest first)
            run_history.sort(key=lambda r: r.get("started") or "", reverse=True)
            # Sort node performance by avg total runtime (slowest first)
            node_performance.sort(key=lambda n: n.get("avg_total_runtime", 0), reverse=True)

            return {
                "lattice_id": lattice_id,
                "summary": {
                    "total_runs": total_runs,
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "running_count": running_count,
                    "success_rate": round(success_count / total_runs, 3) if total_runs > 0 else 0,
                    "avg_duration": round(sum(durations) / len(durations), 2) if durations else None,
                    "min_duration": round(min(durations), 2) if durations else None,
                    "max_duration": round(max(durations), 2) if durations else None,
                },
                "run_history": run_history,
                "node_performance": node_performance,
            }

        @self.mcp.tool("get_system_metrics")
        def get_system_metrics_tool():
            """
            Gets system-level metrics including Redis server info and stream health.

            Returns:
                dict: System metrics with redis info (memory, clients, stats, uptime)
                      and stream health (pending counts, consumer lag).
            """
            # Redis INFO
            info = self.catalyst.client.info()
            redis_metrics = {
                "used_memory_human": info.get("used_memory_human", ""),
                "used_memory_peak_human": info.get("used_memory_peak_human", ""),
                "used_memory": info.get("used_memory", 0),
                "used_memory_peak": info.get("used_memory_peak", 0),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
                "uptime_in_days": info.get("uptime_in_days", 0),
                "total_keys": sum(
                    db_info.get("keys", 0) for key, db_info in info.items()
                    if isinstance(key, str) and key.startswith("db") and isinstance(db_info, dict)
                ),
                "redis_version": info.get("redis_version", ""),
            }

            # Stream health
            stream_names = [
                "oxidizer:streams:invocations",
                "oxidizer:streams:controller",
                "oxidizer:streams:worker",
            ]
            streams = []
            for stream_name in stream_names:
                try:
                    stream_info = self.catalyst.client.xinfo_stream(stream_name)
                    stream_entry = {
                        "name": stream_name,
                        "length": stream_info.get("length", 0),
                        "first_entry": None,
                        "last_entry": None,
                    }
                    # Get consumer group info
                    try:
                        groups = self.catalyst.client.xinfo_groups(stream_name)
                        group_details = []
                        for g in groups:
                            name = g.get("name", b"")
                            if isinstance(name, bytes):
                                name = name.decode("utf-8")
                            group_details.append({
                                "name": name,
                                "consumers": g.get("consumers", 0),
                                "pending": g.get("pending", 0),
                                "last_delivered_id": str(g.get("last-delivered-id", "")),
                            })
                        stream_entry["groups"] = group_details
                    except Exception:
                        stream_entry["groups"] = []
                    streams.append(stream_entry)
                except Exception:
                    streams.append({"name": stream_name, "length": 0, "groups": [], "error": "Stream not found"})

            return {
                "redis": redis_metrics,
                "streams": streams,
            }
        
        

    def _register_oxidizer_mcp_resources(self):
        """
        Registers MCP resources for the Oxidizer.
        """
        pass

    def _register_oxidizer_mcp_prompts(self):
        """
        Registers MCP prompts for the Oxidizer.
        """
        pass


    # ANVILE API Routes
    def _register_anvil_api_routes(self):
        """
        Registers API routes for the Anvil engine endpoints.
        """
        @self.mcp.custom_route("/anvil", methods=["GET"])
        async def anvil_endpoint(request: Request) -> JSONResponse:
            """
            Returns the Anvil API status (enabled or disabled).
            
            Args:
                request (Request): The incoming HTTP request.
            
            Returns:
                JSONResponse: A JSON response indicating the Anvil API status.
            """
            if self.enable_anvil:
                return JSONResponse({"status": "enabled", "service": "anvil", "message": "Anvil API is enabled"})
            else:
                return JSONResponse({"status": "disabled", "service": "anvil", "message": "Anvil API is disabled"})

    # ANVIL MCP Tools, Resources, and Prompts

    def _build_sql_engine(self, lattice_id: str, connection_name: str):
        """
        Builds an SQLEngine instance from a lattice connection config.
        
        Returns:
            tuple: (engine, error_dict) — engine is an SQLEngine on success, None on failure.
                   error_dict is None on success, a dict with "error" key on failure.
        """
        lattice_data = self.catalyst.get_json(f"oxidizer:lattice:{lattice_id}")
        if not lattice_data:
            return None, {"error": f"Lattice '{lattice_id}' not found."}

        connections = lattice_data.get("connections", [])
        conn_config = next((c for c in connections if c.get("name") == connection_name), None)
        if not conn_config:
            return None, {"error": f"Connection '{connection_name}' not found in lattice '{lattice_id}'."}

        conn_type = conn_config.get("type")
        if conn_type == "glue_catalog":
            connection_details = GlueCatalogConnection(
                name=conn_config["name"],
                aws_account_id=str(conn_config.get("aws_account_id", "")),
                aws_s3_bucket=conn_config.get("aws_s3_bucket", ""),
                aws_region=conn_config.get("aws_region"),
                aws_role_arn=conn_config.get("aws_role_arn"),
                aws_sso_profile=conn_config.get("aws_sso_profile"),
            )
        elif conn_type == "ducklake":
            connection_details = DuckLakeConnection(
                name=conn_config["name"],
                s3_endpoint=conn_config.get("s3_endpoint", ""),
                s3_bucket=conn_config.get("s3_bucket", ""),
                s3_prefix=conn_config.get("s3_prefix", ""),
                s3_region=conn_config.get("s3_region", ""),
                s3_access_key_id=conn_config.get("s3_access_key_id", ""),
                s3_secret_access_key=conn_config.get("s3_secret_access_key", ""),
                s3_use_ssl=conn_config.get("s3_use_ssl", True),
                postgres_host=conn_config.get("postgres_host", ""),
                postgres_port=conn_config.get("postgres_port", 5432),
                postgres_user=conn_config.get("postgres_user", ""),
                postgres_password=conn_config.get("postgres_password", ""),
                postgres_db=conn_config.get("postgres_db", ""),
            )
        else:
            return None, {"error": f"Unsupported connection type '{conn_type}'. Only 'glue_catalog' and 'ducklake' are supported."}

        engine = SQLEngine(engine="duckdb", connection_details=connection_details)
        return engine, None

    def _register_anvil_mcp_tools(self):
        """
        Register MCP tools for interacting with the Anvil's Engines (i.e. SQL, API).
        """
        @self.mcp.tool("list_databases")
        def list_databases_tool(lattice_id: str, connection_name: str):
            """
            Lists all databases (schemas) available for a lattice SQL connection.
            
            Args:
                lattice_id (str): The lattice identifier.
                connection_name (str): The name of the SQL connection defined in the lattice.
            
            Returns:
                dict: A dict with "databases" list, or an "error" key on failure.
            """
            try:
                engine, err = self._build_sql_engine(lattice_id, connection_name)
                if err:
                    return err
                catalog = engine.catalog
                cursor = engine.connection.execute(
                    f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{catalog}'"
                )
                databases = [row[0] for row in cursor.fetchall()]
                engine.close()
                return {"databases": databases}
            except Exception as e:
                return {"error": str(e)}

        @self.mcp.tool("list_tables")
        def list_tables_tool(lattice_id: str, connection_name: str, database_name: str):
            """
            Lists all tables in a specific database for a lattice SQL connection.
            
            Args:
                lattice_id (str): The lattice identifier.
                connection_name (str): The name of the SQL connection defined in the lattice.
                database_name (str): The database (schema) name to list tables from.
            
            Returns:
                dict: A dict with "tables" list, or an "error" key on failure.
            """
            try:
                engine, err = self._build_sql_engine(lattice_id, connection_name)
                if err:
                    return err
                catalog = engine.catalog
                cursor = engine.connection.execute(
                    f"SHOW TABLES FROM {catalog}.{database_name}"
                )
                tables = [row[0] for row in cursor.fetchall()]
                engine.close()
                return {"tables": tables}
            except Exception as e:
                return {"error": str(e)}

        @self.mcp.tool("get_table_schema")
        def get_table_schema_tool(lattice_id: str, connection_name: str, database_name: str, table_name: str):
            """
            Gets the column schema of a specific table.
            
            Args:
                lattice_id (str): The lattice identifier.
                connection_name (str): The name of the SQL connection defined in the lattice.
                database_name (str): The database (schema) name containing the table.
                table_name (str): The name of the table to get the schema for.
            
            Returns:
                dict: A dict with "columns" list of {name, type, nullable}, or an "error" key on failure.
            """
            try:
                engine, err = self._build_sql_engine(lattice_id, connection_name)
                if err:
                    return err
                catalog = engine.catalog
                cursor = engine.connection.execute(
                    f"DESCRIBE {catalog}.{database_name}.{table_name}"
                )
                columns = [
                    {"name": row[0], "type": row[1], "nullable": row[2]}
                    for row in cursor.fetchall()
                ]
                engine.close()
                return {"columns": columns}
            except Exception as e:
                return {"error": str(e)}

        @self.mcp.tool("execute_sql")
        def execute_sql_tool(lattice_id: str, connection_name: str, query: str):
            """
            Executes a read-only SQL query against a lattice connection using the Anvil SQLEngine.
            Only SELECT statements are allowed.
            
            Args:
                lattice_id (str): The lattice identifier to look up the connection config.
                connection_name (str): The name of the SQL connection defined in the lattice.
                query (str): The SQL query to execute (SELECT only).
            
            Returns:
                dict: Query results with columns and rows, or an error message.
            """
            stripped = query.strip().rstrip(";").strip()
            if not stripped.upper().startswith("SELECT"):
                return {"error": "Only SELECT queries are allowed."}

            try:
                engine, err = self._build_sql_engine(lattice_id, connection_name)
                if err:
                    return err
                cursor = engine.connection.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                engine.close()

                rows = [dict(zip(columns, row)) for row in results]
                return {"columns": columns, "rows": rows}

            except Exception as e:
                return {"error": str(e)}

        @self.mcp.tool("list_api_endpoints")
        def list_api_endpoints_tool(lattice_id: str, connection_name: str = None):
            """
            Scans a lattice configuration and returns all API endpoints used across nodes.

            Args:
                lattice_id (str): The lattice identifier.
                connection_name (str | None): Optional filter to only show endpoints for a specific API connection.

            Returns:
                dict: A dict with "endpoints" list, each containing connection, endpoint, http_method, node_id, direction (input/output).
            """
            try:
                lattice_data = self.catalyst.get_json(f"oxidizer:lattice:{lattice_id}")
                if not lattice_data:
                    return {"error": f"Lattice '{lattice_id}' not found."}

                endpoints = []
                for layer in lattice_data.get("layers", []):
                    layer_name = layer.get("name", "")
                    for node in layer.get("nodes", []):
                        node_name = node.get("name", "")
                        node_id = f"{layer_name}.{node_name}"

                        # Scan inputs
                        for inp in node.get("inputs", []):
                            for method in inp.get("methods", []):
                                if method.get("method") == "api":
                                    conn = method.get("connection", "")
                                    if connection_name and conn != connection_name:
                                        continue
                                    endpoints.append({
                                        "connection": conn,
                                        "endpoint": method.get("endpoint", ""),
                                        "http_method": method.get("http_method", "GET"),
                                        "node_id": node_id,
                                        "direction": "input",
                                        "payload_template": method.get("payload_template"),
                                    })

                        # Scan outputs
                        output_methods = node.get("outputs", {})
                        if isinstance(output_methods, dict):
                            output_methods = output_methods.get("methods", [])
                        for method in output_methods:
                            if isinstance(method, dict) and method.get("method") == "api":
                                conn = method.get("connection", "")
                                if connection_name and conn != connection_name:
                                    continue
                                endpoints.append({
                                    "connection": conn,
                                    "endpoint": method.get("endpoint", ""),
                                    "http_method": method.get("http_method", "POST"),
                                    "node_id": node_id,
                                    "direction": "output",
                                    "payload_template": method.get("payload_template"),
                                })

                return {"endpoints": endpoints}
            except Exception as e:
                return {"error": str(e)}

        @self.mcp.tool("test_api_endpoint")
        def test_api_endpoint_tool(lattice_id: str, connection_name: str, endpoint: str, http_method: str = "GET", payload: dict = None):
            """
            Tests an API endpoint using the APIEngine built from a lattice connection.

            Args:
                lattice_id (str): The lattice identifier.
                connection_name (str): The name of the API connection defined in the lattice.
                endpoint (str): The API endpoint path (e.g. "/posts").
                http_method (str): The HTTP method to use (GET or POST).
                payload (dict | None): Optional JSON payload for POST requests.

            Returns:
                dict: The API response data, status, and timing, or an error.
            """
            import time
            try:
                lattice_data = self.catalyst.get_json(f"oxidizer:lattice:{lattice_id}")
                if not lattice_data:
                    return {"error": f"Lattice '{lattice_id}' not found."}

                connections = lattice_data.get("connections", [])
                conn_config = next((c for c in connections if c.get("name") == connection_name), None)
                if not conn_config:
                    return {"error": f"Connection '{connection_name}' not found in lattice '{lattice_id}'."}

                if conn_config.get("type") not in ("api", "rest_api"):
                    return {"error": f"Connection '{connection_name}' is not an API connection (type: {conn_config.get('type')})."}

                engine = APIEngine(connection_details=conn_config)

                start = time.time()
                if http_method.upper() == "GET":
                    data = engine.get(endpoint)
                elif http_method.upper() == "POST":
                    data = engine.post(endpoint, data=payload)
                else:
                    return {"error": f"Unsupported HTTP method: {http_method}. Only GET and POST are supported."}
                elapsed = round((time.time() - start) * 1000, 2)

                # Truncate large responses
                response_str = json.dumps(data)
                truncated = False
                if len(response_str) > 50000:
                    if isinstance(data, list):
                        data = data[:20]
                        truncated = True
                    elif isinstance(data, dict):
                        truncated = False
                
                row_count = len(data) if isinstance(data, list) else 1

                return {
                    "status": "ok",
                    "http_method": http_method.upper(),
                    "endpoint": endpoint,
                    "duration_ms": elapsed,
                    "row_count": row_count,
                    "truncated": truncated,
                    "data": data,
                }
            except Exception as e:
                return {"error": str(e)}

    def _register_anvil_mcp_resources(self):
        """
        Registers MCP resources for the Anvil engine.
        """
        pass

    def _register_anvil_mcp_prompts(self):
        """
        Registers MCP prompts for the Anvil engine.
        """
        pass

    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """
        Starts the Microscope MCP server.
        
        Args:
            host (str): The hostname to bind the server to.
            port (int): The port to bind the server to.
        """
        self.residue(self.ash.INFO, f"Starting Microscope on {host}:{port}")
        self.mcp.run(
            transport="streamable-http",
            host=host,
            port=port,
            middleware=self.cors_middleware,
        )

