

import uuid
from datetime import datetime, timezone
from threading import Event

from .core.catalyst import Catalyst
from .core.formula import Formula, NodeStatus, TERMINAL_RUN_STATUSES
from .core.residue import Residue
from .utils.phase import ReagentUpdateMessage, ReagentUpdateType, TaskMessage, WorkerTaskType


class Oxidizer:
    """Coordinate Formula runs, worker dispatch, and Reagent status updates.

    Catalyst carries invocation, task, and update messages. Formula is the
    durable source of DAG and run state. Each :meth:`oxidize` tick processes
    invocations first, applies worker updates second, then dispatches nodes
    whose dependencies are complete. :meth:`oxidize` repeats those ticks until
    stopped; :meth:`oxidize_once` runs a single tick. Reagent owns data fetching and output
    routing; this controller owns durable lifecycle status and scheduling.
    """

    def __init__(self, formula: Formula, catalyst: Catalyst, residue: Residue | None = None, sinks: list | tuple | None = None):
        """Bind dependencies and initialize controller stream resources.

        Args:
            formula: Formula facade used for workflow and run-state storage.
            catalyst: Catalyst facade used for invocation, task, and update streams.
            residue: Optional logger shared with the controller dependencies.
            sinks: Optional sinks attached to the shared logger.
        """
        self.residue = (residue or Residue()).bind(component_name="oxidizer")
        self.formula = formula
        self.catalyst = catalyst
        self.oxidizer_consumer_group = 'oxidizer-consumer-group'
        self.oxidizer_invocation_stream_prefix = 'oxidizer:invocations'
        self.oxidizer_worker_stream = 'oxidizer:reagent'
        self.oxidizer_controller_stream = 'oxidizer:controller'
        self._stop_requested = Event()

        self.__add_residue_sinks(sinks)
        self.__bind_residue_component(self.formula, "formula")
        self.__bind_residue_component(self.catalyst, "catalyst")
        # Prepare invocation messages for the controller consumer group.
        self.catalyst.create_consumer_group(
            self.oxidizer_invocation_stream_prefix,
            self.oxidizer_consumer_group,
        )
        # Ensure workers have a task stream to consume.
        self.catalyst.create_stream(self.oxidizer_worker_stream)
        # Prepare worker updates for the controller consumer group.
        self.catalyst.create_consumer_group(
            self.oxidizer_controller_stream,
            self.oxidizer_consumer_group,
        )

    def __add_residue_sinks(self, sinks: list | tuple | None) -> None:
        """Attach explicitly configured sinks to the shared Residue logger.

        Args:
            sinks: Sink objects to attach, or ``None`` when no extra sinks
                were supplied.
        """
        if sinks is None:
            return
        for sink in sinks:
            self.residue.add_sink(sink)

    def __bind_residue_component(self, component, component_name: str) -> None:
        """Bind a component and its backend to a component-scoped logger.

        Args:
            component: Formula or Catalyst facade to rebind.
            component_name: Backend attribute to rebind when present.
        """
        # Bind both the public wrapper and its backend so their events share
        # this controller's context and configured sinks.
        component_residue = self.residue.bind(component_name=component_name)
        component.residue = component_residue
        backend = getattr(component, component_name, None)
        if backend is not None:
            backend.residue = component_residue

    def __extract_formula_connection_names(self, workflow_config):
        """Return unique connection references in first-use order.

        Connection references may occur at different depths in workflow
        configuration, so inspect nested mappings and lists recursively.

        Args:
            workflow_config: Normalized workflow mapping to inspect.

        Returns:
            Connection names in first-use order, with duplicates removed.
        """
        connection_names = []

        def collect(value):
            if isinstance(value, dict):
                connection_name = value.get("connection")
                if isinstance(connection_name, str) and connection_name not in connection_names:
                    connection_names.append(connection_name)
                for child in value.values():
                    collect(child)
            elif isinstance(value, list):
                for child in value:
                    collect(child)

        collect(workflow_config)
        return connection_names

    def __state_for_storage(self, formula_state):
        state = dict(formula_state)
        legacy_connections = state.pop("formula_connections", {})
        names = state.get("connection_names")
        if names is None:
            names = list(legacy_connections) or self.__extract_formula_connection_names(state.get("dag", {}))
        if not isinstance(names, list) or any(not isinstance(name, str) or not name for name in names):
            raise ValueError("State connection_names must be a list of non-empty strings")
        state["connection_names"] = list(dict.fromkeys(names))
        return state

    def __load_formula_connections(self, connection_names):
        connections = {}
        for name in connection_names:
            connection = self.formula.load_formula_connection(name)
            if not isinstance(connection, dict) or not connection:
                raise ValueError(f"Connection not found: {name}")
            connections[name] = connection
        return connections

    def __resolve_output_schemas(self, formula_dag):
        """Attach run-pinned schema documents to table output methods."""
        table_methods = {"s3table", "s3tables", "s3_table", "s3_tables", "tables"}
        schema_documents = {}
        for node in formula_dag.get("nodes", {}).values():
            outputs = node.get("outputs", {})
            methods = outputs.get("methods", []) if isinstance(outputs, dict) else outputs
            for method in methods or []:
                if not isinstance(method, dict):
                    continue
                method_type = str(method.get("method", method.get("type", ""))).lower()
                if method_type not in table_methods:
                    continue
                schema_name = method.get("schema")
                if not isinstance(schema_name, str) or not schema_name:
                    raise ValueError("Every table output requires a named schema")
                if schema_name not in schema_documents:
                    schema_document = self.formula.load_formula_schema(schema_name)
                    if not isinstance(schema_document, dict) or schema_document.get("name") != schema_name:
                        raise ValueError(f"Schema not found or name mismatch: {schema_name}")
                    schema_documents[schema_name] = schema_document
                method["schema_document"] = schema_documents[schema_name]

    def __prepare_formula_invocation(self, invocation_id, invocation_data):
        """Create initial durable state for one valid invocation.

        Invocation messages may be wrapped in Catalyst's ``message`` field.
        Configs may place layers under ``workflow`` or define them at the top
        level; normalize that shape before loading connections and building
        the DAG. A missing run ID gets a new UUID.

        Args:
            invocation_id: Catalyst stream entry ID for run correlation.
            invocation_data: Decoded invocation payload, optionally wrapped in
                Catalyst's ``message`` field.

        Returns:
            Prepared config, connections, DAG, and initial state, or ``None``
            if the payload has no ``formula_id``.
        """
        invocation_payload = invocation_data.get("message", invocation_data) if isinstance(invocation_data, dict) else {}
        if invocation_payload.get("type") == "shutdown":
            # Only the controller that reads this message stops; others in the group keep running.
            self.residue.emit(
                self.residue.ash.INFO,
                "OXIDIZER: Received shutdown invocation",
                invocation_id=invocation_id,
            )
            self.stop()
            return None
        formula_id = invocation_payload.get("formula_id")
        if not formula_id:
            self.residue.emit(
                self.residue.ash.WARNING,
                "OXIDIZER: Invocation missing formula_id",
                invocation_id=invocation_id,
            )
            return None

        run_id = invocation_payload.get("run_id") or str(uuid.uuid4())
        self.residue.emit(
            self.residue.ash.DEBUG,
            "OXIDIZER: Preparing invocation",
            invocation_id=invocation_id,
            formula_id=formula_id,
            run_id=run_id,
        )
        formula_config = self.formula.load_formula_config(formula_id)
        workflow_config = formula_config.get("workflow", formula_config)
        connection_names = self.__extract_formula_connection_names(workflow_config)
        formula_connections = self.__load_formula_connections(connection_names)
        formula_dag = self.formula.dag(formula_id, workflow_config)
        self.__resolve_output_schemas(formula_dag)
        initial_state = {
            "status": "created",
            "started_at": None,
            "completed_at": None,
            "invocation_id": invocation_id,
            "formula_id": formula_id,
            "run_id": run_id,
            "invocation": invocation_payload,
            "connection_names": connection_names,
            "dag": self.__serializable_dag(formula_dag),
        }
        self.formula.create_formula_state(formula_id=formula_id, state=initial_state, run_id=run_id)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "OXIDIZER: Prepared invocation",
            invocation_id=invocation_id,
            formula_id=formula_id,
            run_id=run_id,
            node_count=len(formula_dag.get("nodes", {})),
        )
        return {
            "invocation_id": invocation_id,
            "invocation": invocation_payload,
            "formula_id": formula_id,
            "run_id": run_id,
            "formula_config": formula_config,
            "formula_connections": formula_connections,
            "formula_dag": formula_dag,
            "formula_state": initial_state,
        }

    def __serializable_dag(self, formula_dag):
        """Convert enum node statuses to strings for durable state storage.

        Args:
            formula_dag: DAG mapping returned by ``Formula.dag``.

        Returns:
            A shallow copy with status values converted to serializable strings.
        """
        return {
            **formula_dag,
            "status": {
                node_id: status.value if hasattr(status, "value") else status
                for node_id, status in formula_dag.get("status", {}).items()
            },
        }

    def __node_dependencies_met(self, node_id, formula_state) -> bool:
        """Check whether all parents are complete for downstream scheduling.

        Args:
            node_id: Fully qualified node ID whose dependencies are checked.
            formula_state: Run state containing DAG edges and node statuses.

        Returns:
            ``True`` when every parent is successful, live, or skipped. Root
            nodes with no parents are ready.
        """
        dag = formula_state.get("dag", {})
        statuses = dag.get("status", {})
        parents = dag.get("reverse_edges", {}).get(node_id, [])
        complete_statuses = {
            NodeStatus.SUCCESS.value,
            NodeStatus.LIVE.value,
            NodeStatus.SKIPPED.value,
        }
        return all(statuses.get(parent) in complete_statuses for parent in parents)

    def __ready_node_ids(self, formula_state):
        """Select eligible nodes whose dependencies are all complete.

        Args:
            formula_state: Current run state containing node statuses and DAG
                dependencies.

        Returns:
            IDs of scheduled, pending, or ready nodes whose parents completed.
        """
        statuses = formula_state.get("dag", {}).get("status", {})
        ready_statuses = {
            NodeStatus.SCHEDULED.value,
            NodeStatus.PENDING.value,
            NodeStatus.READY.value,
        }
        return [
            node_id
            for node_id, status in statuses.items()
            if status in ready_statuses and self.__node_dependencies_met(node_id, formula_state)
        ]

    def __task_message_for_node(self, formula_id, run_id, node_id, formula_state):
        """Build the worker payload from durable node state and run context.

        Args:
            formula_id: Workflow identifier for this run.
            run_id: Invocation identifier for the current execution.
            node_id: Fully qualified ID of the node to dispatch.
            formula_state: Current run state containing node, connection names,
                and checkpoint metadata.

        Returns:
            A ``TaskMessage`` with the node definition and resume context for
            Reagent.
        """
        node = formula_state["dag"]["nodes"][node_id]
        return TaskMessage(
            type=WorkerTaskType.START_NODE.value,
            formula_id=formula_id,
            run_id=run_id,
            node_id=node_id,
            layer_id=node.get("layer_id"),
            node=node,
            connections=self.__load_formula_connections(formula_state["connection_names"]),
            checkpoint_metadata=node.get("checkpoint_metadata"),
        )


    def oxidize_invocations(self, consumer_name: str = "oxidizer", count: int = 1, block: int = 1000, invocation_stream: str | None = None):
        """Read invocations, persist initial state, then acknowledge valid ones.

        Args:
            consumer_name: Catalyst consumer identity for this controller.
            count: Maximum number of invocation messages to read.
            block: Maximum blocking time in milliseconds for the stream read.
            invocation_stream: Optional stream override; defaults to the
                configured invocation stream.

        Returns:
            Prepared records for messages containing a ``formula_id``.
            Preparation and acknowledgement errors are logged and re-raised.
            Failed preparations and ignored messages remain unacknowledged.
        """
        topic_name = invocation_stream or self.oxidizer_invocation_stream_prefix
        try:
            invocations = self.catalyst.read_from_stream(
                topic_name,
                self.oxidizer_consumer_group,
                consumer_name,
                count=count,
                block=block,
            )
            self.residue.emit(
                self.residue.ash.DEBUG,
                "OXIDIZER: Read invocations",
                topic_name=topic_name,
                group_name=self.oxidizer_consumer_group,
                consumer_name=consumer_name,
                invocation_count=len(invocations),
            )
            prepared_invocations = []
            for invocation_id, invocation_data in invocations:
                prepared_invocation = self.__prepare_formula_invocation(invocation_id, invocation_data)
                if prepared_invocation is not None:
                    self.catalyst.ack_message(
                        topic_name,
                        self.oxidizer_consumer_group,
                        invocation_id,
                        consumer_name=consumer_name,
                    )
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "OXIDIZER: Acknowledged invocation",
                        formula_id=prepared_invocation["formula_id"],
                        run_id=prepared_invocation["run_id"],
                        invocation_id=invocation_id,
                        topic_name=topic_name,
                        group_name=self.oxidizer_consumer_group,
                        consumer_name=consumer_name,
                    )
                    prepared_invocations.append(prepared_invocation)
            return prepared_invocations
        except Exception as exc:
            self.residue.emit(
                self.residue.ash.ERROR,
                "OXIDIZER: Failed to read invocations",
                topic_name=topic_name,
                group_name=self.oxidizer_consumer_group,
                consumer_name=consumer_name,
                error=str(exc),
            )
            raise

    def oxidize_reagent_updates(self, consumer_name: str = "oxidizer", count: int = 1, block: int = 1000):
        """Process a batch of worker updates from the controller stream.

        Args:
            consumer_name: Catalyst consumer identity for this controller.
            count: Maximum number of update messages to read.
            block: Maximum blocking time in milliseconds for the stream read.

        Returns:
            Serialized updates whose Formula state writes succeeded. Invalid
            updates remain unacknowledged for inspection.
        """
        return self.__oxidize_reagent_updates(consumer_name=consumer_name, count=count, block=block)

    def __update_formula_state_from_reagent_update(self, update_message: ReagentUpdateMessage):
        """Apply one worker lifecycle update to its node and run state.

        ``started`` marks the node running. A checkpoint persists metadata and
        makes the node successful only when ``is_final`` is true; otherwise it
        becomes ready for another batch. ``failed`` stores error details, and
        ``heartbeat`` updates liveness information. The run status is derived
        from the resulting node-status mapping before the state is written.

        Args:
            update_message: Parsed started, checkpoint, failure, or heartbeat
                update from Reagent.

        Returns:
            Updated run state after it has been persisted through Formula.
        """
        formula_state = self.__state_for_storage(
            self.formula.load_formula_state(update_message.formula_id, run_id=update_message.run_id)
        )
        if formula_state.get("status") in TERMINAL_RUN_STATUSES:
            self.formula.sync_active_formula_run(update_message.formula_id, update_message.run_id, formula_state)
            return formula_state
        observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        formula_state.setdefault("started_at", None)
        formula_state.setdefault("completed_at", None)
        if formula_state["started_at"] is None:
            formula_state["started_at"] = observed_at
        node_status = formula_state["dag"].setdefault("status", {})
        node = formula_state["dag"].setdefault("nodes", {}).setdefault(update_message.node_id, {})

        if update_message.type == ReagentUpdateType.STARTED.value:
            node_status[update_message.node_id] = NodeStatus.RUNNING.value
        elif update_message.type == ReagentUpdateType.CHECKPOINT.value:
            if update_message.checkpoint_metadata is not None:
                node["checkpoint_metadata"] = update_message.checkpoint_metadata
                checkpoint_final = update_message.checkpoint_metadata.get("is_final", False)
                node_status[update_message.node_id] = (
                    NodeStatus.SUCCESS.value if checkpoint_final else NodeStatus.READY.value
                )
        elif update_message.type == ReagentUpdateType.FAILED.value:
            node_status[update_message.node_id] = NodeStatus.FAILED.value
            if update_message.error_details is not None:
                node["error_details"] = update_message.error_details
        elif update_message.type == ReagentUpdateType.HEARTBEAT.value:
            node["heartbeat"] = update_message.heartbeat or {}
        else:
            raise ValueError(f"Unsupported reagent update type: {update_message.type}")

        formula_state["status"] = self.__formula_run_status(node_status)
        if formula_state["status"] in {NodeStatus.SUCCESS.value, NodeStatus.FAILED.value}:
            if formula_state["completed_at"] is None:
                formula_state["completed_at"] = observed_at
        self.formula.update_formula_state(update_message.formula_id, formula_state, run_id=update_message.run_id)
        return formula_state

    def __formula_run_status(self, node_status: dict[str, str]) -> str:
        """Reduce node statuses to one aggregate run status.

        Precedence is: any failure; all nodes successful/skipped; any running
        or live node; any dispatched node; any scheduled/pending/ready node;
        otherwise created. This prevents a completed node from making a run
        successful while another node is still active or waiting.

        Args:
            node_status: Mapping from node ID to its status string.

        Returns:
            ``failed``, ``success``, ``running``, ``dispatched``, ``pending``,
            or ``created``.
        """
        statuses = set(node_status.values())
        if NodeStatus.FAILED.value in statuses:
            return NodeStatus.FAILED.value
        if statuses and statuses.issubset({NodeStatus.SUCCESS.value, NodeStatus.SKIPPED.value}):
            return NodeStatus.SUCCESS.value
        if NodeStatus.RUNNING.value in statuses or NodeStatus.LIVE.value in statuses:
            return NodeStatus.RUNNING.value
        if NodeStatus.DISPATCHED.value in statuses:
            return NodeStatus.DISPATCHED.value
        if statuses.intersection({NodeStatus.SCHEDULED.value, NodeStatus.PENDING.value, NodeStatus.READY.value}):
            return NodeStatus.PENDING.value
        return "created"

    def __oxidize_reagent_updates(self, consumer_name: str = "oxidizer", count: int = 1, block: int = 1000):
        """Persist updates and acknowledge only successfully applied messages.

        Each stream entry is decoded and applied independently. Invalid
        payloads are logged and left pending; valid entries are acknowledged
        only after Formula state persistence succeeds, allowing failed writes
        to be retried by the consumer group.

        Args:
            consumer_name: Catalyst consumer identity for this controller.
            count: Maximum number of controller updates to read.
            block: Maximum blocking time in milliseconds for the stream read.

        Returns:
            Serialized updates whose Formula state persistence succeeded.
            Invalid updates are logged and left pending.
        """
        updates = self.catalyst.read_from_stream(
            self.oxidizer_controller_stream,
            self.oxidizer_consumer_group,
            consumer_name,
            count=count,
            block=block,
        )
        processed_updates = []
        for update_id, update_data in updates:
            update_payload = update_data.get("message", update_data) if isinstance(update_data, dict) else {}
            try:
                update_message = ReagentUpdateMessage.from_dict(update_payload)
                formula_state = self.__update_formula_state_from_reagent_update(update_message)
            except Exception as exc:
                self.residue.emit(
                    self.residue.ash.ERROR,
                    "OXIDIZER: Invalid reagent update",
                    update_id=update_id,
                    update=update_payload,
                    error=str(exc),
                )
                continue

            self.residue.emit(
                self.residue.ash.DEBUG,
                "OXIDIZER: Processed reagent update",
                formula_id=update_message.formula_id,
                run_id=update_message.run_id,
                node_id=update_message.node_id,
                update_id=update_id,
                update_type=update_message.type,
                node_status=formula_state.get("dag", {}).get("status", {}).get(update_message.node_id),
                run_status=formula_state.get("status"),
            )
            # Acknowledge only after the Formula state write succeeds.
            self.catalyst.ack_message(
                self.oxidizer_controller_stream,
                self.oxidizer_consumer_group,
                update_id,
                consumer_name=consumer_name,
            )
            processed_updates.append(update_message.to_dict())

        self.residue.emit(
            self.residue.ash.DEBUG,
            "OXIDIZER: Processed reagent updates",
            processed_count=len(processed_updates),
            controller_stream=self.oxidizer_controller_stream,
        )
        return processed_updates

    def __oxidize_formula_run(self, formula_id: str, run_id: str):
        """Dispatch all currently eligible nodes for one Formula run.

        Eligibility requires a scheduled, pending, or ready status and
        completed parents. A task is published for each eligible node, its
        status is changed to ``dispatched``, and the run state is written once
        after dispatch. When no node is eligible, state is not rewritten.

        Args:
            formula_id: Workflow identifier to load.
            run_id: Run identifier whose ready nodes should be dispatched.

        Returns:
            IDs of nodes dispatched during this call.
        """
        formula_state = self.__state_for_storage(self.formula.load_formula_state(formula_id, run_id=run_id))
        if not formula_state or formula_state.get("status") in TERMINAL_RUN_STATUSES:
            self.formula.sync_active_formula_run(formula_id, run_id, formula_state)
            return []
        ready_node_ids = self.__ready_node_ids(formula_state)

        for node_id in ready_node_ids:
            task_message = self.__task_message_for_node(formula_id, run_id, node_id, formula_state)
            self.catalyst.write_to_stream(self.oxidizer_worker_stream, task_message.to_dict())
            formula_state["dag"]["status"][node_id] = NodeStatus.DISPATCHED.value

        if ready_node_ids:
            formula_state["status"] = "dispatched"
            self.formula.update_formula_state(formula_id, formula_state, run_id=run_id)

        self.residue.emit(
            self.residue.ash.DEBUG,
            "OXIDIZER: Dispatched ready nodes",
            formula_id=formula_id,
            run_id=run_id,
            dispatched_count=len(ready_node_ids),
            node_ids=ready_node_ids,
            worker_stream=self.oxidizer_worker_stream,
        )
        return ready_node_ids

    def oxidize_formulas(self):
        """Scan known Formula runs and dispatch their ready nodes.

        Runs are considered independently; an empty dispatch list is recorded
        for a run with no currently eligible nodes.

        Returns:
            Mapping from ``(formula_id, run_id)`` pairs to the list of node IDs
            dispatched for each run.
        """
        dispatched_runs = {}
        for formula_id, run_id in self.formula.list_active_formula_runs():
            dispatched_runs[(formula_id, run_id)] = self.__oxidize_formula_run(formula_id, run_id)
        return dispatched_runs

    def oxidize_once(self, count: int = 1, block: int = 1000):
        """Run one controller tick in invocation, update, dispatch order.

        Updates received during this tick are persisted before readiness is
        evaluated, so a checkpoint can make its node eligible for dispatch in
        the same tick. A worker update arriving after the update read waits for
        the next tick.

        Args:
            count: Maximum messages to read from each stream during this tick.
            block: Maximum blocking time in milliseconds for stream reads.

        Returns:
            Mapping with prepared ``invocations``, persisted
            ``reagent_updates``, and nodes grouped by run under ``dispatched``.
        """
        return {
            "invocations": self.oxidize_invocations(count=count, block=block),
            "reagent_updates": self.oxidize_reagent_updates(count=count, block=block),
            "dispatched": self.oxidize_formulas(),
        }

    def stop(self) -> None:
        """Ask :meth:`oxidize` to exit after the current tick."""
        self._stop_requested.set()

    def oxidize(self, count: int = 1, block: int = 1000, error_backoff_seconds: float = 1.0) -> None:
        """Run controller ticks until stopped, a shutdown invocation, or Ctrl+C.

        Stream reads block for up to ``block`` milliseconds, which paces idle
        ticks. A failed tick is logged and retried after
        ``error_backoff_seconds`` so transient Redis or storage errors do not
        stop the controller; unacknowledged updates are re-read later.

        Args:
            count: Maximum messages to read from each stream per tick.
            block: Maximum blocking time in milliseconds for stream reads.
            error_backoff_seconds: Delay before retrying after a failed tick.
        """
        self.residue.emit(self.residue.ash.INFO, "OXIDIZER: Controller started")
        try:
            while not self._stop_requested.is_set():
                try:
                    self.oxidize_once(count=count, block=block)
                except Exception as exc:
                    self.residue.emit(
                        self.residue.ash.ERROR,
                        "OXIDIZER: Controller tick failed",
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                    self._stop_requested.wait(error_backoff_seconds)
        except KeyboardInterrupt:
            pass
        self.residue.emit(self.residue.ash.INFO, "OXIDIZER: Controller stopped")

    