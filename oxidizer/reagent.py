import time
import traceback
from collections.abc import Callable, Mapping
from functools import wraps
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any

from oxidizer.core.catalyst import Catalyst
from oxidizer.core.lattice import (
    IcebergRestConnection,
    KafkaConnection,
    RedisConnection,
    S3Connection,
    S3TablesConnection,
)
from oxidizer.core.residue import Residue
from oxidizer.utils.anvil import Anvil
from oxidizer.utils.crucible import Crucible
from oxidizer.utils.phase import InputFetchResult, ReagentUpdateMessage, ReagentUpdateType, TaskMessage


_WORKER_MEMORY_PATHS = (
    Path("/sys/fs/cgroup/memory.current"),
    Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
)


def _read_worker_memory_bytes() -> int | None:
    for path in _WORKER_MEMORY_PATHS:
        try:
            return int(path.read_text(encoding="ascii").strip())
        except (OSError, ValueError):
            continue
    return None


class _PhaseMemorySampler:
    _SAMPLE_INTERVAL_SECONDS = 0.25

    def __init__(self, read_memory: Callable[[], int | None] | None = None):
        self._read_memory = read_memory or _read_worker_memory_bytes
        self._lock = Lock()
        self._stop_event = Event()
        self._phase: str | None = None
        self._peaks: dict[str, int] = {}
        self._thread: Thread | None = None

    @property
    def peaks(self) -> dict[str, int]:
        with self._lock:
            return dict(self._peaks)

    def _sample_locked(self) -> None:
        if self._phase is None:
            return
        try:
            reading = self._read_memory()
        except (OSError, ValueError):
            return
        if reading is not None and reading >= 0:
            self._peaks[self._phase] = max(self._peaks.get(self._phase, 0), reading)

    def sample_once(self) -> None:
        with self._lock:
            self._sample_locked()

    def begin_phase(self, phase: str) -> None:
        with self._lock:
            if self._phase is not None:
                raise RuntimeError("Cannot begin a memory phase before ending the active phase")
            self._phase = phase
            self._sample_locked()

    def end_phase(self, phase: str) -> None:
        with self._lock:
            if self._phase != phase:
                raise RuntimeError(f"Memory phase {phase!r} is not active")
            self._sample_locked()
            self._phase = None

    def end_active_phase(self) -> None:
        with self._lock:
            self._sample_locked()
            self._phase = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self._sample_loop, name="worker-memory-sampler", daemon=True)
        self._thread.start()

    def _sample_loop(self) -> None:
        while not self._stop_event.wait(self._SAMPLE_INTERVAL_SECONDS):
            self.sample_once()

    def stop(self) -> None:
        self.end_active_phase()
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()


class DataHandler:
    """Resolve configured input/output methods and execute their data access.

    This class supports API, Redis/Kafka stream, and S3 Tables methods. It
    returns input acknowledgement metadata to Reagent rather than committing
    source messages itself, so the worker can acknowledge only after outputs
    have been handled successfully.
    """

    def __init__(self, catalyst: Any = None, residue: Residue | None = None):
        """Initialize the default stream client and component-scoped logger.

        Args:
            catalyst: Optional Catalyst or Redis/Kafka-compatible client used
                when a method does not supply its own connection.
            residue: Optional shared logger; a ``data_handler`` child is bound.
        """
        self.catalyst = catalyst
        self.residue = (residue or Residue()).bind(component_name="data_handler")

    @staticmethod
    def _normalize_method_config(method: Any) -> dict[str, Any]:
        """Convert a method declaration into the mapping used by dispatch.

        Args:
            method: Mapping, method-name string, or object exposing
                ``to_dict()``/``__dict__``.

        Returns:
            A shallow dictionary of method options. Strings become a mapping
            containing ``method``.

        Raises:
            TypeError: If the value cannot be represented as a method mapping.
        """
        if isinstance(method, Mapping):
            return dict(method)
        if isinstance(method, str):
            return {"method": method}
        to_dict = getattr(method, "to_dict", None)
        if callable(to_dict):
            details = to_dict()
            if isinstance(details, Mapping):
                return dict(details)
        if hasattr(method, "__dict__"):
            return vars(method)
        raise TypeError("method must be a string, mapping, or object with to_dict()")

    @staticmethod
    def _get_value_at_path(data: Any, path: str | None) -> Any:
        """Select a nested mapping/list value using a simple dotted path.

        A leading ``$`` and dot are ignored. Mapping keys and numeric list
        indices are supported; a missing key, out-of-range index, or scalar
        encountered before the end returns ``None``.

        Args:
            data: Response object to traverse.
            path: Dotted selector, such as ``data.items.0``; ``None`` or an
                empty path returns ``data`` unchanged.

        Returns:
            The selected value, or ``None`` when traversal cannot continue.
        """
        if not path:
            return data
        value = data
        for part in path.lstrip("$").lstrip(".").split("."):
            if isinstance(value, Mapping):
                value = value.get(part)
            elif isinstance(value, list) and part.isdigit():
                index = int(part)
                value = value[index] if index < len(value) else None
            else:
                return None
        return value

    def _catalyst(self, connection: Any = None, *, incoming: bool = False) -> Any:
        """Resolve a stream connection to a Catalyst interface.

        Mapping configs are converted to Redis/Kafka connection objects;
        incoming Kafka access creates a consumer, while outgoing access creates
        producer/admin clients. Existing Catalyst-like objects are returned
        unchanged after interface validation.

        Args:
            connection: Optional per-method connection; defaults to the
                instance-level Catalyst.
            incoming: Whether the resolved Kafka connection will consume data.

        Returns:
            A Catalyst wrapper or a compatible injected client.

        Raises:
            ValueError: If no connection is supplied or its provider is unknown.
            TypeError: If an injected client lacks required stream operations.
        """
        candidate = connection if connection is not None else self.catalyst
        if candidate is None:
            raise ValueError("A Catalyst or Redis/Kafka connection is required")
        if isinstance(candidate, Mapping):
            settings = _normalize_connection_config(candidate)
            connection_type = str(settings.pop("type", settings.pop("provider", ""))).lower()
            settings.pop("name", None)
            if connection_type == "redis":
                candidate = RedisConnection(**settings)
            elif connection_type in ("kafka", "redpanda", "msk", "confluent"):
                settings.setdefault("create_admin_client", not incoming)
                settings.setdefault("create_producer", not incoming)
                settings.setdefault("create_consumer", incoming)
                candidate = KafkaConnection(provider=connection_type, **settings)
            else:
                raise ValueError(f"Unsupported stream connection type: {connection_type}")
        if isinstance(candidate, (RedisConnection, KafkaConnection)):
            return Catalyst(candidate, self.residue)
        required = ("read_from_stream", "write_to_stream", "create_stream")
        if not all(hasattr(candidate, name) for name in required):
            raise TypeError("connection must be a Catalyst or Redis/Kafka connection")
        return candidate

    @staticmethod
    def _task_connection(method: Any, connections: Mapping[str, Any]) -> Any:
        """Look up the connection named by an input or output method.

        Args:
            method: Method config containing an optional ``connection`` name.
            connections: Connection configs attached to the worker task.

        Returns:
            The named connection, or ``None`` when the method is self-contained.

        Raises:
            ValueError: If the method names a connection absent from the task.
        """
        details = DataHandler._normalize_method_config(method)
        if str(details.get("method", details.get("type", ""))).lower() == "sql":
            bindings = details.get("catalogs")
            if "connection" in details or "catalog" in details or not isinstance(bindings, list) or not bindings:
                raise ValueError("SQL input requires a non-empty catalogs list; put connection and catalog inside each entry")
            resolved = {}
            for binding in bindings:
                if not isinstance(binding, Mapping):
                    raise ValueError("Each SQL catalog requires connection and catalog names")
                name, catalog = binding.get("connection"), binding.get("catalog")
                if not isinstance(name, str) or not name or not isinstance(catalog, str) or not catalog:
                    raise ValueError("Each SQL catalog requires connection and catalog names")
                if catalog in resolved:
                    raise ValueError(f"Duplicate SQL catalog: {catalog}")
                if name not in connections:
                    raise ValueError(f"Connection not found: {name}")
                resolved[catalog] = connections[name]
            return resolved
        connection_name = details.get("connection")
        if not connection_name:
            return None
        if connection_name not in connections:
            raise ValueError(f"Connection not found: {connection_name}")
        return connections[connection_name]

    @staticmethod
    def _api_trigger_attribute(trigger: Any, request_method: str) -> str:
        if not isinstance(trigger, Mapping) or trigger.get("type") != "stream":
            raise ValueError("API input_trigger requires type: stream")
        attribute = trigger.get("attribute")
        if attribute not in ("endpoint", "payload"):
            raise ValueError("API input_trigger attribute must be endpoint or payload")
        if attribute == "payload" and request_method != "POST":
            raise ValueError("API payload triggers require http_method: POST")
        return attribute

    def _read_trigger(
        self, trigger: Mapping[str, Any], attribute: str, context: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Claim one trigger message; it is acknowledged after its final page succeeds."""
        stream = trigger.get("stream") or context.get("input_ref")
        if not stream:
            raise ValueError("input_trigger requires a stream or input ref")
        group = trigger.get("consumer_group")
        if not group and context.get("lattice_id") and context.get("node_id"):
            group = f"{context['lattice_id']}.{context['node_id']}"
        group = group or "reagent-input-group"
        consumer = trigger.get("consumer_name", "reagent")
        catalyst = self._catalyst(incoming=True)
        catalyst.create_consumer_group(stream, group)
        messages = catalyst.read_from_stream(stream, group, consumer, count=1, block=int(trigger.get("block", 1000)))
        if not messages:
            return None
        message_id, fields = messages[0]
        record = fields.get("message", fields) if isinstance(fields, Mapping) else fields
        value = record.get(attribute, record) if isinstance(record, Mapping) else record
        if attribute == "endpoint" and (not isinstance(value, str) or not value):
            raise ValueError("API endpoint trigger must provide a non-empty string")
        return {"ack": [stream, group, message_id, consumer], "value": value, "next": None}

    def _paged_trigger_state(
        self, trigger: Any, attribute: str, label: str, context: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Resume the trigger saved in the cursor, or claim a new one; ``None`` means the stream is drained."""
        if not isinstance(trigger, Mapping) or trigger.get("type") != "stream":
            raise ValueError(f"{label} input_trigger requires type: stream")
        if trigger.get("attribute", attribute) != attribute:
            raise ValueError(f"{label} input_trigger attribute must be {attribute}")
        cursor = context.get("cursor")
        return cursor if isinstance(cursor, Mapping) else self._read_trigger(trigger, attribute, context)

    @staticmethod
    def _paged_trigger_result(trigger_state: Mapping[str, Any], result: Mapping[str, Any]) -> InputFetchResult:
        """Carry the trigger to the next offset page, or acknowledge it after its last page."""
        if not result["is_final"]:
            return InputFetchResult(
                data=result["rows"], is_final=False, next_cursor={**trigger_state, "next": result["next_cursor"]}
            )
        return InputFetchResult(data=result["rows"], is_final=False, ack_msgs=[tuple(trigger_state["ack"])])

    def incoming_data(
        self,
        method: Any,
        connection: Any = None,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> InputFetchResult:
        """Fetch one batch from the configured API or stream input method.

        API methods can select response data and provide a pagination cursor.
        Stream methods return decoded records and deferred acknowledgement
        tuples; Reagent owns acknowledging them after successful output routing.

        Args:
            method: Input-method config with provider-specific options.
            connection: Optional resolved API, Redis, or Kafka connection.
            context: Per-fetch routing state, including cursor, input reference,
                and node/run identifiers.

        Returns:
            Data, whether the input is final, any deferred source acknowledgements,
            and the next cursor.

        Raises:
            ValueError: If the method/provider is unsupported or required
                endpoint/stream settings are missing.
        """
        details = self._normalize_method_config(method)
        context = context or {}
        method_type = str(details.get("method", details.get("type", ""))).lower()

        if method_type == "sql":
            if "connection" in details or "catalog" in details or not isinstance(details.get("catalogs"), list) or not details["catalogs"]:
                raise ValueError("SQL input requires a non-empty catalogs list; put connection and catalog inside each entry")
            trigger_state = None
            parameters, offset = details.get("parameters"), context.get("cursor") or 0
            if details.get("input_trigger") is not None:
                trigger_state = self._paged_trigger_state(details["input_trigger"], "parameters", "SQL", context)
                if trigger_state is None:
                    return InputFetchResult(data=[], is_final=True)
                parameters, offset = trigger_state["value"], trigger_state.get("next") or 0
                if isinstance(parameters, Mapping):
                    raise ValueError("SQL parameters trigger must provide a list or a single value under 'parameters'")
                if not isinstance(parameters, list):
                    parameters = [parameters]
            result = Crucible.read_sql_batch(
                connection,
                details.get("query"),
                batch_size=details.get("batch_size", 1000),
                offset=offset,
                parameters=parameters,
                residue=self.residue,
            )
            if trigger_state is None:
                return InputFetchResult(data=result["rows"], is_final=result["is_final"], next_cursor=result["next_cursor"])
            return self._paged_trigger_result(trigger_state, result)

        if method_type == "s3":
            trigger = details.get("input_trigger")
            if trigger is None:
                if not details.get("s3_path"):
                    raise ValueError("S3 input requires s3_path or input_trigger")
                trigger_state = None
                paths, offset = details["s3_path"], context.get("cursor") or 0
            else:
                trigger_state = self._paged_trigger_state(trigger, "s3_path", "S3", context)
                if trigger_state is None:
                    return InputFetchResult(data=[], is_final=True)
                paths, offset = trigger_state["value"], trigger_state.get("next") or 0
            result = Crucible.read_s3_batch(
                connection,
                paths,
                file_format=details.get("format"),
                order_by=details.get("order_by"),
                batch_size=details.get("batch_size", 1000),
                offset=offset,
                residue=self.residue,
            )
            if trigger_state is None:
                return InputFetchResult(data=result["rows"], is_final=result["is_final"], next_cursor=result["next_cursor"])
            return self._paged_trigger_result(trigger_state, result)

        if method_type in ("api", "anvil"):
            request_method = str(details.get("http_method", "GET")).upper()
            if request_method not in ("GET", "POST"):
                raise ValueError(f"Unsupported API method: {request_method}")
            paginator = details.get("paginator")
            paginator_parameter = details.get("paginator_parameter")
            if paginator_parameter is not None and (
                not isinstance(paginator_parameter, str) or not paginator_parameter
            ):
                raise ValueError("API paginator_parameter must be a non-empty string")
            if paginator_parameter and not paginator:
                raise ValueError("API paginator_parameter requires paginator")
            if paginator_parameter and request_method != "GET":
                raise ValueError("API paginator_parameter is supported only for GET requests")
            configured_params = details.get("params") or {}
            if not isinstance(configured_params, Mapping):
                raise ValueError("API params must be a mapping")
            params = dict(configured_params)
            endpoint = details.get("endpoint")
            payload = details.get("payload", details.get("payload_template", {}))
            cursor = context.get("cursor")
            trigger = details.get("input_trigger")
            trigger_state = None
            if trigger is not None:
                attribute = self._api_trigger_attribute(trigger, request_method)
                trigger_state = cursor if isinstance(cursor, Mapping) else self._read_trigger(trigger, attribute, context)
                if trigger_state is None:
                    return InputFetchResult(data=[], is_final=True)
                if attribute == "endpoint":
                    endpoint = trigger_state["value"]
                else:
                    payload = trigger_state["value"]
                next_token = trigger_state.get("next")
                if paginator_parameter:
                    if next_token is not None:
                        params[paginator_parameter] = next_token
                else:
                    endpoint = next_token or endpoint
            else:
                if paginator_parameter:
                    if cursor is not None:
                        params[paginator_parameter] = cursor
                else:
                    endpoint = cursor or endpoint
            if not endpoint:
                raise ValueError("API input requires an endpoint or cursor")
            owns_api = not (isinstance(connection, Anvil) or all(
                hasattr(connection, name) for name in ("get", "post")
            ))
            api = Anvil(_normalize_connection_config(connection), residue=self.residue) if owns_api else connection
            try:
                if request_method == "GET":
                    response = api.get(endpoint, params=params or None)
                else:
                    response = api.post(endpoint, data=payload)
            finally:
                if owns_api:
                    api.close()
            next_page = self._get_value_at_path(response, paginator) if paginator else None
            data = self._get_value_at_path(response, details.get("path", details.get("data_selector")))
            self.residue.emit(self.residue.ash.DEBUG, "REAGENT: Fetched API input", endpoint=endpoint)
            if trigger_state is None:
                return InputFetchResult(data=data, is_final=next_page is None, next_cursor=next_page)
            if next_page is not None:
                return InputFetchResult(data=data, is_final=False, next_cursor={**trigger_state, "next": next_page})
            return InputFetchResult(data=data, is_final=False, ack_msgs=[tuple(trigger_state["ack"])])

        if method_type in ("stream", "redis", "kafka"):
            catalyst = self._catalyst(connection, incoming=True)
            stream = details.get("stream", details.get("topic")) or context.get("input_ref")
            if not stream:
                raise ValueError("Stream input requires a stream, topic, or context input_ref")
            group = details.get("consumer_group")
            if not group and context.get("lattice_id") and context.get("node_id"):
                group = f"{context['lattice_id']}.{context['node_id']}"
            group = group or "reagent-input-group"
            consumer = details.get("consumer_name", "reagent")
            count = int(details.get("batch_size", details.get("count", 1)))
            block = int(details.get("block", 1000))
            catalyst.create_consumer_group(stream, group)
            messages = catalyst.read_from_stream(stream, group, consumer, count=count, block=block)
            records = [
                message.get("message", message) if isinstance(message, Mapping) else message
                for _, message in messages
            ]
            ack_msgs = [(stream, group, message_id, consumer) for message_id, _ in messages]
            return InputFetchResult(data=records, is_final=len(records) < count, ack_msgs=ack_msgs)

        raise ValueError(f"Unsupported input method: {method_type}")

    def outgoing_data(
        self,
        method: Any,
        connection: Any = None,
        data: Any = None,
        *,
        context: Mapping[str, Any] | None = None,
        write_metrics: dict[str, int] | None = None,
    ) -> Any:
        """Write handler output through one configured stream or table method.

        Stream methods write each record to Catalyst. S3 Tables methods insert
        records through Crucible using the configured catalog, namespace, and
        table. This method does not publish computed output to the controller.

        Args:
            method: Output-method config with provider-specific options.
            connection: Optional resolved stream/table connection.
            data: Handler result; lists are treated as record batches.
            context: Fallback routing identifiers, including ``node_id`` for
                stream names when no explicit stream is configured.
            write_metrics: Optional collector for table-write metrics.

        Returns:
            Stream message IDs, table insert count, or the adapter's result.

        Raises:
            ValueError: If the method/provider or required output settings are
                unsupported or missing.
        """
        details = self._normalize_method_config(method)
        context = context or {}
        method_type = str(details.get("method", details.get("type", ""))).lower()

        if method_type in ("stream", "redis", "kafka"):
            catalyst = self._catalyst(connection)
            stream = details.get("stream", details.get("topic")) or context.get("node_id")
            if not stream:
                raise ValueError("Stream output requires a stream, topic, or context node_id")
            catalyst.create_stream(stream)
            records = data if isinstance(data, list) else ([] if data is None else [data])
            write_many = getattr(catalyst, "write_many_to_stream", None)
            if callable(write_many):
                return write_many(stream, records)
            return [catalyst.write_to_stream(stream, record) for record in records]

        if method_type in ("s3table", "s3tables", "s3_table", "s3_tables", "tables"):
            owns_crucible = not isinstance(connection, Crucible)
            table_connection = connection
            connection_details = _normalize_connection_config(connection) if isinstance(connection, Mapping) else {}
            table_bucket = details.get(
                "table_bucket",
                details.get("aws_s3_table_bucket", details.get("bucket", connection_details.get("aws_s3_table_bucket"))),
            )
            catalog = details.get("catalog")
            namespace = details.get("namespace", details.get("database"))
            table = details.get("table")
            if not catalog or not table_bucket or not namespace or not table:
                raise ValueError("S3 Tables output requires catalog, namespace, and table_bucket/table")
            schema_name = details.get("schema")
            schema_document = details.get("schema_document")
            if not isinstance(schema_name, str) or not schema_name:
                raise ValueError("Every table output requires a named schema")
            if not isinstance(schema_document, Mapping) or schema_document.get("name") != schema_name:
                raise ValueError(f"Resolved schema document missing or mismatched: {schema_name}")
            if isinstance(table_connection, Mapping):
                connection_details = _normalize_connection_config(table_connection)
                region = connection_details.get("region_name", connection_details.get("aws_region", "us-east-1"))
                endpoint = connection_details.get("endpoint_url")
                if endpoint:
                    storage = S3Connection(
                        bucket=table_bucket,
                        lazy_client=True,
                        endpoint_url=endpoint,
                        access_key_id=connection_details.get("access_key_id"),
                        secret_access_key=connection_details.get("secret_access_key"),
                        region_name=region,
                        addressing_style=connection_details.get("addressing_style", "path"),
                    )
                    table_connection = Crucible(IcebergRestConnection(
                        storage,
                        warehouse=table_bucket,
                        catalog_endpoint=connection_details.get("catalog_endpoint"),
                    ), residue=self.residue)
                else:
                    table_connection = Crucible(S3TablesConnection(
                        region_name=region,
                        aws_profile_name=connection_details.get("aws_profile_name", connection_details.get("aws_sso_profile")),
                        access_key_id=connection_details.get("access_key_id"),
                        secret_access_key=connection_details.get("secret_access_key"),
                        session_token=connection_details.get("session_token"),
                        role_arn=connection_details.get("role_arn"),
                        role_session_name=connection_details.get("role_session_name", "OxidizerS3TablesSession"),
                        external_id=connection_details.get("external_id"),
                    ), residue=self.residue)
            crucible = table_connection if isinstance(table_connection, Crucible) else Crucible(
                table_connection, residue=self.residue
            )
            records = data if isinstance(data, list) else ([] if data is None else [data])
            try:
                result = crucible.write(table_bucket, catalog, namespace, table, records, schema=schema_document)
                if write_metrics is not None:
                    write_metrics["late_arrivals_ignored"] = (
                        write_metrics.get("late_arrivals_ignored", 0)
                        + getattr(crucible.adapter, "last_ignored_late_records", 0)
                    )
                return result
            finally:
                if owns_crucible:
                    crucible.close()

        raise ValueError(f"Unsupported output method: {method_type}")


def _normalize_connection_config(connection: Any) -> dict[str, Any]:
    """Convert a stored connection value into the settings mapping adapters use.

    Stored connections may be nested under ``connection`` or represented by
    objects that provide ``to_dict()``/``__dict__``.

    Args:
        connection: Mapping, connection object, or ``None``.

    Returns:
        Connection settings as a dictionary.

    Raises:
        ValueError: If no connection is available.
        TypeError: If the value cannot be represented as a mapping.
    """
    if isinstance(connection, Mapping):
        details = dict(connection)
        nested = details.get("connection")
        if isinstance(nested, Mapping):
            return dict(nested)
        return details
    if connection is None:
        raise ValueError("An API connection or Anvil instance is required")
    to_dict = getattr(connection, "to_dict", None)
    if callable(to_dict):
        details = to_dict()
        if isinstance(details, Mapping):
            return dict(details)
    if hasattr(connection, "__dict__"):
        return vars(connection)
    raise TypeError("API connection must be a mapping or expose to_dict()")

class Reagent(DataHandler):
    """Execute dispatched node tasks and report checkpoints to Oxidizer.

    Reagent owns input fetching, user-handler invocation, output routing, and
    source/task acknowledgements. It reports execution metadata to the
    controller; computed handler output travels through the node's configured
    output methods instead.
    """

    _ACK_BATCH_SIZE = 1000

    def __init__(self, catalyst: Catalyst, residue: Residue | None = None, auto_react: bool = True):
        """Configure the worker task consumer and controller update stream.

        Args:
            catalyst: Catalyst instance used for task, source, and update
                streams.
            residue: Optional shared logger used by worker operations.
            auto_react: Whether decorating a handler with :meth:`react`
                immediately starts the :meth:`run` loop. Set ``False`` to
                call the wrapped handler manually, one task per call.
        """
        super().__init__(catalyst=catalyst, residue=residue)
        self.residue = (residue or Residue()).bind(component_name="reagent")
        self.auto_react = auto_react
        self.reagent_task_stream = "oxidizer:reagent"
        self.oxidizer_controller_stream = "oxidizer:controller"
        self.reagent_consumer_group = "reagent-consumer-group"
        self.reagent_consumer_name = "reagent"
        self.last_error: dict[str, Any] | None = None
        self._stop_requested = Event()
        self.catalyst.create_consumer_group(self.reagent_task_stream, self.reagent_consumer_group)

    def incoming_oxidizer_task(
        self,
        consumer_name: str | None = None,
        count: int = 1,
        block: int = 1000,
        stream: str | None = None,
    ):
        """Read one task and notify Oxidizer that the node has started.

        Args:
            consumer_name: Consumer identity; defaults to this Reagent's name.
            count: Maximum number of tasks requested from the stream.
            block: Maximum blocking time in milliseconds.
            stream: Optional task-stream override.

        Returns:
            ``(TaskMessage, message_id)`` for the first task, or ``(None, None)``
            when no task is available. A ``started`` update is published before
            returning a received task; the task itself is not acknowledged here.
        """
        consumer_name = consumer_name or self.reagent_consumer_name
        task_stream = stream or self.reagent_task_stream
        tasks = self.catalyst.read_from_stream(
            task_stream,
            self.reagent_consumer_group,
            consumer_name,
            count=count,
            block=block,
        )
        if not tasks:
            return None, None
        task_id, task_data = tasks[0]
        task_payload = task_data.get("message", task_data) if isinstance(task_data, dict) else {}
        task = TaskMessage.from_dict(task_payload)
        self.outgoing_oxidizer_update(ReagentUpdateType.STARTED.value, task)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "REAGENT: Received task",
            formula_id=task.formula_id,
            run_id=task.run_id,
            node_id=task.node_id,
            task_id=task_id,
        )
        return task, task_id

    def _worker_context(self, task: TaskMessage) -> dict[str, Any]:
        """Build the stable execution context exposed to data/context handlers.

        Args:
            task: Dispatched task containing run, node, and layer identifiers.

        Returns:
            Context fields shared by input routing, output routing, and the
            user handler. ``lattice_id`` falls back to ``formula_id`` because
            the current task message has no separate lattice field.
        """
        node = task.node or {}
        return {
            "lattice_id": getattr(task, "lattice_id", task.formula_id),
            "formula_id": task.formula_id,
            "run_id": task.run_id,
            "layer_id": task.layer_id,
            "layer_name": node.get("layer_name"),
            "node_id": task.node_id,
            "node_name": node.get("node_name"),
        }

    def handle_incoming_data(self, task: TaskMessage) -> tuple[dict[str, Any], list[tuple], dict[str, bool], dict[str, Any]]:
        """Fetch the current batch for each unfinished input on a task.

        The first configured method for each input is used. Checkpoint cursors
        resume paginated inputs; ``batch_final`` inputs are not fetched again.
        Source acknowledgements are collected but deliberately deferred until
        output routing succeeds.

        Args:
            task: Worker task with node input config, connections, and checkpoint.

        Returns:
            A tuple of input data by alias, deferred acknowledgement tuples,
            per-input finality, and per-input next cursors.
        """
        node = task.node or {}
        connections = task.connections if isinstance(task.connections, Mapping) else {}
        checkpoint = task.checkpoint_metadata or {}
        cursors = checkpoint.get("batch_cursors") or {}
        finished = checkpoint.get("batch_final") or {}
        input_data: dict[str, Any] = {}
        ack_msgs: list[tuple] = []
        input_final: dict[str, bool] = {}
        input_cursors: dict[str, Any] = {}
        for input_method in node.get("inputs", []):
            methods = input_method.get("methods", [])
            if not methods:
                continue
            method = methods[0]
            details = self._normalize_method_config(method)
            input_ref = input_method.get("ref") or input_method.get("alias") or task.node_id
            input_name = input_method.get("alias") or input_ref
            if finished.get(input_name, False):
                input_data[input_name] = []
                input_final[input_name] = True
                input_cursors[input_name] = None
                continue
            connection = self._task_connection(method, connections)
            result = self.incoming_data(
                method,
                connection,
                context={
                    **self._worker_context(task),
                    "input_ref": input_ref,
                    "cursor": cursors.get(input_name),
                    "batch_index": checkpoint.get("batch_index", 0),
                },
            )
            input_data[input_name] = result.data
            ack_msgs.extend(result.ack_msgs)
            input_final[input_name] = result.is_final
            input_cursors[input_name] = result.next_cursor
            self.residue.emit(
                self.residue.ash.DEBUG,
                "REAGENT: Fetched node input",
                input_name=input_name,
                input_method=details.get("method"),
                is_final=result.is_final,
            )
        return input_data, ack_msgs, input_final, input_cursors

    @staticmethod
    def _update_checkpoint(
        task: TaskMessage,
        input_data: Mapping[str, Any],
        input_final: Mapping[str, bool],
        input_cursors: Mapping[str, Any],
        preprocess_runtime: float = 0.0,
        function_runtime: float = 0.0,
        postprocess_runtime: float = 0.0,
        late_arrivals_ignored: int = 0,
        phase_memory_peaks: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        """Advance batch metadata in place after a worker batch is handled.

        Args:
            task: Task whose checkpoint metadata is updated.
            input_data: Fetched data keyed by input alias; used to count rows.
            input_final: Whether each input has been exhausted.
            input_cursors: Cursor to use when fetching each input again.
            preprocess_runtime: Seconds spent reading the task and inputs.
            function_runtime: Seconds spent in the wrapped handler.
            postprocess_runtime: Seconds spent routing output.
            late_arrivals_ignored: Late records skipped by table outputs in this batch.
            phase_memory_peaks: Maximum worker-cgroup memory readings for this batch's phases.

        Returns:
            Updated checkpoint dictionary, also assigned to ``task``. The
            checkpoint is final when all configured inputs are final.
        """
        checkpoint = dict(task.checkpoint_metadata or {})
        checkpoint.pop("late_arrivals_ignored", None)
        batch_final = {**(checkpoint.get("batch_final") or {}), **input_final}
        batch_cursors = {**(checkpoint.get("batch_cursors") or {}), **input_cursors}
        rows_processed = sum(
            len(value) if isinstance(value, list) else int(value is not None)
            for value in input_data.values()
        )
        checkpoint.update(
            batch_index=int(checkpoint.get("batch_index", 0)) + 1,
            batch_cursors=batch_cursors,
            batch_final=batch_final,
            is_final=all(batch_final.values()),
            records_processed=rows_processed,
            total_records_so_far=int(checkpoint.get("total_records_so_far", 0)) + rows_processed,
            total_late_arrivals_ignored=(
                int(checkpoint.get("total_late_arrivals_ignored", 0)) + late_arrivals_ignored
            ),
            accumulated_preprocess_runtime=float(checkpoint.get("accumulated_preprocess_runtime", 0.0)) + preprocess_runtime,
            accumulated_function_runtime=float(checkpoint.get("accumulated_function_runtime", 0.0)) + function_runtime,
            accumulated_postprocess_runtime=float(checkpoint.get("accumulated_postprocess_runtime", 0.0)) + postprocess_runtime,
        )
        Reagent._apply_phase_memory_peaks(checkpoint, phase_memory_peaks or {})
        task.checkpoint_metadata = checkpoint
        return checkpoint

    @staticmethod
    def _apply_phase_memory_peaks(
        checkpoint: dict[str, Any], phase_memory_peaks: Mapping[str, int]
    ) -> None:
        for phase in ("preprocess", "function", "postprocess"):
            checkpoint.pop(f"accumulated_{phase}_memory", None)
            key = f"peak_{phase}_memory_bytes"
            previous = checkpoint.get(key)
            current = phase_memory_peaks.get(phase)
            if current is not None:
                checkpoint[key] = max(int(previous or 0), int(current))
            elif key not in checkpoint:
                checkpoint[key] = None

    @staticmethod
    def _update_phase_memory_checkpoint(task: TaskMessage, phase_memory_peaks: Mapping[str, int]) -> None:
        checkpoint = dict(task.checkpoint_metadata or {})
        Reagent._apply_phase_memory_peaks(checkpoint, phase_memory_peaks)
        task.checkpoint_metadata = checkpoint

    def handle_outgoing_data(self, task: TaskMessage, data: Any) -> dict[str, int]:
        """Route handler output through every output method on the node.

        Args:
            task: Worker task containing output methods and their connections.
            data: Computed result to send to the configured destinations.
        """
        node = task.node or {}
        outputs = node.get("outputs", {})
        methods = outputs.get("methods", []) if isinstance(outputs, Mapping) else outputs
        connections = task.connections if isinstance(task.connections, Mapping) else {}
        write_metrics = {"late_arrivals_ignored": 0}
        for method in methods or []:
            connection = self._task_connection(method, connections)
            self.outgoing_data(
                method,
                connection,
                data,
                context=self._worker_context(task),
                write_metrics=write_metrics,
            )
        return write_metrics

    def outgoing_oxidizer_update(
        self,
        update_type: str,
        task: TaskMessage,
        checkpoint_metadata: dict | None = None,
        error_details: dict | None = None,
        heartbeat: dict | None = None,
    ):
        """Publish a lifecycle/checkpoint update to the controller stream.

        Args:
            update_type: One of the supported Reagent update types.
            task: Task identifying the formula run and node.
            checkpoint_metadata: Optional checkpoint override; otherwise the
                task's current checkpoint is included.
            error_details: Structured failure details, when reporting failure.
            heartbeat: Heartbeat payload, when reporting worker liveness.

        Returns:
            The serialized update model. Handler output is intentionally not a
            field in this controller message.
        """
        update = ReagentUpdateMessage(
            type=update_type,
            formula_id=task.formula_id,
            run_id=task.run_id,
            node_id=task.node_id,
            checkpoint_metadata=checkpoint_metadata or task.checkpoint_metadata,
            error_details=error_details,
            heartbeat=heartbeat,
        )
        self.catalyst.write_to_stream(self.oxidizer_controller_stream, update.to_dict())
        self.residue.emit(
            self.residue.ash.DEBUG,
            "REAGENT: Sent update",
            update_type=update_type,
            formula_id=task.formula_id,
            run_id=task.run_id,
            node_id=task.node_id,
        )
        return update

    def outgoing_oxidizer_checkpoint(
        self,
        task_msg_id: str,
        task: TaskMessage,
        input_ack_msgs: list[tuple],
        task_stream: str | None = None,
    ):
        """Report a completed batch, then acknowledge its source and task.

        Final and non-final batches use the same ``checkpoint`` update; the
        controller interprets ``checkpoint_metadata.is_final`` to mark the
        node ready or successful. The update is written before acknowledgments.

        Args:
            task_msg_id: Catalyst message ID of the worker task.
            task: Task carrying the updated checkpoint metadata.
            input_ack_msgs: Source-message acknowledgments returned by input
                handlers; these are committed only after output routing.
            task_stream: Optional override for the worker task stream.

        Returns:
            The checkpoint update model sent to Oxidizer.
        """
        update = self.outgoing_oxidizer_update(
            ReagentUpdateType.CHECKPOINT.value,
            task,
            checkpoint_metadata=task.checkpoint_metadata,
        )
        acknowledgements = [
            *input_ack_msgs,
            (
                task_stream or self.reagent_task_stream,
                self.reagent_consumer_group,
                task_msg_id,
                self.reagent_consumer_name,
            ),
        ]
        self._acknowledge_messages(acknowledgements)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "REAGENT: Acknowledged batch",
            formula_id=task.formula_id,
            run_id=task.run_id,
            node_id=task.node_id,
            task_id=task_msg_id,
            source_message_count=len(input_ack_msgs),
        )
        return update

    def _acknowledge_messages(
        self, acknowledgements: list[tuple[str, str, str, str | None]]
    ) -> None:
        grouped: dict[tuple[str, str, str | None], list[str]] = {}
        for topic_name, group_name, message_id, consumer_name in acknowledgements:
            grouped.setdefault((topic_name, group_name, consumer_name), []).append(message_id)

        acknowledge_batch = getattr(self.catalyst, "ack_messages", None)
        for (topic_name, group_name, consumer_name), message_ids in grouped.items():
            for offset in range(0, len(message_ids), self._ACK_BATCH_SIZE):
                batch = message_ids[offset:offset + self._ACK_BATCH_SIZE]
                if callable(acknowledge_batch):
                    acknowledge_batch(
                        topic_name, group_name, batch, consumer_name=consumer_name
                    )
                else:
                    for message_id in batch:
                        self.catalyst.ack_message(
                            topic_name,
                            group_name,
                            message_id,
                            consumer_name=consumer_name,
                        )

    def react(
        self,
        func=None,
        *,
        dedicated_stream: str | None = None,
        handler_mode: str = "data_context",
    ):
        """Decorate a handler with task fetch, output, checkpoint, and ack flow.

        ``data_context`` is the default handler mode and calls the function as
        ``handler(input_data, context, *args, **kwargs)``. ``task`` mode passes
        the raw ``TaskMessage`` instead. Both modes still fetch inputs, route
        the returned output, advance checkpoint metadata, and report a
        checkpoint to Oxidizer.

        User-handler exceptions are captured in ``last_error`` and reported as
        ``failed`` updates; output or infrastructure failures are reported and
        re-raised. Source messages are acknowledged only after output routing.

        Args:
            func: Optional function when used as ``@reagent.react`` directly.
            dedicated_stream: Optional task-stream override for this handler.
            handler_mode: ``data_context`` or ``task``; other values fail when
                the decorator is created.

        Returns:
            A decorator, or the wrapped function when ``func`` is supplied.
        """
        if handler_mode not in {"data_context", "task"}:
            raise ValueError("handler_mode must be 'data_context' or 'task'")

        def decorator(handler):
            @wraps(handler)
            def wrapper(*args, **kwargs):
                self.last_error = None
                base_residue = self.residue
                task_stream = dedicated_stream or self.reagent_task_stream
                memory_sampler = _PhaseMemorySampler()
                memory_sampler.start()
                memory_sampler.begin_phase("preprocess")
                preprocess_started = time.perf_counter()
                task = None
                try:
                    task, task_msg_id = self.incoming_oxidizer_task(stream=task_stream)
                    if task is None:
                        return None
                    self.residue = base_residue.bind(**self._worker_context(task), task_id=task_msg_id)
                    batch_index = (task.checkpoint_metadata or {}).get("batch_index", 0)
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Input fetch started",
                        batch_index=batch_index,
                    )
                    input_data, input_ack_msgs, input_final, input_cursors = self.handle_incoming_data(task)
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Input fetch completed",
                        batch_index=batch_index,
                        input_count=len(input_data),
                    )
                    preprocess_runtime = time.perf_counter() - preprocess_started
                    memory_sampler.end_phase("preprocess")
                    context = {
                        **self._worker_context(task),
                        "batch_index": (task.checkpoint_metadata or {}).get("batch_index", 0),
                    }
                    memory_sampler.begin_phase("function")
                    function_started = time.perf_counter()
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Handler started",
                        batch_index=batch_index,
                        handler_mode=handler_mode,
                    )
                    try:
                        if handler_mode == "task":
                            output = handler(task, *args, **kwargs)
                        else:
                            output = handler(input_data, context, *args, **kwargs)
                    except Exception as exc:
                        memory_sampler.end_phase("function")
                        self._update_phase_memory_checkpoint(task, memory_sampler.peaks)
                        self.last_error = self._report_worker_error(task, exc, context["batch_index"])
                        return None
                    function_runtime = time.perf_counter() - function_started
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Handler completed",
                        batch_index=batch_index,
                    )
                    memory_sampler.end_phase("function")
                    memory_sampler.begin_phase("postprocess")
                    postprocess_started = time.perf_counter()
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Output routing started",
                        batch_index=batch_index,
                    )
                    output_metrics = self.handle_outgoing_data(task, output)
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Output routing completed",
                        batch_index=batch_index,
                    )
                    postprocess_runtime = time.perf_counter() - postprocess_started
                    memory_sampler.end_phase("postprocess")
                    checkpoint = self._update_checkpoint(
                        task,
                        input_data,
                        input_final,
                        input_cursors,
                        preprocess_runtime=preprocess_runtime,
                        function_runtime=function_runtime,
                        postprocess_runtime=postprocess_runtime,
                        late_arrivals_ignored=output_metrics.get("late_arrivals_ignored", 0),
                        phase_memory_peaks=memory_sampler.peaks,
                    )
                    self.residue.emit(
                        self.residue.ash.DEBUG,
                        "REAGENT: Batch checkpoint prepared",
                        batch_index=checkpoint["batch_index"],
                        records_processed=checkpoint["records_processed"],
                        is_final=checkpoint["is_final"],
                    )
                    self.outgoing_oxidizer_checkpoint(
                        task_msg_id,
                        task,
                        input_ack_msgs,
                        task_stream=task_stream,
                    )
                    return output
                except Exception as exc:
                    memory_sampler.stop()
                    if task is None:
                        raise
                    self._update_phase_memory_checkpoint(task, memory_sampler.peaks)
                    self.residue.emit(
                        self.residue.ash.ERROR,
                        "REAGENT: Worker task failed",
                        formula_id=task.formula_id,
                        run_id=task.run_id,
                        node_id=task.node_id,
                        batch_index=(task.checkpoint_metadata or {}).get("batch_index", 0),
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    self.outgoing_oxidizer_update(
                        ReagentUpdateType.FAILED.value,
                        task,
                        error_details={
                            "error_type": type(exc).__name__,
                            "error_message": str(exc),
                            "traceback": traceback.format_exc(),
                        },
                    )
                    raise
                finally:
                    self.residue = base_residue
                    memory_sampler.stop()
            if self.auto_react:
                self.run(wrapper)
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def stop(self) -> None:
        """Ask :meth:`run` to exit after the current task."""
        self._stop_requested.set()

    def run(self, process: Callable[[], Any], error_backoff_seconds: float = 1.0) -> None:
        """Call a ``react``-wrapped handler repeatedly until stopped or Ctrl+C.

        The wrapped handler's task read blocks while idle, which paces the
        loop. Handler errors are already reported by ``react``; infrastructure
        errors are logged and retried after ``error_backoff_seconds`` so one
        bad task or transient outage does not stop the worker.

        Args:
            process: Function returned by :meth:`react`.
            error_backoff_seconds: Delay before retrying after a failed task.
        """
        self.residue.emit(self.residue.ash.INFO, "REAGENT: Worker started")
        try:
            while not self._stop_requested.is_set():
                try:
                    process()
                except Exception as exc:
                    self.residue.emit(
                        self.residue.ash.ERROR,
                        "REAGENT: Worker cycle failed",
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                    self._stop_requested.wait(error_backoff_seconds)
        except KeyboardInterrupt:
            pass
        self.residue.emit(self.residue.ash.INFO, "REAGENT: Worker stopped")

    def _report_worker_error(self, task: TaskMessage, exc: Exception, batch_index: int) -> dict[str, str]:
        """Record a handler exception and best-effort report it to Oxidizer.

        The caller decides whether to return or re-raise the exception. Failure
        reporting is best-effort so a secondary stream write error does not
        replace the original handler error.

        Args:
            task: Task whose execution failed.
            exc: Original exception raised by the user handler.
            batch_index: Zero-based batch index for diagnostics.

        Returns:
            Structured error details, including exception type, message, and
            formatted traceback.
        """
        error_details = {
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
        }
        self.residue.emit(
            self.residue.ash.ERROR,
            "REAGENT: Worker task failed",
            formula_id=task.formula_id,
            run_id=task.run_id,
            node_id=task.node_id,
            batch_index=batch_index,
            **error_details,
        )
        try:
            self.outgoing_oxidizer_update(
                ReagentUpdateType.FAILED.value,
                task,
                error_details=error_details,
            )
        except Exception as reporting_error:
            self.residue.emit(
                self.residue.ash.ERROR,
                "REAGENT: Failed to report worker error to controller",
                formula_id=task.formula_id,
                run_id=task.run_id,
                node_id=task.node_id,
                reporting_error_type=type(reporting_error).__name__,
                reporting_error_message=str(reporting_error),
            )
        return error_details

    