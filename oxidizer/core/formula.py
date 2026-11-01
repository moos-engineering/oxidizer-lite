import json
import yaml 
from enum import Enum
from collections import defaultdict, deque 
from typing import Dict, List

from ..utils.phase import CheckpointMetadata
from .residue import Residue
from .lattice import S3Connection, RedisConnection



class NodeStatus(Enum):
    """Lifecycle labels stored for nodes in a Formula DAG.

    The enum contains both states used by the current controller and labels
    reserved for extended lifecycle operations. The controller currently
    interprets scheduled/pending/ready, dispatched/running, live, success,
    failed, and skipped states; lock, pause, retry, cancellation, and timeout
    transitions require additional orchestration behavior.
    """
    SCHEDULED        = "scheduled"        # time condition satisfied; may also require upstream deps (configurable)
    PENDING          = "pending"          # waiting for upstream deps or external trigger (API, file arrival, etc.)
    READY            = "ready"            # all deps satisfied; eligible to dispatch
    LOCKED           = "locked"           # waiting to acquire a cross-run node lock (FIFO queue per lock key)
    DISPATCHED       = "dispatched"       # task message published to stream; awaiting worker ACK
    RUNNING          = "running"          # worker ACK received; processing — includes mid-batch checkpoint loops
    LIVE             = "live"             # streaming node running its consume loop (runs indefinitely until stopped)
    PAUSED           = "paused"           # live node manually paused; resumes from last durable cursor
    PAUSED_UPSTREAM  = "paused_upstream"  # live node paused because an upstream dep failed; auto-resumes on recovery
    SUCCESS          = "success"          # completed successfully; releases lock and unblocks downstream nodes
    FAILED           = "failed"           # processing failed; retains last checkpoint cursor for resume
    RETRYING         = "retrying"         # backing off before re-entering READY; tracks attempt count
    SKIPPED          = "skipped"          # upstream failed/skipped and this node is configured to skip on failure
    CANCELLED        = "cancelled"        # externally cancelled by operator or API; not the same as SKIPPED
    TIMED_OUT        = "timed_out"        # exceeded deadline; distinct from FAILED for operational alerting



TERMINAL_RUN_STATUSES = frozenset({"success", "failed", "cancelled", "timed_out", "skipped"})


class _RunIndex:
    def list_formula_runs(self):
        """Return all run identifiers, including logically archived runs."""
        separator = "/" if self.backend == "s3" else ":"
        return [tuple(parts) for state_id in self.list_formula_states()
                if len(parts := state_id.split(separator)) == 2 and all(parts)]

    def _sync_active_marker(self, formula_id, run_id, state):
        if not state or state.get("status") in TERMINAL_RUN_STATUSES:
            self._delete_active_marker(formula_id, run_id)
        else:
            self._write_active_marker(formula_id, run_id)

    def reconcile_active_formula_runs(self):
        """Rebuild markers from authoritative states; run explicitly during maintenance."""
        runs = set(self.list_formula_runs())
        for formula_id, run_id in runs:
            self._sync_active_marker(formula_id, run_id, self.load_formula_state(formula_id, run_id))
        for formula_id, run_id in set(self.list_active_formula_runs()) - runs:
            self._delete_active_marker(formula_id, run_id)
        return self.list_active_formula_runs()


class S3Formula(_RunIndex):
    """Persist Formula configuration and run state as YAML objects in S3.

    Objects are rooted at the ``S3Connection`` prefix and organized into
    ``formulas/``, ``connections/``, ``states/``, and ``analyses/``. Run state
    writes replace the complete YAML object; ``path`` is accepted for facade
    parity but does not enable partial S3 updates.
    """

    def __init__(self, connection: S3Connection, residue: Residue):
        """Bind the S3 connection and Formula-scoped logger.

        Args:
            connection: S3 wrapper providing bucket, prefix, and client.
            residue: Shared logger for storage operations.
        """
        self.connection = connection
        self.residue = residue
        self.backend = "s3"

    def _state_key(self, formula_id: str, run_id: str) -> str:
        """Build the run-state object key relative to the connection prefix.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Key shaped as ``states/<formula_id>/<run_id>.yml`` under the prefix.
        """
        return self.connection.object_key(f"states/{formula_id}/{run_id}.yml")

    def list_formula_configs(self):
        """List YAML workflow IDs across all S3 pages under ``formulas/``.

        Returns:
            Formula IDs derived from matching ``.yml`` object keys.
        """
        prefix = self.connection.object_key("formulas/")
        formula_ids = []
        for key in self._list_run_keys(prefix):
            if key.endswith(".yml"):
                formula_ids.append(key.removeprefix(prefix).removesuffix(".yml"))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Listed configs",
            backend=self.backend,
            bucket=self.connection.bucket,
            prefix=prefix,
            config_count=len(formula_ids),
        )
        return formula_ids

    def create_formula_config(self, formula_id: str, config: dict):
        """Serialize and store one workflow config as YAML.

        Args:
            formula_id: ID used in ``formulas/<formula_id>.yml``.
            config: Workflow configuration mapping to serialize.
        """
        key = self.connection.object_key(f"formulas/{formula_id}.yml")
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=key,
            Body=yaml.safe_dump(config).encode("utf-8"),
            ContentType="application/x-yaml",
        )
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created config",
            backend=self.backend,
            formula_id=formula_id,
            bucket=self.connection.bucket,
            key=key,
        )
        return

    def load_formula_config(self, formula_id: str):
        """Load and parse a workflow YAML object.

        Args:
            formula_id: ID in the ``formulas/`` namespace.

        Returns:
            Parsed configuration mapping, or an empty mapping for an empty file.
        """
        key = self.connection.object_key(f"formulas/{formula_id}.yml")
        response = self.connection.client.get_object(
            Bucket=self.connection.bucket,
            Key=key,
        )
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded config",
            backend=self.backend,
            formula_id=formula_id,
            bucket=self.connection.bucket,
            key=key,
        )
        return yaml.safe_load(response["Body"].read()) or {}

    def list_formula_schemas(self) -> list[str]:
        """List YAML schema names across all S3 pages under ``schemas/``."""
        prefix = self.connection.object_key("schemas/")
        names = [
            key.removeprefix(prefix).removesuffix(".yml")
            for key in self._list_run_keys(prefix)
            if key.endswith(".yml")
        ]
        return sorted(names)

    def create_formula_schema(self, schema_name: str, schema: dict):
        """Serialize and store one schema document as YAML."""
        key = self.connection.object_key(f"schemas/{schema_name}.yml")
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=key,
            Body=yaml.safe_dump(schema).encode("utf-8"),
            ContentType="application/x-yaml",
        )
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created schema",
            backend=self.backend,
            schema_name=schema_name,
            bucket=self.connection.bucket,
            key=key,
        )

    def load_formula_schema(self, schema_name: str) -> dict:
        """Load and parse one named schema YAML object."""
        key = self.connection.object_key(f"schemas/{schema_name}.yml")
        response = self.connection.client.get_object(Bucket=self.connection.bucket, Key=key)
        schema = yaml.safe_load(response["Body"].read()) or {}
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded schema",
            backend=self.backend,
            schema_name=schema_name,
            bucket=self.connection.bucket,
            key=key,
        )
        return schema

    def load_formula_connection(self, connection_name: str):
        """
        Load and parse one global connection YAML object from S3.

        Args:
            connection_name: Name in the ``connections/`` namespace.

        Returns:
            Parsed connection mapping, or an empty mapping for an empty file.
        """
        key = self.connection.object_key(f"connections/{connection_name}.yml")
        response = self.connection.client.get_object(
            Bucket=self.connection.bucket,
            Key=key,
        )
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded connection",
            backend=self.backend,
            connection_name=connection_name,
            bucket=self.connection.bucket,
            key=key,
        )
        return yaml.safe_load(response["Body"].read()) or {}

    def create_formula_connection(self, connection_name: str, config: dict):
        """Serialize and store one global connection YAML object in S3."""
        key = self.connection.object_key(f"connections/{connection_name}.yml")
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=key,
            Body=yaml.safe_dump(config).encode("utf-8"),
            ContentType="application/x-yaml",
        )
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created connection",
            backend=self.backend,
            connection_name=connection_name,
            bucket=self.connection.bucket,
            key=key,
        )

    def list_formula_connections(self) -> list[str]:
        """List connection YAML names, following S3 continuation tokens.

        Returns:
            Sorted connection IDs without the ``connections/`` prefix or
            ``.yml`` suffix.
        """
        prefix = self.connection.object_key("connections/")
        names = []
        continuation_token = None
        while True:
            params = {"Bucket": self.connection.bucket, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            response = self.connection.client.list_objects_v2(**params)
            names.extend(
                item["Key"].removeprefix(prefix).removesuffix(".yml")
                for item in response.get("Contents", [])
                if item["Key"].endswith(".yml")
            )
            continuation_token = response.get("NextContinuationToken")
            if not response.get("IsTruncated") or not continuation_token:
                break
        return sorted(names)

    def list_formula_states(self):
        """List YAML state object IDs below the configured ``states/`` prefix.

        Returns:
            IDs shaped as ``<formula_id>/<run_id>`` without the suffix.
        """
        prefix = self.connection.object_key("states/")
        formula_ids = []
        for key in self._list_run_keys(prefix):
            if key.endswith(".yml"):
                formula_ids.append(key.removeprefix(prefix).removesuffix(".yml"))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Listed states",
            backend=self.backend,
            bucket=self.connection.bucket,
            prefix=prefix,
            state_count=len(formula_ids),
        )
        return formula_ids

    def list_active_formula_runs(self):
        """List only lightweight active markers, without scanning historical states."""
        prefix = self.connection.object_key("active/")
        active_runs = []
        for key in self._list_run_keys(prefix):
            if not key.endswith(".json"):
                continue
            parts = key.removeprefix(prefix).removesuffix(".json").split("/")
            if len(parts) == 2 and all(parts):
                active_runs.append((parts[0], parts[1]))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Listed active runs",
            backend=self.backend,
            active_run_count=len(active_runs),
        )
        return active_runs

    def _list_run_keys(self, prefix):
        options = {"Bucket": self.connection.bucket, "Prefix": prefix}
        while True:
            response = self.connection.client.list_objects_v2(**options)
            yield from (item["Key"] for item in response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            options["ContinuationToken"] = response["NextContinuationToken"]

    def _write_active_marker(self, formula_id, run_id):
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=self.connection.object_key(f"active/{formula_id}/{run_id}.json"),
            Body=json.dumps({"formula_id": formula_id, "run_id": run_id}).encode("utf-8"),
            ContentType="application/json",
        )

    def _delete_active_marker(self, formula_id, run_id):
        self.connection.client.delete_object(
            Bucket=self.connection.bucket,
            Key=self.connection.object_key(f"active/{formula_id}/{run_id}.json"),
        )

    def create_formula_state(self, formula_id: str, state: dict, run_id: str):
        """Create a run-state YAML object.

        Args:
            formula_id: Workflow identifier used in the state object key.
            state: Complete serializable run-state mapping.
            run_id: Invocation identifier used in the state object key.
        """
        key = self._state_key(formula_id, run_id=run_id)
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=key,
            Body=yaml.safe_dump(state).encode("utf-8"),
            ContentType="application/x-yaml",
        )
        self._sync_active_marker(formula_id, run_id, state)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            bucket=self.connection.bucket,
            key=key,
        )
        return

    def load_formula_state(self, formula_id: str, run_id: str):
        """Load and parse one run's YAML state object.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Parsed run-state mapping, or an empty mapping for a missing or empty object.
        """
        key = self._state_key(formula_id, run_id=run_id)
        try:
            response = self.connection.client.get_object(
                Bucket=self.connection.bucket,
                Key=key,
            )
        except Exception as exc:
            error_response = getattr(exc, "response", None)
            if isinstance(error_response, dict) and error_response.get("Error", {}).get("Code") in {
                "NoSuchKey", "NotFound", "404",
            }:
                return {}
            raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            bucket=self.connection.bucket,
            key=key,
        )
        return yaml.safe_load(response["Body"].read()) or {}

    def update_formula_state(self, formula_id: str, state: dict, run_id: str, path: str | None = None):
        """Replace a run-state object with the supplied complete mapping.

        S3 does not support the partial update represented by ``path`` here;
        the argument is retained for Formula facade parity and only recorded in
        diagnostics.

        Args:
            formula_id: Workflow identifier.
            state: Complete state mapping to serialize and write.
            run_id: Invocation identifier.
            path: Optional diagnostic path; it does not limit the S3 write.
        """
        key = self._state_key(formula_id, run_id=run_id)
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=key,
            Body=yaml.safe_dump(state).encode("utf-8"),
            ContentType="application/x-yaml",
        )
        self._sync_active_marker(formula_id, run_id, state)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Updated state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            bucket=self.connection.bucket,
            key=key,
            path=path or "$",
        )
        return

    def expire_formula_state(self, formula_id: str, run_id: str):
        """Delete one run-state object from S3.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.
        """
        key = self._state_key(formula_id, run_id=run_id)
        self.connection.client.delete_object(
            Bucket=self.connection.bucket,
            Key=key,
        )
        self._delete_active_marker(formula_id, run_id)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Expired state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            bucket=self.connection.bucket,
            key=key,
        )
        return

    def _analysis_key(self, formula_id: str, run_id: str) -> str:
        """Build the analysis object key for one Formula run."""
        return self.connection.object_key(f"analyses/{formula_id}/{run_id}.yml")

    def save_formula_analysis(self, formula_id: str, run_id: str, analysis: dict | str):
        """Store a run analysis and best-effort mirror it into run state.

        String analyses are wrapped with their Formula/run identifiers;
        mappings are stored as provided. The standalone analysis object is the
        primary record. Updating existing Formula state is best-effort so a
        missing state object does not prevent saving the analysis.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.
            analysis: Analysis mapping or free-form summary text.

        Returns:
            The normalized payload written to the analysis object.
        """
        key = self._analysis_key(formula_id, run_id=run_id)
        payload = analysis if isinstance(analysis, dict) else {
            "formula_id": formula_id,
            "run_id": run_id,
            "analysis": analysis,
        }
        self.connection.client.put_object(
            Bucket=self.connection.bucket,
            Key=key,
            Body=yaml.safe_dump(payload).encode("utf-8"),
            ContentType="application/x-yaml",
        )
        try:
            state = self.load_formula_state(formula_id, run_id=run_id)
            if state:
                state["analysis"] = payload.get("analysis", payload)
                self.update_formula_state(formula_id, state, run_id=run_id)
        except Exception:
            pass

        self.residue.emit(
            self.residue.ash.INFO,
            "FORMULA: Saved analysis",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            bucket=self.connection.bucket,
            key=key,
        )
        return payload

    def load_formula_analysis(self, formula_id: str, run_id: str) -> dict:
        """Load an analysis object, falling back to the run-state copy.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Analysis mapping, a normalized state fallback, or an empty mapping
            when neither source can provide analysis.
        """
        key = self._analysis_key(formula_id, run_id=run_id)
        try:
            response = self.connection.client.get_object(
                Bucket=self.connection.bucket,
                Key=key,
            )
            return yaml.safe_load(response["Body"].read()) or {}
        except Exception:
            try:
                state = self.load_formula_state(formula_id, run_id=run_id)
                if state and "analysis" in state:
                    val = state["analysis"]
                    return val if isinstance(val, dict) else {
                        "formula_id": formula_id,
                        "run_id": run_id,
                        "analysis": val,
                    }
            except Exception:
                pass
            return {}

    def list_formula_analyses(self, formula_id: str | None = None) -> list[str]:
        """List stored analysis IDs across all S3 pages, optionally for one Formula.

        Args:
            formula_id: Optional workflow ID prefix filter.

        Returns:
            Sorted-by-storage-order IDs shaped as ``<formula_id>/<run_id>``.
        """
        prefix = self.connection.object_key(f"analyses/{formula_id}/" if formula_id else "analyses/")
        analyses = []
        base_prefix = self.connection.object_key("analyses/")
        for key in self._list_run_keys(prefix):
            if key.endswith(".yml"):
                analyses.append(key.removeprefix(base_prefix).removesuffix(".yml"))
        return analyses


class RedisFormula(_RunIndex):
    """Persist Formula configs, connections, states, and analyses in Redis.

    Configs and connections use RedisJSON keys under ``oxidizer:formulas:``
    and ``oxidizer:connections:``. Run states and analyses are stored under
    ``oxidizer:states:`` and ``oxidizer:analyses:`` respectively. State updates
    can replace the whole document or one RedisJSON path.
    """

    def __init__(self, connection: RedisConnection, residue: Residue):
        """Bind the Redis connection and Formula-scoped logger.

        Args:
            connection: Redis wrapper providing RedisJSON and scan operations.
            residue: Shared logger for storage operations.
        """
        self.connection = connection
        self.residue = residue
        self.backend = "redis"

    @staticmethod
    def _state_key(formula_id: str, run_id: str) -> str:
        """Build the Redis key identifying one Formula run state."""
        return f"oxidizer:states:{formula_id}:{run_id}"

    def list_formula_configs(self):
        """List Formula IDs from keys under ``oxidizer:formulas:``.

        Returns:
            IDs with the storage prefix removed.
        """
        prefix = "oxidizer:formulas:"
        formula_ids = []
        for key in self.connection.client.scan_iter(match=f"{prefix}*"):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            formula_ids.append(key.removeprefix(prefix))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Listed configs",
            backend=self.backend,
            redis_prefix=prefix,
            config_count=len(formula_ids),
        )
        return formula_ids

    def create_formula_config(self, formula_id: str, config: dict):
        """Store a workflow mapping as a RedisJSON document.

        Args:
            formula_id: ID used in the ``oxidizer:formulas:`` key.
            config: Workflow configuration mapping.
        """
        redis_key = f"oxidizer:formulas:{formula_id}"
        self.connection.client.json().set(redis_key, "$", config)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created config",
            backend=self.backend,
            formula_id=formula_id,
            redis_key=redis_key,
            path="$",
        )
        return

    def load_formula_config(self, formula_id: str):
        """Load a workflow mapping from RedisJSON.

        Args:
            formula_id: ID in the ``oxidizer:formulas:`` namespace.

        Returns:
            Stored mapping, or an empty mapping when the key is absent.
        """
        redis_key = f"oxidizer:formulas:{formula_id}"
        config = self.connection.client.json().get(redis_key) or {}
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded config",
            backend=self.backend,
            formula_id=formula_id,
            redis_key=redis_key,
        )
        return config

    def list_formula_schemas(self) -> list[str]:
        """List schema names from keys under ``oxidizer:schemas:``."""
        prefix = "oxidizer:schemas:"
        names = []
        for key in self.connection.client.scan_iter(match=f"{prefix}*"):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            names.append(key.removeprefix(prefix))
        return sorted(names)

    def create_formula_schema(self, schema_name: str, schema: dict):
        """Store one schema mapping as a RedisJSON document."""
        redis_key = f"oxidizer:schemas:{schema_name}"
        self.connection.client.json().set(redis_key, "$", schema)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created schema",
            backend=self.backend,
            schema_name=schema_name,
            redis_key=redis_key,
            path="$",
        )

    def load_formula_schema(self, schema_name: str) -> dict:
        """Load one schema mapping from RedisJSON, returning empty if absent."""
        redis_key = f"oxidizer:schemas:{schema_name}"
        schema = self.connection.client.json().get(redis_key) or {}
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded schema",
            backend=self.backend,
            schema_name=schema_name,
            redis_key=redis_key,
        )
        return schema

    def load_formula_connection(self, connection_name: str):
        """
        Load one global connection mapping from RedisJSON.

        Args:
            connection_name: Name in the ``oxidizer:connections:`` namespace.

        Returns:
            Stored mapping, or an empty mapping when the key is absent.
        """
        redis_key = f"oxidizer:connections:{connection_name}"
        connection = self.connection.client.json().get(redis_key) or {}
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded connection",
            backend=self.backend,
            connection_name=connection_name,
            redis_key=redis_key,
        )
        return connection

    def create_formula_connection(self, connection_name: str, config: dict):
        """Store one global connection mapping as a RedisJSON document."""
        redis_key = f"oxidizer:connections:{connection_name}"
        self.connection.client.json().set(redis_key, "$", config)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created connection",
            backend=self.backend,
            connection_name=connection_name,
            redis_key=redis_key,
            path="$",
        )

    def list_formula_connections(self) -> list[str]:
        """List connection names stored under ``oxidizer:connections:``.

        Returns:
            Sorted key suffixes with the storage prefix removed.
        """
        prefix = "oxidizer:connections:"
        names = []
        for key in self.connection.client.scan_iter(match=f"{prefix}*"):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            names.append(key.removeprefix(prefix))
        return sorted(names)

    def list_formula_states(self):
        """List run-state key suffixes under ``oxidizer:states:``.

        Returns:
            IDs shaped as ``<formula_id>:<run_id>``.
        """
        prefix = "oxidizer:states:"
        formula_ids = []
        for key in self.connection.client.scan_iter(match=f"{prefix}*"):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            formula_ids.append(key.removeprefix(prefix))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Listed states",
            backend=self.backend,
            redis_prefix=prefix,
            state_count=len(formula_ids),
        )
        return formula_ids

    def list_active_formula_runs(self):
        """List active markers without scanning historical RedisJSON states."""
        active_runs = []
        prefix = "oxidizer:active:"
        for key in self.connection.client.scan_iter(match=f"{prefix}*"):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            parts = key.removeprefix(prefix).split(":")
            if len(parts) == 2 and all(parts):
                active_runs.append((parts[0], parts[1]))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Listed active runs",
            backend=self.backend,
            active_run_count=len(active_runs),
        )
        return active_runs

    def _write_active_marker(self, formula_id, run_id):
        self.connection.client.json().set(
            f"oxidizer:active:{formula_id}:{run_id}", "$",
            {"formula_id": formula_id, "run_id": run_id},
        )

    def _delete_active_marker(self, formula_id, run_id):
        self.connection.client.json().delete(f"oxidizer:active:{formula_id}:{run_id}", "$")

    def create_formula_state(self, formula_id: str, state: dict, run_id: str):
        """Create or replace a run-state RedisJSON document.

        Args:
            formula_id: Workflow identifier.
            state: Complete run-state mapping.
            run_id: Invocation identifier.
        """
        redis_key = self._state_key(formula_id, run_id=run_id)
        self.connection.client.json().set(redis_key, "$", state)
        self._sync_active_marker(formula_id, run_id, state)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Created state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            redis_key=redis_key,
            path="$",
        )
        return

    def load_formula_state(self, formula_id: str, run_id: str):
        """Load one run-state RedisJSON document.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Stored run-state mapping, or an empty mapping when absent.
        """
        redis_key = self._state_key(formula_id, run_id=run_id)
        state = self.connection.client.json().get(redis_key) or {}
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Loaded state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            redis_key=redis_key,
        )
        return state

    def update_formula_state(self, formula_id: str, state: dict, run_id: str, path: str | None = None):
        """Replace the full state or update one RedisJSON path.

        For a partial update, ``state`` remains the complete mapping supplied
        by the caller; the value at ``path`` is extracted and written only to
        that location in RedisJSON.

        Args:
            formula_id: Workflow identifier.
            state: Complete new state or mapping containing the path value.
            run_id: Invocation identifier.
            path: Optional RedisJSON path. ``None`` and ``$`` replace the
                entire document.
        """
        redis_key = self._state_key(formula_id, run_id=run_id)
        if path is None or path == "$":
            self.connection.client.json().set(redis_key, "$", state)
            self._sync_active_marker(formula_id, run_id, state)
            self.residue.emit(
                self.residue.ash.DEBUG,
                "FORMULA: Updated state",
                backend=self.backend,
                formula_id=formula_id,
                run_id=run_id,
                redis_key=redis_key,
                path="$",
            )
            return

        current_value = state
        for part in path.removeprefix("$.").split("."):
            current_value = current_value[part]
        self.connection.client.json().set(redis_key, path, current_value)
        self._sync_active_marker(formula_id, run_id, self.load_formula_state(formula_id, run_id))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Updated state path",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            redis_key=redis_key,
            path=path,
        )
        return

    def expire_formula_state(self, formula_id: str, run_id: str):
        """Delete a run-state RedisJSON document.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.
        """
        redis_key = self._state_key(formula_id, run_id=run_id)
        self.connection.client.json().delete(redis_key, "$")
        self._delete_active_marker(formula_id, run_id)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "FORMULA: Expired state",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            redis_key=redis_key,
        )
        return

    @staticmethod
    def _analysis_key(formula_id: str, run_id: str) -> str:
        """Build the Redis key identifying one run's analysis payload."""
        return f"oxidizer:analyses:{formula_id}:{run_id}"

    def save_formula_analysis(self, formula_id: str, run_id: str, analysis: dict | str):
        """Save an analysis payload and best-effort mirror it into run state.

        Mappings are stored as supplied; text is wrapped with Formula/run
        identifiers. RedisJSON is preferred when available, otherwise JSON is
        stored as a string. Mirroring into existing Formula state is
        best-effort and does not replace the standalone analysis record.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.
            analysis: Analysis mapping or free-form summary text.

        Returns:
            The normalized analysis payload.
        """
        redis_key = self._analysis_key(formula_id, run_id=run_id)
        payload = analysis if isinstance(analysis, dict) else {
            "formula_id": formula_id,
            "run_id": run_id,
            "analysis": analysis,
        }
        if hasattr(self.connection.client, "json"):
            self.connection.client.json().set(redis_key, "$", payload)
        else:
            self.connection.client.set(redis_key, json.dumps(payload))

        try:
            state = self.load_formula_state(formula_id, run_id=run_id)
            if state:
                state["analysis"] = payload.get("analysis", payload)
                self.update_formula_state(formula_id, state, run_id=run_id)
        except Exception:
            pass

        self.residue.emit(
            self.residue.ash.INFO,
            "FORMULA: Saved analysis",
            backend=self.backend,
            formula_id=formula_id,
            run_id=run_id,
            redis_key=redis_key,
        )
        return payload

    def load_formula_analysis(self, formula_id: str, run_id: str) -> dict:
        """Load a standalone analysis, falling back to the run-state copy.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Analysis mapping or an empty mapping if neither source is available.
        """
        redis_key = self._analysis_key(formula_id, run_id=run_id)
        try:
            if hasattr(self.connection.client, "json"):
                data = self.connection.client.json().get(redis_key)
                if data:
                    return data
            else:
                raw = self.connection.client.get(redis_key)
                if raw:
                    return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception:
            pass
        try:
            state = self.load_formula_state(formula_id, run_id=run_id)
            if state and "analysis" in state:
                val = state["analysis"]
                return val if isinstance(val, dict) else {
                    "formula_id": formula_id,
                    "run_id": run_id,
                    "analysis": val,
                }
        except Exception:
            pass
        return {}

    def list_formula_analyses(self, formula_id: str | None = None) -> list[str]:
        """List analysis IDs, optionally filtered to one Formula.

        The implementation uses ``SCAN`` when available and falls back to
        ``KEYS`` for simpler clients.

        Args:
            formula_id: Optional workflow ID filter.

        Returns:
            Sorted IDs normalized from ``<formula_id>:<run_id>`` to
            ``<formula_id>/<run_id>``.
        """
        match = f"oxidizer:analyses:{formula_id}:*" if formula_id else "oxidizer:analyses:*"
        keys = []
        if hasattr(self.connection.client, "scan_iter"):
            try:
                for k in self.connection.client.scan_iter(match=match):
                    keys.append(k.decode("utf-8") if isinstance(k, bytes) else str(k))
            except Exception:
                pass
        elif hasattr(self.connection.client, "keys"):
            try:
                for k in self.connection.client.keys(match):
                    keys.append(k.decode("utf-8") if isinstance(k, bytes) else str(k))
            except Exception:
                pass
        prefix = "oxidizer:analyses:"
        return [k.removeprefix(prefix).replace(":", "/") for k in sorted(keys)]



class Formula:
    """Backend-neutral API for Formula configuration and run persistence.

    The facade selects an S3 or Redis implementation from the connection
    type, binds a component-scoped logger, and forwards public operations to
    that backend. Backend-specific behavior is preserved: S3 state writes
    replace whole YAML objects, while Redis can update a RedisJSON path.
    """

    def __init__(self, connection: S3Connection | RedisConnection, residue: Residue | None = None):
        """Create a Formula facade for an S3 or Redis connection.

        Args:
            connection: Supported persistence connection used to select the
                concrete backend.
            residue: Optional shared logger; a Formula-scoped logger is created
                when omitted.

        Raises:
            ValueError: If ``connection`` is neither supported connection type.
        """
        self.connection = connection
        self.residue = (residue or Residue()).bind(component_name="formula")
        if isinstance(connection, S3Connection):
            self.formula = S3Formula(connection, self.residue)
        elif isinstance(connection, RedisConnection):
            self.formula = RedisFormula(connection, self.residue)
        else:
            raise ValueError("Unsupported connection type.")

    def list_formula_configs(self):
        """List workflow IDs available in the selected backend.

        Returns:
            Formula configuration identifiers.
        """
        return self.formula.list_formula_configs()

    def create_formula_config(self, formula_id: str, config: dict):
        """Persist a workflow configuration in the selected backend.

        Args:
            formula_id: Identifier used to address the configuration later.
            config: Workflow configuration mapping.
        """
        return self.formula.create_formula_config(formula_id, config)

    def load_formula_config(self, formula_id: str):
        """Load one workflow configuration.

        Args:
            formula_id: Identifier of the configuration to load.

        Returns:
            Configuration mapping, or an empty mapping when absent.
        """
        return self.formula.load_formula_config(formula_id)

    def list_formula_schemas(self) -> list[str]:
        """List named schema documents in the selected backend."""
        return self.formula.list_formula_schemas()

    def create_formula_schema(self, schema_name: str, schema: dict):
        """Persist a schema document in the selected backend."""
        return self.formula.create_formula_schema(schema_name, schema)

    def load_formula_schema(self, schema_name: str) -> dict:
        """Load a schema document, returning an empty mapping when absent.

        The S3 backend raises the storage client's missing-object error, while
        RedisJSON returns an empty mapping for a missing key.
        """
        return self.formula.load_formula_schema(schema_name)

    def load_formula_connection(self, connection_name: str):
        """Load a globally stored connection configuration.

        Args:
            connection_name: Name of the connection configuration.

        Returns:
            Connection mapping, or an empty mapping when absent.
        """
        return self.formula.load_formula_connection(connection_name)

    def create_formula_connection(self, connection_name: str, config: dict):
        """Persist a globally named connection document in the selected backend."""
        return self.formula.create_formula_connection(connection_name, config)

    def list_formula_connections(self) -> list[str]:
        """List globally stored connection names from the selected backend."""
        return self.formula.list_formula_connections()

    def list_formula_states(self):
        """List run-state identifiers available in the selected backend.

        S3 identifiers use ``<formula_id>/<run_id>``; Redis identifiers use
        ``<formula_id>:<run_id>``.
        """
        return self.formula.list_formula_states()

    def list_active_formula_runs(self):
        """Return Formula/run pairs present in the active marker index.

        Returns:
            List of ``(formula_id, run_id)`` pairs.
        """
        return self.formula.list_active_formula_runs()

    def list_formula_runs(self):
        """Return all Formula/run pairs, including archived runs."""
        return self.formula.list_formula_runs()

    def reconcile_active_formula_runs(self):
        """Repair markers and index pre-marker states during maintenance."""
        return self.formula.reconcile_active_formula_runs()

    def sync_active_formula_run(self, formula_id: str, run_id: str, state: dict):
        """Repair one marker from the supplied authoritative run state."""
        self.formula._sync_active_marker(formula_id, run_id, state)

    def load_formula_state(self, formula_id: str, run_id: str):
        """Load one run-state mapping.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Stored state mapping, or an empty mapping when absent.
        """
        return self.formula.load_formula_state(formula_id, run_id=run_id)

    def create_formula_state(self, formula_id: str, state: dict, run_id: str):
        """Create or replace one run-state mapping.

        Args:
            formula_id: Workflow identifier.
            state: Complete state mapping.
            run_id: Invocation identifier.
        """
        return self.formula.create_formula_state(formula_id, state, run_id=run_id)

    def update_formula_state(self, formula_id: str, state: dict, run_id: str, path: str | None = None):
        """Update run state using the selected backend's write semantics.

        Args:
            formula_id: Workflow identifier.
            state: Complete state or mapping containing the path value.
            run_id: Invocation identifier.
            path: Optional JSON path. Redis updates only that path; S3 writes
                the complete supplied state object.
        """
        return self.formula.update_formula_state(formula_id, state, run_id=run_id, path=path)

    def expire_formula_state(self, formula_id: str, run_id: str):
        """Delete one run-state record from the selected backend.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.
        """
        return self.formula.expire_formula_state(formula_id, run_id=run_id)

    def save_formula_analysis(self, formula_id: str, run_id: str, analysis: dict | str):
        """Persist analysis independently and mirror it into state when possible.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.
            analysis: Analysis mapping or free-form summary text.

        Returns:
            The normalized payload saved by the backend.
        """
        return self.formula.save_formula_analysis(formula_id, run_id=run_id, analysis=analysis)

    def load_formula_analysis(self, formula_id: str, run_id: str) -> dict:
        """Load run analysis, falling back to the copy embedded in state.

        Args:
            formula_id: Workflow identifier.
            run_id: Invocation identifier.

        Returns:
            Analysis mapping, or an empty mapping when unavailable.
        """
        return self.formula.load_formula_analysis(formula_id, run_id=run_id)

    def list_formula_analyses(self, formula_id: str | None = None) -> list[str]:
        """List stored analysis IDs from the selected backend.

        Args:
            formula_id: Optional workflow ID filter.

        Returns:
            IDs shaped as ``<formula_id>/<run_id>``.
        """
        return self.formula.list_formula_analyses(formula_id=formula_id)

    def dag(self, formula_id: str, config: dict) -> dict:
        """Build the in-memory DAG representation from a workflow config.

        IDs are qualified: ``layer_id`` is ``<formula_id>.<layer_name>`` and
        ``node_id`` is ``<layer_id>.<node_name>``. Each node mapping is
        annotated in place: the YAML ``name`` is replaced by ``node_name``, and
        ``layer_id``, ``layer_name``, and initial checkpoint metadata are added. Scheduled nodes begin in
        :attr:`NodeStatus.SCHEDULED`; all others begin in
        :attr:`NodeStatus.PENDING`. Input entries with a ``ref`` create a
        parent-to-child edge and the corresponding reverse edge.

        Args:
            formula_id: Identifier used to qualify each layer and node ID.
            config: Workflow topology mapping containing ``layers``, each
                with a name and optional nodes.

        Returns:
            Mapping with ``nodes``, ``edges``, ``reverse_edges``, and initial
            ``status`` values.

        Raises:
            ValueError: If the formula, a layer, or a node name contains a dot.
        """
        #0 - Config Details 
        layers = config.get("layers", [])

        nodes: Dict[str, dict] = {}
        edges: Dict[str, List[str]] = defaultdict(list)
        reverse_edges: Dict[str, List[str]] = defaultdict(list)
        status: Dict[str, NodeStatus] = {}

        def checked(kind: str, value) -> str:
            if not isinstance(value, str) or not value or "." in value:
                raise ValueError(f"{kind} must be a non-empty name without dots: {value!r}")
            return value

        checked("formula_id", formula_id)
        for layer in layers:
            layer_name = checked("layer name", layer.get("name"))
            layer_id = f"{formula_id}.{layer_name}"
            self.residue.emit(self.residue.ash.DEBUG, f"TOPOLOGY: Processing layer '{layer_id}' with {len(layer.get('nodes', []))} nodes.")
            for node in layer.get("nodes", []):
                node_name = checked("node name", node.pop("name", None))
                node_id = f"{formula_id}.{layer_name}.{node_name}"

                base_checkpoint_metadata = CheckpointMetadata(node_id=node_id)

                self.residue.emit(self.residue.ash.DEBUG, f"TOPOLOGY: Registering node '{node_id}' with initial checkpoint metadata: {base_checkpoint_metadata.to_dict()}.")
                nodes[node_id] = node
                nodes[node_id]["layer_id"] = layer_id
                nodes[node_id]["layer_name"] = layer_name
                nodes[node_id]["node_name"] = node_name
                nodes[node_id]["checkpoint_metadata"] = base_checkpoint_metadata.to_dict()
                node_type = node.get("type", None)
                if node_type == "scheduled":
                    status[node_id] = NodeStatus.SCHEDULED  
                    self.residue.emit(self.residue.ash.DEBUG, f"TOPOLOGY: Registered scheduled node '{node_id}' with initial status SCHEDULED.")
                else:
                    status[node_id] = NodeStatus.PENDING 
                    self.residue.emit(self.residue.ash.DEBUG, f"TOPOLOGY: Registered node '{node_id}' with initial status PENDING.")

        #2 - Register Edges
        for layer in layers:
            layer_name = layer["name"]
            for node in layer.get("nodes", []):
                node_name = node["node_name"]
                node_id = f"{formula_id}.{layer_name}.{node_name}"
                self.residue.emit(self.residue.ash.DEBUG, f"TOPOLOGY: Processing node edges '{node_id}' with {len(node.get('inputs', []))} inputs.")
                inputs = node.get("inputs", [])
                for source in inputs:
                    ref = source.get("ref")
                    if ref:
                        # Edge: ref (parent) -> node_id (child)
                        edges[ref].append(node_id)
                        reverse_edges[node_id].append(ref)
                        self.residue.emit(self.residue.ash.DEBUG, f"TOPOLOGY: Registered edge from '{ref}' to '{node_id}'.")

        return { 
            "nodes": nodes,
            "edges": dict(edges),
            "reverse_edges": dict(reverse_edges),
            "status": status
        }
