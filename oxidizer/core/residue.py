from __future__ import annotations

import json
import structlog
import logging
import os
import sys
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from importlib.metadata import version, PackageNotFoundError
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


_SECRET_FIELDS = (
    "password", "passwd", "secret", "token", "apikey", "accesskey",
    "credential", "authorization", "authentication", "username",
)
_URL_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"']+")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(\b(?:password|passwd|secret(?:_access_key)?|api[_-]?key|access[_-]?key(?:_id)?|"
    r"(?:access[_-]?|refresh[_-]?|session[_-]?)?token|credential|authorization|authentication|username)"
    r"[\"']?\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;&]+)"
)


def _sensitive_field(name: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(name).lower())
    return any(fragment in normalized for fragment in _SECRET_FIELDS)


def sanitize_sensitive_data(value: Any) -> Any:
    """Copy structured data, masking sensitive fields and recognizable text credentials."""
    if isinstance(value, Mapping):
        return {
            key: "[REDACTED]" if _sensitive_field(key) else sanitize_sensitive_data(child)
            for key, child in value.items()
        }
    if isinstance(value, (list, tuple)):
        children = [sanitize_sensitive_data(child) for child in value]
        return tuple(children) if isinstance(value, tuple) else children
    if isinstance(value, Exception):
        value = str(value)
    if not isinstance(value, str):
        return value

    def mask_url(match):
        try:
            parts = urlsplit(match.group())
            netloc = parts.netloc
            if "@" in netloc:
                netloc = quote("[REDACTED]", safe="") + "@" + netloc.rsplit("@", 1)[1]
            query = parse_qsl(parts.query, keep_blank_values=True)
            if any(_sensitive_field(key) for key, _ in query):
                query = [(key, "[REDACTED]" if _sensitive_field(key) else child) for key, child in query]
                return urlunsplit((parts.scheme, netloc, parts.path, urlencode(query), parts.fragment))
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        except ValueError:
            return "[REDACTED URL]"

    value = _URL_PATTERN.sub(mask_url, value)
    value = re.sub(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/-]+=*", lambda match: match[1] + " [REDACTED]", value)
    return _SECRET_ASSIGNMENT.sub(lambda match: match[1] + "[REDACTED]", value)


try:
    _version = version("oxidizer-lite")
except PackageNotFoundError:
    _version = "unknown"

_log_level = getattr(logging, os.getenv("OXIDIZER_LOG_LEVEL", "INFO").upper(), logging.INFO)
_log_format = os.getenv("OXIDIZER_LOG_FORMAT", "console").lower()
_renderer = structlog.processors.JSONRenderer() if _log_format == "json" else structlog.dev.ConsoleRenderer()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        _renderer,
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
)

class Ash(Enum):
    """
    Log severity levels mapping directly to Python's standard logging levels.
    Used as the first argument to Residue.emit() to tag every log entry.
    """
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


def log_stream_name(formula_id: Any = None) -> str:
    """Return ``oxidizer:logs:<formula_id>``, or ``oxidizer:logs`` when no formula is known."""
    return f"oxidizer:logs:{formula_id}" if formula_id else "oxidizer:logs"


class RedisStreamLogSink:
    """Persist structured Residue events as entries in Redis Streams.

    Events carrying a ``formula_id`` go to ``oxidizer:logs:<formula_id>``;
    all others go to ``oxidizer:logs``. Each entry stores the complete event
    as JSON in ``event`` and duplicates ``run_id``, ``component``, and
    ``level`` into searchable top-level fields. The sink uses an existing
    Redis connection; it does not create or own the connection lifecycle.
    Redis write failures are reported once to stderr and do not propagate
    into the operation that emitted the log event.
    """

    def __init__(self, *, connection: Any, maxlen: int = 10000, ttl: int | None = None):
        """Configure the connection and retention policy for log entries.

        Args:
            connection: Existing ``RedisConnection``-like object exposing
                ``client.xadd`` and, when ``ttl`` is set, ``client.expire``.
            maxlen: Approximate maximum number of retained entries per
                stream. Redis may retain slightly more because trimming is
                approximate.
            ttl: Optional expiration in seconds for each stream key, not
                for individual entries.
        """
        self.connection = connection
        self.maxlen = maxlen
        self.ttl = ttl
        self._warned = False

    def emit(self, event_dict: dict[str, Any]) -> None:
        """Append one event to its formula's log stream.

        ``event`` contains the JSON-serialized full event. The other fields
        support filtering by run, component, or severity without decoding it.
        Failures are intentionally swallowed after a one-time warning so log
        storage cannot fail the workflow itself.

        Args:
            event_dict: Structured event produced by ``Residue.emit``.
        """
        event_dict = sanitize_sensitive_data(event_dict)
        stream_name = log_stream_name(event_dict.get("formula_id"))
        try:
            self.connection.client.xadd(
                stream_name,
                {
                    "event": json.dumps(event_dict, default=str, sort_keys=True),
                    "run_id": str(event_dict.get("run_id") or ""),
                    "component": str(event_dict.get("component") or ""),
                    "level": str(event_dict.get("level") or ""),
                },
                maxlen=self.maxlen,
                approximate=True,
            )
            if self.ttl is not None:
                self.connection.client.expire(stream_name, self.ttl)
        except Exception as exc:
            self._warn_once(f"failed to write Redis log sink: {exc}")

    def flush(self) -> None:
        """Do nothing; Redis entries are written immediately by ``emit``."""
        return

    def _warn_once(self, message: str) -> None:
        """Write at most one sink failure warning to stderr per instance."""
        if self._warned:
            return
        self._warned = True
        print(f"oxidizer residue sink warning: {sanitize_sensitive_data(message)}", file=sys.stderr)


def configured_residue_sink_names() -> set[str]:
    """Parse optional sink selectors from the process environment.

    ``OXIDIZER_LOG_SINKS`` takes precedence over the legacy singular
    ``OXIDIZER_LOG_SINK``. The helper only returns configured names; it does
    not construct or attach sink instances. ``stdout`` is excluded because it
    is the normal structlog destination rather than a Residue sink object.

    Returns:
        Lowercase, non-empty sink names requested by the environment.
    """
    sink_names = os.getenv("OXIDIZER_LOG_SINKS", os.getenv("OXIDIZER_LOG_SINK", "stdout"))
    return {name.strip().lower() for name in sink_names.split(",") if name.strip() and name.strip().lower() != "stdout"}


class Residue:
    """Emit structured events with shared context, capture, and sink behavior.

    Child Residue objects created by :meth:`bind` share this instance's sink
    list and, when capture is enabled, its event buffer. Components can bind
    their own names and run/node identifiers while publishing through the same
    configured outputs.
    """

    LOG_TTL = 3600  # Default TTL in seconds for Redis log keys
    REDIS_LOG_KEYS_NODE = {"lattice_id", "run_id", "node_id"}  # Full context for node/worker logs
    REDIS_LOG_KEYS_CONTROLLER = {"lattice_id", "run_id"}       # Controller-level logs (no node_id)

    def __init__(
        self,
        component_name: str = "oxidizer",
        logger=None,
        context: dict | None = None,
        sinks: list | None = None,
        *,
        capture_events: bool = False,
        event_buffer: list[dict[str, Any]] | None = None,
        log_level: str | int | None = None,
    ):
        """Create a structured logger and optional event destinations.

        Args:
            component_name: Default component identifier included in events.
            logger: Optional pre-bound structlog logger. A logger bound to
                ``context`` is created when omitted.
            context: Initial event fields; defaults to the component name.
            sinks: Mutable sink collection shared with bound child Residues.
            capture_events: Whether events are also buffered for ``dump()``.
            event_buffer: Optional existing list used as the capture buffer.
                Bound children share this list.
            log_level: Minimum level for console, capture, and sinks, as a
                name such as ``"DEBUG"`` or a ``logging`` number. Defaults to
                ``OXIDIZER_LOG_LEVEL``. Bound children inherit it.
        """
        if log_level is None:
            self.log_level = _log_level
        elif isinstance(log_level, int):
            self.log_level = log_level
        else:
            resolved = getattr(logging, str(log_level).upper(), None)
            if not isinstance(resolved, int):
                raise ValueError(f"Unknown log level: {log_level}")
            self.log_level = resolved
        self.component_name = component_name
        self.context = sanitize_sensitive_data(context or {"component": component_name})
        self.sinks = sinks if sinks is not None else []
        self.capture_events = capture_events
        self._event_buffer = event_buffer if event_buffer is not None else []
        self.logger = logger if logger is not None else structlog.get_logger().bind(**self.context)
        self.ash = Ash

    def bind(self, component_name: str | None = None, **context) -> "Residue":
        """Create a child logger with component-specific structured context.

        New context values override parent values. If ``component_name`` is
        provided, it also replaces the event's ``component`` field. The child
        shares the parent's sink list and capture buffer, keeping the trace
        tree connected to the same outputs.

        Args:
            component_name: Optional component label for child events.
            **context: Additional fields bound to every child event.

        Returns:
            A new Residue wrapper sharing this instance's configured outputs.
        """
        child_component_name = component_name or self.component_name
        bound_context = sanitize_sensitive_data({**self.context, **context})
        if component_name is not None:
            bound_context["component"] = component_name
        return Residue(
            component_name=child_component_name,
            logger=self.logger.bind(**bound_context),
            context=bound_context,
            sinks=self.sinks,
            capture_events=self.capture_events,
            event_buffer=self._event_buffer,
            log_level=self.log_level,
        )

    def add_sink(self, sink) -> None:
        """Attach a sink to this trace and all bound child traces.

        Sinks exposing a non-None ``connection`` are deduplicated by sink type
        and connection identity. Connectionless sinks are attached independently.
        This avoids duplicate event writes when components share a backend.

        Args:
            sink: Object exposing ``emit(event_dict)``; ``flush()`` is optional.
        """
        connection = getattr(sink, "connection", None)
        if connection is not None and getattr(sink, "_oxidizer_connection_id", None) is None:
            sink._oxidizer_connection_id = id(connection)
        for existing_sink in self.sinks:
            if (
                connection is not None
                and type(existing_sink) is type(sink)
                and getattr(existing_sink, "_oxidizer_connection_id", None) == getattr(sink, "_oxidizer_connection_id", None)
            ):
                return
        self.sinks.append(sink)

    def emit(self, level: Ash, message: str, **kwargs) -> None:
        """Create a structured event and send it to the logger and enabled sinks.

        Bound context is merged with call-specific fields (which take
        precedence), then the event message, UTC timestamp, and lowercase
        severity are added. Events below this trace's ``log_level`` are
        dropped before reaching the console, capture buffer, or sinks.

        Args:
            level: Severity from :class:`Ash`.
            message: Human-readable event description.
            **kwargs: Structured fields such as ``run_id`` or ``node_id``.

        Raises:
            ValueError: If ``level`` is not an ``Ash`` member.
        """
        if not isinstance(level, Ash):
            raise ValueError("Invalid log level")
        if level.value < self.log_level:
            return
        log_method = {
            Ash.DEBUG: self.logger.debug,
            Ash.INFO: self.logger.info,
            Ash.WARNING: self.logger.warning,
            Ash.ERROR: self.logger.error,
            Ash.CRITICAL: self.logger.critical,
        }[level]
        fields = sanitize_sensitive_data({**self.context, **kwargs})
        message = sanitize_sensitive_data(message)
        event_dict = {
            **fields,
            "event": message,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": level.name.lower(),
        }
        if self.capture_events:
            self._event_buffer.append(event_dict)
        for sink in self.sinks:
            emit = getattr(sink, "emit", None)
            if emit is not None:
                emit(event_dict)
        log_method(message, **fields)

    def dump(self) -> list[dict[str, Any]]:
        """Return and clear events captured in memory.

        Capture must be enabled with ``capture_events=True``. Bound children
        share the same buffer, so dumping from any child drains events from the
        full trace tree.

        Returns:
            A shallow copy of captured event dictionaries; the shared buffer is
            empty after this call.
        """
        events = self._event_buffer[:]
        self._event_buffer.clear()
        return events

    def flush(self) -> None:
        """Ask attached sinks to flush any buffered events.

        Immediate-write sinks such as ``RedisStreamLogSink`` may implement
        this as a no-op. Sink-specific write failures are handled by the sink.
        """
        for sink in self.sinks:
            flush = getattr(sink, "flush", None)
            if flush is not None:
                flush()
        


    def oxidizer_ascii_art(self):
        """Prints the Oxidizer ASCII art banner with the current version."""
        art = f"""
         ██████  ██   ██ ██ ██████  ██ ██████ ███████ ██████  
        ██    ██  ██ ██  ██ ██   ██ ██    ██  ██      ██   ██ 
        ██    ██   ███   ██ ██   ██ ██   ██   █████   ██████  
        ██    ██  ██ ██  ██ ██   ██ ██  ██    ██      ██   ██ 
         ██████  ██   ██ ██ ██████  ██ ██████ ███████ ██   ██
         
         Version: {_version}
        """
        # version resolved via importlib.metadata at import time
        print(art)