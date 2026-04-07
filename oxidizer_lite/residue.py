import structlog
import json
import logging
import time
from enum import Enum


class Ash(Enum):
    """
    Log severity levels mapping directly to Python's standard logging levels.
    Used as the first argument to Residue.residue() to tag every log entry.
    """
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class Residue:
    """
    Base class providing structured logging for the Oxidizer distributed system.

    Every component (Alloy, Catalyst, Formula, Oxidizer, Reagent, Microscope)
    inherits from Residue. Logging is emitted through structlog with bound
    context (component name), so all log output is uniform regardless of source.
    """

    LOG_TTL = 3600  # Default TTL in seconds for Redis log keys
    REDIS_LOG_KEYS_NODE = {"lattice_id", "run_id", "node_id"}  # Full context for node/worker logs
    REDIS_LOG_KEYS_CONTROLLER = {"lattice_id", "run_id"}       # Controller-level logs (no node_id)

    def __init__(self, component_name: str = "oxidizer"):
        """
        Initialize the Residue logger.

        Args:
            component_name (str): Identifier bound to every log entry from this instance. Subclasses pass their own name (e.g. "catalyst", "reagent").
        """
        self.component_name = component_name
        
        self.logger = structlog.get_logger().bind(component=component_name)
        self.ash = Ash

    def residue(self, level: Ash, message: str, **kwargs) -> None:
        """
        Emit a structured log message at the given severity level.

        Args:
            level (Ash): An Ash enum member (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            message (str): Human-readable log message.
            **kwargs: Arbitrary key-value pairs to include as structured context (e.g. run_id="abc", node_id="agents.users_agent").
        """
        if not isinstance(level, Ash):
            raise ValueError("Invalid log level")
        log_method = {
            Ash.DEBUG: self.logger.debug,
            Ash.INFO: self.logger.info,
            Ash.WARNING: self.logger.warning,
            Ash.ERROR: self.logger.error,
            Ash.CRITICAL: self.logger.critical,
        }[level]
        log_method(message, **kwargs)
        self._redis_log(level, message, **kwargs)

    def _redis_log(self, level: Ash, message: str, **kwargs) -> None:
        """
        Append a log entry to Redis based on available context.
        
        Key patterns (3-tier):
        - Node/Worker:   oxidizer:logs:{lattice_id}:{run_id}:{node_id}:{level}
        - Controller:    oxidizer:logs:{lattice_id}:{run_id}:_controller:{level}
        - System:        oxidizer:logs:_system:{level}
        """
        catalyst = getattr(self, "catalyst", None)
        if catalyst is None:
            return
        
        # Determine which tier based on available context
        has_node_context = self.REDIS_LOG_KEYS_NODE.issubset(kwargs)
        has_controller_context = self.REDIS_LOG_KEYS_CONTROLLER.issubset(kwargs) and "node_id" not in kwargs
        
        if has_node_context:
            key = f"oxidizer:logs:{kwargs['lattice_id']}:{kwargs['run_id']}:{kwargs['node_id']}:{level.name}"
        elif has_controller_context:
            key = f"oxidizer:logs:{kwargs['lattice_id']}:{kwargs['run_id']}:_controller:{level.name}"
        else:
            key = f"oxidizer:logs:_system:{level.name}"
        
        entry = {
            "timestamp": time.time(),
            "level": level.name,
            "component": self.component_name,
            "message": message,
            **{k: str(v) for k, v in kwargs.items()},
        }
        try:
            existing = catalyst.get_json(key)
            if existing is None:
                catalyst.set_json(key, [entry])
            else:
                catalyst.client.json().arrappend(key, ".", entry)
            catalyst.set_ttl(key, self.LOG_TTL)
        except Exception:
            pass

    def oxidizer_ascii_art(self):
        """Prints the Oxidizer ASCII art banner with the current version."""
        art = r"""
         ██████  ██   ██ ██ ██████  ██ ██████ ███████ ██████  
        ██    ██  ██ ██  ██ ██   ██ ██    ██  ██      ██   ██ 
        ██    ██   ███   ██ ██   ██ ██   ██   █████   ██████  
        ██    ██  ██ ██  ██ ██   ██ ██  ██    ██      ██   ██ 
         ██████  ██   ██ ██ ██████  ██ ██████ ███████ ██   ██
         
         Version: 0.1.0
        """
        # FUTURE: How Can We the version pull dynamically?
        print(art)