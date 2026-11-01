from __future__ import annotations

import asyncio
import inspect
import json
import urllib.request
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, Callable

from oxidizer.core.catalyst import Catalyst
from oxidizer.core.formula import Formula
from oxidizer.core.lattice import KafkaConnection, OllamaConnection, RedisConnection
from oxidizer.core.residue import Ash, Residue


class _OllamaLangChainAdapter:
    """Build the LangChain chat model from the project's Ollama connection."""

    def __init__(self, connection: OllamaConnection, model: str):
        self.connection = connection
        self.model = model

    def create_chat_model(self):
        if self.connection.host and ":9379" in self.connection.host:
            try:
                from langchain_openai import ChatOpenAI
            except ImportError as exc:
                raise ImportError("Install oxidizer-lite[mcp] to use LiteRT-LM's OpenAI-compatible API") from exc
            base_url = self.connection.host.rstrip("/")
            if not base_url.endswith("/v1"):
                base_url += "/v1"
            return ChatOpenAI(
                model=self.model,
                base_url=base_url,
                api_key=self.connection.api_key or "not-needed",
                temperature=0,
            )
        try:
            from langchain_ollama import ChatOllama
        except ImportError as exc:
            raise ImportError("Install oxidizer-lite[mcp] to use the Ollama LangChain adapter") from exc

        headers = dict(self.connection.headers)
        if self.connection.api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {self.connection.api_key}"
        client_kwargs = dict(self.connection.client_kwargs)
        if headers:
            client_kwargs["headers"] = headers
        if self.connection.timeout is not None:
            client_kwargs["timeout"] = self.connection.timeout

        kwargs = {"model": self.model, "temperature": 0, "client_kwargs": client_kwargs}
        if self.connection.host:
            kwargs["base_url"] = self.connection.host
        return ChatOllama(**kwargs)


class Microscope:
    """Long-running LangChain agent for diagnosing Formula runs from log triggers."""

    _SECRET_FRAGMENTS = (
        "password", "secret", "token", "api_key", "access_key", "credential", "authorization",
    )
    _SYSTEM_PROMPT = (
        "You are Oxidizer Microscope, a workflow failure analyst. Use the supplied trigger and evidence, "
        "and use only the tools currently exposed by the configured Topology server. The minimal Topology "
        "surface currently provides list_formulas, get_formula, list_formula_states, and get_formula_state. "
        "Use get_formula_state for persisted run evidence; secret-like values in resolved connection snapshots "
        "are redacted. Do not assume other inspection tools are available. "
        "Separate observed facts from hypotheses, cite event and checkpoint evidence, and recommend "
        "read-only next checks first. Never mutate state or acknowledge, delete, replay, or reset messages."
    )

    def __init__(
        self,
        formula: Formula,
        model_connection: Any,
        log_connection: RedisConnection | Catalyst | None = None,
        *,
        catalyst: Catalyst | None = None,
        model: str,
        topology_mcp: Any,
        topology_api_url: str | None = None,
        log_stream: str | None = None,
        log_streams: Sequence[str] | None = None,
        consumer_group: str = "oxidizer:microscope",
        consumer_name: str = "microscope",
        stream_start_id: str = "$",
        max_log_entries: int = 1000,
        analysis_runner: Callable[[dict[str, Any]], Any] | None = None,
        model_adapter: Any = None,
        residue: Residue | None = None,
    ):
        if not isinstance(formula, Formula):
            raise TypeError("Microscope requires a Formula instance")
        bound_residue = (residue or Residue()).bind(component_name="microscope")
        self.residue = Residue(
            component_name="microscope",
            logger=bound_residue.logger,
            context=bound_residue.context,
            sinks=[],
            capture_events=False,
        )
        if catalyst is None:
            if isinstance(log_connection, Catalyst):
                catalyst = log_connection
            elif isinstance(log_connection, (RedisConnection, KafkaConnection)):
                catalyst = Catalyst(log_connection, residue=self.residue)
            elif log_connection is None:
                raise ValueError("Microscope requires a Catalyst or RedisConnection")
            else:
                raise TypeError("Microscope requires a Catalyst or RedisConnection for log streams")
        if not model:
            raise ValueError("model must be provided")
        if topology_mcp is None:
            raise ValueError("topology_mcp must be an MCP URL or FastMCP server")
        if max_log_entries < 1:
            raise ValueError("max_log_entries must be positive")
        if model_adapter is None:
            if isinstance(model_connection, OllamaConnection):
                model_adapter = _OllamaLangChainAdapter(model_connection, model)
            elif hasattr(model_connection, "create_chat_model"):
                model_adapter = model_connection
            else:
                try:
                    model_adapter = _OllamaLangChainAdapter(model_connection, model)
                except Exception:
                    raise TypeError("Microscope requires an OllamaConnection or model_adapter (e.g. for Bedrock)")

        self._streams_explicit = bool(log_streams or log_stream)
        configured_streams = list(log_streams or ([log_stream] if log_stream else ["oxidizer:logs"]))
        self.log_streams = tuple(dict.fromkeys(configured_streams))
        if not self.log_streams or any(not stream for stream in self.log_streams):
            raise ValueError("log_streams must contain at least one non-empty stream name")
        self.formula = formula
        self.model_connection = model_connection
        self.model_adapter = model_adapter
        self.catalyst = catalyst
        self.log_connection = getattr(catalyst, "connection", log_connection)
        self.model = model
        self.topology_mcp = topology_mcp
        self.topology_api_url = topology_api_url
        self.log_stream = self.log_streams[0]
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.stream_start_id = stream_start_id
        self.max_log_entries = max_log_entries
        self.analysis_runner = analysis_runner
        self._known_groups: set[str] = set()

    @classmethod
    def _redact(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                key: "[REDACTED]" if any(fragment in str(key).lower() for fragment in cls._SECRET_FRAGMENTS)
                else cls._redact(child)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [cls._redact(child) for child in value]
        return value

    @staticmethod
    def _text(value: Any) -> str:
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    @classmethod
    def _decode_event(cls, fields: Mapping[str, Any]) -> dict[str, Any] | None:
        normalized = {cls._text(key): value for key, value in fields.items()}
        raw_event = normalized.get("event", normalized.get("message"))
        if isinstance(raw_event, bytes):
            raw_event = raw_event.decode("utf-8")
        if isinstance(raw_event, str):
            try:
                raw_event = json.loads(raw_event)
            except json.JSONDecodeError:
                return None
        return dict(raw_event) if isinstance(raw_event, Mapping) else None

    def fetch_log_streams(self) -> list[str]:
        if self._streams_explicit:
            return list(self.log_streams)
        api_url = self.topology_api_url
        if not api_url and isinstance(self.topology_mcp, str) and self.topology_mcp.startswith(("http://", "https://")):
            base = self.topology_mcp.removesuffix("/mcp").rstrip("/")
            api_url = f"{base}/api/logs/streams"
        if api_url:
            try:
                req = urllib.request.Request(api_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        streams = data.get("streams", [])
                        if streams:
                            return list(dict.fromkeys(streams))
            except Exception as exc:
                self.residue.emit(
                    Ash.DEBUG,
                    "MICROSCOPE: Could not fetch streams from API, falling back",
                    api_url=api_url,
                    error=str(exc),
                )
        if hasattr(self.catalyst, "list_streams"):
            try:
                discovered = self.catalyst.list_streams()
                if discovered:
                    log_streams = [s for s in discovered if "log" in s.lower()]
                    return list(dict.fromkeys(log_streams or discovered))
            except Exception:
                pass
        return list(self.log_streams)

    def _run_logs(self, run_id: str) -> list[dict[str, Any]]:
        matches = []
        client = getattr(getattr(self.catalyst, "connection", None), "client", None)
        if client and hasattr(client, "xrevrange"):
            for stream in self.log_streams:
                try:
                    for message_id, raw_fields in client.xrevrange(stream, count=self.max_log_entries):
                        fields = {self._text(key): value for key, value in raw_fields.items()}
                        event = self._decode_event(fields)
                        if event is None:
                            continue
                        if event.get("run_id") == run_id or self._text(fields.get("run_id", "")) == run_id:
                            matches.append({"stream": stream, "message_id": self._text(message_id), **self._redact(event)})
                except Exception:
                    continue
        matches.sort(key=lambda event: (str(event.get("timestamp", "")), event["stream"], event["message_id"]))
        return matches

    @staticmethod
    def _workflow(config: Mapping[str, Any]) -> Any:
        return config.get("workflow", config)

    @staticmethod
    def _run_summary(state: Mapping[str, Any]) -> dict[str, Any]:
        dag = state.get("dag", {})
        return {
            "status": state.get("status"),
            "dag_status": dag.get("status", {}),
            "nodes": {
                node_id: {
                    "checkpoint_metadata": node.get("checkpoint_metadata"),
                    "error_details": node.get("error_details"),
                }
                for node_id, node in dag.get("nodes", {}).items()
            },
            "edges": dag.get("edges", {}),
            "reverse_edges": dag.get("reverse_edges", {}),
        }

    def collect_evidence(
        self,
        formula_id: str,
        run_id: str,
        *,
        trigger_event: dict[str, Any] | None = None,
        stream: str | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        config = self.formula.load_formula_config(formula_id)
        state = self.formula.load_formula_state(formula_id, run_id=run_id)
        if not state:
            raise ValueError(f"Run not found: {formula_id}/{run_id}")
        logs = self._run_logs(run_id)
        if trigger_event and not any(log.get("message_id") == str(message_id) for log in logs):
            logs.append({"stream": stream or self.log_stream, "message_id": str(message_id or "trigger"), **self._redact(trigger_event)})
        return {
            "formula_id": formula_id,
            "run_id": run_id,
            "workflow": self._redact(self._workflow(config)),
            "run": self._redact(self._run_summary(state)),
            "logs": logs,
            "log_streams": list(self.log_streams),
        }

    @staticmethod
    def _is_trigger(event: Mapping[str, Any]) -> bool:
        level = str(event.get("level", "")).lower()
        message = str(event.get("event", "")).lower()
        return level in {"error", "critical"} or any(word in message for word in ("failed", "failure", "error"))

    def _ensure_consumer_group(self, stream: str) -> None:
        if stream in self._known_groups:
            return
        try:
            self.catalyst.create_consumer_group(
                stream,
                self.consumer_group,
                start_id=self.stream_start_id,
            )
            self._known_groups.add(stream)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            self._known_groups.add(stream)

    @asynccontextmanager
    async def _open_agent(self):
        if self.analysis_runner is not None:
            yield None
            return
        try:
            from langchain.agents import create_agent
            from langchain.mcp import MCPAdapter
        except ImportError as exc:
            raise ImportError("Install oxidizer-lite[mcp] to use LangChain MCP analysis") from exc
        async with MCPAdapter(self.topology_mcp) as adapter:
            tools = await adapter.list_tools()
            agent = create_agent(
                self.model_adapter.create_chat_model(),
                tools,
                system_prompt=self._SYSTEM_PROMPT,
            )
            yield agent

    async def _invoke_agent(self, agent: Any, evidence: dict[str, Any]) -> str:
        prompt = (
            "Analyze this triggered Oxidizer workflow run using the supplied evidence. Topology currently "
            "Use Topology's get_formula_state tool to inspect persisted run and checkpoint state. "
            "Treat supplied logs as triggers and correlate evidence with the returned state. "
            "Separate observed facts from hypotheses, cite event/node/batch "
            "evidence, recommend read-only next checks first, and never mutate state or acknowledge/replay messages.\n\n"
            f"Trigger evidence:\n{json.dumps(evidence, default=str)}"
        )
        result = await agent.ainvoke({"messages": [{"role": "user", "content": prompt}]})
        messages = result.get("messages", []) if isinstance(result, Mapping) else []
        for message in reversed(messages):
            content = message.get("content") if isinstance(message, Mapping) else getattr(message, "content", None)
            if isinstance(content, str) and content:
                return content
            if isinstance(content, list):
                text = "\n".join(
                    block["text"] for block in content
                    if isinstance(block, Mapping) and isinstance(block.get("text"), str)
                )
                if text:
                    return text
        raise TypeError("LangChain agent response did not contain assistant text")

    async def _make_analysis(self, evidence: dict[str, Any], agent: Any = None) -> str:
        if self.analysis_runner is not None:
            result = self.analysis_runner(evidence)
            if inspect.isawaitable(result):
                result = await result
            if not isinstance(result, str):
                raise TypeError("analysis_runner must return analysis text")
            return result
        if agent is not None:
            return await self._invoke_agent(agent, evidence)
        async with self._open_agent() as opened_agent:
            return await self._invoke_agent(opened_agent, evidence)

    async def analyze_run(
        self,
        formula_id: str,
        run_id: str,
        *,
        agent: Any = None,
        trigger_event: dict[str, Any] | None = None,
        stream: str | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        evidence = self.collect_evidence(
            formula_id,
            run_id,
            trigger_event=trigger_event,
            stream=stream,
            message_id=message_id,
        )
        self.residue.emit(
            Ash.INFO,
            "MICROSCOPE: Analyzing workflow run",
            formula_id=formula_id,
            run_id=run_id,
            log_count=len(evidence["logs"]),
            model=self.model,
        )
        analysis = await self._make_analysis(evidence, agent)
        self.residue.emit(
            Ash.INFO,
            "MICROSCOPE: Completed workflow analysis",
            formula_id=formula_id,
            run_id=run_id,
            log_count=len(evidence["logs"]),
            model=self.model,
        )
        result = {
            "formula_id": formula_id,
            "run_id": run_id,
            "model": self.model,
            "log_count": len(evidence["logs"]),
            "analysis": analysis,
        }
        self.formula.save_formula_analysis(formula_id, run_id, result)
        return result

    async def run_once(self, *, count: int = 10, block: int = 1000, agent: Any = None) -> list[dict[str, Any]]:
        streams = await asyncio.to_thread(self.fetch_log_streams)
        results = []
        for stream in streams:
            self._ensure_consumer_group(stream)
            pending_entries = await asyncio.to_thread(
                self.catalyst.read_from_stream,
                stream,
                self.consumer_group,
                self.consumer_name,
                count=count,
                block=0,
                stream_id="0",
            )
            entries = pending_entries
            if not entries:
                entries = await asyncio.to_thread(
                    self.catalyst.read_from_stream,
                    stream,
                    self.consumer_group,
                    self.consumer_name,
                    count=count,
                    block=block,
                    stream_id=">",
                )
            if not entries:
                continue

            for message_id, raw_fields in entries:
                msg_id_str = self._text(message_id)
                event = self._decode_event(raw_fields) if isinstance(raw_fields, Mapping) else None
                if event is None or not self._is_trigger(event):
                    self.catalyst.ack_message(stream, self.consumer_group, msg_id_str, consumer_name=self.consumer_name)
                    continue

                formula_id = event.get("formula_id")
                run_id = event.get("run_id")
                if not formula_id or not run_id:
                    self.residue.emit(
                        Ash.WARNING,
                        "MICROSCOPE: Ignoring uncorrelated log trigger",
                        stream=stream,
                        message_id=msg_id_str,
                    )
                    self.catalyst.ack_message(stream, self.consumer_group, msg_id_str, consumer_name=self.consumer_name)
                    continue

                # Check run status before analyzing: if active/running, pass; if completed or failed, analyze
                try:
                    state = self.formula.load_formula_state(str(formula_id), run_id=str(run_id))
                    run_status = str(state.get("status", "")).lower() if state else ""
                    dag_statuses = set(str(v).lower() for v in state.get("dag", {}).get("status", {}).values()) if state else set()
                    is_active = run_status in ("running", "dispatched", "pending") and "failed" not in dag_statuses
                    if is_active:
                        self.residue.emit(
                            Ash.DEBUG,
                            "MICROSCOPE: Skipping active run, awaiting terminal state",
                            formula_id=formula_id,
                            run_id=run_id,
                            status=run_status,
                        )
                        continue
                except Exception as exc:
                    self.residue.emit(
                        Ash.DEBUG,
                        "MICROSCOPE: Could not check run status prior to analysis",
                        formula_id=formula_id,
                        run_id=run_id,
                        error=str(exc),
                    )

                result = await self.analyze_run(
                    str(formula_id),
                    str(run_id),
                    agent=agent,
                    trigger_event=event,
                    stream=stream,
                    message_id=msg_id_str,
                )
                self.catalyst.ack_message(stream, self.consumer_group, msg_id_str, consumer_name=self.consumer_name)
                results.append({"trigger_stream": stream, "trigger_message_id": msg_id_str, **result})
                self.residue.emit(
                    Ash.INFO,
                    "MICROSCOPE: Analyzed log trigger",
                    formula_id=formula_id,
                    run_id=run_id,
                    node_id=event.get("node_id"),
                    trigger_stream=stream,
                )
        return results

    async def run_forever(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        count: int = 10,
        block: int = 1000,
        retry_delay: float = 2.0,
    ) -> None:
        async with self._open_agent() as agent:
            while stop_event is None or not stop_event.is_set():
                try:
                    await self.run_once(count=count, block=block, agent=agent)
                except Exception as exc:
                    self.residue.emit(
                        Ash.ERROR,
                        "MICROSCOPE: Trigger analysis failed",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    await asyncio.sleep(retry_delay)
