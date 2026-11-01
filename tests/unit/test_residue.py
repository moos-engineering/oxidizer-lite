from oxidizer import Residue, S3Connection
from oxidizer.core.residue import sanitize_sensitive_data


class FakeLogger:
    def __init__(self, context=None):
        self.context = context or {}
        self.events = []

    def bind(self, **context):
        return FakeLogger({**self.context, **context})

    def debug(self, message, **kwargs):
        self.events.append(("debug", message, kwargs))

    def info(self, message, **kwargs):
        self.events.append(("info", message, kwargs))

    def warning(self, message, **kwargs):
        self.events.append(("warning", message, kwargs))

    def error(self, message, **kwargs):
        self.events.append(("error", message, kwargs))

    def critical(self, message, **kwargs):
        self.events.append(("critical", message, kwargs))


def test_residue_bind_preserves_trace_context_and_rebinds_component():
    root = Residue(component_name="oxidizer", logger=FakeLogger({"component": "oxidizer"}))

    trace = root.bind(run_id="run-123", formula_id="daily-users")
    child = trace.bind(component_name="formula", node_id="extract.users")

    assert child.component_name == "formula"
    assert child.logger.context == {
        "component": "formula",
        "run_id": "run-123",
        "formula_id": "daily-users",
        "node_id": "extract.users",
    }


def test_lattice_connection_preserves_bound_trace_context():
    root = Residue(component_name="oxidizer", logger=FakeLogger({"component": "oxidizer"}))
    trace = root.bind(run_id="run-123", lattice_id="local-rustfs")

    connection = S3Connection(bucket="demo", client=object(), residue=trace)

    assert connection.residue.component_name == "lattice"
    assert connection.residue.logger.context == {
        "component": "lattice",
        "run_id": "run-123",
        "lattice_id": "local-rustfs",
    }


def test_residue_sanitizes_context_fields_and_text_before_all_destinations():
    class Sink:
        def __init__(self):
            self.events = []

        def emit(self, event):
            self.events.append(event)

    sink = Sink()
    trace = Residue(logger=FakeLogger(), sinks=[sink], capture_events=True).bind(
        run_id="run-123", password="bound-secret",
    )
    payload = {"headers": {"Authorization": "Bearer header-secret"}, "items": [{"api-key": "field-secret"}]}
    trace.emit(trace.ash.ERROR,
        "request https://url-user:url-secret@example.test/api?api_key=query-secret&limit=10 failed password='text-secret'",
        payload=payload, error_message="Bearer exception-secret",
    )
    captured = trace.dump()
    for destination in (captured, sink.events, trace.logger.events, trace.logger.context):
        rendered = str(destination)
        for secret in ("bound-secret", "header-secret", "field-secret", "url-user", "url-secret", "query-secret", "text-secret", "exception-secret"):
            assert secret not in rendered
        assert "[REDACTED]" in rendered
    assert captured[0]["run_id"] == "run-123"
    assert payload["headers"]["Authorization"] == "Bearer header-secret"


def test_residue_console_receives_sanitized_bound_context(capsys):
    trace = Residue(capture_events=True).bind(username="console-user", secret_access_key="console-secret")
    trace.emit(trace.ash.WARNING, "failure token=message-secret", error=ValueError("password=exception-secret"))
    output = capsys.readouterr().out
    for secret in ("console-user", "console-secret", "message-secret", "exception-secret"):
        assert secret not in output
    assert "REDACTED" in output


def test_sanitizer_preserves_safe_fields_and_masks_url_credentials():
    value = {"run_id": "run-123", "count": 5, "url": "https://example.test/api?limit=10", "rows": (1, None)}
    assert sanitize_sensitive_data(value) == value
    assert sanitize_sensitive_data("redis://user:password@localhost:6379/15") == "redis://%5BREDACTED%5D@localhost:6379/15"
    masked = sanitize_sensitive_data("https://user:password@example.test/?api_key=hidden&limit=10")
    assert "limit=10" in masked
    assert sanitize_sensitive_data(masked) == masked
    assert sanitize_sensitive_data("Authorization: Basic dXNlcjpwYXNz") == "Authorization: [REDACTED] [REDACTED]"
