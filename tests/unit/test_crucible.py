import pytest

from oxidizer import Crucible, IcebergRestConnection, Residue, S3Connection, S3TablesConnection
from oxidizer.utils.crucible import DuckDBCrucible


class FakeTablesClient:
	def __init__(self):
		self.calls = []

	def list_table_buckets(self, **kwargs):
		self.calls.append(kwargs)
		if not kwargs:
			return {"tableBuckets": [], "continuationToken": "next"}
		return {"tableBuckets": [{
			"name": "oxidizer-tables-test",
			"arn": "arn:aws:s3tables:us-west-2:123456789012:bucket/oxidizer-tables-test",
		}]}


class FakeEngine:
	def __init__(self):
		self.statements = []
		self.executed_many = []

	def execute(self, sql, parameters=None):
		self.statements.append(sql)
		return self

	def fetchall(self):
		if "FROM information_schema.columns" in self.statements[-1]:
			return []
		return [(42,)]

	@property
	def description(self):
		return [("value",), ("label",)]

	def fetchmany(self, count):
		return [(42, "ok")]

	def executemany(self, sql, rows):
		self.executed_many.append((sql, rows))
		return self


class FakeSchemaAdapter:
	def __init__(self, columns):
		self.columns = columns
		self.inserts = []
		self.upserts = []
		self.ensured = []

	def _ensure_table_schema(self, table_bucket, catalog, namespace, table, columns):
		self.ensured.append((table_bucket, catalog, namespace, table, columns))

	def _insert_records(self, table_bucket, catalog, namespace, table, records, *, logical_types=None):
		self.inserts.append((table_bucket, catalog, namespace, table, records, logical_types))
		return len(records)

	def _merge_records(self, table_bucket, catalog, namespace, table, records, **options):
		self.upserts.append((table_bucket, catalog, namespace, table, records, options))
		return len(records)

def _quote_schema(operation="append"):
	return {
		"templateVersion": 1,
		"name": "quotes",
		"type": "schema",
		"schema": {
			"version": 1,
			"columns": [
				{"name": "id", "type": "int64", "nullable": False},
				{"name": "label", "type": "string", "nullable": True},
			],
			"write": {"operation": operation},
		},
	}


def test_duckdb_adapter_uses_sso_catalog_and_resolves_bucket_once():
	client = FakeTablesClient()
	engine = FakeEngine()
	connection = S3TablesConnection(
		client=client, aws_profile_name="sandbox-moos-engineering", region_name="us-west-2"
	)
	crucible = Crucible(connection, engine=engine)

	assert crucible.query("oxidizer-tables-test", "local_catalog", "analytics", "events") == [(42,)]
	assert crucible.query("oxidizer-tables-test", "local_catalog", "analytics", "events") == [(42,)]
	assert client.calls == [{}, {"continuationToken": "next"}]
	assert sum("ATTACH " in sql for sql in engine.statements) == 1
	assert any("PROFILE 'sandbox-moos-engineering'" in sql and "CHAIN 'sso'" in sql for sql in engine.statements)
	assert any("ENDPOINT_TYPE s3_tables" in sql for sql in engine.statements)
	assert engine.statements[-1] == 'SELECT * FROM local_catalog."analytics"."events"'


def test_duckdb_adapter_attaches_one_bucket_under_multiple_catalog_aliases():
	client = FakeTablesClient()
	engine = FakeEngine()
	connection = S3TablesConnection(client=client, region_name="us-west-2")
	crucible = Crucible(connection, engine=engine)

	crucible.query("oxidizer-tables-test", "market_catalog", "analytics", "events")
	crucible.query("oxidizer-tables-test", "fundamentals_catalog", "finance", "metrics")
	crucible.query("oxidizer-tables-test", "market_catalog", "analytics", "events")

	attachments = [sql for sql in engine.statements if sql.startswith("ATTACH ")]
	assert len(attachments) == 2
	assert any(" AS market_catalog " in sql for sql in attachments)
	assert any(" AS fundamentals_catalog " in sql for sql in attachments)
	assert 'SELECT * FROM market_catalog."analytics"."events"' in engine.statements
	assert 'SELECT * FROM fundamentals_catalog."finance"."metrics"' in engine.statements


def test_duckdb_adapter_rejects_unsafe_table_names():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())

	with pytest.raises(ValueError, match="Invalid namespace or table name"):
		crucible.query("oxidizer-tables-test", "local_catalog", "analytics; DROP TABLE x", "events")


def test_duckdb_adapter_accepts_bounded_read_only_sql():
	engine = FakeEngine()
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)

	result = crucible.query_sql(
		"oxidizer-tables-test", "local_catalog",
		"SELECT value, label FROM analytics.events WHERE value > ?",
		parameters=[1],
		max_rows=10,
	)

	assert result["columns"] == ["value", "label"]
	assert result["rows"] == [{"value": 42, "label": "ok"}]
	assert engine.statements[-1].startswith("SELECT value, label")


@pytest.fixture
def sql_batch_catalogs(monkeypatch):
	import duckdb

	engines = []
	attachments = []
	connect = duckdb.connect

	def create_engine():
		engine = connect()
		engines.append(engine)
		return engine

	def attach(crucible, bucket, catalog):
		engine = crucible.adapter.engine
		attachments.append((bucket, catalog))
		engine.execute(f"ATTACH ':memory:' AS {catalog}")
		engine.execute(f"CREATE TABLE {catalog}.main.events AS SELECT range AS id FROM range(4)")

	monkeypatch.setattr(duckdb, "connect", create_engine)
	monkeypatch.setattr(Crucible, "attach", attach)
	monkeypatch.setattr(S3TablesConnection, "_create_session", lambda self: None)
	monkeypatch.setattr(S3TablesConnection, "_create_client", lambda self: object())
	monkeypatch.setattr(S3Connection, "_create_session", lambda self: None)
	monkeypatch.setattr(S3Connection, "_create_client", lambda self: object())
	return engines, attachments


def test_sql_input_pages_join_across_catalogs(sql_batch_catalogs):
	engines, attachments = sql_batch_catalogs
	connections = {
		"sales": {"connection": {"type": "s3_tables", "aws_s3_table_bucket": "sales-bucket"}},
		"people": {"connection": {"type": "s3_tables", "aws_s3_table_bucket": "people-bucket",
			"endpoint_url": "http://localhost:9000", "access_key_id": "key", "secret_access_key": "secret"}},
	}
	query = "SELECT s.id, p.id + 10 AS person_id FROM sales.main.events s JOIN people.main.events p USING (id) WHERE s.id >= ? ORDER BY s.id"
	first = Crucible.read_sql_batch(connections, query, batch_size=2, parameters=[0])
	second = Crucible.read_sql_batch(connections, query, batch_size=2, offset=first["next_cursor"], parameters=[0])
	assert first == {"rows": [{"id": 0, "person_id": 10}, {"id": 1, "person_id": 11}], "is_final": False, "next_cursor": 2}
	assert second == {"rows": [{"id": 2, "person_id": 12}, {"id": 3, "person_id": 13}], "is_final": True, "next_cursor": None}
	assert attachments == [("sales-bucket", "sales"), ("people-bucket", "people")] * 2
	for engine in engines:
		with pytest.raises(Exception, match="closed"):
			engine.execute("SELECT 1")


def test_rustfs_sql_pages_do_not_initialize_unused_s3_sdk(sql_batch_catalogs, monkeypatch):
	def fail(connection):
		pytest.fail("DuckDB RustFS reads must not construct an unused S3 SDK")

	monkeypatch.setattr(S3Connection, "_create_session", fail)
	monkeypatch.setattr(S3Connection, "_create_client", fail)
	connections = {"lake": {"type": "s3_tables", "aws_s3_table_bucket": "warehouse",
		"endpoint_url": "http://localhost:9000", "access_key_id": "key", "secret_access_key": "secret"}}
	result = Crucible.read_sql_batch(connections, "SELECT id FROM lake.main.events ORDER BY id", batch_size=2)
	assert result["rows"] == [{"id": 0}, {"id": 1}]


def test_lazy_s3_client_initializes_once_on_access(monkeypatch):
	calls = []
	session, client = object(), object()

	def create_session(connection):
		calls.append("session")
		return session

	def create_client(connection):
		assert connection.session is session
		calls.append("client")
		return client

	monkeypatch.setattr(S3Connection, "_create_session", create_session)
	monkeypatch.setattr(S3Connection, "_create_client", create_client)
	connection = S3Connection(bucket="warehouse", lazy_client=True)
	assert calls == []
	assert connection.client is client
	assert connection.client is client
	assert calls == ["session", "client"]
	assert S3Connection(client=client, lazy_client=True).client is client
	assert calls == ["session", "client"]


def test_sql_input_empty_page_and_query_failure_close_engine(sql_batch_catalogs):
	engines, _attachments = sql_batch_catalogs
	connections = {"lake": {"type": "s3_tables", "aws_s3_table_bucket": "warehouse"}}
	assert Crucible.read_sql_batch(connections, "SELECT id FROM lake.main.events WHERE id > 99") == {
		"rows": [], "is_final": True, "next_cursor": None,
	}
	with pytest.raises(Exception, match="missing"):
		Crucible.read_sql_batch(connections, "SELECT missing FROM lake.main.events")
	for engine in engines:
		with pytest.raises(Exception, match="closed"):
			engine.execute("SELECT 1")


@pytest.mark.parametrize("options", [
	{"batch_size": 0}, {"offset": -1}, {"offset": True}, {"parameters": "bad"},
])
def test_sql_input_rejects_invalid_pagination(options):
	with pytest.raises(ValueError):
		Crucible.read_sql_batch({"lake": {}}, "SELECT 1", **options)


@pytest.mark.parametrize("query", [None, "", "DELETE FROM lake.main.events", "SELECT 1; SELECT 2"])
def test_sql_input_rejects_unsafe_queries(query):
	with pytest.raises(ValueError):
		Crucible.read_sql_batch({"lake": {}}, query)


@pytest.mark.parametrize("connection", [None, {}, {"type": "api"}, {"type": "s3_tables"}])
def test_sql_input_rejects_invalid_connection_documents(sql_batch_catalogs, connection):
	with pytest.raises(ValueError):
		Crucible.read_sql_batch({"lake": connection}, "SELECT 1")


@pytest.fixture
def local_s3(monkeypatch, tmp_path):
	"""Route s3://raw/ reads to tmp_path and record the setup statements DuckDB would run."""
	duckdb = pytest.importorskip("duckdb")
	connect = duckdb.connect
	statements = []

	class LocalEngine:
		def __init__(self):
			self.engine = connect()

		def __enter__(self):
			return self

		def __exit__(self, *args):
			self.engine.close()

		def execute(self, sql, parameters=None):
			statements.append(sql)
			if sql.startswith(("INSTALL", "LOAD", "CREATE SECRET")):
				return None
			return self.engine.execute(sql.replace("s3://raw/", f"{tmp_path}/"), parameters)

	monkeypatch.setattr(duckdb, "connect", LocalEngine)
	engine = connect()
	engine.execute(f"COPY (SELECT * FROM (VALUES (3, 'c'), (1, 'a'), (2, 'b')) t(id, label)) TO '{tmp_path}/a.parquet' (FORMAT parquet)")
	engine.execute(f"COPY (SELECT * FROM (VALUES (5, 'e'), (4, 'd')) t(id, label)) TO '{tmp_path}/b.csv' (HEADER)")
	engine.close()
	return statements


S3_CONNECTION = {"connection": {
	"type": "s3", "bucket": "raw", "endpoint_url": "http://rustfs.test:9000",
	"access_key_id": "key", "secret_access_key": "secret",
}}


def test_s3_input_pages_ordered_parquet(local_s3):
	first = Crucible.read_s3_batch(S3_CONNECTION, "a.parquet", order_by="id", batch_size=2)
	second = Crucible.read_s3_batch(S3_CONNECTION, ["s3://raw/a.parquet"], order_by=["id DESC"], batch_size=2, offset=2)
	assert first == {"rows": [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}], "is_final": False, "next_cursor": 2}
	assert second == {"rows": [{"id": 1, "label": "a"}], "is_final": True, "next_cursor": None}
	secret = next(sql for sql in local_s3 if sql.startswith("CREATE SECRET"))
	assert "ENDPOINT 'rustfs.test:9000'" in secret and "URL_STYLE 'path'" in secret and "USE_SSL false" in secret


def test_s3_input_uses_assume_role_with_sso_source_profile(local_s3):
	connection = {"connection": {
		"type": "s3",
		"bucket": "raw",
		"aws_sso_profile": "sandbox-moos-engineering",
		"role_arn": "arn:aws:iam::123456789012:role/presentation-test",
		"external_id": "presentation-e2e",
		"aws_region": "us-west-2",
	}}

	Crucible.read_s3_batch(connection, "a.parquet", batch_size=1)
	secret = next(sql for sql in local_s3 if sql.startswith("CREATE SECRET"))
	assert "PROVIDER credential_chain" in secret
	assert "CHAIN 'sts'" in secret
	assert "ASSUME_ROLE_ARN 'arn:aws:iam::123456789012:role/presentation-test'" in secret
	assert "PROFILE 'sandbox-moos-engineering'" in secret
	assert "EXTERNAL_ID 'presentation-e2e'" in secret


def test_s3_input_rejects_assume_role_with_explicit_keys():
	connection = {"type": "s3", "bucket": "raw", "access_key_id": "source-key", "role_arn": "arn:aws:iam::123456789012:role/test"}
	with pytest.raises(ValueError, match="requires AWS credential-chain credentials"):
		Crucible.read_s3_batch(connection, "a.parquet")


def test_s3_input_reads_csv_with_explicit_format(local_s3):
	result = Crucible.read_s3_batch(S3_CONNECTION, "b.csv", file_format="csv", order_by="id")
	assert result["rows"] == [{"id": 4, "label": "d"}, {"id": 5, "label": "e"}]
	assert result["is_final"] is True


def test_s3_input_returns_json_compatible_values(local_s3, tmp_path):
	import duckdb, json
	with duckdb.connect() as engine:
		engine.execute(
			"COPY (SELECT 9.50::DECIMAL(6,2) AS price, DATE '2026-10-01' AS day, [1, 2] AS ids) "
			f"TO '{tmp_path}/typed.parquet' (FORMAT parquet)"
		)
	rows = Crucible.read_s3_batch(S3_CONNECTION, "typed.parquet")["rows"]
	assert rows == [{"price": "9.50", "day": "2026-10-01", "ids": [1, 2]}]
	json.dumps(rows)


@pytest.mark.parametrize("options", [
	{"paths": []}, {"paths": "a.json"}, {"paths": ["a.parquet", "b.csv"]}, {"paths": "http://x/a.parquet"},
	{"order_by": "id; DROP"}, {"batch_size": 0}, {"offset": -1}, {"file_format": "json"},
])
def test_s3_input_rejects_invalid_options(options):
	kwargs = {"paths": "a.parquet", **options}
	with pytest.raises(ValueError):
		Crucible.read_s3_batch(S3_CONNECTION, kwargs.pop("paths"), **kwargs)


def test_s3_input_requires_s3_connection():
	with pytest.raises(ValueError, match="type s3"):
		Crucible.read_s3_batch({"type": "s3_tables"}, "a.parquet")


def test_crucible_close_is_idempotent_and_clears_catalog_state(monkeypatch):
	from types import SimpleNamespace
	import sys

	closed = []
	pool = SimpleNamespace(release_unused=lambda: closed.append("arrow"))
	monkeypatch.setitem(sys.modules, "pyarrow", SimpleNamespace(default_memory_pool=lambda: pool))
	crucible = Crucible(S3TablesConnection(client=object()))
	crucible.close()
	assert crucible.adapter.engine is None
	crucible.adapter.engine = SimpleNamespace(close=lambda: closed.append(True))
	crucible.adapter.table_bucket = "warehouse"
	crucible.adapter.table_bucket_arn = "arn:bucket"
	crucible.adapter._attached_catalogs["lake"] = "warehouse"
	crucible.close()
	crucible.close()
	assert closed == [True]
	assert crucible.adapter.engine is None
	assert crucible.adapter.table_bucket is None
	assert crucible.adapter.table_bucket_arn is None
	assert crucible.adapter._attached_catalogs == {}


def test_duckdb_adapter_rejects_non_read_only_sql():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())

	with pytest.raises(ValueError, match="read-only|forbidden|SELECT or WITH"):
		crucible.query_sql("oxidizer-tables-test", "local_catalog", "DELETE FROM analytics.events")


def test_duckdb_adapter_appends_arrow_batches_and_rolls_back_failures():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."events" '
		'(id BIGINT CHECK (id >= 0), label VARCHAR)'
	)

	class RecordingEngine:
		def __init__(self):
			self.statements = []
			self.registered = set()

		def register(self, name, batch):
			self.registered.add(name)
			return engine.register(name, batch)

		def unregister(self, name):
			self.registered.remove(name)
			return engine.unregister(name)

		def execute(self, sql, parameters=None):
			self.statements.append(sql)
			return engine.execute(sql, parameters)

	recording = RecordingEngine()
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=recording)
	adapter.attach = lambda table_bucket, catalog: None
	rows = [
		{"id": 1, "label": "value'; DROP TABLE events"},
		{"id": 2, "label": "two"},
		{"id": 1, "label": "duplicate key is still appended"},
	]
	assert adapter._insert_records(
		"warehouse", "local_catalog", "analytics", "events", rows,
		logical_types={"id": "int64", "label": "string"},
	) == 3
	assert engine.execute(
		'SELECT id, label FROM local_catalog."analytics"."events" ORDER BY rowid'
	).fetchall() == [(1, "value'; DROP TABLE events"), (2, "two"), (1, "duplicate key is still appended")]
	target_inserts = [
		sql for sql in recording.statements
		if sql.startswith('INSERT INTO local_catalog."analytics"."events"')
	]
	assert len(target_inserts) == 1
	assert "SELECT" in target_inserts[0]
	assert recording.registered == set()
	assert engine.execute(
		"SELECT COUNT(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'append_%'"
	).fetchone() == (0,)

	with pytest.raises(duckdb.ConstraintException):
		adapter._insert_records(
			"warehouse", "local_catalog", "analytics", "events",
			[{"id": 3, "label": "would be rolled back"}, {"id": -1, "label": "fails"}],
		)
	assert engine.execute('SELECT COUNT(*) FROM local_catalog."analytics"."events"').fetchone() == (3,)
	assert recording.registered == set()
	assert engine.execute(
		"SELECT COUNT(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'append_%'"
	).fetchone() == (0,)


def test_crucible_write_requires_a_schema():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())

	with pytest.raises(TypeError, match="schema"):
		crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": 1}])


def test_crucible_write_creates_missing_table_and_reconciles_schema():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = _quote_schema()
	assert crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": 1}], schema=schema) == 1
	schema["schema"]["columns"].append({"name": "price", "type": "double", "nullable": True})
	assert crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": 2, "price": 1.5}], schema=schema) == 1
	assert engine.execute("SELECT id, price FROM local_catalog.analytics.events ORDER BY id").fetchall() == [(1, None), (2, 1.5)]


def test_crucible_validates_records_before_schema_ddl_and_can_provision_explicitly():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	with pytest.raises(ValueError, match="Invalid value"):
		crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": "bad"}], schema=_quote_schema())
	assert engine.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events'").fetchone() == (0,)
	assert crucible.write("warehouse", "local_catalog", "analytics", "events", [], schema=_quote_schema()) == 0
	assert engine.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events'").fetchone() == (0,)
	crucible.ensure_table("warehouse", "local_catalog", "analytics", "events", schema=_quote_schema())
	assert engine.execute("SELECT COUNT(*) FROM local_catalog.analytics.events").fetchone() == (0,)


def test_duckdb_schema_reconciliation_creates_and_adds_nullable_columns():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	adapter.attach = lambda table_bucket, catalog: None
	columns = _quote_schema()["schema"]["columns"]
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	engine.execute("INSERT INTO local_catalog.analytics.events VALUES (1, 'first')")
	columns = columns + [{"name": "price", "type": "double", "nullable": True}]
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	assert engine.execute("SELECT * FROM local_catalog.analytics.events").fetchall() == [(1, "first", None)]


def test_duckdb_schema_reconciliation_loads_lazy_catalog_columns():
	class LazyCatalogEngine(FakeEngine):
		def fetchall(self):
			if "FROM information_schema.columns" in self.statements[-1]:
				return [("__", "UNKNOWN", "YES")]
			if self.statements[-1].startswith("DESCRIBE"):
				return [("id", "BIGINT", "YES", None, None, None), ("label", "VARCHAR", "YES", None, None, None)]
			return super().fetchall()

	engine = LazyCatalogEngine()
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	adapter.attach = lambda table_bucket, catalog: None
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", _quote_schema()["schema"]["columns"])
	assert engine.statements[-1] == 'DESCRIBE local_catalog."analytics"."events"'
	assert not any(sql.startswith("ALTER TABLE") for sql in engine.statements)


@pytest.mark.parametrize("initial, widened, original, incoming", [
	({"type": "int32"}, {"type": "int64"}, 17, 2 ** 40),
	({"type": "float"}, {"type": "double"}, 1.5, 1e100),
	({"type": "decimal", "precision": 8, "scale": 2},
	 {"type": "decimal", "precision": 18, "scale": 2}, "12.50", "123456789012.50"),
])
def test_crucible_write_widens_types_and_preserves_existing_rows(initial, widened, original, incoming):
	from decimal import Decimal

	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = _quote_schema()
	schema["schema"]["columns"] = [{"name": "value", "nullable": False, **initial}]
	assert crucible.write("warehouse", "local_catalog", "analytics", "values_table", [{"value": original}], schema=schema) == 1
	schema["schema"]["version"] = 2
	schema["schema"]["columns"][0] = {"name": "value", "nullable": False, **widened}
	assert crucible.write("warehouse", "local_catalog", "analytics", "values_table", [{"value": incoming}], schema=schema) == 1
	crucible.ensure_table("warehouse", "local_catalog", "analytics", "values_table", schema=schema)
	expected = [Decimal(original), Decimal(incoming)] if initial["type"] == "decimal" else [original, incoming]
	assert engine.execute("SELECT value FROM local_catalog.analytics.values_table ORDER BY value").fetchall() == [(value,) for value in expected]


@pytest.mark.parametrize("actual, expected", [
	("BIGINT", "INTEGER"), ("DOUBLE", "FLOAT"), ("BIGINT", "DOUBLE"),
	("DECIMAL(18,2)", "DECIMAL(8,2)"), ("DECIMAL(8,2)", "DECIMAL(18,3)"),
])
def test_duckdb_rejects_narrowing_and_cross_type_promotions(actual, expected):
	assert not DuckDBCrucible._can_widen_type(actual, expected)


@pytest.mark.parametrize("rest_backend", [False, True])
def test_schema_widening_is_validated_before_any_alter(rest_backend):
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute("CREATE SCHEMA local_catalog.analytics")
	engine.execute("CREATE TABLE local_catalog.analytics.events (id INTEGER, label VARCHAR)")
	connection = S3TablesConnection(client=FakeTablesClient())
	if rest_backend:
		storage = S3Connection(client=object(), bucket="warehouse", endpoint_url="http://localhost:9000")
		connection = IcebergRestConnection(storage, warehouse="warehouse")
	adapter = DuckDBCrucible(connection, engine=engine)
	adapter.attach = lambda table_bucket, catalog: None
	columns = [{"name": "id", "type": "int64", "nullable": True}, {"name": "label", "type": "string", "nullable": True}]
	if rest_backend:
		error = "not supported for IcebergRestConnection"
	else:
		columns[1]["type"] = "int64"
		error = "type mismatch"
	with pytest.raises(ValueError, match=error):
		adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	assert engine.execute("DESCRIBE local_catalog.analytics.events").fetchall()[0][1] == "INTEGER"


@pytest.mark.parametrize("change, error", [
	("type", "type mismatch"), ("nullable", "nullability mismatch"),
	("remove", "column removal"), ("required", "Only nullable columns"),
])
def test_duckdb_schema_reconciliation_rejects_unsafe_changes(change, error):
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	adapter.attach = lambda table_bucket, catalog: None
	columns = _quote_schema()["schema"]["columns"]
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	if change == "type":
		columns[0]["type"] = "string"
	elif change == "nullable":
		columns[0]["nullable"] = True
	elif change == "remove":
		columns.pop()
	else:
		columns.append({"name": "required", "type": "string", "nullable": False})
	with pytest.raises(ValueError, match=error):
		adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	assert engine.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'events'").fetchone() == (2,)


def test_iceberg_rest_schema_reconciliation_creates_but_does_not_alter():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	storage = S3Connection(client=object(), bucket="warehouse", endpoint_url="http://localhost:9000")
	connection = IcebergRestConnection(storage, warehouse="warehouse", catalog_endpoint="http://localhost:9000/iceberg")
	adapter = DuckDBCrucible(connection, engine=engine)
	adapter.attach = lambda table_bucket, catalog: None
	columns = _quote_schema()["schema"]["columns"]
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)
	columns.append({"name": "price", "type": "double", "nullable": True})
	with pytest.raises(ValueError, match="not supported for IcebergRestConnection"):
		adapter._ensure_table_schema("warehouse", "local_catalog", "analytics", "events", columns)


def test_spark_crucible_append_reports_unsupported_operation():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), adapter="spark")
	with pytest.raises(NotImplementedError, match="Spark S3 Tables adapter is not implemented"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "events", [{"id": 1, "label": "value"}],
			schema=_quote_schema(),
		)


def test_crucible_schema_append_normalizes_records_before_insert():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([
		{"column_name": "id", "data_type": "BIGINT", "is_nullable": "NO", "column_default": None},
		{"column_name": "label", "data_type": "VARCHAR", "is_nullable": "YES", "column_default": None},
	])
	crucible.adapter = adapter

	count = crucible.write(
		"warehouse", "local_catalog", "analytics", "events",
		[{"label": "first", "id": 1, "ignored": True}, {"id": 2}],
		schema=_quote_schema(),
	)

	assert count == 2
	assert adapter.inserts == [(
		"warehouse", "local_catalog", "analytics", "events",
		[{"id": 1, "label": "first"}, {"id": 2, "label": None}],
		{"id": "int64", "label": "string"},
	)]


def test_crucible_schema_refs_map_json_payload_fields_to_named_columns():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([
		{"column_name": "current_price", "data_type": "DOUBLE", "is_nullable": "YES", "column_default": None},
		{"column_name": "market", "data_type": "VARCHAR", "is_nullable": "YES", "column_default": None},
	])
	crucible.adapter = adapter
	schema = _quote_schema()
	schema["schema"]["columns"] = [
		{"name": "current_price", "ref": "$.quote.c", "type": "double", "nullable": True},
		{"name": "market", "ref": "$.quote.market", "type": "string", "nullable": True},
	]

	count = crucible.write(
		"warehouse", "local_catalog", "analytics", "events",
		[{"quote": {"c": 227.5, "market": "AAPL"}, "ignored": True}],
		schema=schema,
	)

	assert count == 1
	assert adapter.inserts[0][4] == [{"current_price": 227.5, "market": "AAPL"}]


def test_crucible_schema_ref_uses_nullable_default_when_path_is_missing():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([
		{"column_name": "current_price", "data_type": "DOUBLE", "is_nullable": "YES", "column_default": None},
	])
	crucible.adapter = adapter
	schema = _quote_schema()
	schema["schema"]["columns"] = [
		{"name": "current_price", "ref": "$.c", "type": "double", "nullable": True},
	]

	crucible.write("warehouse", "local_catalog", "analytics", "events", [{}], schema=schema)

	assert adapter.inserts[0][4] == [{"current_price": None}]


def test_crucible_schema_ref_rejects_multiple_values():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([])
	crucible.adapter = adapter
	schema = _quote_schema()
	schema["schema"]["columns"] = [
		{"name": "current_price", "ref": "$.quotes[*].c", "type": "double", "nullable": True},
	]

	with pytest.raises(ValueError, match="matched multiple values"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "events",
			[{"quotes": [{"c": 1.0}, {"c": 2.0}]}], schema=schema,
		)
	assert adapter.inserts == []


def test_crucible_schema_ref_rejects_invalid_jsonpath():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	schema = _quote_schema()
	schema["schema"]["columns"] = [
		{"name": "current_price", "ref": "$.quote[", "type": "double", "nullable": True},
	]

	with pytest.raises(ValueError, match="Invalid JSONPath ref"):
		crucible.write("warehouse", "local_catalog", "analytics", "events", [{}], schema=schema)


def test_crucible_rejects_unsupported_schema_write_before_insert():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([])
	crucible.adapter = adapter

	with pytest.raises(ValueError, match="write.operation must be 'append', 'upsert', 'scd1', or 'scd2'"):
		crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": 1}], schema=_quote_schema("delete"))

	assert adapter.inserts == []


def test_crucible_dispatches_validated_ordered_upsert():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([
		{"column_name": "id", "data_type": "BIGINT", "is_nullable": "NO", "column_default": None},
		{"column_name": "label", "data_type": "VARCHAR", "is_nullable": "YES", "column_default": None},
	])
	crucible.adapter = adapter
	schema = _quote_schema("upsert")
	schema["schema"]["keys"] = ["id"]
	schema["schema"]["write"]["conflict"] = {"order_by": "label", "direction": "descending"}

	count = crucible.write(
		"warehouse", "local_catalog", "analytics", "events", [{"id": 7, "label": "quote"}], schema=schema,
	)

	assert count == 1
	assert adapter.inserts == []
	assert adapter.upserts == [(
		"warehouse", "local_catalog", "analytics", "events",
		[{"id": 7, "label": "quote"}],
		{
			"keys": ["id"], "order_by": "label", "direction": "descending",
			"update_columns": ["label"], "tie_columns": None,
			"logical_types": {"id": "int64", "label": "string"},
		},
	)]


def test_crucible_dispatches_insert_only_upsert_without_ordering():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([
		{"column_name": "id", "data_type": "BIGINT", "is_nullable": "NO", "column_default": None},
		{"column_name": "label", "data_type": "VARCHAR", "is_nullable": "YES", "column_default": None},
	])
	crucible.adapter = adapter
	schema = _quote_schema("upsert")
	schema["schema"]["keys"] = ["id"]
	schema["schema"]["write"]["update"] = {"mode": "insert_only"}

	count = crucible.write(
		"warehouse", "local_catalog", "analytics", "events", [{"id": 7, "label": "quote"}], schema=schema,
	)

	assert count == 1
	assert adapter.upserts[0][-1] == {
		"keys": ["id"], "order_by": None, "direction": None, "update_columns": [], "tie_columns": None,
		"logical_types": {"id": "int64", "label": "string"},
	}


def test_crucible_upsert_rejects_missing_keys_and_duplicate_batch_keys():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([
		{"column_name": "id", "data_type": "BIGINT", "is_nullable": "NO", "column_default": None},
		{"column_name": "label", "data_type": "VARCHAR", "is_nullable": "YES", "column_default": None},
	])
	crucible.adapter = adapter
	schema = _quote_schema("upsert")
	with pytest.raises(ValueError, match="requires at least one schema key"):
		crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": 1}], schema=schema)

	schema["schema"]["keys"] = ["id"]
	schema["schema"]["write"]["conflict"] = {"order_by": "label", "direction": "descending"}
	with pytest.raises(ValueError, match="Duplicate key in write batch"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "events",
			[{"id": 1, "label": "first"}, {"id": 1, "label": "second"}], schema=schema,
		)
	assert adapter.upserts == []


def test_duckdb_upsert_merges_by_key_and_rejects_conflicting_order_ties():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."events" '
		'(id BIGINT, label VARCHAR, observed_at BIGINT)'
	)
	engine.execute('INSERT INTO local_catalog."analytics"."events" VALUES (1, \'old\', 100)')
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	adapter.attach = lambda table_bucket, catalog: None
	options = {
		"keys": ["id"],
		"order_by": "observed_at",
		"direction": "descending",
		"update_columns": ["label", "observed_at"],
	}

	count = adapter._merge_records(
		"warehouse", "local_catalog", "analytics", "events",
		[{"id": 1, "label": "stale", "observed_at": 99}, {"id": 2, "label": "new", "observed_at": 1}],
		**options,
	)
	assert count == 2
	assert engine.execute('SELECT * FROM local_catalog."analytics"."events" ORDER BY id').fetchall() == [
		(1, "old", 100), (2, "new", 1),
	]

	adapter._merge_records(
		"warehouse", "local_catalog", "analytics", "events",
		[{"id": 1, "label": "latest", "observed_at": 101}], **options,
	)
	assert engine.execute('SELECT * FROM local_catalog."analytics"."events" WHERE id = 1').fetchone() == (
		1, "latest", 101,
	)
	adapter._merge_records(
		"warehouse", "local_catalog", "analytics", "events",
		[{"id": 1, "label": "latest", "observed_at": 101}], **options,
	)
	assert engine.execute('SELECT COUNT(*) FROM local_catalog."analytics"."events"').fetchone() == (2,)
	assert engine.execute('SELECT * FROM local_catalog."analytics"."events" WHERE id = 1').fetchone() == (
		1, "latest", 101,
	)
	insert_only_options = {"keys": ["id"], "order_by": None, "direction": None, "update_columns": []}
	adapter._merge_records(
		"warehouse", "local_catalog", "analytics", "events",
		[{"id": 1, "label": "ignored", "observed_at": 200}, {"id": 3, "label": "inserted", "observed_at": 3}],
		**insert_only_options,
	)
	assert engine.execute('SELECT * FROM local_catalog."analytics"."events" ORDER BY id').fetchall() == [
		(1, "latest", 101), (2, "new", 1), (3, "inserted", 3),
	]
	with pytest.raises(ValueError, match="equal ordering values conflict"):
		adapter._merge_records(
			"warehouse", "local_catalog", "analytics", "events",
			[{"id": 1, "label": "conflict", "observed_at": 101}], **options,
		)
	assert engine.execute('SELECT * FROM local_catalog."analytics"."events" WHERE id = 1').fetchone() == (
		1, "latest", 101,
	)


def test_duckdb_scd1_arrow_staging_chunks_and_rolls_back_merge_failures():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."bulk_customers" '
		'(customer_id BIGINT, observed_at BIGINT, score BIGINT CHECK (score >= 0))'
	)
	engine.execute('INSERT INTO local_catalog."analytics"."bulk_customers" VALUES (1, 100, 1)')

	class RecordingEngine:
		def __init__(self):
			self.statements = []
			self.registered = set()
			self.register_calls = 0
			self.unregister_calls = 0

		def register(self, name, batch):
			self.registered.add(name)
			self.register_calls += 1
			return engine.register(name, batch)

		def unregister(self, name):
			self.registered.remove(name)
			self.unregister_calls += 1
			return engine.unregister(name)

		def execute(self, sql, parameters=None):
			self.statements.append(sql)
			return engine.execute(sql, parameters)

	recording = RecordingEngine()
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=recording)
	adapter.attach = lambda table_bucket, catalog: None
	adapter._ARROW_BATCH_SIZE = 2
	options = {
		"keys": ["customer_id"], "order_by": "observed_at", "direction": "descending",
		"update_columns": ["observed_at", "score"], "tie_columns": ["score"],
	}
	records = [
		{"customer_id": customer_id, "observed_at": observed_at, "score": score}
		for customer_id, observed_at, score in (
			(1, 101, 2), (2, 20, 3), (3, 30, 4), (4, 40, 5), (5, 50, 6),
		)
	]
	assert adapter._merge_records(
		"warehouse", "local_catalog", "analytics", "bulk_customers", records, **options,
	) == len(records)
	merge_statements = [sql for sql in recording.statements if sql.startswith("MERGE INTO")]
	assert len(merge_statements) == 1
	assert "USING merge_" in merge_statements[0]
	assert "VALUES (?, ?, ?)" not in merge_statements[0]
	assert recording.register_calls == recording.unregister_calls == 3
	assert recording.registered == set()
	assert engine.execute('SELECT COUNT(*) FROM local_catalog."analytics"."bulk_customers"').fetchone() == (5,)
	assert engine.execute(
		"SELECT COUNT(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'merge_%'"
	).fetchone() == (0,)

	with pytest.raises(duckdb.ConstraintException):
		adapter._merge_records(
			"warehouse", "local_catalog", "analytics", "bulk_customers",
			[{"customer_id": 6, "observed_at": 60, "score": -1}], **options,
		)
	assert recording.registered == set()
	assert engine.execute('SELECT COUNT(*) FROM local_catalog."analytics"."bulk_customers"').fetchone() == (5,)
	assert engine.execute(
		"SELECT COUNT(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'merge_%'"
	).fetchone() == (0,)


def test_duckdb_scd1_keeps_latest_entity_state_and_rejects_tracked_ties():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."customers" '
		'(customer_id BIGINT NOT NULL, observed_at BIGINT NOT NULL, tier VARCHAR NOT NULL, note VARCHAR)'
	)
	engine.execute(
		'INSERT INTO local_catalog."analytics"."customers" VALUES (1, 100, \'bronze\', \'original\')'
	)
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = {
		"templateVersion": 1,
		"name": "customers",
		"type": "schema",
		"schema": {
			"version": 1,
			"keys": ["customer_id"],
			"columns": [
				{"name": "customer_id", "type": "int64", "nullable": False},
				{"name": "observed_at", "type": "int64", "nullable": False},
				{"name": "tier", "type": "string", "nullable": False},
				{"name": "note", "type": "string", "nullable": True},
			],
			"write": {
				"operation": "scd1",
				"ordering": {"column": "observed_at", "direction": "descending"},
				"compare": {"columns": ["tier"]},
			},
		},
	}

	with pytest.raises(ValueError, match="Duplicate key in write batch"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "customers",
			[
				{"customer_id": 3, "observed_at": 10, "tier": "bronze", "note": None},
				{"customer_id": 3, "observed_at": 11, "tier": "silver", "note": None},
			],
			schema=schema,
		)

	count = crucible.write(
		"warehouse", "local_catalog", "analytics", "customers",
		[
			{"customer_id": 1, "observed_at": 90, "tier": "silver", "note": "stale"},
			{"customer_id": 2, "observed_at": 20, "tier": "bronze", "note": None},
		],
		schema=schema,
	)
	assert count == 2
	assert engine.execute('SELECT * FROM local_catalog."analytics"."customers" ORDER BY customer_id').fetchall() == [
		(1, 100, "bronze", "original"), (2, 20, "bronze", None),
	]

	crucible.write(
		"warehouse", "local_catalog", "analytics", "customers",
		[{"customer_id": 1, "observed_at": 110, "tier": "gold", "note": "latest"}], schema=schema,
	)
	assert engine.execute('SELECT * FROM local_catalog."analytics"."customers" WHERE customer_id = 1').fetchone() == (
		1, 110, "gold", "latest",
	)
	with pytest.raises(ValueError, match="equal ordering values conflict"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "customers",
			[{"customer_id": 1, "observed_at": 110, "tier": "platinum", "note": "conflict"}], schema=schema,
		)
	crucible.write(
			"warehouse", "local_catalog", "analytics", "customers",
			[{"customer_id": 1, "observed_at": 110, "tier": "gold", "note": "replay"}], schema=schema,
	)
	assert engine.execute('SELECT * FROM local_catalog."analytics"."customers" WHERE customer_id = 1').fetchone() == (
		1, 110, "gold", "latest",
	)
def test_duckdb_scd2_tracks_versions_and_rejects_late_or_conflicting_records():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."customers" '
		'(customer_id BIGINT NOT NULL, observed_at BIGINT NOT NULL, valid_from BIGINT NOT NULL, '
		'valid_to BIGINT, is_current BOOLEAN NOT NULL, record_hash VARCHAR NOT NULL, '
		'tier VARCHAR NOT NULL, note VARCHAR)'
	)
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = {
		"templateVersion": 1,
		"name": "customers_history",
		"type": "schema",
		"schema": {
			"version": 1,
			"keys": ["customer_id"],
			"columns": [
				{"name": "customer_id", "type": "int64", "nullable": False},
				{"name": "observed_at", "type": "int64", "nullable": False},
				{"name": "valid_from", "type": "int64", "nullable": False},
				{"name": "valid_to", "type": "int64", "nullable": True},
				{"name": "is_current", "type": "boolean", "nullable": False},
				{"name": "record_hash", "type": "string", "nullable": False},
				{"name": "tier", "type": "string", "nullable": False},
				{"name": "note", "type": "string", "nullable": True},
			],
			"write": {
				"operation": "scd2",
				"ordering": {"column": "observed_at", "direction": "ascending"},
				"history": {
					"valid_from": "valid_from",
					"valid_to": "valid_to",
					"current_flag": "is_current",
					"record_hash": "record_hash",
					"open_ended_value": None,
				},
				"compare": {"columns": ["tier"]},
			},
		},
	}
	table = 'local_catalog."analytics"."customers"'

	count = crucible.write(
		"warehouse", "local_catalog", "analytics", "customers",
		[
			{"customer_id": 1, "observed_at": 10, "tier": "bronze", "note": "initial", "record_hash": "ignored"},
			{"customer_id": 1, "observed_at": 20, "tier": "silver", "note": "changed"},
			{"customer_id": 1, "observed_at": 25, "tier": "silver", "note": "same tracked value"},
		],
		schema=schema,
	)
	assert count == 3
	rows = engine.execute(f"SELECT * FROM {table} ORDER BY valid_from").fetchall()
	assert [(row[2], row[3], row[4], row[6], row[7]) for row in rows] == [
		(10, 20, False, "bronze", "initial"),
		(20, None, True, "silver", "changed"),
	]
	assert rows[0][5] != "ignored"

	crucible.write(
		"warehouse", "local_catalog", "analytics", "customers",
		[{"customer_id": 1, "observed_at": 30, "tier": "silver", "note": "untracked change"}], schema=schema,
	)
	crucible.write(
		"warehouse", "local_catalog", "analytics", "customers",
		[{"customer_id": 2, "observed_at": 50, "tier": "bronze", "note": "second entity"}], schema=schema,
	)
	with pytest.raises(ValueError, match="late-arriving"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "customers",
			[
				{"customer_id": 1, "observed_at": 40, "tier": "gold", "note": "must rollback"},
				{"customer_id": 2, "observed_at": 40, "tier": "silver", "note": "late"},
			],
			schema=schema,
		)
	rows = engine.execute(f"SELECT * FROM {table} WHERE customer_id = 1 ORDER BY valid_from").fetchall()
	assert [(row[2], row[3], row[4], row[6]) for row in rows] == [
		(10, 20, False, "bronze"), (20, None, True, "silver"),
	]
	with pytest.raises(ValueError, match="late-arriving"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "customers",
			[{"customer_id": 1, "observed_at": 15, "tier": "gold", "note": "late"}], schema=schema,
		)
	with pytest.raises(ValueError, match="equal ordering values conflict"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "customers",
			[{"customer_id": 1, "observed_at": 20, "tier": "gold", "note": "tie"}], schema=schema,
		)
	crucible.write(
		"warehouse", "local_catalog", "analytics", "customers",
		[{"customer_id": 1, "observed_at": 20, "tier": "silver", "note": "replay"}], schema=schema,
	)
	rows = engine.execute(f"SELECT * FROM {table} WHERE customer_id = 1 ORDER BY valid_from").fetchall()
	assert len(rows) == 2
	assert rows[1][7] == "changed"


def test_duckdb_scd2_ignore_late_records_logs_count_and_keeps_newer_versions():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."weather_history" ('
		'place VARCHAR NOT NULL, observed_at TIMESTAMPTZ NOT NULL, valid_from TIMESTAMPTZ NOT NULL, '
		'valid_to TIMESTAMPTZ, is_current BOOLEAN NOT NULL, record_hash VARCHAR NOT NULL, '
		'temperature_2m DOUBLE NOT NULL)'
	)

	events = []

	class EventSink:
		def emit(self, event):
			events.append(event)

	residue = Residue(log_level="WARNING")
	residue.add_sink(EventSink())
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine, residue=residue)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = {
		"templateVersion": 1,
		"name": "weather_history",
		"type": "schema",
		"schema": {
			"version": 1,
			"keys": ["place"],
			"columns": [
				{"name": "place", "type": "string", "nullable": False},
				{"name": "observed_at", "type": "timestamp", "nullable": False},
				{"name": "valid_from", "type": "timestamp", "nullable": False},
				{"name": "valid_to", "type": "timestamp", "nullable": True},
				{"name": "is_current", "type": "boolean", "nullable": False},
				{"name": "record_hash", "type": "string", "nullable": False},
				{"name": "temperature_2m", "type": "double", "nullable": False},
			],
			"write": {
				"operation": "scd2",
				"late_arrivals": "ignore",
				"ordering": {"column": "observed_at", "direction": "ascending"},
				"history": {
					"valid_from": "valid_from",
					"valid_to": "valid_to",
					"current_flag": "is_current",
					"record_hash": "record_hash",
					"open_ended_value": None,
				},
				"compare": {"columns": ["temperature_2m"]},
			},
		},
	}
	table = 'local_catalog."analytics"."weather_history"'
	first_batch = [
		{"place": "Seattle", "observed_at": "2026-10-13T10:00:00Z", "temperature_2m": 10.0},
		{"place": "Seattle", "observed_at": "2026-10-13T11:00:00Z", "temperature_2m": 11.0},
	]
	crucible.write("warehouse", "local_catalog", "analytics", "weather_history", first_batch, schema=schema)

	second_batch = [
		{"place": "Seattle", "observed_at": "2026-10-13T10:30:00Z", "temperature_2m": 9.0},
		{"place": "Seattle", "observed_at": "2026-10-13T12:00:00Z", "temperature_2m": 12.0},
	]
	assert crucible.write("warehouse", "local_catalog", "analytics", "weather_history", second_batch, schema=schema) == 2
	counts = engine.execute(
		f"SELECT COUNT(*), COUNT(*) FILTER (WHERE is_current), "
		f"COUNT(*) FILTER (WHERE observed_at = TIMESTAMPTZ '2026-10-13T10:30:00Z') FROM {table}"
	).fetchone()
	assert counts == (3, 1, 0)
	warning = next(event for event in events if event["event"] == "CRUCIBLE: Ignored late-arriving SCD2 records")
	assert warning["input_records"] == 2
	assert warning["ignored_late_records"] == 1

	with pytest.raises(ValueError, match="equal ordering values conflict"):
		crucible.write(
			"warehouse", "local_catalog", "analytics", "weather_history",
			[{"place": "Seattle", "observed_at": "2026-10-13T12:00:00Z", "temperature_2m": 13.0}],
			schema=schema,
		)


def test_duckdb_scd2_forecast_hour_replays_are_idempotent_with_retrieval_order():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."forecast_history" ('
		'place VARCHAR NOT NULL, observed_at TIMESTAMPTZ NOT NULL, retrieved_at TIMESTAMPTZ NOT NULL, '
		'valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ, is_current BOOLEAN NOT NULL, '
		'record_hash VARCHAR NOT NULL, temperature_2m DOUBLE NOT NULL)'
	)
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = {
		"templateVersion": 1,
		"name": "forecast_history",
		"type": "schema",
		"schema": {
			"version": 1,
			"keys": ["place", "observed_at"],
			"columns": [
				{"name": "place", "type": "string", "nullable": False},
				{"name": "observed_at", "type": "timestamp", "nullable": False},
				{"name": "retrieved_at", "type": "timestamp", "nullable": False},
				{"name": "valid_from", "type": "timestamp", "nullable": False},
				{"name": "valid_to", "type": "timestamp", "nullable": True},
				{"name": "is_current", "type": "boolean", "nullable": False},
				{"name": "record_hash", "type": "string", "nullable": False},
				{"name": "temperature_2m", "type": "double", "nullable": False},
			],
			"write": {
				"operation": "scd2",
				"ordering": {"column": "retrieved_at", "direction": "ascending"},
				"history": {
					"valid_from": "valid_from",
					"valid_to": "valid_to",
					"current_flag": "is_current",
					"record_hash": "record_hash",
					"open_ended_value": None,
				},
				"compare": {"columns": ["temperature_2m"]},
			},
		},
	}
	table = 'local_catalog."analytics"."forecast_history"'

	def snapshot(retrieved_at, later_temperature):
		return [
			{
				"place": "Seattle, WA",
				"observed_at": observed_at,
				"retrieved_at": retrieved_at,
				"temperature_2m": temperature,
			}
			for observed_at, temperature in (
				("2026-10-13T10:00:00Z", 10.0),
				("2026-10-13T11:00:00Z", later_temperature),
			)
		]

	crucible.write(
		"warehouse", "local_catalog", "analytics", "forecast_history",
		snapshot("2026-10-13T09:00:00Z", 11.0), schema=schema,
	)
	crucible.write(
		"warehouse", "local_catalog", "analytics", "forecast_history",
		snapshot("2026-10-13T09:10:00Z", 12.0), schema=schema,
	)
	crucible.write(
		"warehouse", "local_catalog", "analytics", "forecast_history",
		snapshot("2026-10-13T09:20:00Z", 12.0), schema=schema,
	)

	assert engine.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (3,)
	assert engine.execute(f"SELECT COUNT(*) FROM {table} WHERE is_current").fetchone() == (2,)


def test_duckdb_scd2_supports_timestamp_ordering_without_datetime_materialization():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."status_history" '
		'(entity_id BIGINT NOT NULL, observed_at TIMESTAMPTZ NOT NULL, valid_from TIMESTAMPTZ NOT NULL, '
		'valid_to TIMESTAMPTZ, is_current BOOLEAN NOT NULL, record_hash VARCHAR NOT NULL, status VARCHAR NOT NULL)'
	)
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	schema = {
		"templateVersion": 1,
		"name": "status_history",
		"type": "schema",
		"schema": {
			"version": 1,
			"keys": ["entity_id"],
			"columns": [
				{"name": "entity_id", "type": "int64", "nullable": False},
				{"name": "observed_at", "type": "timestamp", "nullable": False},
				{"name": "valid_from", "type": "timestamp", "nullable": False},
				{"name": "valid_to", "type": "timestamp", "nullable": True},
				{"name": "is_current", "type": "boolean", "nullable": False},
				{"name": "record_hash", "type": "string", "nullable": False},
				{"name": "status", "type": "string", "nullable": False},
			],
			"write": {
				"operation": "scd2",
				"ordering": {"column": "observed_at", "direction": "ascending"},
				"history": {
					"valid_from": "valid_from",
					"valid_to": "valid_to",
					"current_flag": "is_current",
					"record_hash": "record_hash",
				},
				"compare": {"columns": ["status"]},
			},
		},
	}
	crucible.write(
		"warehouse", "local_catalog", "analytics", "status_history",
		[{"entity_id": 1, "observed_at": "2026-10-05T10:00:00Z", "status": "active"}], schema=schema,
	)
	crucible.write(
		"warehouse", "local_catalog", "analytics", "status_history",
		[{"entity_id": 1, "observed_at": "2026-10-05T11:00:00Z", "status": "inactive"}], schema=schema,
	)
	rows = engine.execute(
		'SELECT CAST(valid_from AT TIME ZONE \'UTC\' AS VARCHAR), '
		'CAST(valid_to AT TIME ZONE \'UTC\' AS VARCHAR), is_current, status '
		'FROM local_catalog."analytics"."status_history" ORDER BY valid_from'
	).fetchall()
	assert rows == [
		("2026-10-05 10:00:00", "2026-10-05 11:00:00", False, "active"),
		("2026-10-05 11:00:00", None, True, "inactive"),
	]


def test_duckdb_scd2_bulk_merge_preserves_history_and_rolls_back_insert_failure():
	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute('CREATE SCHEMA local_catalog."analytics"')
	engine.execute(
		'CREATE TABLE local_catalog."analytics"."bulk_history" ('
		'entity_id BIGINT, observed_at BIGINT, valid_from BIGINT, valid_to BIGINT, '
		'is_current BOOLEAN, record_hash VARCHAR, value BIGINT CHECK (value >= 0))'
	)

	class RecordingEngine:
		def __init__(self):
			self.statements = []
			self.registered = set()

		def register(self, name, batch):
			self.registered.add(name)
			return engine.register(name, batch)

		def unregister(self, name):
			self.registered.remove(name)
			return engine.unregister(name)

		def execute(self, sql, parameters=None):
			self.statements.append(sql)
			return engine.execute(sql, parameters or [])

	recording = RecordingEngine()
	adapter = DuckDBCrucible(S3TablesConnection(client=FakeTablesClient()), engine=recording)
	adapter.attach = lambda table_bucket, catalog: None
	history = {
		"valid_from": "valid_from", "valid_to": "valid_to",
		"current_flag": "is_current", "record_hash": "record_hash", "open_ended_value": None,
	}
	options = {"keys": ["entity_id"], "order_by": "observed_at", "history": history}
	target = 'local_catalog."analytics"."bulk_history"'

	def record(entity_id, observed_at, value):
		return {
			"entity_id": entity_id, "observed_at": observed_at, "valid_from": observed_at,
			"valid_to": None, "is_current": True, "record_hash": str(value), "value": value,
		}

	records = [record(entity_id, observed_at, value) for entity_id in range(1000) for observed_at, value in (
		(10, 1), (20, 2), (25, 2), (30, 1),
	)]
	assert adapter._scd2_records("warehouse", "local_catalog", "analytics", "bulk_history", records, **options) == 4000
	assert sum(sql.startswith("MERGE INTO") for sql in recording.statements) == 1
	assert len(recording.statements) < 30
	assert recording.registered == set()
	assert engine.execute(f"SELECT COUNT(*), COUNT(*) FILTER (WHERE is_current) FROM {target}").fetchone() == (3000, 1000)
	assert engine.execute(f"SELECT valid_from, valid_to, is_current, value FROM {target} WHERE entity_id = 0 ORDER BY valid_from").fetchall() == [
		(10, 20, False, 1), (20, 30, False, 2), (30, None, True, 1),
	]
	assert engine.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'scd2_%'").fetchone() == (0,)

	adapter._scd2_records(
		"warehouse", "local_catalog", "analytics", "bulk_history",
		[record(0, 10, 1), record(0, 20, 2), record(0, 30, 1)], **options,
	)
	assert engine.execute(f"SELECT COUNT(*) FROM {target}").fetchone() == (3000,)
	with pytest.raises(duckdb.ConstraintException):
		adapter._scd2_records(
			"warehouse", "local_catalog", "analytics", "bulk_history",
			[record(0, 40, -1), record(1001, 10, 1)], **options,
		)
	assert engine.execute(f"SELECT COUNT(*), COUNT(*) FILTER (WHERE is_current) FROM {target}").fetchone() == (3000, 1000)
	assert engine.execute(f"SELECT valid_to FROM {target} WHERE entity_id = 0 AND is_current").fetchone() == (None,)
	assert recording.registered == set()
	assert engine.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE 'scd2_%'").fetchone() == (0,)
	with pytest.raises(ValueError, match="equal ordering values conflict"):
		adapter._scd2_records(
			"warehouse", "local_catalog", "analytics", "bulk_history",
			[record(0, 40, 3), record(0, 40, 4)], **options,
		)
	with pytest.raises(ValueError, match="ascending batch order"):
		adapter._scd2_records(
			"warehouse", "local_catalog", "analytics", "bulk_history",
			[record(0, 50, 3), record(0, 40, 4)], **options,
		)


def test_duckdb_scd2_arrow_preserves_decimal_date_binary_and_json_values():
	from datetime import date
	from decimal import Decimal

	duckdb = pytest.importorskip("duckdb")
	engine = duckdb.connect()
	engine.execute("ATTACH ':memory:' AS local_catalog")
	engine.execute("CREATE SCHEMA local_catalog.analytics")
	engine.execute(
		"CREATE TABLE local_catalog.analytics.typed_history ("
		"entity_id BIGINT NOT NULL, observed_at BIGINT NOT NULL, valid_from BIGINT NOT NULL, valid_to BIGINT, "
		"is_current BOOLEAN NOT NULL, record_hash VARCHAR NOT NULL, amount DECIMAL(10,2) NOT NULL, "
		"day DATE NOT NULL, payload JSON NOT NULL, binary_data BLOB NOT NULL)"
	)
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=engine)
	crucible.adapter.attach = lambda table_bucket, catalog: None
	columns = [
		{"name": name, "type": logical_type, "nullable": nullable}
		for name, logical_type, nullable in (
			("entity_id", "int64", False), ("observed_at", "int64", False),
			("valid_from", "int64", False), ("valid_to", "int64", True),
			("is_current", "boolean", False), ("record_hash", "string", False),
			("amount", "decimal", False), ("day", "date", False),
			("payload", "json", False), ("binary_data", "binary", False),
		)
	]
	columns[6].update(precision=10, scale=2)
	schema = {
		"templateVersion": 1, "name": "typed_history", "type": "schema",
		"schema": {
			"version": 1, "keys": ["entity_id"], "columns": columns,
			"write": {
				"operation": "scd2", "ordering": {"column": "observed_at", "direction": "ascending"},
				"compare": {"columns": ["amount", "day", "payload", "binary_data"]},
				"history": {
					"valid_from": "valid_from", "valid_to": "valid_to",
					"current_flag": "is_current", "record_hash": "record_hash",
				},
			},
		},
	}
	assert crucible.write("warehouse", "local_catalog", "analytics", "typed_history", [
		{"entity_id": 1, "observed_at": 10, "amount": Decimal("1.20"), "day": date(2026, 10, 5), "payload": {"value": [1, 2]}, "binary_data": b"\x00\xff"},
		{"entity_id": 1, "observed_at": 20, "amount": "1.30", "day": "2026-10-06", "payload": 7, "binary_data": b"\x01\xfe"},
	], schema=schema) == 2
	assert engine.execute(
		"SELECT valid_from, valid_to, is_current, amount, day, CAST(payload AS VARCHAR), binary_data "
		"FROM local_catalog.analytics.typed_history ORDER BY valid_from"
	).fetchall() == [
		(10, 20, False, Decimal("1.20"), date(2026, 10, 5), '{"value": [1, 2]}', b"\x00\xff"),
		(20, None, True, Decimal("1.30"), date(2026, 10, 6), "7", b"\x01\xfe"),
	]


def test_crucible_rejects_schema_evolution_configuration():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), engine=FakeEngine())
	adapter = FakeSchemaAdapter([])
	crucible.adapter = adapter
	schema = _quote_schema()
	schema["schema"]["evolution"] = {"mode": "compatible", "allow_additive_columns": True}

	with pytest.raises(ValueError, match="Schema evolution is deferred"):
		crucible.write("warehouse", "local_catalog", "analytics", "events", [{"id": 1}], schema=schema)
	assert adapter.inserts == []


def test_duckdb_adapter_uses_connection_keys_and_token():
	engine = FakeEngine()
	connection = S3TablesConnection(
		client=FakeTablesClient(), region_name="us-west-2", access_key_id="test-key",
		secret_access_key="test-secret", session_token="test-token",
	)
	Crucible(connection, engine=engine).query("oxidizer-tables-test", "local_catalog", "analytics", "events")

	assert any("KEY_ID 'test-key'" in sql and "SECRET 'test-secret'" in sql and
	           "SESSION_TOKEN 'test-token'" in sql for sql in engine.statements)


def test_duckdb_adapter_uses_assumed_role_credentials():
	class Credentials:
		access_key = "assumed-key"
		secret_key = "assumed-secret"
		token = "assumed-token"

		def get_frozen_credentials(self):
			return self

	class Session:
		def get_credentials(self):
			return Credentials()

	engine = FakeEngine()
	connection = S3TablesConnection(
		client=FakeTablesClient(), session=Session(), role_arn="arn:aws:iam::123456789012:role/test",
	)
	Crucible(connection, engine=engine).query("oxidizer-tables-test", "local_catalog", "analytics", "events")

	assert any("KEY_ID 'assumed-key'" in sql and "SESSION_TOKEN 'assumed-token'" in sql
	           for sql in engine.statements)


def test_rustfs_connection_requires_s3_endpoint():
	storage = S3Connection(bucket="oxidizer-tables-test", client=object())

	with pytest.raises(ValueError, match="explicit S3 endpoint_url"):
		IcebergRestConnection(storage, warehouse="oxidizer-tables-test")


def test_rustfs_connection_requires_matching_storage_bucket():
	storage = S3Connection(bucket="another-bucket", endpoint_url="http://localhost:9000", client=object())

	with pytest.raises(ValueError, match="must match"):
		IcebergRestConnection(storage, warehouse="oxidizer-tables-test")


def test_rustfs_adapter_uses_explicit_catalog_endpoint():
	engine = FakeEngine()
	storage = S3Connection(
		bucket="oxidizer-tables-test", endpoint_url="http://localhost:9000",
		access_key_id="rustfsadmin", secret_access_key="rustfsadmin", client=object(),
	)
	connection = IcebergRestConnection(storage, warehouse="oxidizer-tables-test")
	crucible = Crucible(connection, engine=engine)

	assert crucible.query("oxidizer-tables-test", "local_catalog", "analytics", "events") == [(42,)]
	assert any("ENDPOINT 'localhost:9000'" in sql and "USE_SSL false" in sql for sql in engine.statements)
	assert any("ENDPOINT 'http://localhost:9000/iceberg'" in sql and "AUTHORIZATION_TYPE sigv4" in sql and
	           "SIGV4_SERVICE 's3'" in sql and "SIGV4_REGION 'us-east-1'" in sql and
	           "STAGE_CREATE_TABLES false" in sql for sql in engine.statements)


def test_spark_adapter_is_explicitly_unimplemented():
	crucible = Crucible(S3TablesConnection(client=FakeTablesClient()), adapter="spark")
	with pytest.raises(NotImplementedError, match="Spark"):
		crucible.query("oxidizer-tables-test", "local_catalog", "analytics", "events")