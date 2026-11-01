from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID, uuid4

from oxidizer.core.lattice import IcebergRestConnection, S3Connection, S3TablesConnection
from oxidizer.core.residue import Ash, Residue


def _sql_string(value: str) -> str:
	"""Quote a value as a DuckDB string literal, escaping embedded single quotes."""
	return "'" + value.replace("'", "''") + "'"


def _json_safe(value: Any) -> Any:
	"""Convert a DuckDB result value into a JSON-serializable equivalent.

	Dates and times become ISO strings, decimals/UUIDs/intervals become
	strings, bytes become base64, and lists/structs are converted recursively.
	Other values are returned unchanged.
	"""
	if isinstance(value, (date, datetime, time)):
		return value.isoformat()
	if isinstance(value, (Decimal, UUID, timedelta)):
		return str(value)
	if isinstance(value, bytes):
		return base64.b64encode(value).decode("ascii")
	if isinstance(value, Mapping):
		return {str(key): _json_safe(item) for key, item in value.items()}
	if isinstance(value, (list, tuple)):
		return [_json_safe(item) for item in value]
	return value


class DuckDBCrucible:
	"""Read and write Iceberg tables through an embedded DuckDB engine.

	The adapter attaches an AWS S3 Tables bucket or a RustFS Iceberg REST
	warehouse as a DuckDB catalog, then runs bounded read-only queries,
	schema checks, and transactional append/merge/SCD2 writes against it.
	The DuckDB engine is created lazily unless one is injected.
	"""

	_MAX_QUERY_ROWS = 1000
	_ARROW_BATCH_SIZE = 65536
	_FORBIDDEN_QUERY_WORDS = re.compile(
		r"\b(insert|update|delete|merge|create|drop|alter|attach|copy|install|load|call|pragma|replace|truncate)\b",
		re.IGNORECASE,
	)

	def __init__(self, connection: S3TablesConnection | IcebergRestConnection, *, engine: Any = None):
		"""Create an adapter for one table-bucket connection.

		Args:
			connection: AWS S3 Tables or RustFS Iceberg REST connection.
			engine: Optional DuckDB connection. When omitted, one is created
				on first use.
		"""
		self.connection = connection
		self.engine = engine
		self.table_bucket: str | None = None
		self.table_bucket_arn: str | None = None
		self._attached_catalogs: dict[str, str] = {}
		self.last_ignored_late_records = 0

	def _engine(self):
		"""Return the DuckDB engine, creating an in-memory connection if needed.

		Raises:
			ImportError: If DuckDB is not installed.
		"""
		if self.engine is None:
			try:
				import duckdb
			except ImportError as exc:
				raise ImportError("Install oxidizer-lite[duckdb] to use the DuckDB adapter") from exc
			self.engine = duckdb.connect()
		return self.engine

	def _bucket_arn(self, table_bucket: str) -> str:
		"""Resolve an S3 Tables bucket name to its ARN.

		Args:
			table_bucket: Bucket name or ARN. ARNs are returned unchanged.

		Returns:
			The table bucket ARN, found by paging ``list_table_buckets``.

		Raises:
			ValueError: If no bucket with that name exists.
		"""
		if table_bucket.startswith("arn:"):
			return table_bucket
		continuation_token = None
		while True:
			params = {"continuationToken": continuation_token} if continuation_token else {}
			response = self.connection.client.list_table_buckets(**params)
			for bucket in response.get("tableBuckets", []):
				if bucket["name"] == table_bucket:
					return bucket["arn"]
			continuation_token = response.get("continuationToken")
			if not continuation_token:
				raise ValueError(f"Table bucket not found: {table_bucket}")

	@staticmethod
	def _validate_catalog(catalog: str) -> str:
		"""Return ``catalog`` if it is a safe SQL identifier, else raise ``ValueError``."""
		if not isinstance(catalog, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
			raise ValueError("Invalid catalog name")
		return catalog

	def _attach_rustfs(self, warehouse: str, catalog: str, engine: Any) -> None:
		"""Attach a RustFS Iceberg REST warehouse using a path-scoped S3 secret.

		Args:
			warehouse: Warehouse name; must match the connection warehouse.
			catalog: Validated DuckDB catalog alias.
			engine: DuckDB engine with the iceberg and httpfs extensions loaded.

		Raises:
			ValueError: If the warehouse differs or static credentials are missing.
		"""
		connection = self.connection
		if warehouse != connection.warehouse:
			raise ValueError(f"RustFS warehouse must match configured warehouse {connection.warehouse!r}")
		storage = connection.storage_connection
		secret_name = f"{catalog}_secret"
		if not storage.access_key_id or not storage.secret_access_key:
			raise ValueError("RustFS requires access_key_id and secret_access_key")
		endpoint = storage.endpoint_url.removeprefix("http://").removeprefix("https://")
		engine.execute(
			f"CREATE SECRET {secret_name} (TYPE s3, PROVIDER config, "
			f"KEY_ID {_sql_string(storage.access_key_id)}, "
			f"SECRET {_sql_string(storage.secret_access_key)}, "
			f"REGION {_sql_string(storage.region_name)}, ENDPOINT {_sql_string(endpoint)}, "
			f"SCOPE {_sql_string('s3://' + connection.warehouse + '/')}, "
			f"URL_STYLE 'path', USE_SSL {str(storage.endpoint_url.startswith('https://')).lower()})"
		)
		engine.execute(
			f"ATTACH {_sql_string(connection.warehouse)} AS {catalog} (TYPE iceberg, "
			f"ENDPOINT {_sql_string(connection.catalog_endpoint)}, AUTHORIZATION_TYPE sigv4, "
			f"SIGV4_SERVICE {_sql_string(connection.signing_service)}, "
			f"SIGV4_REGION {_sql_string(storage.region_name)}, SECRET {secret_name}, "
			"STAGE_CREATE_TABLES false)"
		)

	def attach(self, table_bucket: str, catalog: str) -> None:
		"""Attach a table bucket as a DuckDB catalog, once per adapter.

		AWS connections create an S3 secret from static keys, an assumed-role
		session, or the credential chain/SSO profile, then attach the bucket
		ARN with the ``s3_tables`` endpoint type. RustFS connections attach the
		Iceberg REST catalog with SigV4. Repeated calls for the same bucket and
		alias are no-ops.

		Args:
			table_bucket: Bucket name or ARN (AWS), or warehouse name (RustFS).
			catalog: DuckDB catalog alias used in queries.

		Raises:
			ValueError: If the alias is invalid or already bound to another
				bucket, the adapter is bound to another bucket, or the
				connection cannot produce credentials for DuckDB.
		"""
		catalog = self._validate_catalog(catalog)
		if self.table_bucket == table_bucket and getattr(self, "catalog", None) == catalog:
			return
		arn = table_bucket if isinstance(self.connection, IcebergRestConnection) else self._bucket_arn(table_bucket)
		attached_arn = self._attached_catalogs.get(catalog)
		if attached_arn == arn:
			self.table_bucket = table_bucket
			return
		if attached_arn is not None:
			raise ValueError(f"Catalog {catalog!r} is already attached to a different table bucket")
		if self.table_bucket_arn is not None and self.table_bucket_arn != arn:
			raise ValueError("This DuckDB adapter is already attached to a different table bucket")
		if isinstance(self.connection, S3TablesConnection):
			if not arn.startswith("arn:aws:s3tables:"):
				raise ValueError("AWS S3 Tables requires a table bucket ARN")
			if self.connection.role_arn and self.connection.session is None:
				raise ValueError("Assumed-role DuckDB access requires the S3 Tables connection session")

		engine = self._engine()
		for extension in ("iceberg", "httpfs", "aws"):
			engine.execute(f"INSTALL {extension}")
			engine.execute(f"LOAD {extension}")
		if isinstance(self.connection, S3TablesConnection):
			secret_options = ["TYPE s3", f"REGION {_sql_string(self.connection.region_name)}"]
			if self.connection.role_arn and self.connection.session:
				credentials = self.connection.session.get_credentials().get_frozen_credentials()
				access_key_id, secret_access_key, session_token = credentials.access_key, credentials.secret_key, credentials.token
			elif self.connection.access_key_id:
				access_key_id = self.connection.access_key_id
				secret_access_key = self.connection.secret_access_key
				session_token = self.connection.session_token
			else:
				access_key_id = None
			if access_key_id:
				secret_options.extend(("PROVIDER config", f"KEY_ID {_sql_string(access_key_id)}", f"SECRET {_sql_string(secret_access_key)}"))
				if session_token:
					secret_options.append(f"SESSION_TOKEN {_sql_string(session_token)}")
			else:
				secret_options.append("PROVIDER credential_chain")
				if self.connection.aws_profile_name:
					secret_options.extend(("CHAIN 'sso'", f"PROFILE {_sql_string(self.connection.aws_profile_name)}"))
			secret_name = f"{catalog}_secret"
			engine.execute(f"CREATE SECRET {secret_name} ({', '.join(secret_options)})")
			engine.execute(
				f"ATTACH {_sql_string(arn)} AS {catalog} "
				f"(TYPE iceberg, ENDPOINT_TYPE s3_tables, SECRET {secret_name})"
			)
		else:
			self._attach_rustfs(table_bucket, catalog, engine)
		self.table_bucket = table_bucket
		self.table_bucket_arn = arn
		self.catalog = catalog
		self._attached_catalogs[catalog] = arn

	def query(self, table_bucket: str, catalog: str, namespace: str, table: str) -> list[tuple]:
		"""Return every row of one table.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Table name.

		Returns:
			All rows as tuples. This is unbounded; prefer ``query_sql`` for
			interactive reads.

		Raises:
			ValueError: If the namespace or table is not a safe identifier.
		"""
		for identifier in (namespace, table):
			if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
				raise ValueError("Invalid namespace or table name")
		self.attach(table_bucket, catalog)
		return self.engine.execute(f'SELECT * FROM {catalog}."{namespace}"."{table}"').fetchall()

	@staticmethod
	def _column_sql_type(column: Mapping[str, Any]) -> str:
		"""Map a schema column's logical type to its DuckDB SQL type."""
		if column["type"] == "decimal":
			if column["precision"] > 38:
				raise ValueError("DuckDB decimal precision must not exceed 38")
			return f'DECIMAL({column["precision"]},{column["scale"]})'
		return {
			"boolean": "BOOLEAN", "int32": "INTEGER", "int64": "BIGINT",
			"float": "FLOAT", "double": "DOUBLE", "string": "VARCHAR",
			"date": "DATE", "timestamp": "TIMESTAMP WITH TIME ZONE",
			"json": "VARCHAR", "binary": "BLOB",
		}[column["type"]]

	@staticmethod
	def _can_widen_type(actual_type: str, expected_type: str) -> bool:
		"""Return whether an existing column type can be safely widened.

		Allowed widenings are INTEGER to BIGINT, FLOAT to DOUBLE, and DECIMAL
		precision increases with the same scale.
		"""
		if (actual_type, expected_type) in {("INTEGER", "BIGINT"), ("FLOAT", "DOUBLE")}:
			return True
		actual_decimal = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", actual_type)
		expected_decimal = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", expected_type)
		return bool(
			actual_decimal and expected_decimal
			and int(expected_decimal[1]) > int(actual_decimal[1])
			and expected_decimal[2] == actual_decimal[2]
		)

	def _ensure_table_schema(
		self, table_bucket: str, catalog: str, namespace: str, table: str,
		columns: list[Mapping[str, Any]],
	) -> None:
		"""Create the table, or reconcile an existing table with the schema.

		Missing tables are created along with their namespace. Existing tables
		may gain nullable columns and widen compatible types; removing columns,
		incompatible type changes, and tightening nullability are rejected.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Table name.
			columns: Validated schema column definitions.

		Raises:
			ValueError: If identifiers are invalid, the existing table is
				incompatible, or alteration is required on an Iceberg REST
				connection, which does not support it.
		"""
		catalog = self._validate_catalog(catalog)
		for identifier in (namespace, table):
			if not isinstance(identifier, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
				raise ValueError("Invalid namespace or table name")
		declared = {column["name"]: (column, self._column_sql_type(column)) for column in columns}
		self.attach(table_bucket, catalog)
		physical = self.engine.execute(
			"SELECT column_name, data_type, is_nullable FROM information_schema.columns "
			"WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position",
			[catalog, namespace, table],
		).fetchall()
		target = f'{catalog}."{namespace}"."{table}"'
		if not physical:
			definitions = [
				f'"{name}" {sql_type}' + ("" if column["nullable"] else " NOT NULL")
				for name, (column, sql_type) in declared.items()
			]
			self.engine.execute(f'CREATE SCHEMA IF NOT EXISTS {catalog}."{namespace}"')
			self.engine.execute(f"CREATE TABLE {target} ({', '.join(definitions)})")
			return
		physical = [row[:3] for row in self.engine.execute(f"DESCRIBE {target}").fetchall()]
		physical_names = set()
		widenings = []
		for name, data_type, nullable in physical:
			physical_names.add(name)
			if name not in declared:
				raise ValueError(f"Schema would remove existing column {name!r}; column removal is not supported")
			column, sql_type = declared[name]
			actual_type = re.sub(r"\s+", "", data_type.upper())
			expected_type = re.sub(r"\s+", "", sql_type)
			if actual_type != expected_type and not (column["type"] == "json" and actual_type == "JSON"):
				if not self._can_widen_type(actual_type, expected_type):
					raise ValueError(f"Schema type mismatch for column {name!r}: {data_type} versus {sql_type}")
				widenings.append((name, sql_type))
			if nullable == "NO" and column["nullable"]:
				raise ValueError(f"Schema nullability mismatch for column {name!r}")
		additions = [(name, column, sql_type) for name, (column, sql_type) in declared.items() if name not in physical_names]
		if (additions or widenings) and isinstance(self.connection, IcebergRestConnection):
			raise ValueError("Schema alteration is not supported for IcebergRestConnection")
		if any(not column["nullable"] for _, column, _ in additions):
			raise ValueError("Only nullable columns can be added to an existing table")
		for name, sql_type in widenings:
			self.engine.execute(f'ALTER TABLE {target} ALTER COLUMN "{name}" TYPE {sql_type}')
		for name, _, sql_type in additions:
			self.engine.execute(f'ALTER TABLE {target} ADD COLUMN "{name}" {sql_type}')

	@classmethod
	def _validate_query(cls, sql: str, max_rows: int) -> str:
		"""Validate a read-only query and its row limit.

		This keyword guard rejects obvious writes and session changes; it is not
		a security sandbox, so run queries with least-privilege credentials.

		Args:
			sql: A single SELECT or WITH statement; one trailing semicolon is allowed.
			max_rows: Requested row limit, from 1 to ``_MAX_QUERY_ROWS``.

		Returns:
			The trimmed query without its trailing semicolon.

		Raises:
			ValueError: If the query is empty, has multiple statements, is not
				SELECT/WITH, contains a forbidden keyword, or the limit is out
				of range.
		"""
		if not isinstance(sql, str) or not sql.strip():
			raise ValueError("sql must be a non-empty read-only query")
		if not isinstance(max_rows, int) or not 1 <= max_rows <= cls._MAX_QUERY_ROWS:
			raise ValueError(f"max_rows must be between 1 and {cls._MAX_QUERY_ROWS}")
		query = sql.strip().rstrip(";").strip()
		if ";" in query or not re.match(r"^(select|with)\b", query, re.IGNORECASE):
			raise ValueError("Only a single SELECT or WITH query is allowed")
		if cls._FORBIDDEN_QUERY_WORDS.search(query):
			raise ValueError("Query contains a forbidden write or session-changing operation")
		return query

	def query_sql(
		self,
		table_bucket: str,
		catalog: str,
		sql: str,
		parameters: list[Any] | tuple[Any, ...] | None = None,
		max_rows: int = 100,
	) -> dict[str, Any]:
		"""Run a bounded read-only query against an attached catalog.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias referenced by the query.
			sql: A single SELECT or WITH statement.
			parameters: Optional positional query parameters.
			max_rows: Maximum number of rows to return.

		Returns:
			A mapping with the normalized ``query``, ``columns``, ``row_count``,
			``truncated`` (whether more rows existed), and ``rows`` as dictionaries.

		Raises:
			ValueError: If the query or row limit fails validation.
		"""
		query = self._validate_query(sql, max_rows)
		self.attach(table_bucket, catalog)
		cursor = self.engine.execute(query, parameters or [])
		rows = cursor.fetchmany(max_rows + 1)
		columns = [description[0] for description in cursor.description or []]
		truncated = len(rows) > max_rows
		rows = rows[:max_rows]
		return {
			"query": query,
			"columns": columns,
			"row_count": len(rows),
			"truncated": truncated,
			"rows": [dict(zip(columns, row)) for row in rows],
		}

	def list_namespaces(self, table_bucket: str, catalog: str) -> list[str]:
		"""Return namespace names in the attached catalog, sorted by name."""
		result = self.query_sql(
			table_bucket,
			catalog,
			f"SELECT schema_name FROM information_schema.schemata "
			f"WHERE catalog_name = '{catalog}' ORDER BY schema_name",
			max_rows=self._MAX_QUERY_ROWS,
		)
		return [row["schema_name"] for row in result["rows"]]

	def list_tables(self, table_bucket: str, catalog: str, namespace: str | None = None) -> list[dict[str, str]]:
		"""List tables in the attached catalog.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias.
			namespace: Optional namespace filter.

		Returns:
			Sorted ``namespace``/``table``/``type`` mappings.

		Raises:
			ValueError: If the namespace is not a safe identifier.
		"""
		query = (
			"SELECT table_schema, table_name, table_type "
			f"FROM information_schema.tables WHERE table_catalog = '{catalog}'"
		)
		parameters: list[Any] = []
		if namespace is not None:
			if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
				raise ValueError("Invalid namespace")
			query += " AND table_schema = ?"
			parameters.append(namespace)
		query += " ORDER BY table_schema, table_name"
		result = self.query_sql(table_bucket, catalog, query, parameters=parameters, max_rows=self._MAX_QUERY_ROWS)
		return [
			{"namespace": row["table_schema"], "table": row["table_name"], "type": row["table_type"]}
			for row in result["rows"]
		]

	def _insert_records(
		self,
		table_bucket: str,
		catalog: str,
		namespace: str,
		table: str,
		records: list[dict],
		*,
		logical_types: Mapping[str, str] | None = None,
	) -> int:
		"""Append records to a table in one transaction.

		Records are staged through Arrow into a temporary table shaped like the
		target, then inserted. Any failure rolls the transaction back.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Target table.
			records: Non-empty dictionaries with identical column order.
			logical_types: Schema logical type per column, used for staging
				conversions.

		Returns:
			The number of records inserted.

		Raises:
			ValueError: If identifiers, columns, or record shapes are invalid.
		"""
		for identifier in (namespace, table):
			if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
				raise ValueError("Invalid namespace or table name")
		if not records:
			return 0
		if not all(isinstance(record, dict) and record for record in records):
			raise ValueError("records must be a list of non-empty dictionaries")
		columns = list(records[0])
		if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) for column in columns):
			raise ValueError("Invalid column name")
		if any(list(record) != columns for record in records):
			raise ValueError("All records must have the same columns in the same order")
		self.attach(table_bucket, catalog)
		column_sql = ", ".join(f'"{column}"' for column in columns)
		target = f'{catalog}."{namespace}"."{table}"'
		stage = f"append_{uuid4().hex}_stage"
		batch_name = f"append_{uuid4().hex}_batch"
		self.engine.execute("BEGIN TRANSACTION")
		try:
			self.engine.execute(
				f'CREATE TEMP TABLE {stage} AS SELECT {column_sql} FROM {target} WHERE FALSE'
			)
			self._stage_arrow_records(stage, batch_name, columns, records, logical_types)
			self.engine.execute(
				f'INSERT INTO {target} ({column_sql}) SELECT {column_sql} FROM {stage}'
			)
			self.engine.execute(f"DROP TABLE {stage}")
			self.engine.execute("COMMIT")
		except Exception:
			try:
				self.engine.execute("ROLLBACK")
			except Exception:
				pass
			raise
		return len(records)

	def _stage_arrow_records(
		self,
		stage: str,
		batch_name: str,
		columns: list[str],
		records: list[dict],
		logical_types: Mapping[str, str] | None = None,
		sequence_column: str | None = None,
	) -> None:
		"""Insert records into a staging table in Arrow batches.

		JSON values are serialized to strings and decimals/dates to their string
		form so DuckDB casts them into the staging column types.

		Args:
			stage: Existing temporary table to insert into.
			batch_name: Name used to register each Arrow batch.
			columns: Record columns to stage, in order.
			records: Records to stage.
			logical_types: Schema logical type per column.
			sequence_column: Optional extra column filled with each record's
				input position, used to preserve batch order.

		Raises:
			ImportError: If pyarrow is not installed.
		"""
		try:
			import pyarrow as arrow
		except ImportError as exc:
			raise ImportError("Install oxidizer-lite[duckdb] to use Arrow-backed DuckDB writes") from exc
		column_sql = ", ".join(f'"{column}"' for column in columns)
		select_sql = column_sql
		if sequence_column is not None:
			select_sql += f', "{sequence_column}"'
		for offset in range(0, len(records), self._ARROW_BATCH_SIZE):
			chunk = records[offset:offset + self._ARROW_BATCH_SIZE]
			batch_columns = {}
			for column in columns:
				values = [record[column] for record in chunk]
				logical_type = (logical_types or {}).get(column)
				if logical_type == "json":
					values = [json.dumps(value, allow_nan=False) if value is not None else None for value in values]
				elif logical_type in {"decimal", "date"}:
					values = [str(value) if value is not None else None for value in values]
				batch_columns[column] = arrow.array(values)
			if sequence_column is not None:
				batch_columns[sequence_column] = arrow.array(
					range(offset, offset + len(chunk)), type=arrow.int64()
				)
			self.engine.register(batch_name, arrow.Table.from_pydict(batch_columns))
			try:
				self.engine.execute(f'INSERT INTO {stage} SELECT {select_sql} FROM {batch_name}')
			finally:
				self.engine.unregister(batch_name)

	def _scd2_records(
		self,
		table_bucket: str,
		catalog: str,
		namespace: str,
		table: str,
		records: list[dict],
		*,
		keys: list[str],
		order_by: str,
		history: Mapping[str, Any],
		late_arrivals: str = "reject",
		logical_types: Mapping[str, str] | None = None,
	) -> int:
		"""Apply SCD Type 2 history for a batch in one transaction.

		Existing history for the batch keys is checked for consistency first.
		Records already recorded at the same ordering value are skipped, and
		consecutive unchanged hashes collapse into one version. The previous
		current row is closed at the first new ``valid_from``, and new versions
		are inserted with ``valid_to`` set to the next version or the open-ended
		value. Any failure rolls the transaction back.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Target history table.
			records: Normalized records with derived history columns.
			keys: Business key columns.
			order_by: Column that orders versions; it becomes ``valid_from``.
			history: History column names and ``open_ended_value``.
			late_arrivals: ``reject`` raises on records older than the current
				version; ``ignore`` skips them and counts them in
				``last_ignored_late_records``.
			logical_types: Schema logical type per column.

		Returns:
			The number of input records processed.

		Raises:
			ValueError: If existing history is inconsistent, records conflict at
				the same ordering value, late arrivals are rejected, or batch
				order is not ascending per key.
		"""
		if late_arrivals not in {"reject", "ignore"}:
			raise ValueError("SCD2 late_arrivals must be 'reject' or 'ignore'")
		self.last_ignored_late_records = 0
		for identifier in (namespace, table):
			if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
				raise ValueError("Invalid namespace or table name")
		if not records:
			return 0
		columns = list(records[0])
		self.attach(table_bucket, catalog)
		target = f'{catalog}."{namespace}"."{table}"'
		prefix = f"scd2_{uuid4().hex}"
		stage, snapshot, pending, changes, plan = (
			f"{prefix}_{suffix}" for suffix in ("stage", "snapshot", "pending", "changes", "plan")
		)
		sequence = f"{prefix}_sequence"
		previous_order = f"{prefix}_previous_order"
		next_version = f"{prefix}_next_version"
		action = f"{prefix}_action"
		valid_from = f'"{history["valid_from"]}"'
		valid_to = f'"{history["valid_to"]}"'
		current = f'"{history["current_flag"]}"'
		record_hash = f'"{history["record_hash"]}"'
		ordering = f'"{order_by}"'
		key_sql = ", ".join(f'"{key}"' for key in keys)
		column_sql = ", ".join(f'"{column}"' for column in columns)
		key_match = " AND ".join(f'source."{key}" = existing."{key}"' for key in keys)
		current_key_match = " AND ".join(f'source."{key}" = current_row."{key}"' for key in keys)
		prior_key_match = " AND ".join(f'source."{key}" = prior."{key}"' for key in keys)
		merge_match = " AND ".join(f'target."{key}" = source."{key}"' for key in keys)
		source_keys = ", ".join(f'source."{key}"' for key in keys)
		snapshot_columns = ", ".join(f'existing."{column}"' for column in [*keys, *(
			history[field] for field in ("valid_from", "valid_to", "current_flag", "record_hash")
		)])
		input_columns = ", ".join(f'source."{column}"' for column in columns)

		self.engine.execute("BEGIN TRANSACTION")
		try:
			self.engine.execute(
				f'CREATE TEMP TABLE {stage} AS SELECT {column_sql}, CAST(NULL AS BIGINT) AS "{sequence}" '
				f"FROM {target} WHERE FALSE"
			)
			batch_name = f"{prefix}_batch"
			self._stage_arrow_records(stage, batch_name, columns, records, logical_types, sequence)
			self.engine.execute(
				f"CREATE TEMP TABLE {snapshot} AS SELECT {snapshot_columns} FROM {target} AS existing "
				f"WHERE EXISTS (SELECT 1 FROM {stage} AS source WHERE {key_match})"
			)
			checks = [
				(f"SELECT 1 FROM {snapshot} GROUP BY {key_sql}, {valid_from} HAVING COUNT(*) > 1",
				 "SCD2 history has duplicate versions for one key and valid_from"),
				(f"SELECT 1 FROM {snapshot} GROUP BY {key_sql} HAVING COUNT(*) FILTER (WHERE {current}) > 1",
				 "SCD2 target has multiple current rows for one key"),
				(f"SELECT 1 FROM {snapshot} GROUP BY {key_sql} HAVING COUNT(*) FILTER (WHERE {current}) = 0",
				 "SCD2 target has history but no current row for one key"),
				(f"SELECT 1 FROM {snapshot} WHERE {current} AND {valid_from} IS NULL",
				 "SCD2 current row has a null valid_from value"),
				(f"SELECT 1 FROM {snapshot} WHERE {current} AND {record_hash} IS NULL",
				 "SCD2 current row has a null record_hash value"),
				(f"SELECT 1 FROM {stage} GROUP BY {key_sql}, {ordering} HAVING COUNT(DISTINCT {record_hash}) > 1",
				 "SCD2 records with equal ordering values conflict"),
				(f"SELECT 1 FROM {stage} AS source JOIN {snapshot} AS existing ON {key_match} "
				 f"AND source.{ordering} = existing.{valid_from} "
				 f"WHERE source.{record_hash} IS DISTINCT FROM existing.{record_hash}",
				 "SCD2 records with equal ordering values conflict"),
			]
			for query, message in checks:
				if self.engine.execute(f"SELECT 1 FROM ({query}) AS invalid LIMIT 1").fetchone():
					raise ValueError(message)
			if self.engine.execute(
				f"SELECT 1 FROM {snapshot} WHERE {current} AND {valid_to} IS DISTINCT FROM ? LIMIT 1",
				[history["open_ended_value"]],
			).fetchone():
				raise ValueError("SCD2 current row has an invalid valid_to value")
			ignored_late_records = 0
			late_filter = ""
			if late_arrivals == "ignore":
				ignored_late_records = self.engine.execute(
					f"SELECT COUNT(*) FROM {stage} AS source JOIN {snapshot} AS current_row "
					f"ON {current_key_match} AND current_row.{current} "
					f"WHERE source.{ordering} < current_row.{valid_from} "
					f"AND NOT EXISTS (SELECT 1 FROM {snapshot} AS prior WHERE {prior_key_match} "
					f"AND source.{ordering} = prior.{valid_from})"
				).fetchone()[0]
				late_filter = (
					f"AND NOT EXISTS (SELECT 1 FROM {snapshot} AS current_row WHERE {current_key_match} "
					f"AND current_row.{current} AND source.{ordering} < current_row.{valid_from}) "
				)
			self.engine.execute(
				f"CREATE TEMP TABLE {pending} AS SELECT source.* FROM {stage} AS source "
				f"WHERE NOT EXISTS (SELECT 1 FROM {snapshot} AS existing WHERE {key_match} "
				f"AND source.{ordering} = existing.{valid_from}) "
				f"{late_filter}"
				f'QUALIFY ROW_NUMBER() OVER (PARTITION BY {key_sql}, {ordering} ORDER BY "{sequence}") = 1'
			)
			if late_arrivals == "reject" and self.engine.execute(
				f"SELECT 1 FROM {pending} AS source JOIN {snapshot} AS existing ON {key_match} "
				f"WHERE existing.{current} AND source.{ordering} <= existing.{valid_from} LIMIT 1"
			).fetchone():
				raise ValueError("SCD2 late-arriving record is older than the current version")
			if self.engine.execute(
				f'SELECT 1 FROM (SELECT {ordering}, LAG({ordering}) OVER '
				f'(PARTITION BY {key_sql} ORDER BY "{sequence}") AS "{previous_order}" FROM {pending}) AS ordered '
				f'WHERE {ordering} < "{previous_order}" LIMIT 1'
			).fetchone():
				raise ValueError("SCD2 late-arriving records are not in ascending batch order")
			self.engine.execute(
				f"CREATE TEMP TABLE {changes} AS SELECT {input_columns}, source.\"{sequence}\" "
				f"FROM {pending} AS source LEFT JOIN {snapshot} AS existing "
				f"ON {key_match} AND existing.{current} "
				f"QUALIFY source.{record_hash} IS DISTINCT FROM COALESCE("
				f"LAG(source.{record_hash}) OVER (PARTITION BY {source_keys} "
				f'ORDER BY source."{sequence}"), existing.{record_hash})'
			)
			self.engine.execute(
				f"CREATE TEMP TABLE {plan} AS SELECT *, LEAD({valid_from}) OVER "
				f'(PARTITION BY {key_sql} ORDER BY "{sequence}") AS "{next_version}" FROM {changes}'
			)
			insert_expressions = []
			for column in columns:
				if column == history["valid_to"]:
					insert_expressions.append(f'COALESCE(source."{next_version}", source.{valid_to})')
				elif column == history["current_flag"]:
					insert_expressions.append(f'source."{next_version}" IS NULL')
				else:
					insert_expressions.append(f'source."{column}"')
			merge_source = (
				f'SELECT *, FALSE AS "{action}" FROM {plan} UNION ALL '
				f'SELECT *, TRUE AS "{action}" FROM {plan} '
				f'QUALIFY ROW_NUMBER() OVER (PARTITION BY {key_sql} ORDER BY "{sequence}") = 1'
			)
			self.engine.execute(
				f"MERGE INTO {target} AS target USING ({merge_source}) AS source "
				f'ON source."{action}" AND {merge_match} AND target.{current} '
				f"WHEN MATCHED THEN UPDATE SET {valid_to} = source.{valid_from}, {current} = FALSE "
				f'WHEN NOT MATCHED AND NOT source."{action}" THEN INSERT ({column_sql}) '
				f"VALUES ({', '.join(insert_expressions)})"
			)
			for temporary in (plan, changes, pending, snapshot, stage):
				self.engine.execute(f"DROP TABLE {temporary}")
			self.engine.execute("COMMIT")
			self.last_ignored_late_records = ignored_late_records
		except Exception:
			try:
				self.engine.execute("ROLLBACK")
			except Exception:
				pass
			raise
		return len(records)

	def _merge_records(
		self,
		table_bucket: str,
		catalog: str,
		namespace: str,
		table: str,
		records: list[dict],
		*,
		keys: list[str],
		order_by: str | None,
		direction: str | None,
		update_columns: list[str],
		tie_columns: list[str] | None = None,
		logical_types: Mapping[str, str] | None = None,
	) -> int:
		"""Upsert records with ``MERGE`` in one transaction (upsert and SCD1).

		Without an ordering column, only missing keys are inserted. With one,
		matched rows update only when the incoming value wins according to
		``direction`` or the target value is null. Equal ordering values with
		different ``tie_columns`` are rejected.

		Args:
			table_bucket: Bucket name or ARN to attach.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Target table.
			records: Normalized records with identical column order.
			keys: Merge key columns.
			order_by: Optional conflict-resolution column.
			direction: ``ascending`` (newer is greater) or ``descending``.
			update_columns: Columns overwritten on a winning match.
			tie_columns: Columns compared on ordering ties; defaults to
				``update_columns``.
			logical_types: Schema logical type per column.

		Returns:
			The number of input records processed.

		Raises:
			ValueError: If identifiers, record shapes, or key/update columns are
				invalid, or records conflict on a tie.
		"""
		for identifier in (namespace, table):
			if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
				raise ValueError("Invalid namespace or table name")
		if not records:
			return 0
		if not all(isinstance(record, dict) and record for record in records):
			raise ValueError("records must be a list of non-empty dictionaries")
		columns = list(records[0])
		if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) for column in columns):
			raise ValueError("Invalid column name")
		if any(list(record) != columns for record in records):
			raise ValueError("All records must have the same columns in the same order")
		if not keys or any(key not in columns for key in keys):
			raise ValueError("Upsert keys must be present in every record")
		if any(column not in columns for column in update_columns):
			raise ValueError("Upsert update columns must be present in every record")
		self.attach(table_bucket, catalog)
		quoted_columns = ", ".join(f'"{column}"' for column in columns)
		target = f'{catalog}."{namespace}"."{table}"'
		stage = f"merge_{uuid4().hex}_stage"
		batch_name = f"merge_{uuid4().hex}_batch"
		key_match = " AND ".join(f'target."{key}" = source."{key}"' for key in keys)
		insert_values = ", ".join(f'source."{column}"' for column in columns)
		merge_sql = (
			f"MERGE INTO {target} AS target USING {stage} AS source ON {key_match} "
			f'WHEN NOT MATCHED THEN INSERT ({quoted_columns}) VALUES ({insert_values})'
		)
		if order_by is not None and update_columns:
			comparison = ">" if direction == "descending" else "<"
			assignments = ", ".join(f'"{column}" = source."{column}"' for column in update_columns)
			merge_sql = (
				f"MERGE INTO {target} AS target USING {stage} AS source ON {key_match} "
				f'WHEN MATCHED AND (target."{order_by}" IS NULL OR '
				f'source."{order_by}" {comparison} target."{order_by}") '
				f"THEN UPDATE SET {assignments} "
				f'WHEN NOT MATCHED THEN INSERT ({quoted_columns}) VALUES ({insert_values})'
			)

		self.engine.execute("BEGIN TRANSACTION")
		try:
			self.engine.execute(
				f'CREATE TEMP TABLE {stage} AS SELECT {quoted_columns} FROM {target} WHERE FALSE'
			)
			self._stage_arrow_records(stage, batch_name, columns, records, logical_types)
			if order_by is not None and update_columns:
				changed_columns = " OR ".join(
					f'target."{column}" IS DISTINCT FROM source."{column}"'
					for column in (tie_columns if tie_columns is not None else update_columns)
				)
				tie_sql = (
					f"SELECT 1 FROM {stage} AS source JOIN {target} AS target ON {key_match} "
					f'WHERE source."{order_by}" = target."{order_by}" '
					f"AND ({changed_columns}) LIMIT 1"
				)
				if changed_columns and self.engine.execute(tie_sql).fetchone() is not None:
					raise ValueError("Records with equal ordering values conflict")
			self.engine.execute(merge_sql)
			self.engine.execute(f"DROP TABLE {stage}")
			self.engine.execute("COMMIT")
		except Exception:
			try:
				self.engine.execute("ROLLBACK")
			except Exception:
				pass
			raise
		return len(records)


class SparkCrucible:
	"""Placeholder Spark adapter; every operation raises ``NotImplementedError``."""

	def __init__(self, connection: S3TablesConnection | IcebergRestConnection):
		self.connection = connection

	def query(self, table_bucket_arn: str, catalog: str, namespace: str, table: str) -> list[tuple]:
		raise NotImplementedError("Spark S3 Tables adapter is not implemented yet")

	def attach(self, table_bucket: str, catalog: str) -> None:
		raise NotImplementedError("Spark catalog attachment is not implemented yet")

	def query_sql(self, table_bucket: str, catalog: str, sql: str, parameters=None, max_rows: int = 100):
		raise NotImplementedError("Spark SQL query adapter is not implemented yet")

	def list_namespaces(self, table_bucket: str, catalog: str) -> list[str]:
		raise NotImplementedError("Spark namespace discovery is not implemented yet")

	def list_tables(self, table_bucket: str, catalog: str, namespace: str | None = None) -> list[dict[str, str]]:
		raise NotImplementedError("Spark table discovery is not implemented yet")

	def _ensure_table_schema(self, table_bucket, catalog, namespace, table, columns):
		raise NotImplementedError("Spark S3 Tables adapter is not implemented yet")

	def _insert_records(
		self,
		table_bucket: str,
		catalog: str,
		namespace: str,
		table: str,
		records: list[dict],
		*,
		logical_types: Mapping[str, str] | None = None,
	) -> int:
		raise NotImplementedError("Spark S3 Tables adapter is not implemented yet")


class Crucible:
	"""Schema-validated table I/O and SQL/S3 batch reads for Oxidizer.

	An instance wraps one table-bucket connection and delegates engine work to
	a DuckDB (or placeholder Spark) adapter. ``write`` validates records against
	a versioned schema document before append, upsert, SCD1, or SCD2 writes.
	The ``read_sql_batch`` and ``read_s3_batch`` class methods open a
	short-lived DuckDB session per page for workflow inputs.
	"""

	_LOGICAL_TYPES = {
		"boolean", "int32", "int64", "float", "double", "decimal",
		"string", "date", "timestamp", "json", "binary",
	}
	_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

	@staticmethod
	def _parse_jsonpath(ref: str):
		"""Compile a JSONPath column ref.

		Raises:
			ImportError: If jsonpath-ng is not installed.
			ValueError: If the expression is invalid.
		"""
		try:
			from jsonpath_ng.ext import parse
		except ImportError as exc:
			raise ImportError("Install oxidizer-lite[duckdb] to use schema column refs") from exc
		try:
			return parse(ref)
		except Exception as exc:
			raise ValueError(f"Invalid JSONPath ref: {ref!r}") from exc

	def __init__(
		self,
		connection: S3TablesConnection | IcebergRestConnection,
		*,
		adapter: str = "duckdb",
		engine: Any = None,
		residue: Residue | None = None,
	):
		"""Create a Crucible for one table-bucket connection.

		Args:
			connection: AWS S3 Tables or RustFS Iceberg REST connection.
			adapter: ``duckdb`` (default) or ``spark`` (not implemented).
			engine: Optional DuckDB engine shared with the adapter.
			residue: Optional logger for write diagnostics.

		Raises:
			TypeError: If the connection type is unsupported.
			ValueError: If the adapter name is unknown.
		"""
		if not isinstance(connection, (S3TablesConnection, IcebergRestConnection)):
			raise TypeError("Crucible requires an S3TablesConnection or IcebergRestConnection")
		self.residue = (residue or Residue()).bind(component_name="crucible")
		if adapter == "duckdb":
			self.adapter = DuckDBCrucible(connection, engine=engine)
		elif adapter == "spark":
			self.adapter = SparkCrucible(connection)
		else:
			raise ValueError(f"Unsupported Crucible adapter: {adapter}")

	@classmethod
	def read_sql_batch(
		cls, connections: Mapping[str, Mapping[str, Any]], sql: str, *,
		batch_size: int = 1000, offset: int = 0,
		parameters: list[Any] | None = None, residue: Residue | None = None,
	) -> dict[str, Any]:
		"""Read one page of a read-only query across named catalogs.

		Each call opens a fresh DuckDB session, attaches every catalog, runs
		``SELECT * FROM (sql) LIMIT batch_size + 1 OFFSET offset``, and closes
		the session. The extra lookahead row decides finality without an empty
		read. Pages do not share a snapshot, so the query needs a deterministic
		``ORDER BY``.

		Args:
			connections: ``s3_tables`` connection documents keyed by catalog alias.
			sql: A single SELECT or WITH statement referencing those aliases.
			batch_size: Maximum rows per page.
			offset: Rows to skip; the previous page's ``next_cursor``.
			parameters: Optional positional query parameters.
			residue: Optional logger passed to the connections.

		Returns:
			A mapping with ``rows`` (dictionaries), ``is_final``, and
			``next_cursor`` (the next offset, or ``None`` when final).

		Raises:
			ValueError: If the query, paging options, parameters, catalog
				aliases, or connection documents are invalid, or result column
				names are duplicated.
		"""
		if not isinstance(sql, str) or not sql.strip():
			raise ValueError("SQL input requires a non-empty query")
		query = DuckDBCrucible._validate_query(sql, 1)
		if type(batch_size) is not int or batch_size < 1:
			raise ValueError("SQL batch_size must be a positive integer")
		if type(offset) is not int or offset < 0:
			raise ValueError("SQL cursor must be a non-negative integer offset")
		if parameters is not None and not isinstance(parameters, list):
			raise ValueError("SQL parameters must be a list")
		if not isinstance(connections, Mapping) or not connections:
			raise ValueError("SQL input requires at least one catalog connection")
		for catalog in connections:
			DuckDBCrucible._validate_catalog(catalog)
		import duckdb
		with duckdb.connect() as engine:
			for catalog, document in connections.items():
				if not isinstance(document, Mapping):
					raise ValueError("SQL input connections must be mappings")
				settings = document.get("connection", document)
				if not isinstance(settings, Mapping) or settings.get("type") != "s3_tables":
					raise ValueError("SQL input connections must have type s3_tables")
				bucket = settings.get("aws_s3_table_bucket")
				if not isinstance(bucket, str) or not bucket:
					raise ValueError("SQL input connection requires aws_s3_table_bucket")
				options = {
					"region_name": settings.get("region_name", settings.get("aws_region", "us-east-1")),
					"aws_profile_name": settings.get("aws_profile_name", settings.get("aws_sso_profile")),
					"access_key_id": settings.get("access_key_id"),
					"secret_access_key": settings.get("secret_access_key"),
					"session_token": settings.get("session_token"),
					"role_arn": settings.get("role_arn"),
					"role_session_name": settings.get("role_session_name", "OxidizerS3TablesSession"),
					"external_id": settings.get("external_id"),
				}
				if settings.get("endpoint_url"):
					connection = IcebergRestConnection(
						S3Connection(bucket=bucket, endpoint_url=settings["endpoint_url"], lazy_client=True,
							addressing_style=settings.get("addressing_style", "path"), residue=residue, **options),
						warehouse=bucket, catalog_endpoint=settings.get("catalog_endpoint"),
						signing_service=settings.get("signing_service", "s3"),
					)
				else:
					connection = S3TablesConnection(residue=residue, **options)
				cls(connection, engine=engine, residue=residue).attach(bucket, catalog)
			cursor = engine.execute(
				f"SELECT * FROM ({query}) AS oxidizer_sql_input LIMIT ? OFFSET ?",
				[*(parameters or []), batch_size + 1, offset],
			)
			rows = cursor.fetchmany(batch_size + 1)
			columns = [description[0] for description in cursor.description or []]
			if len(columns) != len(set(columns)):
				raise ValueError("SQL input columns must have unique names; alias joined columns")
			is_final = len(rows) <= batch_size
			return {
				"rows": [dict(zip(columns, row)) for row in rows[:batch_size]],
				"is_final": is_final,
				"next_cursor": None if is_final else offset + batch_size,
			}

	_S3_FORMATS = {"parquet": "read_parquet", "csv": "read_csv_auto"}
	_ORDER_BY = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", re.IGNORECASE)

	@classmethod
	def read_s3_batch(
		cls, connection: Mapping[str, Any], paths: str | list[str], *,
		file_format: str | None = None, order_by: str | list[str] | None = None,
		batch_size: int = 1000, offset: int = 0, residue: Residue | None = None,
	) -> dict[str, Any]:
		"""Read one ordered page of Parquet or CSV objects from S3.

		Each call opens a fresh DuckDB session with an S3 secret from the
		connection (static keys, or the credential chain/SSO profile, plus an
		optional S3-compatible endpoint). It then runs
		``SELECT * FROM read_*(paths) ORDER BY ... LIMIT batch_size + 1 OFFSET offset``.
		The extra lookahead row decides finality, and values are converted to
		JSON-compatible types.

		Args:
			connection: Connection document with ``type: s3`` and an optional
				default ``bucket``.
			paths: One path or a list of paths. Paths without a scheme are
				relative to the connection bucket; globs are passed to DuckDB.
			file_format: ``parquet`` or ``csv``. When omitted, it is inferred
				from the extensions, which must all agree.
			order_by: Column name or list of names, each optionally followed by
				``ASC``/``DESC``. Use a unique ordering for stable paging.
			batch_size: Maximum rows per page.
			offset: Rows to skip; the previous page's ``next_cursor``.
			residue: Optional logger for the page read event.

		Returns:
			A mapping with ``rows`` (JSON-compatible dictionaries), ``is_final``,
			and ``next_cursor`` (the next offset, or ``None`` when final).

		Raises:
			ValueError: If the connection is not ``type: s3``, paths are empty or
				not ``s3://``, the format is unknown or ambiguous, an
				``order_by`` term is invalid, or paging options are invalid.
		"""
		settings = connection.get("connection", connection) if isinstance(connection, Mapping) else None
		if not isinstance(settings, Mapping) or settings.get("type") != "s3":
			raise ValueError("S3 input connection must have type s3")
		if type(batch_size) is not int or batch_size < 1:
			raise ValueError("S3 batch_size must be a positive integer")
		if type(offset) is not int or offset < 0:
			raise ValueError("S3 cursor must be a non-negative integer offset")
		bucket = settings.get("bucket")
		resolved = []
		for path in [paths] if isinstance(paths, str) else paths if isinstance(paths, list) else []:
			if not isinstance(path, str) or not path:
				raise ValueError("s3_path values must be non-empty strings")
			if "://" not in path:
				if not bucket:
					raise ValueError("Relative s3_path requires a connection bucket")
				path = f"s3://{bucket}/{path.lstrip('/')}"
			if not path.startswith("s3://"):
				raise ValueError("s3_path must use the s3:// scheme")
			resolved.append(path)
		if not resolved:
			raise ValueError("S3 input requires at least one s3_path")
		if file_format is None:
			formats = {
				"parquet" if path.endswith(".parquet") else "csv" if re.search(r"\.csv(\.gz)?$", path) else None
				for path in resolved
			}
			if len(formats) != 1 or None in formats:
				raise ValueError("S3 input format must be set when paths are not all .parquet or all .csv")
			file_format = formats.pop()
		reader = cls._S3_FORMATS.get(str(file_format).lower())
		if reader is None:
			raise ValueError("S3 input format must be parquet or csv")
		order_terms = []
		for term in [order_by] if isinstance(order_by, str) else order_by or []:
			match = cls._ORDER_BY.fullmatch(term.strip()) if isinstance(term, str) else None
			if match is None:
				raise ValueError(f"Invalid S3 order_by term: {term!r}")
			order_terms.append(f'"{match.group(1)}" {(match.group(2) or "ASC").upper()}')

		secret = ["TYPE s3", f"REGION {_sql_string(settings.get('region_name', settings.get('aws_region', 'us-east-1')))}"]
		role_arn = settings.get("role_arn")
		if role_arn:
			if settings.get("access_key_id") or settings.get("secret_access_key") or settings.get("session_token"):
				raise ValueError("S3 input role_arn requires AWS credential-chain credentials, not explicit keys")
			secret.extend(("PROVIDER credential_chain", "CHAIN 'sts'", f"ASSUME_ROLE_ARN {_sql_string(role_arn)}"))
			external_id = settings.get("external_id")
			if external_id:
				secret.append(f"EXTERNAL_ID {_sql_string(external_id)}")
			profile = settings.get("aws_profile_name", settings.get("aws_sso_profile"))
			if profile:
				secret.append(f"PROFILE {_sql_string(profile)}")
		elif settings.get("access_key_id"):
			secret += ["PROVIDER config", f"KEY_ID {_sql_string(settings['access_key_id'])}",
				f"SECRET {_sql_string(settings.get('secret_access_key') or '')}"]
			if settings.get("session_token"):
				secret.append(f"SESSION_TOKEN {_sql_string(settings['session_token'])}")
		else:
			secret.append("PROVIDER credential_chain")
			profile = settings.get("aws_profile_name", settings.get("aws_sso_profile"))
			if profile:
				secret += ["CHAIN 'sso'", f"PROFILE {_sql_string(profile)}"]
		endpoint_url = settings.get("endpoint_url")
		if endpoint_url:
			secret += [
				f"ENDPOINT {_sql_string(endpoint_url.removeprefix('http://').removeprefix('https://'))}",
				f"URL_STYLE {_sql_string(settings.get('addressing_style', 'path'))}",
				f"USE_SSL {str(endpoint_url.startswith('https://')).lower()}",
			]
		import duckdb
		with duckdb.connect() as engine:
			extensions = ("httpfs",) if settings.get("access_key_id") else ("httpfs", "aws")
			for extension in extensions:
				engine.execute(f"INSTALL {extension}")
				engine.execute(f"LOAD {extension}")
			engine.execute(f"CREATE SECRET s3_input ({', '.join(secret)})")
			source = "[" + ", ".join(_sql_string(path) for path in resolved) + "]"
			order_sql = f" ORDER BY {', '.join(order_terms)}" if order_terms else ""
			cursor = engine.execute(
				f"SELECT * FROM {reader}({source}){order_sql} LIMIT ? OFFSET ?", [batch_size + 1, offset]
			)
			rows = cursor.fetchmany(batch_size + 1)
			columns = [description[0] for description in cursor.description or []]
		is_final = len(rows) <= batch_size
		(residue or Residue()).emit(Ash.DEBUG, "Read S3 input page", paths=resolved, offset=offset, rows=min(len(rows), batch_size))
		return {
			"rows": [{column: _json_safe(value) for column, value in zip(columns, row)} for row in rows[:batch_size]],
			"is_final": is_final,
			"next_cursor": None if is_final else offset + batch_size,
		}

	def close(self) -> None:
		"""Close the DuckDB engine and reset attachments; repeated calls are safe."""
		if isinstance(self.adapter, DuckDBCrucible) and self.adapter.engine is not None:
			self.adapter.engine.close()
			self.adapter.engine = None
			self.adapter.table_bucket = None
			self.adapter.table_bucket_arn = None
			self.adapter._attached_catalogs.clear()

	def query(self, table_bucket: str, catalog: str, namespace: str, table: str) -> list[tuple]:
		"""Return every row of one table; see ``DuckDBCrucible.query``."""
		return self.adapter.query(table_bucket, catalog, namespace, table)

	def attach(self, table_bucket: str, catalog: str) -> None:
		"""Attach a table bucket as a catalog alias; see ``DuckDBCrucible.attach``."""
		return self.adapter.attach(table_bucket, catalog)

	def query_sql(self, table_bucket: str, catalog: str, sql: str, parameters=None, max_rows: int = 100) -> dict[str, Any]:
		"""Run a bounded read-only query; see ``DuckDBCrucible.query_sql``."""
		return self.adapter.query_sql(table_bucket, catalog, sql, parameters=parameters, max_rows=max_rows)

	def list_namespaces(self, table_bucket: str, catalog: str) -> list[str]:
		"""Return sorted namespace names in the attached catalog."""
		return self.adapter.list_namespaces(table_bucket, catalog)

	def list_tables(self, table_bucket: str, catalog: str, namespace: str | None = None) -> list[dict[str, str]]:
		"""List tables, optionally within one namespace."""
		return self.adapter.list_tables(table_bucket, catalog, namespace=namespace)

	def ensure_table(
		self, table_bucket: str, catalog: str, namespace: str, table: str,
		*, schema: Mapping[str, Any],
	) -> None:
		"""Validate a schema document and create or reconcile its table.

		Args:
			table_bucket: Bucket name or ARN.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Table name.
			schema: Versioned schema document.

		Raises:
			ValueError: If the schema is invalid or the existing table is
				incompatible with it.
		"""
		context = self._schema_context(schema)
		self.adapter._ensure_table_schema(table_bucket, catalog, namespace, table, context["columns"])

	@staticmethod
	def _valid_value(logical_type: str, value: Any, column: Mapping[str, Any]) -> bool:
		"""Return whether a Python value is valid for a schema column.

		``None`` is valid only for nullable columns. Integers are range-checked,
		floats must be finite, decimals must fit precision/scale, dates accept
		``date`` or ISO strings, and timestamps must be ISO strings ending in ``Z``.
		"""
		if value is None:
			return bool(column["nullable"])
		if logical_type == "boolean":
			return type(value) is bool
		if logical_type in ("int32", "int64"):
			if type(value) is not int:
				return False
			bits = 32 if logical_type == "int32" else 64
			return -(2 ** (bits - 1)) <= value < 2 ** (bits - 1)
		if logical_type in ("float", "double"):
			if type(value) not in (int, float):
				return False
			try:
				return math.isfinite(float(value))
			except OverflowError:
				return False
		if logical_type == "decimal":
			if type(value) is bool:
				return False
			try:
				decimal_value = Decimal(value) if isinstance(value, (str, int)) else value
			except (InvalidOperation, TypeError, ValueError):
				return False
			if not isinstance(decimal_value, Decimal) or not decimal_value.is_finite():
				return False
			precision = column.get("precision")
			scale = column.get("scale")
			if type(precision) is not int or type(scale) is not int or precision < 1 or scale < 0 or scale > precision:
				return False
			tuple_value = decimal_value.as_tuple()
			fractional_digits = max(-tuple_value.exponent, 0)
			integer_digits = max(len(tuple_value.digits) + tuple_value.exponent, 0)
			return fractional_digits <= scale and integer_digits <= precision - scale
		if logical_type == "string":
			return isinstance(value, str)
		if logical_type == "date":
			if isinstance(value, datetime):
				return False
			if isinstance(value, date):
				return True
			if not isinstance(value, str):
				return False
			try:
				date.fromisoformat(value)
				return True
			except ValueError:
				return False
		if logical_type == "timestamp":
			if not isinstance(value, str) or not value.endswith("Z") or "T" not in value:
				return False
			try:
				datetime.fromisoformat(value[:-1] + "+00:00")
				return True
			except ValueError:
				return False
		if logical_type == "json":
			try:
				json.dumps(value, allow_nan=False)
				return True
			except (TypeError, ValueError):
				return False
		if logical_type == "binary":
			return isinstance(value, bytes)
		return False

	@classmethod
	def _scd2_record_hash(
		cls,
		record: Mapping[str, Any],
		compare_columns: list[str],
		columns_by_name: Mapping[str, Mapping[str, Any]],
	) -> str:
		"""Return a SHA-256 hash of the compare columns for SCD2 change detection.

		Values are canonicalized by logical type first, such as normalized
		decimals, UTC timestamps, and sorted JSON, so equivalent inputs hash
		identically.
		"""
		values = []
		for name in compare_columns:
			logical_type = columns_by_name[name]["type"]
			value = record[name]
			if value is not None:
				if logical_type == "decimal":
					decimal_value = Decimal(str(value)).normalize()
					value = format(decimal_value, "f")
				elif logical_type == "date":
					value = value.isoformat() if isinstance(value, date) else date.fromisoformat(value).isoformat()
				elif logical_type == "timestamp":
					timestamp = datetime.fromisoformat(value[:-1] + "+00:00")
					value = timestamp.astimezone(timezone.utc).isoformat()
				elif logical_type == "binary":
					value = value.hex()
				elif logical_type == "json":
					value = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
				elif logical_type in {"float", "double"}:
					value = float(value)
			values.append((name, logical_type, value))
		payload = json.dumps(values, sort_keys=True, separators=(",", ":"), allow_nan=False)
		return hashlib.sha256(payload.encode("utf-8")).hexdigest()

	@classmethod
	def _schema_context(cls, schema: Mapping[str, Any]) -> dict[str, Any]:
		"""Validate a schema document and index it for writes.

		Args:
			schema: Document with ``templateVersion: 1``, ``type: schema``, and a
				``schema`` table definition.

		Returns:
			The write ``operation`` and ``write`` options, ``columns``,
			``column_names``, ``columns_by_name``, compiled ``ref_expressions``,
			and ``keys``.

		Raises:
			ValueError: If the envelope, operation, columns, types, defaults, or
				keys are invalid, or ``evolution`` is present.
		"""
		if not isinstance(schema, Mapping) or type(schema.get("templateVersion")) is not int or schema["templateVersion"] != 1:
			raise ValueError("Schema must use templateVersion 1")
		if schema.get("type") != "schema" or not isinstance(schema.get("name"), str) or not schema["name"]:
			raise ValueError("Schema must have type 'schema' and a non-empty name")
		definition = schema.get("schema")
		if not isinstance(definition, Mapping):
			raise ValueError("Schema definition must be a mapping")
		if type(definition.get("version")) is not int or definition["version"] < 1:
			raise ValueError("Schema version must be a positive integer")
		if definition.get("kind", "table") != "table":
			raise ValueError("Only table schemas are supported for writes")
		write = definition.get("write")
		if (
			not isinstance(write, Mapping)
			or not isinstance(write.get("operation"), str)
			or write["operation"] not in {"append", "upsert", "scd1", "scd2"}
		):
			raise ValueError("Schema write.operation must be 'append', 'upsert', 'scd1', or 'scd2'")
		if "evolution" in definition:
			raise ValueError("Schema evolution is deferred and schema.evolution is not supported")
		columns = definition.get("columns")
		if not isinstance(columns, list) or not columns:
			raise ValueError("Schema columns must be a non-empty list")
		column_names = []
		ref_expressions = {}
		for column in columns:
			if not isinstance(column, Mapping):
				raise ValueError("Each schema column must be a mapping")
			name = column.get("name")
			logical_type = column.get("type")
			if not isinstance(name, str) or not cls._IDENTIFIER.fullmatch(name):
				raise ValueError("Invalid schema column name")
			if name in column_names:
				raise ValueError(f"Duplicate schema column: {name}")
			if "ref" in column:
				ref = column["ref"]
				if not isinstance(ref, str) or not ref.startswith("$"):
					raise ValueError(f"Schema column {name!r} ref must be an absolute JSONPath string")
				ref_expressions[name] = cls._parse_jsonpath(ref)
			if not isinstance(logical_type, str) or logical_type not in cls._LOGICAL_TYPES:
				raise ValueError(f"Unsupported schema logical type: {logical_type}")
			if type(column.get("nullable")) is not bool:
				raise ValueError(f"Schema column {name!r} must declare nullable as a boolean")
			if logical_type == "decimal" and (
				type(column.get("precision")) is not int
				or type(column.get("scale")) is not int
				or column["precision"] < 1
				or column["scale"] < 0
				or column["scale"] > column["precision"]
			):
				raise ValueError(f"Decimal column {name!r} requires valid precision and scale")
			if "default" in column and not cls._valid_value(logical_type, column["default"], column):
				raise ValueError(f"Invalid default for schema column {name!r}")
			column_names.append(name)
		keys = definition.get("keys", [])
		if (
			not isinstance(keys, list)
			or any(not isinstance(key, str) for key in keys)
			or len(set(keys)) != len(keys)
			or any(key not in column_names for key in keys)
		):
			raise ValueError("Schema keys must be unique declared column names")
		return {
			"operation": write["operation"],
			"write": write,
			"columns": columns,
			"column_names": column_names,
			"columns_by_name": {column["name"]: column for column in columns},
			"ref_expressions": ref_expressions,
			"keys": keys,
		}

	@classmethod
	def _normalize_records(
		cls,
		context: Mapping[str, Any],
		records: list[dict],
		*,
		order_by: str | None = None,
		managed_columns: set[str] | None = None,
	) -> list[dict[str, Any]]:
		"""Project records onto the schema columns and validate every value.

		Each value comes from the column's JSONPath ``ref``, the same-named key,
		the column default, or ``None`` for nullable columns. Managed columns are
		left as ``None`` for the caller to derive. Keys and the ordering column
		must be non-null, and upsert/SCD1 batches must not repeat a key.

		Args:
			context: Result of ``_schema_context``.
			records: Input records.
			order_by: Optional write-ordering column that must be non-null.
			managed_columns: Columns populated by the caller, such as SCD2 history.

		Returns:
			Records with exactly the schema columns, in schema order.

		Raises:
			ValueError: If a record is not a mapping, a required value is missing
				or invalid, a ref matches several values, or a key is duplicated.
		"""
		if not isinstance(records, list):
			raise ValueError("Schema write records must be a list")
		managed_columns = managed_columns or set()
		normalized = []
		seen_keys = set()
		keys = context["keys"]
		columns_by_name = context["columns_by_name"]
		ref_expressions = context["ref_expressions"]
		for record_index, record in enumerate(records):
			if not isinstance(record, Mapping):
				raise ValueError(f"Record {record_index} must be a mapping")
			output_record = {}
			for column in context["columns"]:
				name = column["name"]
				managed = name in managed_columns
				if managed:
					value = None
				elif name in ref_expressions:
					matches = ref_expressions[name].find(record)
					if len(matches) > 1:
						raise ValueError(
							f"JSONPath ref for record {record_index}, column {name!r} matched multiple values"
						)
					if matches:
						value = matches[0].value
					elif "default" in column:
						value = column["default"]
					elif column["nullable"]:
						value = None
					else:
						raise ValueError(f"Record {record_index} is missing required column {name!r}")
				elif name in record:
					value = record[name]
				elif "default" in column:
					value = column["default"]
				elif column["nullable"]:
					value = None
				else:
					raise ValueError(f"Record {record_index} is missing required column {name!r}")
				if not managed and not cls._valid_value(column["type"], value, column):
					raise ValueError(f"Invalid value for record {record_index}, column {name!r}")
				if name in keys and value is None:
					raise ValueError(f"Record {record_index} has null key column {name!r}")
				if name == order_by and value is None:
					raise ValueError(f"Record {record_index} has null write ordering column {name!r}")
				output_record[name] = value
			if context["operation"] in {"upsert", "scd1"}:
				key_parts = []
				for key in keys:
					value = output_record[key]
					logical_type = columns_by_name[key]["type"]
					if logical_type == "decimal":
						value = Decimal(str(value))
					elif logical_type == "json":
						value = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
					elif logical_type == "binary":
						value = value.hex()
					elif logical_type == "date":
						value = date.fromisoformat(value) if isinstance(value, str) else value
					key_parts.append(value)
				key_identity = tuple(key_parts)
				if key_identity in seen_keys:
					raise ValueError(f"Duplicate key in write batch at record {record_index}")
				seen_keys.add(key_identity)
			normalized.append(output_record)
		return normalized

	@classmethod
	def _prepare_append(cls, context: Mapping[str, Any], records: list[dict]):
		"""Normalize records for an append write; returns (columns, records, options)."""
		if context["operation"] != "append":
			raise ValueError("Append preparation requires write.operation 'append'")
		return context["columns"], cls._normalize_records(context, records), {"operation": "append"}

	@classmethod
	def _prepare_upsert(cls, context: Mapping[str, Any], records: list[dict]):
		"""Validate upsert options and normalize records.

		``insert_only`` mode inserts only new keys. ``all_columns`` mode requires
		a non-key, orderable ``conflict.order_by`` and ``direction`` that decide
		which version wins.

		Returns:
			A tuple of columns, normalized records, and merge options.

		Raises:
			ValueError: If keys or update/conflict settings are invalid.
		"""
		if context["operation"] != "upsert":
			raise ValueError("Upsert preparation requires write.operation 'upsert'")
		keys = context["keys"]
		if not keys:
			raise ValueError("Upsert requires at least one schema key")
		write = context["write"]
		update = write.get("update", {})
		if not isinstance(update, Mapping):
			raise ValueError("Upsert write.update must be a mapping")
		update_mode = update.get("mode", "all_columns")
		if not isinstance(update_mode, str) or update_mode not in {"all_columns", "insert_only"}:
			raise ValueError("Upsert update.mode must be 'all_columns' or 'insert_only'")
		order_by = None
		direction = None
		update_columns = []
		if update_mode == "all_columns":
			conflict = write.get("conflict")
			if not isinstance(conflict, Mapping):
				raise ValueError("Upsert all_columns mode requires write.conflict")
			order_by = conflict.get("order_by")
			direction = conflict.get("direction")
			if not isinstance(order_by, str) or order_by not in context["column_names"]:
				raise ValueError("Upsert conflict.order_by must name a declared column")
			if not isinstance(direction, str) or direction not in {"ascending", "descending"}:
				raise ValueError("Upsert conflict.direction must be 'ascending' or 'descending'")
			if context["columns_by_name"][order_by]["type"] in {"json", "binary"}:
				raise ValueError("Upsert conflict.order_by must use an orderable logical type")
			if order_by in keys:
				raise ValueError("Upsert conflict.order_by cannot be a schema key")
			update_columns = [name for name in context["column_names"] if name not in keys]
		options = {
			"operation": "upsert", "keys": keys, "order_by": order_by,
			"direction": direction, "update_columns": update_columns, "tie_columns": None,
		}
		return context["columns"], cls._normalize_records(context, records, order_by=order_by), options

	@classmethod
	def _prepare_scd1(cls, context: Mapping[str, Any], records: list[dict]):
		"""Validate SCD1 options and normalize records.

		SCD1 overwrites non-key columns when ``ordering.column`` wins. Equal
		ordering values with different ``compare.columns`` are treated as
		conflicts.

		Returns:
			A tuple of columns, normalized records, and merge options.

		Raises:
			ValueError: If keys, ordering, or compare settings are invalid.
		"""
		if context["operation"] != "scd1":
			raise ValueError("SCD1 preparation requires write.operation 'scd1'")
		keys = context["keys"]
		if not keys:
			raise ValueError("SCD1 requires at least one schema key")
		write = context["write"]
		late_arrivals = write.get("late_arrivals", "reject")
		if late_arrivals not in {"reject", "ignore"}:
			raise ValueError("SCD2 write.late_arrivals must be 'reject' or 'ignore'")
		ordering = write.get("ordering")
		if not isinstance(ordering, Mapping):
			raise ValueError("SCD1 requires write.ordering")
		order_by = ordering.get("column")
		direction = ordering.get("direction")
		compare = write.get("compare")
		compare_columns = compare.get("columns") if isinstance(compare, Mapping) else None
		if not isinstance(order_by, str) or order_by not in context["column_names"]:
			raise ValueError("SCD1 ordering.column must name a declared column")
		if not isinstance(direction, str) or direction not in {"ascending", "descending"}:
			raise ValueError("SCD1 ordering.direction must be 'ascending' or 'descending'")
		if context["columns_by_name"][order_by]["type"] in {"json", "binary"}:
			raise ValueError("SCD1 ordering.column must use an orderable logical type")
		if order_by in keys:
			raise ValueError("SCD1 ordering.column cannot be a schema key")
		if (
			not isinstance(compare_columns, list)
			or not compare_columns
			or any(not isinstance(name, str) or name not in context["column_names"] for name in compare_columns)
			or len(set(compare_columns)) != len(compare_columns)
			or any(name in keys or name == order_by for name in compare_columns)
		):
			raise ValueError("SCD1 compare.columns must be unique declared non-key columns other than ordering.column")
		options = {
			"operation": "scd1", "keys": keys, "order_by": order_by,
			"direction": direction,
			"update_columns": [name for name in context["column_names"] if name not in keys],
			"tie_columns": compare_columns,
		}
		return context["columns"], cls._normalize_records(context, records, order_by=order_by), options

	@classmethod
	def _prepare_scd2(cls, context: Mapping[str, Any], records: list[dict]):
		"""Validate SCD2 options, normalize records, and derive history columns.

		Each record gets ``valid_from`` from the ordering column, the open-ended
		``valid_to``, ``current_flag = True``, and a ``record_hash`` of the
		compare columns. The adapter then closes and links versions.

		Returns:
			A tuple of columns, normalized records, and SCD2 options.

		Raises:
			ValueError: If keys, ordering, compare columns, history columns, or
				the open-ended value are invalid or incompatible.
		"""
		if context["operation"] != "scd2":
			raise ValueError("SCD2 preparation requires write.operation 'scd2'")
		keys = context["keys"]
		if not keys:
			raise ValueError("SCD2 requires at least one schema key")
		write = context["write"]
		late_arrivals = write.get("late_arrivals", "reject")
		if late_arrivals not in {"reject", "ignore"}:
			raise ValueError("SCD2 write.late_arrivals must be 'reject' or 'ignore'")
		ordering = write.get("ordering")
		if not isinstance(ordering, Mapping):
			raise ValueError("SCD2 requires write.ordering")
		order_by = ordering.get("column")
		if ordering.get("direction") != "ascending":
			raise ValueError("SCD2 ordering.direction must be 'ascending'")
		if not isinstance(order_by, str) or order_by not in context["column_names"] or order_by in keys:
			raise ValueError("SCD2 ordering.column must name a declared non-key column")
		order_column = context["columns_by_name"][order_by]
		if order_column["type"] in {"json", "binary"}:
			raise ValueError("SCD2 ordering.column must use an orderable logical type")
		compare = write.get("compare")
		compare_columns = compare.get("columns") if isinstance(compare, Mapping) else None
		history_definition = write.get("history")
		if not isinstance(history_definition, Mapping):
			raise ValueError("SCD2 requires write.history")
		history_names = {
			field: history_definition.get(field)
			for field in ("valid_from", "valid_to", "current_flag", "record_hash")
		}
		if any(not isinstance(name, str) or name not in context["column_names"] for name in history_names.values()):
			raise ValueError("SCD2 history fields must name declared columns")
		if len(set(history_names.values())) != len(history_names):
			raise ValueError("SCD2 history fields must be distinct columns")
		managed_columns = set(history_names.values())
		if (
			not isinstance(compare_columns, list)
			or not compare_columns
			or any(not isinstance(name, str) or name not in context["column_names"] for name in compare_columns)
			or len(set(compare_columns)) != len(compare_columns)
			or any(name in keys or name == order_by or name in managed_columns for name in compare_columns)
		):
			raise ValueError("SCD2 compare.columns must be unique declared non-key, non-history columns")
		if order_by in managed_columns or any(name in keys for name in managed_columns):
			raise ValueError("SCD2 history fields cannot be keys or the ordering column")
		valid_from_column = context["columns_by_name"][history_names["valid_from"]]
		valid_to_column = context["columns_by_name"][history_names["valid_to"]]
		current_column = context["columns_by_name"][history_names["current_flag"]]
		hash_column = context["columns_by_name"][history_names["record_hash"]]
		if (
			valid_from_column["type"] != order_column["type"]
			or valid_to_column["type"] != order_column["type"]
			or current_column["type"] != "boolean"
			or hash_column["type"] != "string"
			or valid_from_column["nullable"]
			or current_column["nullable"]
			or hash_column["nullable"]
		):
			raise ValueError("SCD2 history fields have incompatible types or nullability")
		if order_column["type"] == "decimal" and any(
			(column.get("precision"), column.get("scale"))
			!= (order_column.get("precision"), order_column.get("scale"))
			for column in (valid_from_column, valid_to_column)
		):
			raise ValueError("SCD2 valid_from and valid_to must match ordering decimal precision and scale")
		open_ended_value = history_definition.get("open_ended_value")
		if open_ended_value is None and not valid_to_column["nullable"]:
			raise ValueError("SCD2 valid_to must be nullable when open_ended_value is null")
		if open_ended_value is not None and not cls._valid_value(
			valid_to_column["type"], open_ended_value, valid_to_column
		):
			raise ValueError("SCD2 open_ended_value is invalid for valid_to")
		history = {**history_names, "open_ended_value": open_ended_value}
		normalized = cls._normalize_records(
			context, records, order_by=order_by, managed_columns=managed_columns
		)
		for record in normalized:
			derived_history = {
				history["valid_from"]: record[order_by],
				history["valid_to"]: open_ended_value,
				history["current_flag"]: True,
				history["record_hash"]: cls._scd2_record_hash(
					record, compare_columns, context["columns_by_name"]
				),
			}
			for name, value in derived_history.items():
				if not cls._valid_value(context["columns_by_name"][name]["type"], value, context["columns_by_name"][name]):
					raise ValueError(f"Invalid derived SCD2 history value for column {name!r}")
				record[name] = value
		return context["columns"], normalized, {
			"operation": "scd2", "keys": keys, "order_by": order_by,
			"history": history, "late_arrivals": late_arrivals,
		}

	@classmethod
	def _prepare_write(cls, schema: Mapping[str, Any], records: list[dict]):
		"""Validate a schema and dispatch to the preparer for its write operation."""
		context = cls._schema_context(schema)
		preparers = {
			"append": cls._prepare_append,
			"upsert": cls._prepare_upsert,
			"scd1": cls._prepare_scd1,
			"scd2": cls._prepare_scd2,
		}
		return preparers[context["operation"]](context, records)

	def write(
		self,
		table_bucket: str,
		catalog: str,
		namespace: str,
		table: str,
		records: list[dict],
		*,
		schema: Mapping[str, Any],
	) -> int:
		"""Validate records against a schema and write them to a table.

		The table is created or reconciled first. The schema's
		``write.operation`` then selects append, upsert, SCD1, or SCD2. Each
		write runs in one adapter transaction. Ignored SCD2 late arrivals are
		logged as a warning and exposed on ``adapter.last_ignored_late_records``.

		Args:
			table_bucket: Bucket name or ARN.
			catalog: DuckDB catalog alias.
			namespace: Iceberg namespace.
			table: Target table.
			records: Input records, projected through the schema columns.
			schema: Versioned schema document.

		Returns:
			The number of records processed, or 0 for an empty batch.

		Raises:
			ValueError: If the schema or records are invalid, the table is
				incompatible, a write conflict is detected, or the adapter does
				not support the operation.
		"""
		self.adapter.last_ignored_late_records = 0
		columns, normalized, write_options = self._prepare_write(schema, records)
		if not normalized:
			return 0
		self.adapter._ensure_table_schema(table_bucket, catalog, namespace, table, columns)
		if write_options["operation"] == "append":
			return self.adapter._insert_records(
				table_bucket,
				catalog,
				namespace,
				table,
				normalized,
				logical_types={column["name"]: column["type"] for column in columns},
			)
		if write_options["operation"] == "scd2":
			scd2_records = getattr(self.adapter, "_scd2_records", None)
			if not callable(scd2_records):
				raise ValueError("The selected Crucible adapter does not support scd2 writes")
			result = scd2_records(
				table_bucket,
				catalog,
				namespace,
				table,
				normalized,
				keys=write_options["keys"],
				order_by=write_options["order_by"],
				history=write_options["history"],
				late_arrivals=write_options["late_arrivals"],
				logical_types={column["name"]: column["type"] for column in columns},
			)
			ignored_late_records = getattr(self.adapter, "last_ignored_late_records", 0)
			if ignored_late_records:
				self.residue.emit(
					Ash.WARNING,
					"CRUCIBLE: Ignored late-arriving SCD2 records",
					table_bucket=table_bucket,
					catalog=catalog,
					namespace=namespace,
					table=table,
					input_records=len(records),
					ignored_late_records=ignored_late_records,
				)
			return result
		merge_records = getattr(self.adapter, "_merge_records", None)
		if not callable(merge_records):
			raise ValueError(f"The selected Crucible adapter does not support {write_options['operation']} writes")
		return merge_records(
			table_bucket,
			catalog,
			namespace,
			table,
			normalized,
			keys=write_options["keys"],
			order_by=write_options["order_by"],
			direction=write_options["direction"],
			update_columns=write_options["update_columns"],
			tie_columns=write_options["tie_columns"],
			logical_types={column["name"]: column["type"] for column in columns},
		)
