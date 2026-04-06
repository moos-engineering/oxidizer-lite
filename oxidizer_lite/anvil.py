import os

import duckdb
import requests
from requests.auth import HTTPBasicAuth
import json

from oxidizer_lite.phase import DuckLakeConnection, GlueCatalogConnection
from oxidizer_lite.residue import Residue, Ash



class SQLEngine(Residue):
    
    def __init__(self, engine="duckdb", connection_details: DuckLakeConnection | GlueCatalogConnection = None):
        """
        Initializes the SQLEngine with the specified engine type and connection details.
        
        Args:
            engine (str): The SQL engine type to use (e.g. "duckdb").
            connection_details (DuckLakeConnection | GlueCatalogConnection | None): The connection configuration for the SQL engine.
        """
        super().__init__(component_name="sql_engine")
        self.engine = engine
        self.connection_details = connection_details
        self.catalog = connection_details.name if connection_details else None
        self.catalog_type = connection_details.type if connection_details else None
        self.connection = None
        self.create_connection()

    def _ducklake_connection_str(self, details: DuckLakeConnection):
        """
        Constructs a DuckLake connection string based on the provided connection details.
        
        Args:
            details (DuckLakeConnection): The connection configuration for DuckLake.
        
        Returns:
            str: The constructed DuckLake connection string.
        """
        connection_str = f"""
            INSTALL ducklake; 
            INSTALL postgres; 
            LOAD ducklake; 
            LOAD postgres;
            CREATE OR REPLACE SECRET (
                TYPE postgres,
                HOST '{details.postgres_host}',
                PORT {details.postgres_port},
                DATABASE '{details.postgres_db}',
                USER '{details.postgres_user}',
                PASSWORD '{details.postgres_password}'
            );
            CREATE OR REPLACE SECRET s3_secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{details.s3_access_key_id}',
                SECRET '{details.s3_secret_access_key}',
                REGION '{details.s3_region}',
                ENDPOINT '{details.s3_endpoint}',
                URL_STYLE 'path',
                USE_SSL {str(details.s3_use_ssl).lower()}
            );
            ATTACH 'ducklake:postgres:dbname={details.postgres_db}' AS {details.name} (DATA_PATH 's3://{details.s3_bucket}/{details.s3_prefix}', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
            USE {details.name};
        """
        return connection_str
    
    def _glue_catalog_connection_str(self, details: GlueCatalogConnection):
        """
        Constructs a DuckLake connection string for AWS Glue Catalog based on the provided connection details.
        
        Args:
            details (GlueCatalogConnection): The connection configuration for AWS Glue Catalog.
        Returns:
            str: The constructed DuckLake connection string for AWS Glue Catalog.
        """

        if details.aws_role_arn:
            secret_string = f"""
            CREATE OR REPLACE SECRET glue_role_secret (
                TYPE aws_iam_role,
                ROLE_ARN '{details.aws_role_arn}'
            );
            """
        elif details.aws_sso_profile:
            os.environ["AWS_PROFILE"] = details.aws_sso_profile
            os.environ["AWS_REGION"] = details.aws_region 
            secret_string = f"""
            CREATE OR REPLACE SECRET s3_credential_chain (
                TYPE s3,
                PROVIDER credential_chain
            );
            """
        else: 
            secret_string = f"""
            CREATE OR REPLACE SECRET glue_role_secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{details.s3_access_key_id}',
                SECRET '{details.s3_secret_access_key}',
                REGION '{details.s3_region}'
            );
            """

        connection_str = f"""
            INSTALL s3;
            INSTALL aws;
            INSTALL iceberg;
            LOAD s3;
            LOAD aws;
            LOAD iceberg;
            {secret_string}
            ATTACH '{details.aws_account_id}' AS {details.name} (TYPE ICEBERG, ENDPOINT_TYPE GLUE);

        """
        return connection_str

    def create_connection(self):
        """
        Create a connection to the specified SQL engine based on the provided connection details. 
        """
        if self.catalog_type == "glue_catalog":
            self.connection = duckdb.connect()
            glue_catalog_conn_query = self._glue_catalog_connection_str(self.connection_details)
            self.connection.execute(glue_catalog_conn_query)
        
        elif self.catalog_type == "ducklake":
            self.connection = duckdb.connect()
            ducklake_conn_query = self._ducklake_connection_str(self.connection_details)
            self.connection.execute(ducklake_conn_query)
        else:
            raise ValueError(f"Unsupported Catalog Type: {self.catalog_type}")
        
    def check_database_exists(self, database_name):
        """
        Checks if a database exists in the SQL engine.
        
        Args:
            database_name (str): The name of the database to check.
        
        Returns:
            bool: True if the database exists, False otherwise.
        """
        if self.catalog_type == "glue_catalog":
            databases = [row[0] for row in self.connection.execute(f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{self.catalog}'").fetchall()]
            return database_name in databases
        elif self.catalog_type == "ducklake":
            # Use SHOW DATABASES or equivalent to check if the database exists in DuckLake
            databases = [row[0] for row in self.connection.execute("SHOW DATABASES").fetchall()]
            return database_name in databases
        else:
            raise ValueError(f"Unsupported Catalog Type: {self.catalog_type}")

    def create_database(self, database_name):
        """
        Creates a new database (schema) in the SQL engine.
        
        Args:
            database_name (str): The name of the database to create.
        """
        if self.catalog_type == "glue_catalog":
            self.residue(self.ash.WARNING, f"Currently Unsupported at this time. Ensure the database '{database_name}' exists in Glue Catalog.")
        elif self.catalog_type == "ducklake":
            self.connection.execute(f"CREATE SCHEMA {database_name}")
        else:
            raise ValueError(f"Unsupported Catalog Type: {self.catalog_type}")
        
    def check_table_exists(self, database_name, table_name):
        """
        Checks if a table exists in the specified database.
        
        Args:
            database_name (str): The name of the database containing the table.
            table_name (str): The name of the table to check.
        
        Returns:
            bool: True if the table exists, False otherwise.
        """
        if self.catalog_type == "glue_catalog":
            tables = [row[0] for row in self.connection.execute(f"SHOW TABLES FROM {self.catalog}.{database_name}").fetchall()]
            return table_name in tables
        elif self.catalog_type == "ducklake":
            tables = [row[0] for row in self.connection.execute(f"SHOW TABLES FROM {self.catalog}.{database_name}").fetchall()]
            return table_name in tables
        else:
            raise ValueError(f"Unsupported Catalog Type: {self.catalog_type}")

    def create_table(self, database_name, table_name, schema, table_description=None):
        """
        Creates a new table in the specified database.
        
        Args:
            database_name (str): The name of the database to create the table in.
            table_name (str): The name of the table to create.
            schema (list[dict]): Column definitions, each dict must have 'name' and 'type' keys, optional 'description'.
            table_description (str | None): An optional description to attach as a table comment.
        """
        # Convert SchemaField dataclass instances to dicts if needed
        schema = [col.to_dict() if hasattr(col, 'to_dict') else col for col in schema]
        if not isinstance(schema, list) or not all(isinstance(col, dict) and "name" in col and "type" in col for col in schema):
            raise ValueError("Schema must be a list of dicts with 'name' and 'type' keys")
        
        # Prepare column definitions for CREATE TABLE
        columns_def = ", ".join([
            f"{col['name']} {col['type']}" for col in schema
        ])
        

        if self.catalog_type == "glue_catalog":
            # Create Iceberg Table In Glue Catalog with S3 Location and Table Properties
            s3_location = f"s3://{self.connection_details.aws_s3_bucket}/{database_name}/{table_name}"
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{database_name}.{table_name} ({columns_def})
                WITH (
                    'location' = '{s3_location}'
                );
            """
            self.connection.execute(create_table_query)
            # Add Column Descriptions or Comments
            # FUTURE 

        elif self.catalog_type == "ducklake":
            create_table_query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ({columns_def})"
            self.connection.execute(create_table_query)
            # Add Column Descriptions or Comments
            for col in schema:
                if "description" in col:
                    comment = col["description"]
                    col_name = col["name"]
                    self.connection.execute(f"COMMENT ON COLUMN {database_name}.{table_name}.{col_name} IS '{comment}'")
            # Table comment support can be added here if needed
            if table_description:
                self.connection.execute(f"COMMENT ON TABLE {database_name}.{table_name} IS '{table_description}'")
        else:
            raise ValueError(f"Unsupported Catalog Type: {self.catalog_type}")

        # Add Column Descriptions or Comments if present
        
 
    def select_query_str(self, database, table, columns="*", where=None, order_by=None, limit=None):
        """
        Builds a SELECT query string with optional filtering, ordering, and limiting.
        
        Args:
            database (str): The database name containing the target table.
            database (str): The database name containing the target table.
            table (str): The table name to select from.
            columns (str | list): Column specification, either "*" or a list of column name strings/dicts.
            where (str | list | None): Optional WHERE clause as a string or list of condition dicts.
            order_by (str | list | None): Optional ORDER BY clause as a string or list of order dicts.
            limit (int | None): Maximum number of rows to return.
        
        Returns:
            str: The constructed SQL query string.
        """
        if isinstance(columns, list):
            # IF expression in column detail, handle that appropriately instead of quoting as a column name
            formatted_columns = []
            for col in columns:
                if isinstance(col, dict) and "name" in col and "expression" not in col:
                    formatted_columns.append(col["name"])
                elif isinstance(col, dict) and "expression" in col:
                    name = col.get("name", "col")
                    alias = col.get("alias", name)
                    column = f"{col['expression']} AS {alias}" 
                    formatted_columns.append(column)
                else:
                    raise ValueError("Invalid column format. Must be str or dict with 'expression' key.")
            columns = ", ".join(formatted_columns)
        elif isinstance(columns, str):
            pass  # Use as is (e.g. "*" or "col1, col2")
        else:
            raise ValueError("Columns must be a string or a list of strings/dicts.")
        
        # Construct filter string for WHERE clause if provided
        if where:
            if isinstance(where, list):
                where_clauses = []
                for condition in where:
                    col = condition["column"]
                    op = condition["operator"]
                    val = condition["value"]
                    if isinstance(val, str):
                        val = f"'{val}'"  # Quote string values
                    where_clauses.append(f"{col} {op} {val}")
                where = " AND ".join(where_clauses)
            elif isinstance(where, str):
                pass  # Use as is
            else:
                raise ValueError("Where clause must be a string or a list of conditions.")
        
        # Construct order_by string if provided
        if order_by:
            if isinstance(order_by, list):
                order_by_clauses = []
                for ob in order_by:
                    col = ob["column"]
                    direction = ob.get("direction", "ASC").upper()
                    if direction not in ["ASC", "DESC"]:
                        raise ValueError("Order by direction must be 'ASC' or 'DESC'.")
                    order_by_clauses.append(f"{col} {direction}")
                order_by = ", ".join(order_by_clauses)
            elif isinstance(order_by, str):
                pass  # Use as is
            else:
                raise ValueError("Order by clause must be a string or a list of dicts with 'column' and optional 'direction'.")
        
        # Construct the final SQL query with batching using OFFSET and LIMIT
        sql = f"SELECT {columns} FROM {self.catalog}.{database}.{table}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {limit}"
        
        return sql
    
    def load_staging_table(self, data: list[dict], staging_table: str = "staging"):
        """
        Loads a list of dicts into a temporary staging table.
        
        Args:
            data (list[dict]): The rows to load into the staging table.
            staging_table (str): The name of the temp table. Defaults to "staging".
        
        Returns:
            tuple: A tuple of (staging_table name, list of column names).
        """
        columns = list(data[0].keys())
        values_str = ', '.join([str(tuple(row.values())) for row in data])
        columns_str = ', '.join(columns)
        self.connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {staging_table} AS SELECT * FROM (VALUES {values_str}) AS t({columns_str})"
        )
        self.residue(self.ash.INFO, f"Loaded {len(data)} rows into temp table {staging_table}")
        return staging_table, columns

    def drop_staging_table(self, staging_table: str = "staging"):
        """
        Drops a temporary staging table.
        
        Args:
            staging_table (str): The name of the temp table to drop.
        """
        self.connection.execute(f"DROP TABLE IF EXISTS {staging_table}")

    def insert_query_str(self, database, table_name, source_table: str):
        """
        Builds an INSERT query string that selects from a staging table.
        
        Args:
            database (str): The database name containing the target table.
            table_name (str): The name of the table to insert into.
            source_table (str): The name of the temp/staging table to select from.
        
        Returns:
            str: The constructed SQL INSERT query string.
        """
        if self.catalog_type == "glue_catalog":
            sql = f"INSERT INTO {self.catalog}.{database}.{table_name} SELECT * FROM {source_table}"
        elif self.catalog_type == "ducklake":
            sql = f"INSERT INTO {database}.{table_name} SELECT * FROM {source_table}"
        else:
            raise ValueError(f"Unsupported Catalog Type: {self.catalog_type}")
        self.residue(self.ash.INFO, f"Insert query built for {database}.{table_name} from source {source_table}")
        return sql
        
    def scd_type2_query_str(self, database, table_name, source_table: str, columns: list[str], primary_key: list[str], begin_date_col: str, end_date_col: str, is_current_col: str = "is_current", tracked_columns: list[str] = None, timestamp_expr: str = "CURRENT_TIMESTAMP"):
        """
        Builds a SQL query string to perform a Type 2 Slowly Changing Dimension (SCD) merge operation.
        Uses DuckDB's MERGE statement with a UNION ALL staging query to combine expiring old records
        and inserting new/changed records in a single statement.
        
        Handles intra-batch duplicates: when the staging table contains multiple records for the same
        primary key, only the latest (by timestamp_expr) is treated as the "current" candidate.
        Earlier records within the batch are inserted as historical records via the pre_insert_historical_sql.
        
        Args:
            database (str): The database name containing the target table.
            table_name (str): The name of the table to merge into.
            source_table (str): The name of the temp/staging table containing incoming data.
            columns (list[str]): All column names present in the source table.
            primary_key (list[str]): The primary key column name(s) used to identify records.
            begin_date_col (str): The column name for the begin/effective date of records.
            end_date_col (str): The column name for the end/expiration date of records.
            is_current_col (str): The column name for the current record flag. Defaults to "is_current".
            tracked_columns (list[str] | None): Columns to compare for change detection. 
                Defaults to all columns minus primary_key(s), begin_date_col, end_date_col, is_current_col.
            timestamp_expr (str): SQL expression for the SCD timestamp values. 
                Defaults to "CURRENT_TIMESTAMP" (wall-clock time). Use a column reference
                like "staged_changes.updated_at" to track against a source event timestamp.
        
        Returns:
            list[str]: A list of SQL query strings [insert_historical_sql, merge_sql] to execute sequentially.
                The first handles intra-batch historical records, the second performs the SCD Type 2 merge.
        """
        target_ref = f"{self.catalog}.{database}.{table_name}"

        # Normalize primary_key to a list
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Extract timestamp column name for window functions
        ts_col = timestamp_expr.replace("staged_changes.", "").replace("s.", "")

        # Determine which columns to use for change detection
        meta_cols = set(primary_key) | {begin_date_col, end_date_col, is_current_col}
        if tracked_columns is None:
            tracked_columns = [col for col in columns if col not in meta_cols]

        # Build the change detection WHERE clause (source != target for tracked columns)
        change_conditions = " OR ".join(
            [f"s.{col} <> t.{col}" for col in tracked_columns]
        )

        # All source columns for SELECT (prefixed with staged_changes alias)
        source_cols = ", ".join([f"s.{col}" for col in columns])
        staged_cols = ", ".join([f"staged_changes.{col}" for col in columns])

        # Build merge key columns for UNION ALL staging query
        merge_key_nulls = ", ".join([f"NULL AS merge_key_{pk}" for pk in primary_key])
        merge_key_vals = ", ".join([f"s.{pk} AS merge_key_{pk}" for pk in primary_key])

        # Build JOIN condition for the changed-rows subquery
        join_conditions = " AND ".join([f"s.{pk} = t.{pk}" for pk in primary_key])

        # Build ON clause for the MERGE (match on all merge keys)
        merge_on_conditions = " AND ".join([f"t.{pk} = staged_changes.merge_key_{pk}" for pk in primary_key])

        # Build primary key partition expression
        pk_partition = ", ".join(primary_key)

        # Column list for INSERT
        all_target_cols = columns + [begin_date_col, end_date_col, is_current_col]
        # Remove duplicates while preserving order (in case begin/end/is_current are already in columns)
        seen = set()
        insert_cols = []
        for col in all_target_cols:
            if col not in seen:
                seen.add(col)
                insert_cols.append(col)
        insert_cols_str = ", ".join(insert_cols)

        # Build INSERT values — use staged_changes for data columns, SCD defaults for meta columns
        insert_values = []
        for col in insert_cols:
            if col == begin_date_col:
                insert_values.append(timestamp_expr)
            elif col == end_date_col:
                insert_values.append("NULL")
            elif col == is_current_col:
                insert_values.append("true")
            else:
                insert_values.append(f"staged_changes.{col}")
        insert_values_str = ", ".join(insert_values)

        # Build column list for deduplication (all source columns)
        source_cols_str = ", ".join(columns)

        # --- First deduplicate exact duplicates by all columns ---
        # This handles retries/failures that may have inserted the same records multiple times
        deduped_staging = f"""(
            SELECT {source_cols_str}
            FROM {source_table}
            GROUP BY {source_cols_str}
        )"""

        # --- Subquery to rank staging records for intra-batch deduplication ---
        ranked_staging_subquery = f"""(
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {pk_partition} ORDER BY {ts_col} DESC) AS _rn,
                   LEAD({ts_col}) OVER (PARTITION BY {pk_partition} ORDER BY {ts_col} ASC) AS _next_ts
            FROM {deduped_staging}
        )"""

        # --- Step 0: INSERT HISTORICAL — insert non-latest intra-batch records as historical ---
        historical_select_values = []
        for col in insert_cols:
            if col == begin_date_col:
                historical_select_values.append(f"s.{ts_col} AS {begin_date_col}")
            elif col == end_date_col:
                historical_select_values.append(f"s._next_ts AS {end_date_col}")
            elif col == is_current_col:
                historical_select_values.append(f"false AS {is_current_col}")
            else:
                historical_select_values.append(f"s.{col}")
        historical_select_values_str = ", ".join(historical_select_values)

        # Build NOT EXISTS check on PK + timestamp to prevent duplicate historical records
        hist_not_exists_join = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_key])

        insert_historical_sql = f"""INSERT INTO {target_ref} ({insert_cols_str})
                    SELECT {historical_select_values_str}
                    FROM {ranked_staging_subquery} AS s
                    WHERE s._rn > 1
                    AND NOT EXISTS (
                        SELECT 1 FROM {target_ref} AS t
                        WHERE {hist_not_exists_join}
                        AND t.{begin_date_col} = s.{ts_col}
                    )"""

        # --- Step 1: MERGE — only use the LATEST staging record per PK (_rn = 1) ---
        # Use a subquery to filter to only the latest records before the MERGE
        latest_staging = f"""(SELECT * FROM {ranked_staging_subquery} WHERE _rn = 1)"""

        merge_sql = f"""MERGE INTO {target_ref} AS t
                    USING (
                        SELECT {merge_key_nulls}, {source_cols}
                        FROM {latest_staging} AS s

                        UNION ALL

                        SELECT {merge_key_vals}, {source_cols}
                        FROM {latest_staging} AS s
                        JOIN {target_ref} AS t
                            ON {join_conditions}
                            AND t.{is_current_col} = true
                        WHERE {change_conditions}
                    ) AS staged_changes
                    ON {merge_on_conditions}
                        AND t.{is_current_col} = true

                    WHEN MATCHED THEN UPDATE SET
                        {end_date_col} = {timestamp_expr},
                        {is_current_col} = false

                    WHEN NOT MATCHED THEN INSERT ({insert_cols_str})
                        VALUES ({insert_values_str})"""

        self.residue(self.ash.INFO, f"SCD Type 2 merge query built for {target_ref} using source {source_table}")
        return [insert_historical_sql, merge_sql]

    def scd_type2_update_insert_query_strs(self, database, table_name, source_table: str, columns: list[str], primary_key: list[str], begin_date_col: str, end_date_col: str, is_current_col: str = "is_current", tracked_columns: list[str] = None, timestamp_expr: str = "CURRENT_TIMESTAMP"):
        """
        Builds SQL statements (INSERT historical, UPDATE, INSERT current) to perform an SCD Type 2 operation
        without using MERGE INTO. This is the Iceberg-compatible alternative to scd_type2_query_str,
        since the DuckDB Iceberg extension supports UPDATE and INSERT individually but not MERGE.
        
        Handles intra-batch duplicates: when the staging table contains multiple records for the same
        primary key, only the latest (by timestamp_expr) is treated as the "current" candidate.
        Earlier records within the batch are inserted as historical records with is_current=false.
        
        Step 0 (INSERT HISTORICAL): Insert non-latest intra-batch duplicates as historical records.
        Step 1 (UPDATE): Expire current target records where the LATEST staging record has changed.
        Step 2 (INSERT CURRENT): Insert the LATEST staging record per PK as current where no current exists.
        
        Args:
            database (str): The database name containing the target table.
            table_name (str): The name of the table to update/insert into.
            source_table (str): The name of the temp/staging table containing incoming data.
            columns (list[str]): All column names present in the source table.
            primary_key (list[str]): The primary key column name(s) used to identify records.
            begin_date_col (str): The column name for the begin/effective date of records.
            end_date_col (str): The column name for the end/expiration date of records.
            is_current_col (str): The column name for the current record flag. Defaults to "is_current".
            tracked_columns (list[str] | None): Columns to compare for change detection.
                Defaults to all columns minus primary_key(s), begin_date_col, end_date_col, is_current_col.
            timestamp_expr (str): SQL expression for the SCD timestamp values.
                Defaults to "CURRENT_TIMESTAMP". Use a column reference like "s.updated_at"
                to track against a source event timestamp.
        
        Returns:
            list[str]: A list of SQL query strings [insert_historical_sql, update_sql, insert_current_sql] to execute sequentially.
        """
        target_ref = f"{self.catalog}.{database}.{table_name}"

        # Normalize primary_key to a list
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Remap staged_changes.X → s.X since the MERGE alias doesn't exist here
        ts_expr = timestamp_expr.replace("staged_changes.", "s.")
        # Also create a version without the s. prefix for window functions
        ts_col = ts_expr.replace("s.", "")

        # Determine which columns to use for change detection
        meta_cols = set(primary_key) | {begin_date_col, end_date_col, is_current_col}
        if tracked_columns is None:
            tracked_columns = [col for col in columns if col not in meta_cols]

        # Build the change detection conditions (source != target for tracked columns)
        change_conditions = " OR ".join(
            [f"s.{col} <> t.{col}" for col in tracked_columns]
        )

        # Build primary key partition/join expressions
        pk_partition = ", ".join(primary_key)
        pk_join = " AND ".join([f"s.{pk} = t.{pk}" for pk in primary_key])

        # Column list for INSERT
        all_target_cols = columns + [begin_date_col, end_date_col, is_current_col]
        seen = set()
        insert_cols = []
        for col in all_target_cols:
            if col not in seen:
                seen.add(col)
                insert_cols.append(col)
        insert_cols_str = ", ".join(insert_cols)

        # Build column list for deduplication (all source columns)
        source_cols_str = ", ".join(columns)

        # --- First deduplicate exact duplicates by (PK + timestamp) ---
        # This handles retries/failures that may have inserted the same records multiple times
        deduped_staging = f"""(
            SELECT {source_cols_str}
            FROM {source_table}
            GROUP BY {source_cols_str}
        )"""

        # --- Subquery to rank staging records and compute next timestamp for historical records ---
        # _rn = 1 is the LATEST record per PK (what we compare against target)
        # _rn > 1 are older intra-batch records (inserted as historical)
        # _next_ts is the timestamp of the record that supersedes this one (for end_date of historical)
        ranked_staging_subquery = f"""(
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {pk_partition} ORDER BY {ts_col} DESC) AS _rn,
                   LEAD({ts_col}) OVER (PARTITION BY {pk_partition} ORDER BY {ts_col} ASC) AS _next_ts
            FROM {deduped_staging}
        )"""

        # --- Step 0: INSERT HISTORICAL — insert non-latest intra-batch records as historical ---
        # These are records superseded by later records within the same batch
        # Use NOT EXISTS to prevent duplicate inserts on retries (check PK + begin_date)
        historical_select_values = []
        for col in insert_cols:
            if col == begin_date_col:
                historical_select_values.append(f"s.{ts_col} AS {begin_date_col}")
            elif col == end_date_col:
                historical_select_values.append(f"s._next_ts AS {end_date_col}")
            elif col == is_current_col:
                historical_select_values.append(f"false AS {is_current_col}")
            else:
                historical_select_values.append(f"s.{col}")
        historical_select_values_str = ", ".join(historical_select_values)

        # Build NOT EXISTS check on PK + timestamp to prevent duplicate historical records
        hist_not_exists_join = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_key])

        insert_historical_sql = f"""INSERT INTO {target_ref} ({insert_cols_str})
                    SELECT {historical_select_values_str}
                    FROM {ranked_staging_subquery} AS s
                    WHERE s._rn > 1
                    AND NOT EXISTS (
                        SELECT 1 FROM {target_ref} AS t
                        WHERE {hist_not_exists_join}
                        AND t.{begin_date_col} = s.{ts_col}
                    )"""

        # --- Step 1: UPDATE — expire current target records where LATEST staging record has changed ---
        # Only compare against the latest staging record per PK (_rn = 1)
        # Use FROM clause join pattern instead of correlated subquery (better Iceberg compatibility)
        if ts_expr == "CURRENT_TIMESTAMP":
            update_ts_expr = "CURRENT_TIMESTAMP"
        else:
            update_ts_expr = f"s.{ts_col}"

        # Build join conditions for FROM clause
        update_pk_join = " AND ".join([f"s.{pk} = t.{pk}" for pk in primary_key])

        # Use FROM clause syntax for UPDATE - more reliable on Iceberg than correlated subqueries
        update_sql = f"""UPDATE {target_ref} AS t
                    SET {end_date_col} = {update_ts_expr}, {is_current_col} = false
                    FROM {ranked_staging_subquery} AS s
                    WHERE {update_pk_join}
                    AND s._rn = 1
                    AND t.{is_current_col} = true
                    AND ({change_conditions})"""

        # --- Step 2: INSERT CURRENT — insert LATEST staging records as current where no current exists ---
        # After the UPDATE, changed records no longer have is_current=true,
        # so NOT EXISTS on is_current=true correctly picks up both new and changed keys.
        current_select_values = []
        for col in insert_cols:
            if col == begin_date_col:
                current_select_values.append(f"s.{ts_col} AS {begin_date_col}")
            elif col == end_date_col:
                current_select_values.append(f"NULL AS {end_date_col}")
            elif col == is_current_col:
                current_select_values.append(f"true AS {is_current_col}")
            else:
                current_select_values.append(f"s.{col}")
        current_select_values_str = ", ".join(current_select_values)

        # Build NOT EXISTS join on primary key + is_current
        not_exists_join = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_key])

        insert_current_sql = f"""INSERT INTO {target_ref} ({insert_cols_str})
                    SELECT {current_select_values_str}
                    FROM {ranked_staging_subquery} AS s
                    WHERE s._rn = 1
                    AND NOT EXISTS (
                        SELECT 1 FROM {target_ref} AS t
                        WHERE {not_exists_join}
                        AND t.{is_current_col} = true
                    )"""

        self.residue(self.ash.INFO, f"SCD Type 2 UPDATE+INSERT queries built for {target_ref} using source {source_table}")
        return [insert_historical_sql, update_sql, insert_current_sql]

    def execute_batch_query(self, query, batch_idx, batch_size=1000):
        """
        Executes a query and returns a specific batch of results as JSON.
        
        Args:
            query (str): The SQL query to execute.
            batch_idx (int): The zero-based index of the batch to return.
            batch_size (int): The number of rows per batch.
        
        Returns:
            str | list: JSON string of the batch data, or an empty list if the batch index is out of range.
        """
        results = self.connection.execute(query)
        batch_reader = results.fetch_record_batch(rows_per_batch=batch_size)
        for idx, batch in enumerate(batch_reader):
            if idx == batch_idx:
                batch_data = batch.to_pylist()  # Convert the RecordBatch to JSON format for easier handling downstream. Adjust as needed for different formats or data handling requirements.
                return batch_data
        return []
    
    def exectute_single_query(self, query):
        """
        Executes a single query and returns all results. Use with caution for large result sets.
        
        Args:
            query (str): The SQL query to execute.
        
        Returns:
            list: The query results as a list of tuples, or an empty list on error.
        """
        try:
            results = self.connection.execute(query).fetchall()
            return results
        except Exception as e:
            self.residue(self.ash.ERROR, f"Error executing query: {e}")
            return []

    def close(self):
        """
        Closes the SQL engine connection and releases resources.
        """
        # self.connection.close()
        self.connection = None





class APIEngine(Residue):
    def __init__(self, connection_details=None):
        """
        Initializes the APIEngine with optional connection details.
        
        Args:
            connection_details (dict | None): Configuration dict containing 'base_url' and optional 'auth' settings.
        """
        super().__init__(component_name="api_engine")
        self.connection_details = connection_details or {}
        self.base_url = None
        self.session = requests.Session()
        self._setup_connection()
            
        self.session.headers.update({"Content-Type": "application/json"})

    def _setup_connection(self):
        """
        Set up the API connection based on the provided connection details, including authentication if specified.
        """
        self.base_url = self.connection_details.get("base_url", "")
        if self.connection_details.get("auth"):
            auth_details = self.connection_details["auth"]
            if auth_details["type"] == "auth_token":
                token = auth_details["token"]
                self.session.headers.update({"Authorization": f"Bearer {token}"})
            elif auth_details["type"] == "api_key":
                key_name = auth_details["key_name"]
                key_value = auth_details["key_value"]
                self.session.headers.update({key_name: key_value})
            else:
                raise ValueError(f"Unsupported authentication type: {auth_details['type']}")


    def get(self, endpoint, params=None):
        """
        Makes a GET request to the specified API endpoint with optional query parameters.
        
        Args:
            endpoint (str): The API endpoint path, e.g. "/data".
            params (dict | None): Query parameters for the GET request.
        
        Returns:
            dict: JSON response from the API.
        """
        url = f"{self.base_url}{endpoint}" 
        response = self.session.get(url, params=params)
        return self._handle_response(response)

    def post(self, endpoint, data=None):
        """
        Makes a POST request to the specified API endpoint with optional JSON data.
        
        Args:
            endpoint (str): The API endpoint path, e.g. "/submit".
            data (dict | None): Data to include in the POST request body.
        
        Returns:
            dict: JSON response from the API.
        """
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data)
        return self._handle_response(response)

    def _handle_response(self, response):
        """
        Handles the API response, checking for errors and returning the JSON data if successful.
        
        Args:
            response (requests.Response): The HTTP response object to process.
        
        Returns:
            dict: JSON data from the response if successful.
        
        Raises:
            HTTPError: If the response contains an HTTP error status code.
        """
        if response.status_code in [200, 201]:
            return response.json()
        else:
            response.raise_for_status() # Raises exception for 4xx/5xx errors








class BedrockEngine(Residue):
    def __init__(self, connection_details):
        """
        Initializes the BedrockEngine with AWS Bedrock connection details.
        
        Args:
            connection_details (dict): Configuration for connecting to AWS Bedrock.
        """
        super().__init__(component_name="bedrock_engine")
        self.connection_details = connection_details
    
    def bedrock_auth(self):
        """
        Authenticates with the AWS Bedrock service.
        """
        pass

    def call_bedrock(self, model, input_data):
        """
        Calls an AWS Bedrock model with the provided input data.
        
        Args:
            model (str): The Bedrock model identifier to invoke.
            input_data (dict): The input payload to send to the model.
        
        Returns:
            dict: The model response.
        """
        pass

class OllamaEngine(Residue):
    def __init__(self, connection_details):
        """
        Initializes the OllamaEngine with Ollama connection details.
        
        Args:
            connection_details (dict): Configuration for connecting to the Ollama service.
        """
        super().__init__(component_name="ollama_engine")
        self.connection_details = connection_details
    
    def ollama_auth(self):
        """
        Authenticates with the Ollama service.
        """
        pass

    def call_ollama(self, model, input_data):
        """
        Calls an Ollama model with the provided input data.
        
        Args:
            model (str): The Ollama model identifier to invoke.
            input_data (dict): The input payload to send to the model.
        
        Returns:
            dict: The model response.
        """
        pass

class AnthropicEngine(Residue):
    def __init__(self, connection_details):
        """
        Initializes the AnthropicEngine with Anthropic API connection details.
        
        Args:
            connection_details (dict): Configuration for connecting to the Anthropic API.
        """
        super().__init__(component_name="anthropic_engine")
        self.connection_details = connection_details
    
    def anthropic_auth(self):
        """
        Authenticates with the Anthropic API service.
        """
        pass

    def call_anthropic(self, model, input_data):
        """
        Calls an Anthropic model with the provided input data.
        
        Args:
            model (str): The Anthropic model identifier to invoke.
            input_data (dict): The input payload to send to the model.
        
        Returns:
            dict: The model response.
        """
        pass