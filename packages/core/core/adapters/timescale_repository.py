# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""TimescaleDB Repository implementation."""

from typing import Any, Union, final, get_args, get_origin

from psycopg2.extensions import cursor as Psycopg2Cursor
from psycopg2.extras import execute_values

from core.ports.repository import LoadOptions, Repository, RepositoryError, SaveResult, UpsertResult
from core.utils.dataframe import DataContainer, DataFrame, read_database

from .timescale_client import TimescaleClient


@final
class TimescaleRepository(TimescaleClient, Repository):
    """
    Repository implementation that saves and retrieves data from a Timescale database. It uses
    hyper-tables to store the data in a time-series format and normal tables for relational data.
    For more information, visit:
        https://docs.timescale.com/use-timescale/latest/hypertables/
        https://docs.timescale.com/quick-start/latest/python/
    """

    def __init__(self) -> None:
        TimescaleClient.__init__(self)
        Repository.__init__(self)

    def _config_table(self, dc: DataContainer, cursor: Psycopg2Cursor) -> None:
        """This method creates the table and the trigger for the given data container."""

        if self._table_exists(dc.name, cursor):
            return

        self._create_table(dc, cursor)

        # Create the trigger for updating the updated_at field on update.
        self._create_updated_at_trigger(dc.name, cursor)

        if dc.kind == "timeseries" and not self._hypertable_exists(dc.name, cursor):
            self._create_hypertable(dc, cursor)

    def _close_cursor(self, cursor: Psycopg2Cursor) -> None:
        """Commit the transaction and close the cursor."""
        cursor.connection.commit()
        cursor.close()

    def _hypertable_exists(self, name: str, cursor: Psycopg2Cursor):
        """Check if the table is a hypertable."""
        query = f"""
            SELECT * FROM timescaledb_information.hypertables
            where hypertable_name = '{name}';
        """

        cursor.execute(query)
        result = cursor.fetchall()
        return len(result) > 0

    def _table_exists(self, name: str, cursor: Psycopg2Cursor) -> bool:
        """Check if a table exists in the current PostgreSQL database."""

        # Get the current schema.
        cursor.execute("SELECT CURRENT_SCHEMA")
        result = cursor.fetchone()
        schema = result[0] if result else "public"

        query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{name}'
            );
        """

        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else False

    def _parse_dtypes(self, dtypes: dict[str, str]) -> dict[str, str]:
        """Parse the data types from the DataFrame schema to TimescaleDB types."""

        mapping = {
            "Bool": "BOOLEAN",
            "Int64": "INTEGER",
            "Float32": "REAL",
            "Float64": "DOUBLE PRECISION",
            "String": "TEXT",
            "DateTime": "TIMESTAMPTZ",
            "Date": "DATE",
            "Categorical": "TEXT",
        }

        return {col: mapping[dtype] for col, dtype in dtypes.items()}

    def _create_table(self, dc: DataContainer, cursor: Psycopg2Cursor) -> None:
        """Create a table in the database if it does not exist."""

        dtypes = self._parse_dtypes(dc.dtypes())
        columns: list[Any] = []
        for field, field_schema in dc.schema.columns.items():
            is_optional = field_schema.nullable or (
                get_origin(field_schema.dtype) is Union
                and type(None) in get_args(field_schema.dtype)
            )
            sql_type = dtypes.get(field, "TEXT")
            nullable = "NULL" if is_optional else "NOT NULL"
            columns.append(f"{field} {sql_type} {nullable}")

        # Add the created_at and updated_at columns.
        columns.append("created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL")
        columns.append("updated_at TIMESTAMPTZ NULL")

        # Add UNIQUE constraints.
        if dc.kind == "timeseries":
            # In timeseries, the unique must contain the primary key.
            all_fields = list(set([dc.primary_key] + dc.unique_fields))
            pk_constraints = f"UNIQUE ({', '.join(all_fields)})"
            uq_constraints = ""
        else:
            uq_constraints = ", ".join([f"UNIQUE ({field})" for field in dc.unique_fields])
            pk_constraints = f"PRIMARY KEY ({dc.primary_key})"

        # Combine columns, unique constraints, and primary key constraints.
        columns_def = ", ".join(columns)
        constraints = ", ".join(filter(None, [uq_constraints, pk_constraints]))

        if len(constraints) > 0:
            query = f"CREATE TABLE IF NOT EXISTS {dc.name} ({columns_def}, {constraints});"
        else:
            query = f"CREATE TABLE IF NOT EXISTS {dc.name} ({columns_def});"

        cursor.execute(query)

    def _create_hypertable(self, dc: DataContainer, cursor: Psycopg2Cursor) -> None:
        """Create a hypertable in the database."""

        query = f"""
            SELECT create_hypertable('{dc.name}', by_range('{dc.primary_key}'));
        """
        cursor.execute(query)

        for dim in dc.unique_fields:
            query = f"SELECT add_dimension('{dc.name}', by_hash('{dim}', 4));"
            cursor.execute(query)

    def _create_updated_at_trigger(self, table_name: str, cursor: Psycopg2Cursor) -> None:
        """Create a trigger to automatically update the 'updated_at' column on row update."""

        # Create a function to update the updated_at field.
        create_function_query = """
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """
        cursor.execute(create_function_query)

        # Create the trigger that will call the function before each UPDATE.
        create_trigger_query = f"""
            CREATE OR REPLACE TRIGGER update_{table_name}_updated_at
            BEFORE UPDATE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        cursor.execute(create_trigger_query)

    def _parse_load_options(self, options: LoadOptions) -> str:
        """Parse load options into a compatible SQL statement."""

        query = ""
        if options.filters:
            query += " WHERE "
            for i, filter in enumerate(options.filters):
                value = filter.get("value")
                if isinstance(value, str):
                    value = f"'{value}'"
                query += f"{filter.get('field')} {filter.get('operator')} {value}"
                if i < len(options.filters) - 1:
                    query += " AND "

        if options.limit:
            query += f" LIMIT {options.limit}"

        return query

    def save(self, dc: DataContainer) -> SaveResult:
        """Saves data into the Timescale table and returns the result."""
        df = dc.df()

        if df.is_empty():
            return SaveResult(status="error", message="Dataframe is empty", rows_affected=0)

        cursor = self.connect().cursor()
        self._config_table(dc, cursor)

        try:
            # Prepare the insert query.
            placeholders = ", ".join("%s" for _ in df.columns)
            insert_query = f"""
                INSERT INTO {dc.name} ({', '.join(df.columns)})
                VALUES ({placeholders});
            """

            # Convert values for insertion.
            values = df.rows()
            cursor.executemany(insert_query, values)
            self._close_cursor(cursor)

            return SaveResult(status="success", message=None, rows_affected=df.shape[0])
        except Exception as e:
            return SaveResult(status="error", message=str(e), rows_affected=0)
        finally:
            cursor.close()

    def load(self, options: LoadOptions) -> DataFrame:
        """Loads data from a Timescale table and returns a polars DataFrame."""
        cursor = self.connect().cursor()

        if not self._table_exists(options.name, cursor):
            raise RepositoryError(
                message=f"Table {options.name} does not exist",
                code="CANNOT_FIND_REPOSITORY",
            )

        parsed_filters = self._parse_load_options(options) if options else ""
        query = f"SELECT * FROM {options.name}{parsed_filters};"

        # Using polars to read data from the database.
        df = read_database(connection=cursor.connection, query=query)
        self._close_cursor(cursor)

        return df

    def upsert(self, dc: DataContainer) -> UpsertResult:
        """Upsert data into the Timescale table and returns the result."""
        df = dc.df()

        if df.is_empty():
            return UpsertResult(
                status="error",
                message="Dataframe is empty",
                rows_inserted=0,
                rows_updated=0,
            )

        cursor = self.connect().cursor()
        self._config_table(dc, cursor)

        # Get the columns and keys for the upsert. The primary key is used by default.
        if dc.kind != "timeseries":
            keys = (
                [dc.primary_key] if dc.primary_key else dc.unique_fields if dc.unique_fields else []
            )
        else:
            keys = list(set([dc.primary_key] + dc.unique_fields))

        try:
            # Create the ON CONFLICT clause with the composite primary keys.
            conflict_keys = ", ".join(keys)

            # Create the SQL part for updates, excluding the primary key columns.
            update_cols = ", ".join(
                [f"{col}=excluded.{col}" for col in df.columns if col not in keys]
            )

            # SQL for upsert.
            upsert_sql = f"""
                INSERT INTO {dc.name} ({", ".join(df.columns)})
                VALUES %s
                ON CONFLICT({conflict_keys})
                DO UPDATE SET {update_cols};
            """

            # SQL to count the number of updated rows.
            # XXX: This is a workaround to get the number of updated rows because, and can be failed
            # if the upsert takes more than 5 seconds.
            count_updated_sql = f"""
                SELECT
                    COUNT(*) AS num_updated
                FROM {dc.name}
                WHERE updated_at IS NOT NULL
                AND updated_at > localtimestamp - INTERVAL '5 second';
            """

            # Execute the upsert for all rows at once and get the number of updated rows.
            execute_values(cursor, upsert_sql, df.rows())
            cursor.execute(count_updated_sql)
            results = cursor.fetchall()

            # Count insertions and updates
            num_updated = results[0][0]
            num_inserted = df.shape[0] - num_updated if df.shape[0] < num_updated else 0

            return UpsertResult(
                status="success",
                message=None,
                rows_inserted=num_inserted,
                rows_updated=num_updated,
            )
        except Exception as e:
            return UpsertResult(status="error", message=str(e), rows_inserted=0, rows_updated=0)
        finally:
            self._close_cursor(cursor)
