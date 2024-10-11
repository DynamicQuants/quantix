# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""TimescaleDB Repository implementation."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, Union, final, get_args, get_origin

import patito as pt
import polars as pl
import pytz
from psycopg2.extensions import cursor as Psycopg2Cursor
from pydantic import AwareDatetime

from core.ports.repository import Repository, SaveResult, UpsertResult

from .timescale_client import TimescaleClient


@dataclass
class SQLWhere:
    """Defines a where clause for SQL queries."""

    column: str
    operator: Literal["=", ">", "<", ">=", "<=", "LIKE"]
    value: Union[str, int, float, bool, date, datetime]


@dataclass
class SQLFilter:
    """Defines a filter for SQL queries."""

    where: list[SQLWhere] | None = None
    limit: int | None = None


@dataclass
class TimescaleTable:
    """Defines a Timescale table."""

    model: type[pt.Model]  # Used only for table creation.
    db: str
    schema: str
    name: str
    hypertable_key: str | None = None
    primary_keys: list[str] | None = None
    unique_fields: list[str] | None = None


@dataclass
class TimescaleSaveOptions:
    """Defines timescale options for saving data to an Timescale table."""

    table: TimescaleTable
    df: pl.DataFrame


@dataclass
class TimescaleLoadOptions:
    """Defines timescale options for loading data from an Timescale table."""

    table: TimescaleTable
    filters: SQLFilter | None = None


@final
class TimescaleRepository(
    TimescaleClient,
    Repository[
        TimescaleSaveOptions,
        TimescaleLoadOptions,
        TimescaleSaveOptions,
    ],
):
    """
    Repository implementation that saves and retrieves data from a Timescale database. It uses
    hyper-tables to store the data in a time-series format and normal tables for relational data.
    For more information, visit:
        https://docs.timescale.com/use-timescale/latest/hypertables/
        https://docs.timescale.com/quick-start/latest/python/
    """

    def _get_cursor(self, table: TimescaleTable) -> Psycopg2Cursor:
        """
        Get a cursor for the given table. If table does not exist, create it using the table
        model definition.
        """
        cursor = self.connect().cursor()
        self._create_table(table, cursor)
        return cursor

    def _hypertable_exists(self, table: TimescaleTable, cursor: Psycopg2Cursor):
        """Check if the table is a hypertable."""
        name = table.name
        query = (
            f"SELECT * FROM timescaledb_information.hypertables where hypertable_name = '{name}';"
        )
        cursor.execute(query)
        result = cursor.fetchall()
        return len(result) > 0

    def _table_exists(self, table: TimescaleTable, cursor: Psycopg2Cursor) -> bool:
        """Check if a table exists in the current PostgreSQL database."""

        query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{table.schema}'
                AND table_name = '{table.name}'
            );
        """

        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else False

    def _create_table(self, table: TimescaleTable, cursor: Psycopg2Cursor) -> None:
        """Create a table in the database if it does not exist."""

        # If table already exists in the database, we don't need to create it.
        if self._table_exists(table, cursor):
            return

        # Set default values for primary_keys and unique_fields.
        if table.unique_fields is None:
            table.unique_fields = []

        if table.primary_keys is None:
            table.primary_keys = []

        columns: list[Any] = []
        for field, type_hint in table.model.__annotations__.items():
            is_optional = get_origin(type_hint) is Union and type(None) in get_args(type_hint)
            base_type = get_args(type_hint)[0] if is_optional else type_hint

            sql_type = {
                int: "INTEGER",
                str: "TEXT",
                float: "REAL",
                bool: "INTEGER",
                bytes: "BLOB",
                date: "DATE",
                datetime: "TIMESTAMPTZ",
                AwareDatetime: "TIMESTAMPTZ",
            }.get(base_type, "TEXT")

            nullable = "NULL" if is_optional else "NOT NULL"
            columns.append(f"{field} {sql_type} {nullable}")

        # Add the created_at and updated_at columns.
        columns.append("created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL")
        columns.append("updated_at TIMESTAMPTZ NULL")

        # Add UNIQUE constraints.
        unique_constraints = ", ".join([f"UNIQUE ({field})" for field in table.unique_fields])

        # Add PRIMARY KEY constraints.
        primary_keys_constraints = ", ".join(
            [f"PRIMARY KEY ({field})" for field in table.primary_keys]
        )

        # Combine columns, unique constraints, and primary key constraints.
        columns_def = ", ".join(columns)
        constraints = ", ".join(filter(None, [unique_constraints, primary_keys_constraints]))

        if table.unique_fields or table.primary_keys:
            query = f"CREATE TABLE IF NOT EXISTS {table.name} ({columns_def}, {constraints});"
        else:
            query = f"CREATE TABLE IF NOT EXISTS {table.name} ({columns_def});"

        # Execute the create table query.
        cursor.execute(query)

        # Create a hypertable if the table has a hypertable key.
        if table.hypertable_key and not self._hypertable_exists(table, cursor):
            query = f"SELECT create_hypertable('{table.name}', by_range('{table.hypertable_key}'));"
            cursor.execute(query)

        # Create the trigger for updating the updated_at field on update.
        self._create_updated_at_trigger(table.name, cursor)

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

    def _parse_filters(self, filters: SQLFilter) -> str:
        """Parse filters into a SQL statement."""

        query = ""
        if filters.where:
            query += " WHERE "
            for i, where in enumerate(filters.where):
                value = where.value
                if isinstance(value, str):
                    value = f"'{value}'"
                query += f"{where.column} {where.operator} {value}"
                if i < len(filters.where) - 1:
                    query += " AND "

        if filters.limit:
            query += f" LIMIT {filters.limit}"

        return query

    def _commit_and_close(self, cursor: Psycopg2Cursor) -> None:
        """Commit the transaction and close the cursor."""
        cursor.connection.commit()
        cursor.close()

    def save(self, options: TimescaleSaveOptions) -> SaveResult:
        """Saves data into the Timescale table and returns the result."""

        if options.df.is_empty():
            return SaveResult(status="error", message="Dataframe is empty", rows_affected=0)

        cursor = self._get_cursor(options.table)
        try:
            df = options.df
            columns = options.df.columns
            name = options.table.name

            # Prepare the insert query.
            placeholders = ", ".join("%s" for _ in columns)
            insert_query = f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({placeholders});"

            # Convert values for insertion.
            values = df.to_numpy()
            cursor.executemany(insert_query, values)
            self._commit_and_close(cursor)

            return SaveResult(status="success", message=None, rows_affected=df.shape[0])
        except Exception as e:
            return SaveResult(status="error", message=str(e), rows_affected=0)
        finally:
            cursor.close()

    def load(self, options: TimescaleLoadOptions) -> pl.DataFrame:
        """Loads data from a Timescale table and returns a polars DataFrame."""
        table = options.table
        filters = options.filters

        cursor = self._get_cursor(table)
        parsed_filters = self._parse_filters(filters) if filters else ""
        query = f"SELECT * FROM {table.name}{parsed_filters};"

        # Using polars to read data from the database.
        df = pl.read_database(connection=cursor.connection, query=query)
        self._commit_and_close(cursor)

        return df

    def upsert(self, options: TimescaleSaveOptions) -> UpsertResult:
        """Upsert data into the Timescale table and returns the result."""
        if options.df.is_empty():
            return UpsertResult(
                status="error",
                message="Dataframe is empty",
                rows_inserted=0,
                rows_updated=0,
            )

        cursor = self._get_cursor(options.table)
        df = options.df
        columns = options.df.columns
        table = options.table
        keys = (
            table.primary_keys
            if table.primary_keys
            else table.unique_fields if table.unique_fields else []
        )

        try:
            # Create the ON CONFLICT clause with the composite primary keys.
            conflict_keys = ", ".join(keys)

            # Create the placeholders for the values.
            placeholders = ", ".join("%s" for _ in columns)

            # Create the SQL part for updates, excluding the primary key columns.
            update_cols = ", ".join([f"{col}=excluded.{col}" for col in columns if col not in keys])

            # SQL for upsert.
            upsert_sql = f"""
                INSERT INTO {table.name} ({", ".join(columns)})
                VALUES ({placeholders})
                ON CONFLICT({conflict_keys})
                DO UPDATE SET {update_cols}
                RETURNING created_at, updated_at;
            """

            # Upsert and counting the number of rows inserted and updated.
            values = df.to_numpy()
            num_inserted = 0
            num_updated = 0
            for value in values:
                # Execute the upsert for each row.
                cursor.execute(upsert_sql, value)

                # Get the created_at and updated_at values.
                result = cursor.fetchone()
                updated_at = result[1] if result else None

                # Count insertions and updates.
                now = datetime.now(pytz.UTC)
                if updated_at and (now - updated_at).total_seconds() < 1:
                    num_updated += 1
                else:
                    num_inserted += 1

            return UpsertResult(
                status="success",
                message=None,
                rows_inserted=num_inserted,
                rows_updated=num_updated,
            )
        except Exception as e:
            return UpsertResult(status="error", message=str(e), rows_inserted=0, rows_updated=0)
        finally:
            self._commit_and_close(cursor)
