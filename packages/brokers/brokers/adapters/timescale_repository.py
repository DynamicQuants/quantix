# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""TimescaleDB Repository implementation."""

from dataclasses import astuple, dataclass
from datetime import date, datetime
from typing import Any, Optional, Union, final, get_args, get_origin

import patito as pt
import polars as pl
import pytz
from psycopg2.extensions import cursor as Psycopg2Cursor
from pydantic import AwareDatetime

from core.ports.repository import (
    LoadOptions,
    RegistryAction,
    RegistryItem,
    Repository,
    RepositoryError,
    SaveResult,
    UpsertResult,
)

from .timescale_client import TimescaleClient


@dataclass
class TimescaleRepositoryPayload:
    """Defines the payload shape for Timescale repository."""

    model: type[pt.Model] | object
    db: str
    schema: str
    table_name: str
    hypertable_key: str | None = None
    primary_key: str | None = None
    unique_fields: list[str] | None = None


@final
class TimescaleRepository(TimescaleClient, Repository):
    """
    Repository implementation that saves and retrieves data from a Timescale database. It uses
    hyper-tables to store the data in a time-series format and normal tables for relational data.
    For more information, visit:
        https://docs.timescale.com/use-timescale/latest/hypertables/
        https://docs.timescale.com/quick-start/latest/python/
    """

    def __init__(self, payloads: dict[str, TimescaleRepositoryPayload]) -> None:
        TimescaleClient.__init__(self)
        Repository.__init__(self, payloads)

    def _get_cursor(self, payload: TimescaleRepositoryPayload) -> Psycopg2Cursor:
        """
        Get a cursor for the given payload. If table_name does not exist, create it using the table
        model definition.
        """
        cursor = self.connect().cursor()

        if not self._table_exists(payload, cursor):
            self._create_table(payload, cursor)

            # Create the trigger for updating the updated_at field on update.
            self._create_updated_at_trigger(payload.table_name, cursor)

        if payload.hypertable_key and not self._hypertable_exists(payload, cursor):
            self._create_hypertable(payload, cursor)

        return cursor

    def _close_cursor(self, cursor: Psycopg2Cursor) -> None:
        """Commit the transaction and close the cursor."""
        cursor.connection.commit()
        cursor.close()

    def _hypertable_exists(self, table: TimescaleRepositoryPayload, cursor: Psycopg2Cursor):
        """Check if the table is a hypertable."""
        name = table.table_name
        query = (
            f"SELECT * FROM timescaledb_information.hypertables where hypertable_name = '{name}';"
        )
        cursor.execute(query)
        result = cursor.fetchall()
        return len(result) > 0

    def _table_exists(self, table: TimescaleRepositoryPayload, cursor: Psycopg2Cursor) -> bool:
        """Check if a table exists in the current PostgreSQL database."""

        query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{table.schema}'
                AND table_name = '{table.table_name}'
            );
        """

        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else False

    def _create_table(self, table: TimescaleRepositoryPayload, cursor: Psycopg2Cursor) -> None:
        """Create a table in the database if it does not exist."""

        # Set default values for primary_keys and unique_fields.
        if table.unique_fields is None:
            table.unique_fields = []

        columns: list[Any] = []
        for field, type_hint in table.model.__annotations__.items():
            is_optional = get_origin(type_hint) is Union and type(None) in get_args(type_hint)
            base_type = get_args(type_hint)[0] if is_optional else type_hint

            sql_type = {
                int: "INTEGER",
                str: "TEXT",
                float: "REAL",
                bool: "BOOLEAN",
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
        primary_keys_constraints = f"PRIMARY KEY ({table.primary_key})" if table.primary_key else ""

        # Combine columns, unique constraints, and primary key constraints.
        columns_def = ", ".join(columns)
        constraints = ", ".join(filter(None, [unique_constraints, primary_keys_constraints]))

        if table.unique_fields or table.primary_key:
            query = f"CREATE TABLE IF NOT EXISTS {table.table_name} ({columns_def}, {constraints});"
        else:
            query = f"CREATE TABLE IF NOT EXISTS {table.table_name} ({columns_def});"

        # Execute the create table query.
        cursor.execute(query)

    def _create_hypertable(self, table: TimescaleRepositoryPayload, cursor: Psycopg2Cursor) -> None:
        """Create a hypertable in the database."""

        query = f"""
            SELECT create_hypertable('{table.table_name}', by_range('{table.hypertable_key}'));
        """
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

    def save(self, key: str, df: pl.DataFrame) -> SaveResult:
        """Saves data into the Timescale table and returns the result."""

        if df.is_empty():
            return SaveResult(status="error", message="Dataframe is empty", rows_affected=0)

        payload = self.get_payload(key, TimescaleRepositoryPayload)
        cursor = self._get_cursor(payload)

        try:
            columns = df.columns
            name = payload.table_name

            # Prepare the insert query.
            placeholders = ", ".join("%s" for _ in columns)
            insert_query = f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({placeholders});"

            # Convert values for insertion.
            values = df.to_numpy()
            cursor.executemany(insert_query, values)
            self._close_cursor(cursor)

            return SaveResult(status="success", message=None, rows_affected=df.shape[0])
        except Exception as e:
            return SaveResult(status="error", message=str(e), rows_affected=0)
        finally:
            cursor.close()

    def load(self, key: str, options: Optional[LoadOptions] = None) -> pl.DataFrame:
        """Loads data from a Timescale table and returns a polars DataFrame."""
        payload = self.get_payload(key, TimescaleRepositoryPayload)
        cursor = self._get_cursor(payload)

        parsed_filters = self._parse_load_options(options) if options else ""
        query = f"SELECT * FROM {payload.table_name}{parsed_filters};"

        # Using polars to read data from the database.
        df = pl.read_database(connection=cursor.connection, query=query)
        self._close_cursor(cursor)

        return df

    def upsert(self, key: str, df: pl.DataFrame) -> UpsertResult:
        """Upsert data into the Timescale table and returns the result."""

        if df.is_empty():
            return UpsertResult(
                status="error",
                message="Dataframe is empty",
                rows_inserted=0,
                rows_updated=0,
            )

        payload = self.get_payload(key, TimescaleRepositoryPayload)
        cursor = self._get_cursor(payload)

        # Get the columns and keys for the upsert. The primary key is used by default.
        keys = (
            [payload.primary_key]
            if payload.primary_key
            else payload.unique_fields if payload.unique_fields else []
        )

        try:
            # Create the ON CONFLICT clause with the composite primary keys.
            conflict_keys = ", ".join(keys)

            # Create the placeholders for the values.
            placeholders = ", ".join("%s" for _ in df.columns)

            # Create the SQL part for updates, excluding the primary key columns.
            update_cols = ", ".join(
                [f"{col}=excluded.{col}" for col in df.columns if col not in keys]
            )

            # SQL for upsert.
            upsert_sql = f"""
                INSERT INTO {payload.table_name} ({", ".join(df.columns)})
                VALUES ({placeholders})
                ON CONFLICT({conflict_keys})
                DO UPDATE SET {update_cols}
                RETURNING created_at, updated_at;
            """

            # Upsert and counting the number of rows inserted and updated.
            values = df.rows()
            num_inserted = 0
            num_updated = 0
            for value in values:
                # Execute the upsert for each row.
                cursor.execute(upsert_sql, value)

                # Get the updated_at value to check if the row was updated.
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
            self._close_cursor(cursor)

    def add_registry(self, item: RegistryItem) -> None:
        """Add a new item to the registry table."""

        payload = self.get_payload("registry", TimescaleRepositoryPayload)
        cursor = self._get_cursor(payload)

        try:
            columns = item.__annotations__.keys()
            values = astuple(item)
            placeholders = ", ".join("%s" for _ in columns)
            query = (
                f"INSERT INTO {payload.table_name} ({', '.join(columns)}) VALUES ({placeholders});"
            )
            cursor.execute(query, values)
        except Exception as e:
            raise RepositoryError(
                message=str(e),
                code="CANNOT_ADD_REGISTRY_ITEM",
            )
        finally:
            self._close_cursor(cursor)

    def get_registry(self, action: RegistryAction, since: datetime) -> list[RegistryItem]:
        """Get all registry items with the given action and created after the given timestamp."""

        payload = self.get_payload("registry", TimescaleRepositoryPayload)
        cursor = self._get_cursor(payload)

        try:
            query = f"SELECT * FROM {payload.table_name} WHERE action = %s AND created_at > %s;"
            cursor.execute(query, (action, since))
            result = cursor.fetchall()

            # Convert the result into a list of RegistryItem objects. Note we use the first 6 fields
            # of the result because the last two are created_at and updated_at (which are not part
            # of the RegistryItem model).
            return [RegistryItem(*item[:6]) for item in result]
        except Exception as e:
            raise RepositoryError(
                message=str(e),
                code="CANNOT_GET_REGISTRY_ITEM",
            )
        finally:
            self._close_cursor(cursor)
