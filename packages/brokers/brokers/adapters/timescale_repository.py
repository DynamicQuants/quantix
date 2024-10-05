# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""TimescaleDB Repository implementation."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, Union, get_args, get_origin

import patito as pt

from core.ports.repository import Repository, SaveResult

from .timescale_client import TimeScaleClient


@dataclass
class SQLWhere:
    """Defines a where clause for SQLite queries."""

    column: str
    operator: Literal["=", ">", "<", ">=", "<=", "LIKE"]
    value: Union[str, int, float, bool]


@dataclass
class SQLFilter:
    """Defines a filter for SQLite queries."""

    where: list[SQLWhere] | None
    limit: int | None


@dataclass
class TimescaleTable:
    """Defines a SQLite table."""

    model: pt.Model
    db: str
    name: str
    hypertable_key: str | None = None
    primary_keys: list[str] | None = None
    unique_fields: list[str] | None = None


@dataclass
class TimescaleSaveOptions:
    """Defines timescale options for saving data to an SQLite table."""

    table: TimescaleTable
    df: pt.DataFrame


@dataclass
class TimescaleLoadOptions:
    """Defines timescale options for loading data from an SQLite table."""

    table: TimescaleTable
    filters: SQLFilter | None = None


class TimescaleRepository(
    TimeScaleClient, Repository[TimescaleSaveOptions, TimescaleLoadOptions, Any]
):
    """
    Repository implementation that saves and retrieves data from a Timescale database. It uses
    hyper-tables to store the data in a time-series format and normal tables for relational data.
    For more information, visit:
        https://docs.timescale.com/use-timescale/latest/hypertables/
        https://docs.timescale.com/quick-start/latest/python/
    """

    def __init__(self):
        self._conn = "postgresql://postgres:postgres@localhost:5432/test"

    def _create_table(self, table: TimescaleTable) -> None:
        if not isinstance(table.model, pt.Model):
            raise ValueError("The model must be a dataclass")

        cursor = self.connect().cursor()

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
                datetime: "DATETIME",
            }.get(base_type, "TEXT")

            nullable = "NULL" if is_optional else "NOT NULL"
            columns.append(f"{field} {sql_type} {nullable}")

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
        if table.hypertable_key:
            query = f"SELECT create_hypertable('{table.name}', by_range('{table.hypertable_key}'));"
            cursor.execute(query)

        cursor.close()

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
            query += " LIMIT {filters.limit}"

        return query

    def save(self, options: TimescaleSaveOptions) -> SaveResult:
        if options.df.is_empty:
            return SaveResult(status="error", message="Dataframe is empty", rows_affected=0)

        table = options.table
        df = options.df
        conn = self.connect()

        df.write_database(
            table_name=table.name,
            connection=conn,
            if_table_exists="append",
        )

        return SaveResult(status="success", message=None, rows_affected=df.shape[0])
