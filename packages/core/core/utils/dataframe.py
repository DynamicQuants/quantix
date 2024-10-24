# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Provides utilities for working with polars data frames."""

from dataclasses import dataclass
from typing import Annotated, Literal, Type

import pandera.polars as pa
import pandera.typing.polars as pat
import polars as pl

# Aliases for the data frame and series types.
DataFrameModel = pa.DataFrameModel
DataFrameSchema = pa.DataFrameSchema
SchemaErrors = pa.errors.SchemaErrors
SchemaError = pa.errors.SchemaError
Series = pat.Series
Field = pa.Field
Column = pa.Column
Check = pa.Check
dataframe_check = pa.dataframe_check
PolarsData = pa.PolarsData

# Polars data frame types.
LazyFrame = pl.LazyFrame
DataFrame = pl.DataFrame
col = pl.col
concat_str = pl.concat_str
lit = pl.lit
read_database = pl.read_database

# Pandera extensions.
check = pa.check

# Custom types for the data frame columns.
Timestamp = Annotated[pl.Datetime, "ns", "UTC"]
Date = pl.Date
Category = pl.Categorical
Float32 = pl.Float32
String = pl.String
Boolean = pl.Boolean


@dataclass
class DataContainerConfig:
    """Configuration for the data container."""

    name: str
    model: Type[DataFrameModel]
    lf: pl.LazyFrame
    kind: Literal["relational", "non-relational", "timeseries"]
    primary_key: str
    unique_fields: list[str]


@dataclass
class DataFrameValidationError(Exception):
    """Exception raised when a data container validation fails."""

    def __init__(self, message: str, failure_cases: DataFrame):
        self.message = message
        self.failure_cases = failure_cases
        super().__init__(self.message)


class DataContainer:
    """A wrapper around a LazyFrame that validates the data frame against a pandera schema."""

    def __init__(self, config: DataContainerConfig):
        self.name = config.name
        self.primary_key = config.primary_key
        self.unique_fields = config.unique_fields
        self.kind = config.kind
        self.schema = config.model.to_schema()

        # This ensures that the data frame has the same columns as the schema.
        cleaned_df = config.lf.select(self.schema.columns).collect()
        try:
            validated_df = self.schema.validate(cleaned_df, lazy=True)
        except SchemaErrors as e:
            print(e.failure_cases)
            raise DataFrameValidationError(
                message=f"Validation failed for {self.name} data container.",
                failure_cases=e.failure_cases,
            )

        self._df = validated_df.collect() if isinstance(validated_df, LazyFrame) else validated_df

    def df(self) -> pl.DataFrame:
        """Returns the validated data frame."""
        return self._df

    def dtypes(self) -> dict[str, str]:
        """Returns the data types of the data frame."""
        return {col: dtype_pa.__class__.__name__ for col, dtype_pa in self.schema.dtypes.items()}
