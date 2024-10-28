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
from pandera.api.polars.model_config import BaseConfig
from pandera.errors import SchemaError, SchemaErrors

# Aliases for the data frame and series types.
DataFrameModel = pa.DataFrameModel
DataFrameSchema = pa.DataFrameSchema
SchemaErrors = SchemaErrors
SchemaError = SchemaError
Series = pat.Series
Field = pa.Field
dataframe_check = pa.dataframe_check
PolarsData = pa.PolarsData

# Polars data frame types.
LazyFrame = pl.LazyFrame
DataFrame = pl.DataFrame

# Utility functions.
col = pl.col
concat_str = pl.concat_str
lit = pl.lit
read_database = pl.read_database

# Custom types for the data frame columns.
Timestamp = Annotated[pl.Datetime, "ns", "UTC"]
DateTime = pl.Datetime
Date = pl.Date
Categorical = pl.Categorical
Float32 = pl.Float32
Float64 = pl.Float64
String = pl.String
Boolean = pl.Boolean
Time = pl.Time


@dataclass
class DataContainerConfig:
    """Configuration for the data container."""

    name: str
    model: Type[DataFrameModel]
    lf: pl.LazyFrame
    kind: Literal["relational", "non-relational", "timeseries"]
    primary_key: str
    unique_fields: list[str]


ErrorCode = Literal["VALIDATION_ERROR", "POLARS_ERROR"]
"""
Error codes for the data container validation errors. Both indicate a failure in the
validation process.
"""


@dataclass
class DataFrameValidationError(Exception):
    """Exception raised when a data container validation fails."""

    def __init__(self, message: str, failure_cases: DataFrame, error_code: ErrorCode):
        self.message = message
        self.failure_cases = failure_cases
        self.error_code = error_code
        super().__init__(self.message)


class DataFrameBaseModel(DataFrameModel):
    """Pandera base dataframe model."""

    class Config(BaseConfig):
        """Configuration for the base model."""

        # Make sure all specified columns are in the validated dataframe - if "filter",
        # removes columns not specified in the schema.
        strict = "filter"

        # Drop invalid rows. This raises an error if there are any invalid rows.
        drop_invalid_rows = False


class DataContainer:
    """A wrapper around a LazyFrame that validates the data frame against a pandera schema."""

    def __init__(self, config: DataContainerConfig):

        self.name = config.name
        self.primary_key = config.primary_key
        self.unique_fields = config.unique_fields
        self.kind = config.kind
        self.schema = config.model.to_schema()

        try:
            validated_df = self.schema.validate(config.lf.collect(), lazy=True)
        except SchemaErrors as e:
            raise DataFrameValidationError(
                message=f"Validation failed for {self.name} data container.",
                failure_cases=e.failure_cases,
                error_code="VALIDATION_ERROR",
            )
        except pl.exceptions.PanicException as es:
            # XXX:This should never happen. But there is a bug in polars that raises a
            # PanicException instead of a SchemaError.
            # Check: https://github.com/unionai-oss/pandera/issues/1841
            raise DataFrameValidationError(
                message=str(es),
                failure_cases=pl.DataFrame(),
                error_code="POLARS_ERROR",
            )

        self._df = validated_df.collect() if isinstance(validated_df, LazyFrame) else validated_df

    def df(self) -> pl.DataFrame:
        """Returns the validated data frame."""
        return self._df

    def dtypes(self) -> dict[str, str]:
        """Returns the data types of the data frame."""
        return {col: dtype_pa.__class__.__name__ for col, dtype_pa in self.schema.dtypes.items()}
