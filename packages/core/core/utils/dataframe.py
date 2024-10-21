# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Provides utilities for working with polars data frames."""

from dataclasses import dataclass
from typing import Annotated, Literal, cast

import pandera.polars as pa
import pandera.typing.polars as pat
import polars as pl

# Aliases for the data frame and series types.
DataFrameModel = pa.DataFrameModel
Series = pat.Series
Field = pa.Field
dataframe_check = pa.dataframe_check
PolarsData = pa.PolarsData

# Polars data frame types.
LazyFrame = pl.LazyFrame
DataFrame = pl.DataFrame
col = pl.col
concat_str = pl.concat_str
lit = pl.lit
read_database = pl.read_database

# Custom types for the data frame columns.
Timestamp = Annotated[pl.Datetime, "ns", "UTC"]
Date = pl.Date
Category = pl.Categorical
Float32 = pl.Float32


@dataclass
class DataContainerConfig:
    """Configuration for the data container."""

    name: str
    schema: pa.DataFrameSchema
    lf: pl.LazyFrame
    kind: Literal["relational", "non-relational", "timeseries"]
    primary_key: str
    unique_fields: list[str]


class DataContainer:
    """A wrapper around a LazyFrame that validates the data frame against a pandera schema."""

    def __init__(self, config: DataContainerConfig):
        self.name = config.name
        self.primary_key = config.primary_key
        self.schema = config.schema
        self.unique_fields = config.unique_fields
        self.kind = config.kind

        # This ensures that the data frame has the same columns as the schema.
        cleaned_lf = config.lf.select(config.schema.columns)
        self.lf = cast(pl.LazyFrame, config.schema.validate(cleaned_lf, lazy=True))

    def df(self) -> pl.DataFrame:
        """Returns the validated data frame."""
        return self.lf.collect()

    def dtypes(self) -> dict[str, str]:
        """Returns the data types of the data frame."""
        return {col: dtype_pa.__class__.__name__ for col, dtype_pa in self.schema.dtypes.items()}
