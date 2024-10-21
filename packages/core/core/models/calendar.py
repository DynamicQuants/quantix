# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of a calendar that represents the trading hours of a trading venue."""


from typing import final

from core.utils.dataframe import (
    Category,
    DataContainer,
    DataContainerConfig,
    DataFrameModel,
    Date,
    Field,
    LazyFrame,
    PolarsData,
    Series,
    Timestamp,
    col,
    dataframe_check,
)

from .broker import Broker


@final
class Calendar(DataFrameModel):
    """
    Calendar data for a specific trading venue.

    Depending the market, the trading day may start and end at different times. This model defines
    the date of the trading day, as well as the opening and closing times of the trading day.
    """

    broker: Series[Category] = Field(
        nullable=False,
        coerce=True,
        isin=[broker.value for broker in Broker],
    )
    """The broker that provided the calendar."""

    date: Series[Date] = Field(nullable=False, coerce=True)
    """Date of the trading day."""

    open: Series[Timestamp] = Field(nullable=False, coerce=True)
    """Opening time of the trading day."""

    close: Series[Timestamp] = Field(nullable=False, coerce=True)
    """Closing time of the trading day."""

    @dataframe_check
    def open_lower_than_close(cls, df: PolarsData):
        """Check if the product of the open and close prices is negative."""
        return df.lazyframe.select(col("open").gt(col("close")))


@final
class CalendarData(DataContainer):
    """
    Calendar data container which contains the calendar dataframe and validates it using the
    Calendar model.
    """

    def __init__(self, lf: LazyFrame) -> None:
        super().__init__(
            DataContainerConfig(
                name="calendar",
                schema=Calendar.to_schema(),
                lf=lf,
                kind="timeseries",
                primary_key="date",
                unique_fields=["broker"],
            )
        )
