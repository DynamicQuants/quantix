# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of a calendar that represents the trading hours of a trading venue."""


from typing import final

from core.utils.dataframe import (
    Categorical,
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
class CalendarModel(DataFrameModel):
    """
    Calendar model that defines the data of a calendar that represents the trading hours of a
    trading venue. Depending the market, the trading day may start and end at different times.
    """

    broker: Series[Categorical] = Field(
        description="""The broker that provided the calendar.""",
        nullable=False,
        coerce=True,
        isin=[broker.value for broker in Broker],
    )

    date: Series[Date] = Field(
        description="""The date of the trading day.""",
        nullable=False,
        coerce=True,
    )

    open: Series[Timestamp] = Field(
        description="""Opening time of the trading day.""",
        nullable=False,
        coerce=True,
    )

    close: Series[Timestamp] = Field(
        description="""Closing time of the trading day.""",
        nullable=False,
        coerce=True,
    )

    @dataframe_check
    def open_lower_than_close(cls, data: PolarsData):
        """Check if the product of the open and close prices is negative."""
        return data.lazyframe.select(col("close").gt(col("open")))


@final
class CalendarData(DataContainer):
    """
    Calendar data container which contains a dataframe of calendars and validates it using the
    Calendar model. If the dataframe is not valid, a `DataFrameValidationError` exception is
    raised.
    """

    def __init__(self, lf: LazyFrame) -> None:
        super().__init__(
            DataContainerConfig(
                name="calendar",
                model=CalendarModel,
                lf=lf,
                kind="timeseries",
                primary_key="date",
                unique_fields=["broker"],
            )
        )
