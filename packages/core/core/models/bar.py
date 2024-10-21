# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of a bar that represents the price movement of an asset."""

from typing import Optional, final

from core.utils.dataframe import (
    Category,
    DataContainer,
    DataContainerConfig,
    DataFrameModel,
    Field,
    LazyFrame,
    Series,
    Timestamp,
)

from .broker import Broker


@final
class Bar(DataFrameModel):
    """
    A bar better known as a candlestick is a representation of the price movement of an asset over
    a specific period of time. It contains the opening, high, low, and closing prices of the asset
    as well as the volume of the asset traded during that period.
    """

    timestamp: Series[Timestamp] = Field(coerce=True)
    """The timestamp of the bar, in seconds since the Unix epoch."""

    broker: Series[Category] = Field(
        nullable=False,
        coerce=True,
        isin=[broker.value for broker in Broker],
    )
    """The broker that provided the bar."""

    symbol: Series[str] = Field(nullable=False)
    """The asset symbol of the bar."""

    timeframe: Series[str] = Field(nullable=False)
    """The timeframe of the bar."""

    open: Series[float] = Field(gt=0)
    """The opening price of the bar."""

    high: Series[float] = Field(gt=0)
    """The highest price of the bar."""

    low: Series[float] = Field(gt=0)
    """The lowest price of the bar."""

    close: Series[float] = Field(gt=0)
    """The closing price of the bar."""

    volume: Series[float] = Field(gt=0)
    """The volume of the bar."""

    vwap: Optional[Series[float]] = Field(nullable=True)
    """The volume-weighted average price of the bar."""


@final
class BarData(DataContainer):
    """
    Bar data container which contains the bar dataframe and validates it using the Bar model.
    """

    def __init__(self, lf: LazyFrame) -> None:
        super().__init__(
            DataContainerConfig(
                name="bars",
                schema=Bar.to_schema(),
                lf=lf,
                kind="timeseries",
                primary_key="timestamp",
                unique_fields=["broker", "symbol", "timeframe"],
            )
        )
