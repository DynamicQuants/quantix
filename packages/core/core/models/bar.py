# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of a bar that represents the price movement of an asset."""

from typing import Optional, final

from core.utils.dataframe import (
    Categorical,
    DataContainer,
    DataContainerConfig,
    DataFrameBaseModel,
    Field,
    LazyFrame,
    Series,
    Timestamp,
)

from .broker import Broker
from .timeframe import TFPreset, TimeFrame


@final
class BarModel(DataFrameBaseModel):
    """
    A bar better known as a candlestick is a representation of the price movement of an asset over
    a specific period of time. It contains the opening, high, low, and closing prices of the asset
    as well as the volume of the asset traded during that period.
    """

    timestamp: Series[Timestamp] = Field(
        description="""The timestamp of the bar, in seconds since the Unix epoch.""",
        coerce=True,
    )

    broker: Series[Categorical] = Field(
        description="""The broker that provided the bar.""",
        nullable=False,
        coerce=True,
        isin=[broker.value for broker in Broker],
    )

    symbol: Series[str] = Field(
        description="""The asset symbol of the bar.""",
        nullable=False,
    )

    timeframe: Series[Categorical] = Field(
        description="""The bar timeframe.""",
        nullable=False,
        coerce=True,
        # Only include the timeframes that are defined in the TFPreset.
        isin=[tf.name_value for tf in TFPreset.__dict__.values() if isinstance(tf, TimeFrame)],
    )

    open: Series[float] = Field(
        description="""The opening price of the bar.""",
        nullable=False,
        gt=0,
    )

    high: Series[float] = Field(
        description="""The highest price of the bar.""",
        nullable=False,
        gt=0,
    )

    low: Series[float] = Field(
        description="""The lowest price of the bar.""",
        nullable=False,
        gt=0,
    )

    close: Series[float] = Field(
        description="""The closing price of the bar.""",
        nullable=False,
        gt=0,
    )

    volume: Series[float] = Field(
        description="""The volume of the bar.""",
        nullable=False,
        gt=0,
    )

    vwap: Optional[Series[float]] = Field(
        description="""The volume-weighted average price of the bar.""",
        nullable=True,
        gt=0,
    )


@final
class BarData(DataContainer):
    """
    Bar data container which contains a dataframe of bars and validates it using the Bar model.
    If the dataframe is not valid, a `DataFrameValidationError` exception is raised.
    """

    def __init__(self, lf: LazyFrame) -> None:
        super().__init__(
            DataContainerConfig(
                name="bars",
                model=BarModel,
                lf=lf,
                kind="timeseries",
                primary_key="timestamp",
                unique_fields=["broker", "symbol", "timeframe"],
            )
        )
