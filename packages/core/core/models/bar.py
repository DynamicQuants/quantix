# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of a bar that represents the price movement of an asset."""

from typing import Optional, final

import patito as pt
from pydantic import AwareDatetime


@final
class Bar(pt.Model):
    """
    A bar better known as a candlestick is a representation of the price movement of an asset over
    a specific period of time. It contains the opening, high, low, and closing prices of the asset
    as well as the volume of the asset traded during that period.
    """

    symbol: str
    """The asset symbol of the bar."""

    timestamp: AwareDatetime = pt.Field(unique=True)
    """The timestamp of the bar, in seconds since the Unix epoch."""

    open: float = pt.Field(gt=0)
    """The opening price of the bar."""

    high: float = pt.Field(gt=0)
    """The highest price of the bar."""

    low: float = pt.Field(gt=0)
    """The lowest price of the bar."""

    close: float = pt.Field(gt=0)
    """The closing price of the bar."""

    volume: float = pt.Field(gt=0)
    """The volume of the bar."""

    vwap: Optional[float] = None
    """The volume-weighted average price of the bar."""


BarDataFrame = pt.DataFrame[Bar]
"""
A DataFrame containing a list of bars. This DataFrame must be validated using the Bar model.

For example:
.. code-block:: python
    data = Bar.examples(
        data={
            "symbol": ["AAPL", "AAPL"],
            "timestamp": ["2021-01-01T09:30:00Z","2021-01-01T09:31:00Z"],
            "open": [100.0, 101.0],
            "high": [100.0, 101.0],
            "low": [100.0, 101.0],
            "close": [100.0, 101.0],
            "volume": [1000, 1000],
        }
    )
"""
