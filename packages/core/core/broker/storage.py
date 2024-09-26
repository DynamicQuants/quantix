# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the interface for the storage used to save and retrieve market data."""

from abc import ABC, abstractmethod

from core.broker.fetcher import FetchBarsParams, FetchCalendarParams
from core.models.asset import AssetDataFrame
from core.models.bar import BarDataFrame
from core.models.calendar import CalendarDataFrame
from core.models.timeframe import TimeFrame


class Storage(ABC):
    """
    Storage abstract class which defines the methods that must be implemented in order to store and
    retrieve market data from the storage. The storage can be a database, a file, or any other data
    structure that can store data. The data manager uses this port as a persistence layer and
    sometimes as a cache.
    """

    @abstractmethod
    def save_calendar(self, calendar: CalendarDataFrame): ...

    @abstractmethod
    def load_calendar(self, params: FetchCalendarParams | None = None) -> CalendarDataFrame: ...

    @abstractmethod
    def save_assets(self, assets: AssetDataFrame): ...

    @abstractmethod
    def load_assets(self) -> AssetDataFrame: ...

    @abstractmethod
    def save_bars(self, symbol: str, timeframe: TimeFrame, bars: BarDataFrame): ...

    @abstractmethod
    def load_bars(self, params: FetchBarsParams) -> BarDataFrame: ...
