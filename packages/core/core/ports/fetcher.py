# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the interface for fetching market data from a broker."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime

from core.models.asset import AssetData
from core.models.bar import BarData
from core.models.calendar import CalendarData
from core.models.timeframe import TimeFrame


@dataclass
class FetchCalendarParams:
    """Represents the optional filtering to get a calendar of trading days."""

    start: date | None = None
    """The start date to get the calendar from."""

    end: date | None = None
    """The end date to get the calendar up to."""


@dataclass
class FetchBarsParams:
    """Defines the parameters for fetching bars for a symbol."""

    symbol: str
    """The symbol of the asset to fetch data for."""

    timeframe: TimeFrame
    """The time interval to fetch data for."""

    start: datetime
    """The start timestamp for the data to be fetched."""

    end: datetime | None
    """The end timestamp for the data to be fetched. If None, fetch data up to the current time."""

    limit: int | None
    """The maximum number of bars to fetch. If None, fetch all available bars."""

    def __init__(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: datetime,
        end: datetime | None = None,
        limit: int | None = None,
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.limit = limit
        self._validate_params()

    def _validate_params(self):
        """Validates the fetch parameters. If the parameters are invalid, it raises a ValueError."""
        if self.start >= datetime.now():
            raise ValueError("Start date cannot be in the future.")

        if self.end and self.end >= datetime.now():
            raise ValueError("End date cannot be in the future.")

        if self.end and self.end <= self.start:
            raise ValueError("End date must be later than start date.")


class Fetcher(ABC):
    """
    Abstract class that defines the interface for fetching market data from a broker. This class
    should be implemented by concrete classes that provide the implementation for fetching market
    data from a specific broker.
    """

    def __init__(self, has_calendar: bool):
        self.has_calendar = has_calendar

    @abstractmethod
    def fetch_calendar(self, params: FetchCalendarParams | None = None) -> CalendarData:
        """
        Fetches the trading calendar for the specified broker.

        This method should return a DataFrame containing the trading calendar for the specified
        broker.
        """
        ...

    @abstractmethod
    def fetch_assets(self) -> AssetData:
        """
        Fetches the list of assets available for trading by the broker.

        This method should return a DataFrame containing the list of assets available for trading
        by the broker.
        """
        ...

    @abstractmethod
    def fetch_bars(self, params: FetchBarsParams) -> BarData:
        """
        Fetches the bars for the specified asset symbol and timeframe (check the FetchBarsParams).

        This method should return a DataFrame containing the bars for the specified asset symbol
        and timeframe.
        """
