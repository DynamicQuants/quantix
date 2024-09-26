# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the interface for fetching market data from a broker."""

from abc import ABC, abstractmethod
from datetime import date, datetime

from pydantic.dataclasses import dataclass

from core.models.asset import AssetDataFrame
from core.models.bar import BarDataFrame
from core.models.broker import Broker
from core.models.calendar import CalendarDataFrame
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

    def __init__(self, broker: Broker, has_calendar: bool):
        self.broker = broker
        self.has_calendar = has_calendar

    def __str__(self):
        return f"Fetcher<{self.broker.value}>"

    @abstractmethod
    def fetch_calendar(self, params: FetchCalendarParams | None = None) -> CalendarDataFrame:
        """
        Fetches the trading calendar for the specified broker.

        This method should return a DataFrame containing the trading calendar for the specified
        broker. The DataFrame must be validated using the Calendar model.
        """
        ...

    @abstractmethod
    def fetch_assets(self) -> AssetDataFrame:
        """
        Fetches the list of assets available for trading by the broker.

        This method should return a DataFrame containing the list of assets available for trading
        by the broker. The DataFrame must be validated using the Asset model.
        """
        ...

    @abstractmethod
    def fetch_bars(self, params: FetchBarsParams) -> BarDataFrame:
        """
        Fetches the bars for the specified asset symbol and timeframe (check the FetchBarsParams).

        This method should return a DataFrame containing the bars for the specified asset symbol
        and timeframe. The DataFrame must be validated using the Bar model.
        """
