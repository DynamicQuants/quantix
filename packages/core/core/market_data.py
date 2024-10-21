# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""MarketData class that is responsible for fetching, storing, and retrieving market data."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Optional

from core.utils.performance import PerformanceTimer
from core.utils.registry import Registry

from .models.broker import Broker
from .models.timeframe import TimeFrame
from .ports.fetcher import FetchBarsParams, FetchCalendarParams, Fetcher
from .ports.repository import Repository
from .utils.dataframe import DataFrame

DEFAULT_BAR_START = datetime(1970, 1, 1)


@dataclass
class GetCalendarOptions:
    """Represents the optional filtering to get a calendar of trading days."""

    start: date | None = None
    end: date | None = None


@dataclass
class GetBarsOptions:
    """Represents the optional filtering to get bars for a symbol."""

    start: datetime
    end: datetime | None = None
    limit: int | None = None
    batch_size: int | None = None


class MarketDataError(Exception):
    """Market data error."""

    def __init__(
        self,
        message: str,
        code: Literal[
            "SAVE_CALENDAR_FAILED",
            "SAVE_BARS_FAILED",
            "SAVE_ASSETS_FAILED",
        ],
    ) -> None:
        super().__init__(message)
        self.code = code


class MarketData:
    """
    MarketData class that is responsible for fetching, storing, and retrieving market data. It
    handles the communication between the fetcher and the repository in a optimized way. All
    operations are saved in a registry for logging purposes.
    """

    def __init__(self, broker: Broker, fetcher: Fetcher, repository: Repository) -> None:
        self._broker = broker
        self._fetcher = fetcher
        self._repository = repository
        self._registry = Registry(repository, broker)

    def get_calendar(self, options: Optional[GetCalendarOptions] = None) -> DataFrame:
        """Get the calendar of trading days for a given broker."""

        with PerformanceTimer("Fetch calendar") as fetch_timer:
            params = FetchCalendarParams(start=options.start, end=options.end) if options else None
            calendar = self._fetcher.fetch_calendar(params)

        with PerformanceTimer("Upsert calendar") as upsert_timer:
            result = self._repository.upsert(calendar)

        if result.status == "error":
            raise MarketDataError(
                message=f"Failed to upsert calendar: {result.message}",
                code="SAVE_CALENDAR_FAILED",
            )

        self._registry.add(
            action="GET_CALENDAR",
            log="Calendar fetched and upserted",
            rows_inserted=result.rows_inserted,
            rows_updated=result.rows_updated,
            execution_time=fetch_timer.elapsed_time + upsert_timer.elapsed_time,
        )

        return calendar.df()

    def get_assets(self) -> DataFrame:
        """Get the assets for a given broker."""

        with PerformanceTimer("Fetch assets") as fetch_timer:
            assets = self._fetcher.fetch_assets()

        with PerformanceTimer("Upsert assets") as upsert_timer:
            result = self._repository.upsert(assets)

        if result.status == "error":
            raise MarketDataError(
                message=f"Failed to upsert assets: {result.message}",
                code="SAVE_ASSETS_FAILED",
            )

        self._registry.add(
            action="GET_ASSETS",
            log="Assets fetched and upserted",
            rows_inserted=result.rows_inserted,
            rows_updated=result.rows_updated,
            execution_time=fetch_timer.elapsed_time + upsert_timer.elapsed_time,
        )

        return assets.df()

    def get_bars(
        self,
        symbol: str,
        timeframe: TimeFrame,
        options: Optional[GetBarsOptions] = None,
    ) -> DataFrame:
        """Get bars for a symbol and timeframe using the given options."""

        params = (
            FetchBarsParams(
                symbol=symbol,
                timeframe=timeframe,
                start=options.start,
                end=options.end,
                limit=options.limit,
            )
            if options
            else FetchBarsParams(symbol=symbol, timeframe=timeframe, start=DEFAULT_BAR_START)
        )

        with PerformanceTimer("Fetch bars") as fetch_timer:
            bars = self._fetcher.fetch_bars(params)

        with PerformanceTimer("Upsert bars") as upsert_timer:
            result = self._repository.upsert(bars)

        if result.status == "error":
            raise MarketDataError(
                message=f"Failed to upsert bars: {result.message}",
                code="SAVE_BARS_FAILED",
            )

        self._registry.add(
            action="GET_BARS",
            log="Bars fetched and upserted",
            rows_inserted=result.rows_inserted,
            rows_updated=result.rows_updated,
            execution_time=fetch_timer.elapsed_time + upsert_timer.elapsed_time,
        )

        return bars.df()
