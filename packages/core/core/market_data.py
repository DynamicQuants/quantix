# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""MarketData class that is responsible for fetching, storing, and retrieving market data."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Optional

import polars as pl

from .models.broker import Broker
from .ports.fetcher import FetchCalendarParams, Fetcher
from .ports.repository import RegistryAction, RegistryItem, Repository


@dataclass
class GetCalendarOptions:
    """Represents the optional filtering to get a calendar of trading days."""

    start: date | None = None
    end: date | None = None


class MarketDataError(Exception):
    """Market data error."""

    def __init__(self, message: str, code: Literal["SAVE_CALENDAR_FAILED"]) -> None:
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

    def get_calendar(self, options: Optional[GetCalendarOptions] = None) -> pl.DataFrame:
        """Get the calendar of trading days for a given broker."""
        params = FetchCalendarParams(start=options.start, end=options.end) if options else None
        calendar = self._fetcher.fetch_calendar(params)
        result = self._repository.upsert("calendar", calendar)

        if result.status == "error":
            raise MarketDataError(
                message=f"Failed to upsert calendar: {result.message}",
                code="SAVE_CALENDAR_FAILED",
            )

        # If everything went well, save a log in the registry.
        self._repository.add_registry(
            RegistryItem(
                timestamp=datetime.now(),
                action=RegistryAction.FETCH_CALENDAR,
                broker=self._broker,
                log="Calendar fetched and upserted",
                n_records=calendar.shape[0],
                was_full=False,
            )
        )

        return calendar
