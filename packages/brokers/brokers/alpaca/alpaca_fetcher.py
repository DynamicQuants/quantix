# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Alpaca Markets Data Fetcher implementation."""

from typing import List, cast, final

from alpaca.data.models import Bar as AlpacaBar
from alpaca.data.models import BarSet
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame
from alpaca.trading.enums import AssetClass as AlpacaAssetClass
from alpaca.trading.enums import AssetStatus as AlpacaAssetStatus
from alpaca.trading.models import Asset as AlpacaAsset
from alpaca.trading.models import Calendar as AlpacaCalendar
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest

from core.models.asset import AssetClass, AssetData, AssetStatus
from core.models.bar import BarData
from core.models.broker import Broker
from core.models.calendar import CalendarData
from core.ports.fetcher import FetchBarsParams, FetchCalendarParams, Fetcher
from core.utils.dataframe import LazyFrame, col, concat_str, lit

from .alpaca_client import AlpacaDataClient


@final
class AlpacaFetcher(AlpacaDataClient, Fetcher):
    """
    Alpaca Markets Data Fetcher implementation that fetches market data from the Alpaca API.
    It uses the official Alpaca Markets Python SDK to fetch data from the Alpaca API. For more
    information, visit: https://alpaca.markets/sdks/python/
    """

    def __init__(self):
        AlpacaDataClient.__init__(self)
        Fetcher.__init__(self, has_calendar=True)

    def fetch_calendar(self, params: FetchCalendarParams | None = None) -> CalendarData:
        """Fetch the calendar from the Alpaca API."""
        request = GetCalendarRequest(start=params.start, end=params.end) if params else None
        response = self.get_calendar(request)
        calendar = cast(list[AlpacaCalendar], response)
        lf = LazyFrame(calendar).with_columns([lit(Broker.ALPACA.value).alias("broker")])
        return CalendarData(lf)

    def fetch_assets(self) -> AssetData:
        """Fetch the assets from the Alpaca API."""
        request = GetAssetsRequest(
            asset_class=AlpacaAssetClass.US_EQUITY,
            status=AlpacaAssetStatus.ACTIVE,
        )
        assets = cast(List[AlpacaAsset], self.get_all_assets(request))

        # Maps the Alpaca asset status to the AssetDataFrame Asset status.
        status_map = {
            AlpacaAssetStatus.ACTIVE: AssetStatus.ACTIVE.value,
            AlpacaAssetStatus.INACTIVE: AssetStatus.INACTIVE.value,
        }

        # Maps the Alpaca asset class to the AssetDataFrame Asset class.
        asset_class_map = {
            AlpacaAssetClass.US_EQUITY: AssetClass.EQUITY.value,
            AlpacaAssetClass.US_OPTION: AssetClass.OPTION.value,
            AlpacaAssetClass.CRYPTO: AssetClass.CRYPTO.value,
        }

        lf = LazyFrame(assets).with_columns(
            [
                concat_str([col("name"), col("symbol")], separator="-").alias("name"),
                lit(Broker.ALPACA.value).alias("broker"),
                col("status").replace_strict(status_map).alias("status"),
                col("asset_class").replace_strict(asset_class_map).alias("asset_class"),
            ]
        )

        return AssetData(lf)

    def fetch_bars(self, params: FetchBarsParams) -> BarData:
        """Fetch the bars from the Alpaca API."""
        request = StockBarsRequest(
            symbol_or_symbols=params.symbol,
            timeframe=AlpacaTimeFrame(
                params.timeframe.amount_value,
                params.timeframe.unit_value,
            ),
            start=params.start,
            end=params.end,
            limit=params.limit,
        )

        response = self.get_stock_bars(request)
        bars = cast(List[AlpacaBar], cast(BarSet, response)[params.symbol])
        lf = LazyFrame(bars).with_columns(
            [
                lit(params.symbol).alias("symbol"),
                lit(Broker.ALPACA.value).alias("broker"),
                lit(params.timeframe.name_value).alias("timeframe"),
            ]
        )

        return BarData(lf)
