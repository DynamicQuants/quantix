# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Alpaca Markets Data Fetcher implementation."""

from typing import List, cast, final

import polars as pl
from alpaca.data.models import Bar as AlpacaBar
from alpaca.data.models import BarSet
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame
from alpaca.trading.enums import AssetClass as AlpacaAssetClass
from alpaca.trading.enums import AssetStatus as AlpacaAssetStatus
from alpaca.trading.models import Asset as AlpacaAsset
from alpaca.trading.models import Calendar as AlpacaCalendar
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest

from core.models.asset import Asset, AssetClass, AssetDataFrame, AssetStatus
from core.models.bar import Bar, BarDataFrame
from core.models.broker import Broker
from core.models.calendar import Calendar, CalendarDataFrame
from core.ports.fetcher import FetchBarsParams, FetchCalendarParams, Fetcher

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
        Fetcher.__init__(self, broker=Broker.ALPACA, has_calendar=True)

    def fetch_calendar(self, params: FetchCalendarParams | None = None) -> CalendarDataFrame:
        request = GetCalendarRequest(start=params.start, end=params.end) if params else None
        response = self.get_calendar(request)
        calendar = cast(list[AlpacaCalendar], response)
        return CalendarDataFrame(calendar).set_model(Calendar).drop().validate()

    def fetch_assets(self) -> AssetDataFrame:
        request = GetAssetsRequest(
            asset_class=AlpacaAssetClass.US_EQUITY,
            status=AlpacaAssetStatus.ACTIVE,
        )
        assets = cast(List[AlpacaAsset], self.get_all_assets(request))

        # Maps the Alpaca asset status to the AssetDataFrame Asset status.
        status_map = {
            AlpacaAssetStatus.ACTIVE: AssetStatus.ACTIVE,
            AlpacaAssetStatus.INACTIVE: AssetStatus.INACTIVE,
        }

        # Maps the Alpaca asset class to the AssetDataFrame Asset class.
        asset_class_map = {
            AlpacaAssetClass.US_EQUITY: AssetClass.EQUITY,
            AlpacaAssetClass.US_OPTION: AssetClass.OPTION,
            AlpacaAssetClass.CRYPTO: AssetClass.CRYPTO,
        }

        return (
            AssetDataFrame(assets)
            .set_model(Asset)
            .with_columns(
                [
                    pl.concat_str([pl.col("name"), pl.col("symbol")], separator="-").alias("name"),
                    pl.lit(Broker.ALPACA).alias("broker"),
                    pl.col("status").replace_strict(status_map).alias("status"),
                    pl.col("asset_class").replace_strict(asset_class_map).alias("asset_class"),
                ]
            )
            .drop()
            .validate()
        )

    def fetch_bars(self, params: FetchBarsParams) -> BarDataFrame:
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
        return BarDataFrame(bars).set_model(Bar).drop().validate()
