# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Alpaca Markets API client class."""

import os

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from dotenv import load_dotenv

# Get the environment variables from the .env file (or .env.test).
load_dotenv()


class AlpacaApiKeys:
    """Contains the API keys for the Alpaca Broker."""

    ALPACA_MARKET_DATA_KEY = os.environ.get("ALPACA_MARKET_DATA_KEY")
    ALPACA_MARKET_DATA_SECRET = os.environ.get("ALPACA_MARKET_DATA_SECRET")


class AlpacaDataClient:
    """
    Alpaca client class that provides access to the Alpaca API for data fetching and trading data.
    This class does not execute any trading actions, only fetches data.
    """

    def __init__(self):
        data_client = StockHistoricalDataClient(
            api_key=AlpacaApiKeys.ALPACA_MARKET_DATA_KEY,
            secret_key=AlpacaApiKeys.ALPACA_MARKET_DATA_SECRET,
            raw_data=False,
        )

        trading_client = TradingClient(
            api_key=AlpacaApiKeys.ALPACA_MARKET_DATA_KEY,
            secret_key=AlpacaApiKeys.ALPACA_MARKET_DATA_SECRET,
            raw_data=False,
        )

        # Only expose the necessary methods.
        self.get_all_assets = trading_client.get_all_assets
        self.get_calendar = trading_client.get_calendar
        self.get_stock_bars = data_client.get_stock_bars
        self.get_stock_latest_bar = data_client.get_stock_latest_bar
