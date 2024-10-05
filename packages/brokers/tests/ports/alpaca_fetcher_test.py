from datetime import date, datetime

import pytest

from brokers.adapters.alpaca_fetcher import AlpacaFetcher
from core.models.timeframe import TFPreset
from core.ports.fetcher import FetchBarsParams, FetchCalendarParams


class TestAlpacaFetcher:
    @pytest.fixture
    def alpaca_fetcher(self):
        return AlpacaFetcher()

    def test_fetch_calendar(self, alpaca_fetcher: AlpacaFetcher):
        calendar = alpaca_fetcher.fetch_calendar(
            FetchCalendarParams(
                start=date(2024, 9, 1),
                end=date(2024, 9, 10),
            )
        )

        assert calendar.shape[0] == 6

    def test_fetch_assets(self, alpaca_fetcher: AlpacaFetcher):
        assets = alpaca_fetcher.fetch_assets()
        assert assets.shape[0] > 0

    def test_fetch_bars(self, alpaca_fetcher: AlpacaFetcher):
        bars = alpaca_fetcher.fetch_bars(
            FetchBarsParams(
                symbol="AAPL",
                timeframe=TFPreset.Tf_D,
                start=datetime(2024, 9, 1),
                end=datetime(2024, 9, 10),
            )
        )

        assert bars.shape[0] == 5
