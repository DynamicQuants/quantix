from datetime import date

import pytest

from brokers.alpaca_market_data import AlpacaMarketData
from core.market_data import GetCalendarOptions


class TestAlpacaMarketData:
    """Test Alpaca market data class."""

    @pytest.fixture
    def alpaca_market_data(self):
        return AlpacaMarketData()

    def test_get_calendar(self, alpaca_market_data: AlpacaMarketData):
        calendar = alpaca_market_data.get_calendar(
            GetCalendarOptions(
                start=date(2021, 1, 1),
                end=date(2024, 12, 31),
            )
        )

        assert calendar.shape[0] > 0
        assert calendar.shape[1] == 3
        assert calendar.columns == ["date", "open", "close"]
