from datetime import date, datetime

import pytest

from brokers.alpaca.alpaca_data import AlpacaData
from core.data import GetBarsOptions, GetCalendarOptions
from core.models.asset import AssetModel
from core.models.bar import BarModel
from core.models.calendar import CalendarModel
from core.models.timeframe import TFPreset


class TestAlpacaData:
    """Test Alpaca data class."""

    @pytest.fixture
    def alpaca_data(self):
        return AlpacaData()

    def test_get_calendar(self, alpaca_data: AlpacaData):
        calendar = alpaca_data.get_calendar(
            GetCalendarOptions(
                start=date(2021, 1, 1),
                end=date(2024, 12, 31),
            )
        )

        assert calendar.shape[0] > 0
        assert calendar.shape[1] == 4
        assert sorted(calendar.columns) == sorted(CalendarModel.to_schema().columns)

    def test_get_assets(self, alpaca_data: AlpacaData):
        assets = alpaca_data.get_assets()
        assert assets.shape[0] > 0
        assert assets.shape[1] == 7
        assert sorted(assets.columns) == sorted(AssetModel.to_schema().columns)

    def test_get_bars(self, alpaca_data: AlpacaData):
        bars = alpaca_data.get_bars(
            symbol="GOOG",
            timeframe=TFPreset.Tf_D,
        )

        assert bars.shape[0] > 0
        assert bars.shape[1] == 10
        assert sorted(bars.columns) == sorted(BarModel.to_schema().columns)

    def test_get_bars_with_options_range(self, alpaca_data: AlpacaData):
        bars = alpaca_data.get_bars(
            symbol="AAPL",
            timeframe=TFPreset.Tf_D,
            options=GetBarsOptions(
                start=datetime(2024, 1, 1),
                end=datetime(2024, 6, 1),
            ),
        )

        assert bars.shape[0] > 5
        assert bars.shape[1] == 10
        assert sorted(bars.columns) == sorted(BarModel.to_schema().columns)

    def test_get_bars_with_options_limit(self, alpaca_data: AlpacaData):
        bars = alpaca_data.get_bars(
            symbol="AAPL",
            timeframe=TFPreset.Tf_D,
            options=GetBarsOptions(
                start=datetime(2024, 1, 1),
                limit=5,
            ),
        )

        assert bars.shape[0] == 5
        assert bars.shape[1] == 10
        assert sorted(bars.columns) == sorted(BarModel.to_schema().columns)
