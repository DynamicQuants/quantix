from datetime import datetime

import pytest

from core.adapters.timescale_repository import TimescaleRepository
from core.models.bar import BarData
from core.models.broker import Broker
from core.models.timeframe import TFPreset
from core.ports.repository import LoadFilters, LoadOptions
from core.utils.dataframe import LazyFrame

# This is a hack to avoid name collision in tests.
TEST_NAME = f"bars_{datetime.now().strftime('%Y%m%d%H%M%S')}"


class TestTimescaleRepository:
    @pytest.fixture
    def repository(self):
        return TimescaleRepository()

    @pytest.fixture
    def bars(self):
        bd = BarData(
            lf=LazyFrame(
                {
                    "timestamp": [datetime(2021, 1, 1, 9, 30, 0), datetime(2021, 1, 1, 9, 31, 0)],
                    "broker": [Broker.ALPACA.value, Broker.ALPACA.value],
                    "timeframe": [TFPreset.Tf_D.name_value, TFPreset.Tf_D.name_value],
                    "symbol": ["AAPL", "GOOG"],
                    "open": [100.0, 101.0],
                    "high": [100.0, 101.0],
                    "low": [100.0, 101.0],
                    "close": [100.0, 101.0],
                    "volume": [1000.0, 3000.0],
                    "vwap": [100.0, 101.0],
                }
            )
        )

        # Renaming the default name to avoid name collision in tests.
        bd.name = TEST_NAME
        return bd

    def test_save(self, repository: TimescaleRepository, bars: BarData):
        result = repository.save(bars)
        assert result.status == "success"

    def test_upsert(self, repository: TimescaleRepository, bars: BarData):
        result = repository.upsert(bars)
        assert result.status == "success"
        assert result.rows_inserted == 0
        assert result.rows_updated == 2

    def test_load_with_filters(self, repository: TimescaleRepository):
        options = LoadOptions(
            name=TEST_NAME,
            filters=[
                LoadFilters(field="symbol", operator="=", value="AAPL"),
            ],
        )
        df = repository.load(options)
        assert df.shape[0] == 1

    def test_load_with_limit(self, repository: TimescaleRepository):
        options = LoadOptions(
            name=TEST_NAME,
            limit=1,
        )
        df = repository.load(options)
        assert df.shape[0] == 1
