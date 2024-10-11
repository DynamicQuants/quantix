from datetime import datetime

import pytest

from brokers.adapters.timescale_repository import (
    SQLFilter,
    SQLWhere,
    TimescaleLoadOptions,
    TimescaleRepository,
    TimescaleSaveOptions,
    TimescaleTable,
)
from core.models.bar import Bar, BarDataFrame

BARS_TABLE = TimescaleTable(
    model=Bar,
    db="test",
    schema="public",
    name=f"test_bars_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    unique_fields=["symbol, timestamp"],
    hypertable_key="timestamp",
)


class TestTimescaleRepository:
    @pytest.fixture
    def repository(self):
        return TimescaleRepository()

    @pytest.fixture
    def bars(self):
        return (
            BarDataFrame(
                {
                    "symbol": ["AAPL", "GOOG"],
                    "timestamp": [
                        datetime(2021, 1, 1, 9, 30, 0),
                        datetime(2021, 1, 1, 9, 31, 0),
                    ],
                    "open": [100.0, 101.0],
                    "high": [100.0, 101.0],
                    "low": [100.0, 101.0],
                    "close": [100.0, 101.0],
                    "volume": [1000.0, 3000.0],
                    "vwap": [100.0, 101.0],
                }
            )
            .set_model(Bar)
            .validate()
        )

    def test_save(self, repository: TimescaleRepository, bars: BarDataFrame):
        options = TimescaleSaveOptions(table=BARS_TABLE, df=bars)
        result = repository.save(options)
        assert result.status == "success"

    def test_upsert(self, repository: TimescaleRepository, bars: BarDataFrame):
        options = TimescaleSaveOptions(table=BARS_TABLE, df=bars.limit(1))
        result = repository.upsert(options)
        assert result.status == "success"
        assert result.rows_inserted == 0
        assert result.rows_updated == 1

    def test_load(self, repository: TimescaleRepository):
        options = TimescaleLoadOptions(table=BARS_TABLE)
        df = repository.load(options)
        assert df.shape[0] == 2

    def test_load_with_filters(self, repository: TimescaleRepository):
        options = TimescaleLoadOptions(
            table=BARS_TABLE,
            filters=SQLFilter(where=[SQLWhere("symbol", "=", "AAPL")]),
        )

        df = repository.load(options)
        assert df.shape[0] == 1

    def test_load_with_limit(self, repository: TimescaleRepository):
        options = TimescaleLoadOptions(table=BARS_TABLE, filters=SQLFilter(limit=1))
        df = repository.load(options)
        assert df.shape[0] == 1
