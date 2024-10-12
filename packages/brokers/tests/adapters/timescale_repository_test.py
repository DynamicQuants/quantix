from datetime import datetime

import pytest

from brokers.adapters.timescale_repository import TimescaleRepository, TimescaleRepositoryPayload
from core.models.bar import Bar, BarDataFrame
from core.ports.repository import LoadOptions

BARS_TABLE = TimescaleRepositoryPayload(
    model=Bar,
    db="test",
    schema="public",
    table_name=f"test_bars_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    unique_fields=["symbol, timestamp"],
    hypertable_key="timestamp",
)


class TestTimescaleRepository:
    @pytest.fixture
    def repository(self):
        return TimescaleRepository(payloads={"bars": BARS_TABLE})

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
        result = repository.save("bars", bars)
        assert result.status == "success"

    def test_upsert(self, repository: TimescaleRepository, bars: BarDataFrame):
        result = repository.upsert("bars", bars.limit(1))
        assert result.status == "success"
        assert result.rows_inserted == 0
        assert result.rows_updated == 1

    def test_load(self, repository: TimescaleRepository):
        df = repository.load("bars")
        assert df.shape[0] == 2

    def test_load_with_filters(self, repository: TimescaleRepository):
        options = LoadOptions(filters=[{"field": "symbol", "operator": "=", "value": "AAPL"}])
        df = repository.load("bars", options)
        assert df.shape[0] == 1

    def test_load_with_limit(self, repository: TimescaleRepository):
        options = LoadOptions(limit=1)
        df = repository.load("bars", options)
        assert df.shape[0] == 1
