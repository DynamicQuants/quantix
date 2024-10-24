"""Test the Bar model."""

from datetime import datetime

import pytest

from core.models.bar import BarData, BarModel
from core.models.broker import Broker
from core.models.timeframe import TFPreset
from core.utils.dataframe import (
    Categorical,
    DataFrameValidationError,
    DateTime,
    Float64,
    LazyFrame,
    String,
)


class TestBarData:
    @pytest.fixture
    def bar_data(self):
        return {
            "timestamp": [datetime(2021, 1, 1, 9, 30, 0), datetime(2021, 1, 1, 9, 31, 0)],
            "broker": [Broker.ALPACA.value, Broker.ALPACA.value],
            "symbol": ["AAPL", "GOOG"],
            "timeframe": [TFPreset.Tf_D.name_value, TFPreset.Tf_D.name_value],
            "open": [100.0, 100.0],
            "high": [100.0, 101.0],
            "low": [100.0, 101.0],
            "close": [100.0, 101.0],
            "volume": [1000.0, 3000.0],
            "vwap": [100.0, 101.0],
        }

    @pytest.fixture
    def sample_bar_lf(self, bar_data):
        return LazyFrame(bar_data)

    def test_bar_model_fields(self):
        assert set(BarModel.to_schema().columns) == {
            "timestamp",
            "broker",
            "symbol",
            "timeframe",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
        }

    def test_bar_dtypes(self, sample_bar_lf):
        bar_data = BarData(sample_bar_lf)

        print("bar_data.df().dtypes -----> ", bar_data.df().dtypes)
        print("bar_data.dtypes -----> ", bar_data.dtypes())

        assert bar_data.dtypes() == {
            "timestamp": "DateTime",
            "broker": "Categorical",
            "symbol": "String",
            "timeframe": "Categorical",
            "open": "Float64",
            "high": "Float64",
            "low": "Float64",
            "close": "Float64",
            "volume": "Float64",
            "vwap": "Float64",
        }
        assert bar_data.df().dtypes == [
            DateTime,
            Categorical,
            String,
            Categorical,
            Float64,
            Float64,
            Float64,
            Float64,
            Float64,
            Float64,
        ]

    def test_bar_data_validation_success(self, bar_data):
        bars = BarData(lf=LazyFrame(bar_data)).df()
        assert bars.shape == (2, 10)
        assert bars.columns == [
            "timestamp",
            "broker",
            "symbol",
            "timeframe",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
        ]

    def test_bar_data_validation_failure(self, bar_data):
        invalid_data = bar_data.copy()
        invalid_data["broker"][0] = "InvalidBroker"
        invalid_data["timeframe"][0] = "InvalidTimeframe"
        invalid_data["open"][0] = -100.0
        invalid_data["close"][0] = -100.0
        invalid_data["high"][0] = -100.0
        invalid_data["low"][0] = -100.0
        invalid_data["volume"][0] = -100.0
        invalid_data["vwap"][0] = -100.0

        lz = LazyFrame(invalid_data)
        with pytest.raises(DataFrameValidationError) as e:
            BarData(lz)

        assert e.value.failure_cases.shape == (8, 6)

    def test_bar_data_validation_failure_with_nulls(self, bar_data):
        invalid_data = bar_data.copy()
        invalid_data["open"][0] = None
        invalid_data["close"][0] = None
        invalid_data["high"][0] = None
        invalid_data["low"][0] = None
        invalid_data["volume"][0] = None

        lz = LazyFrame(invalid_data)
        with pytest.raises(DataFrameValidationError) as e:
            BarData(lz)

        assert e.value.failure_cases.shape == (5, 6)
