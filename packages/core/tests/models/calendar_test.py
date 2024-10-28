from datetime import time

import pytest

from core.models.broker import Broker
from core.models.calendar import CalendarData, CalendarModel
from core.utils.dataframe import Categorical, DataFrameValidationError, Date, LazyFrame, Time


class TestCalendar:
    @pytest.fixture
    def calendar_data(self):
        return {
            "broker": [Broker.ALPACA.value, Broker.BINANCE.value],
            "date": ["2023-01-01", "2023-01-02"],
            "open": [time(9, 0, 0), time(9, 0, 0)],
            "close": [time(16, 0, 0), time(16, 0, 0)],
        }

    @pytest.fixture
    def sample_calendar_lf(self, calendar_data):
        return LazyFrame(calendar_data)

    def test_calendar_model_fields(self):
        assert set(CalendarModel.to_schema().columns) == {
            "broker",
            "date",
            "open",
            "close",
        }

    def test_calendar_dtypes(self, sample_calendar_lf):
        calendar_data = CalendarData(sample_calendar_lf)
        assert calendar_data.dtypes() == {
            "broker": "Categorical",
            "date": "Date",
            "open": "Time",
            "close": "Time",
        }
        assert calendar_data.df().dtypes == [
            Categorical,
            Date,
            Time,
            Time,
        ]

    def test_calendar_data_schema(self, sample_calendar_lf):
        calendar_data = CalendarData(sample_calendar_lf)
        schema = calendar_data.schema
        assert set(schema.columns) == {
            "broker",
            "date",
            "open",
            "close",
        }

    def test_calendar_data_validation_success(self, sample_calendar_lf):
        calendar_data = CalendarData(sample_calendar_lf)
        df = calendar_data.df()
        assert df.shape == (2, 4)
        assert set(df.columns) == {
            "broker",
            "date",
            "open",
            "close",
        }

    def test_calendar_data_validation_failure(self, calendar_data):
        invalid_data = calendar_data.copy()
        invalid_data["broker"][0] = "InvalidBroker"
        invalid_data["date"][0] = None
        lz = LazyFrame(invalid_data)

        with pytest.raises(DataFrameValidationError) as e:
            CalendarData(lz)

        if e.value.error_code == "VALIDATION_ERROR":
            assert e.value.failure_cases.shape == (2, 6)
        else:
            assert e.value.failure_cases.is_empty() == True

    def test_open_lower_than_close(self, calendar_data):
        """
        Test that the open time is lower than the close time.
        """
        invalid_data = calendar_data.copy()
        invalid_data["close"][0] = time(8, 0, 0)
        lz = LazyFrame(invalid_data)

        with pytest.raises(DataFrameValidationError) as e:
            CalendarData(lz)

        if e.value.error_code == "VALIDATION_ERROR":
            assert e.value.failure_cases.shape == (1, 6)
        else:
            assert e.value.failure_cases.is_empty() == True
