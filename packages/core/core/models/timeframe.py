# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the TimeFrame and TimeFrameUnit classes for specifying time intervals."""

from enum import Enum


class TimeFrameUnit(str, Enum):
    """The base unit that is used to measure the TimeFrame."""

    Minute = "Min"
    Hour = "Hour"
    Day = "Day"
    Week = "Week"
    Month = "Month"


class TimeFrame:
    """
    A timeframe is a time interval that is used to specify the frequency of data points. In
    trading and finance, timeframes are used to check the price movement of an asset over a specific
    period of time. The TimeFrame class is used to define the time interval for fetching data from
    a broker.
    """

    amount_value: int
    """The number of multiples of the _TimeFrameUnit interval."""

    unit_value: TimeFrameUnit
    """The base unit that is used to measure the TimeFrame."""

    value: str
    """The string representation of the TimeFrame."""

    def __init__(self, amount: int, unit: TimeFrameUnit) -> None:
        self.validate_timeframe(amount, unit)
        self.amount_value = amount
        self.unit_value = unit
        self.value = f"{self.amount_value}{self.unit_value}"

    @staticmethod
    def validate_timeframe(amount: int, unit: TimeFrameUnit):
        """Validates the amount value against the TimeFrameUnit value for consistency."""
        if amount <= 0:
            raise ValueError("Amount must be a positive integer value.")

        if unit == TimeFrameUnit.Minute and amount > 59:
            raise ValueError(
                "Second or Minute units can only be " + "used with amounts between 1-59."
            )

        if unit == TimeFrameUnit.Hour and amount > 23:
            raise ValueError("Hour units can only be used with amounts 1-23")

        if unit in (TimeFrameUnit.Day, TimeFrameUnit.Week) and amount != 1:
            raise ValueError("Day and Week units can only be used with amount 1")

        if unit == TimeFrameUnit.Month and amount not in (1, 2, 3, 6, 12):
            raise ValueError("Month units can only be used with amount 1, 2, 3, 6 and 12")

    @property
    def name_value(self) -> str:
        """The name of the TimeFrame. For example, '1m' for 1 minute."""

        match self.unit_value:
            case TimeFrameUnit.Minute:
                return f"{self.amount_value}m"
            case TimeFrameUnit.Hour:
                return f"{self.amount_value}h"
            case TimeFrameUnit.Day:
                return f"{self.amount_value}d"
            case TimeFrameUnit.Week:
                return f"{self.amount_value}w"
            case TimeFrameUnit.Month:
                return f"{self.amount_value}M"


class TFPreset:
    """A list of most commonly used TimeFrames in candlestick charting."""

    Tf_M = TimeFrame(1, TimeFrameUnit.Month)
    """TimeFrame: A time interval of 1 month."""

    Tf_W = TimeFrame(1, TimeFrameUnit.Week)
    """TimeFrame: A time interval of 1 week."""

    Tf_D = TimeFrame(1, TimeFrameUnit.Day)
    """TimeFrame: A time interval of 1 day."""

    Tf_4h = TimeFrame(4, TimeFrameUnit.Hour)
    """TimeFrame: A time interval of 4 hours."""

    Tf_2h = TimeFrame(2, TimeFrameUnit.Hour)
    """TimeFrame: A time interval of 2 hours."""

    Tf_1h = TimeFrame(1, TimeFrameUnit.Hour)
    """TimeFrame: A time interval of 1 hour."""

    Tf_30m = TimeFrame(30, TimeFrameUnit.Minute)
    """TimeFrame: A time interval of 30 minutes."""

    Tf_15m = TimeFrame(15, TimeFrameUnit.Minute)
    """TimeFrame: A time interval of 15 minutes."""

    Tf_5m = TimeFrame(5, TimeFrameUnit.Minute)
    """TimeFrame: A time interval of 5 minutes."""

    Tf_1m = TimeFrame(1, TimeFrameUnit.Minute)
    """TimeFrame: A time interval of 1 minute."""
