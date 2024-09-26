# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of a calendar that represents the trading hours of a trading venue."""

import datetime
from typing import final

import patito as pt
from pydantic import AwareDatetime, field_validator, model_validator

from core.utils.datetime_utils import DateTimeUtils


@final
class Calendar(pt.Model):
    """
    Calendar data for a specific trading venue.

    Depending the market, the trading day may start and end at different times. This model defines
    the date of the trading day, as well as the opening and closing times of the trading day.
    """

    date: datetime.date = pt.Field(unique=True)
    """The date of the trading day."""

    open: AwareDatetime
    """The opening time of the trading day."""

    close: AwareDatetime
    """The closing time of the trading day."""

    @field_validator("open")
    @classmethod
    def validate_open(cls, value: AwareDatetime) -> AwareDatetime:
        return DateTimeUtils.ensure_utc(value)

    @field_validator("close")
    @classmethod
    def validate_close(cls, value: AwareDatetime) -> AwareDatetime:
        return DateTimeUtils.ensure_utc(value)

    @model_validator(mode="after")
    def validate_open_lower_than_close(self):
        if self.open >= self.close:
            raise ValueError("The opening time must be earlier than the closing time.")
        return self


CalendarDataFrame = pt.DataFrame[Calendar]
"""
A DataFrame containing a list of calendar data. This DataFrame must be validated using the
Calendar model.

For example:
.. code-block:: python
    data = Calendar.examples(
        data={
            "date": ["2021-01-01", "2021-01-02"],
            "open": ["2021-01-01T09:30:00Z", "2021-01-02T09:30:00Z"],
            "close": ["2021-01-01T16:00:00Z", "2021-01-02T16:00:00Z"],
        }
    )
"""
