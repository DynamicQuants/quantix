# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Utility class for working with datetimes."""

from datetime import datetime, timezone
from typing import final

import pytz


@final
class DateTimeUtils:
    """Utility class for working with datetimes."""

    @staticmethod
    def ensure_utc(dt: datetime) -> datetime:
        """Converts the datetime to UTC, adding the timezone if necessary."""
        if dt.tzinfo is None:
            # If the datetime is naive, assume it is in UTC.
            return dt.replace(tzinfo=timezone.utc)
        else:
            # If it already has a timezone, convert it to UTC.
            return dt.astimezone(timezone.utc)

    @staticmethod
    def to_local_time(dt: datetime, tz_str: str) -> datetime:
        """Converts a datetime (preferably in UTC) to the specified local timezone."""
        if dt.tzinfo is None:
            # If the datetime is naive, assume it's in UTC
            dt = dt.replace(tzinfo=timezone.utc)
        # Convert from UTC (or any other tz) to the desired local timezone
        local_tz = pytz.timezone(tz_str)
        return dt.astimezone(local_tz)

    @staticmethod
    def utc_now() -> datetime:
        """Returns the current time in UTC."""
        return datetime.now(pytz.UTC)
