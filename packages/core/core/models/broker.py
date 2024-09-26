# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the supported brokers that provides market data and trading services."""

from enum import Enum


class Broker(str, Enum):
    """List of supported brokers."""

    ALPACA = "Alpaca"
    BINANCE = "Binance"

    def __str__(self) -> str:
        return str(self.value)
