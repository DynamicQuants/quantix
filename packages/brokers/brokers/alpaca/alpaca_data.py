# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Market data class for Alpaca broker using timescale as repository."""

from typing import final

from core.adapters.timescale_repository import TimescaleRepository
from core.data import Data
from core.models.broker import Broker

from .alpaca_fetcher import AlpacaFetcher


@final
class AlpacaData(Data):
    """Market data class for Alpaca broker using timescale as repository."""

    def __init__(self):
        Data.__init__(
            self,
            broker=Broker.ALPACA,
            fetcher=AlpacaFetcher(),
            repository=TimescaleRepository(),
        )
