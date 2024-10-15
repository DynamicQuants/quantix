# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Market data class for Alpaca broker using timescale as repository."""

from typing import final

from core.market_data import MarketData
from core.models.broker import Broker
from core.models.calendar import Calendar
from core.ports.repository import RegistryItem

from .adapters.alpaca_fetcher import AlpacaFetcher
from .adapters.timescale_repository import TimescaleRepository, TimescaleRepositoryPayload

_PAYLOAD: dict[str, TimescaleRepositoryPayload] = {
    "registry": TimescaleRepositoryPayload(
        model=RegistryItem,
        db="test",
        schema="public",
        table_name="registry",
        hypertable_key="timestamp",
    ),
    "calendar": TimescaleRepositoryPayload(
        model=Calendar,
        db="test",
        schema="public",
        table_name="calendar",
        primary_key="date",
    ),
}


@final
class AlpacaMarketData(MarketData):
    """Market data class for Alpaca broker using timescale as repository."""

    def __init__(self):
        MarketData.__init__(
            self,
            broker=Broker.ALPACA,
            fetcher=AlpacaFetcher(),
            repository=TimescaleRepository(payloads=_PAYLOAD),
        )
