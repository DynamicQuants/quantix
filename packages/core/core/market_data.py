# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""MarketData class that is responsible for fetching, storing, and retrieving market data."""

from typing import final

from core.ports.fetcher import Fetcher
from core.ports.registry import Registry
from core.ports.repository import Repository


@final
class MarketData:
    def __init__(self, fetcher: Fetcher, repository: Repository, registry: Registry):
        self._fetcher = fetcher
        self._repository = repository
        self._registry = registry
