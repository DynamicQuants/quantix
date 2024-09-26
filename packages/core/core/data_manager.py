# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

from typing import final

from core.broker.fetcher import Fetcher
from core.broker.registry import Registry
from core.broker.storage import Storage


@final
class DataManager:
    def __init__(self, fetcher: Fetcher, storage: Storage, registry: Registry):
        self._fetcher = fetcher
        self._storage = storage
        self._registry = registry
