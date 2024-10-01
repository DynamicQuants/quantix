# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""
Defines the interface for the registry used to keep track of the actions performed in the
core system.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Literal

from pydantic.dataclasses import dataclass

from core.models.broker import Broker

RegistryErrorCodes = Literal["CANNOT_GET_REGISTRY_ITEM", "CANNOT_ADD_REGISTRY_ITEM"]


class RegistryError(Exception):
    """Base class for exceptions related to the registry."""

    def __init__(self, message: str, code: RegistryErrorCodes) -> None:
        self.message = message
        self.code = code
        super().__init__(self.message)


class RegistryAction(str, Enum):
    """
    Represents the types of actions that can be stored in the registry. Mostly related to the
    data fetching process.
    """

    FETCH_ASSETS = "FETCH_ASSETS"
    FETCH_BARS = "FETCH_BARS"
    FETCH_CALENDAR = "FETCH_CALENDAR"


@dataclass
class RegistryItem:
    """
    A registry item is an entry in the registry that stores the logs of the actions performed by
    the data manager.
    """

    timestamp: datetime
    action: RegistryAction
    broker: Broker
    log: str
    n_records: int
    was_full: bool = False


class Registry(ABC):
    """
    A registry is a data structure that stores the logs of the actions performed by the data
    manager. It is used to keep track previous actions and make decisions in terms of fetching data.
    """

    def __init__(self, broker: Broker):
        self.broker = broker

    @abstractmethod
    def add(self, item: RegistryItem) -> None:
        """Add a new item to the registry."""
        ...

    @abstractmethod
    def get(self, action: RegistryAction, since: datetime) -> List[RegistryItem]:
        """
        Get all the items in the registry that match the given action and broker since the
        given date.
        """
        ...
