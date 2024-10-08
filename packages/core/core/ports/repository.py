# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the interface for a repository used to save, load, and upsert data."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, Type, TypeVar

import patito as pt


@dataclass
class SaveResult:
    """Defines the result of saving data."""

    status: Literal["success", "error"]
    message: str | None
    rows_affected: int


@dataclass
class UpsertResult:
    """Defines the result of upsert data."""

    status: Literal["success", "error"]
    message: str | None
    rows_inserted: int
    rows_updated: int


SaveOptions = TypeVar("SaveOptions")
LoadOptions = TypeVar("LoadOptions")
UpdateOptions = TypeVar("UpdateOptions")
M = TypeVar("M", bound=pt.Model)
DF = TypeVar("DF", bound=pt.DataFrame)


class Repository(ABC, Generic[SaveOptions, LoadOptions, UpdateOptions]):
    """
    Repository interface that defines the methods to save, load, and upsert data. A repository
    is a data access layer that abstracts the underlying storage mechanism. It can be a database,
    a file, or any other data source that can store and retrieve data.
    """

    @abstractmethod
    def save(self, options: SaveOptions) -> SaveResult: ...

    @abstractmethod
    def load(self, model: Type[M], df: DF, options: LoadOptions) -> DF: ...

    @abstractmethod
    def upsert(self, options: UpdateOptions) -> UpsertResult: ...
