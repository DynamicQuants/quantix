# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the interface for a repository used to save, load, and upsert data."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

import polars as pl


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
UpsertOptions = TypeVar("UpsertOptions")


class Repository(ABC, Generic[SaveOptions, LoadOptions, UpsertOptions]):
    """
    Repository interface that defines the methods to save, load, and upsert data. A repository
    is a data access layer that abstracts the underlying storage mechanism. It can be a database,
    a file, or any other data source that can store and retrieve data.

    It is important to note that the repository interface is generic and can be implemented by
    different storage mechanisms. For example, a repository can be implemented to save data to a
    file, a database, or a cloud storage service. The implementation details are hidden from the
    client code, which only interacts with the repository interface.

    The repository should not validate the data or perform any business logic. It should only
    handle the storage and retrieval of data. The validation and business logic should be handled
    by the client code before calling the repository methods.
    """

    @abstractmethod
    def save(self, options: SaveOptions) -> SaveResult: ...

    @abstractmethod
    def load(self, options: LoadOptions) -> pl.DataFrame: ...

    @abstractmethod
    def upsert(self, options: UpsertOptions) -> UpsertResult: ...
