# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the interface for a repository used to save, load, and upsert data."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal, Optional, Type, TypedDict, TypeVar, Union, cast

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


class LoadFilters(TypedDict):
    """Defines the filters to apply when loading data."""

    # TODO: Add support for AND/OR conditions.
    field: str
    operator: Literal["=", ">", "<", ">=", "<=", "LIKE"]
    value: Union[str, int, float, bool, date, datetime]


@dataclass
class LoadOptions:
    filters: list[LoadFilters] | None = None
    limit: int | None = None


class RepositoryError(Exception):
    """Exception raised for errors in the repository."""

    def __init__(self, message: str, code: Literal["KEY_NOT_FOUND"]) -> None:
        self.message = message
        self.code = code
        super().__init__(self.message)


Payload = TypeVar("Payload")


class Repository(ABC):
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

    def __init__(self, payloads: dict[str, Any]) -> None:
        self.payloads = payloads

    def get_payload(self, key: str, t: Type[Payload]) -> Payload:
        """Get the payload data associated with a key."""
        try:
            return cast(Payload, self.payloads[key])
        except KeyError:
            raise RepositoryError(
                message=f"Key {key} not found in repository payloads mapping.",
                code="KEY_NOT_FOUND",
            )

    @abstractmethod
    def save(self, key: str, df: pl.DataFrame) -> SaveResult: ...

    @abstractmethod
    def load(self, key: str, options: Optional[LoadOptions] = None) -> pl.DataFrame: ...

    @abstractmethod
    def upsert(self, key: str, df: pl.DataFrame) -> UpsertResult: ...
