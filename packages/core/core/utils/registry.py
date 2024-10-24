# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.


"""
Data registry module.

This module contains the implementation of the data registry, which is a collection of registry
items that store the logs of the actions performed by the core components like market data, trading
engine, risk engine, etc.
"""


import uuid
from datetime import datetime, timezone

from core.models.broker import Broker
from core.ports.repository import LoadOptions, Repository

from .dataframe import (
    Category,
    DataContainer,
    DataContainerConfig,
    DataFrame,
    DataFrameModel,
    Field,
    Float32,
    LazyFrame,
    Series,
    Timestamp,
)
from .datatypes import float32


class RegistryItem(DataFrameModel):
    """
    A registry item is an entry in the registry that stores the logs of the actions performed by
    the market data.
    """

    uuid: Series[str] = Field(unique=True)
    timestamp: Series[Timestamp] = Field(unique=True, coerce=True)
    broker: Series[Category] = Field(
        nullable=False,
        coerce=True,
        isin=[broker.value for broker in Broker],
    )
    action: Series[str] = Field(nullable=False)
    log: Series[str] = Field(nullable=False)
    rows_inserted: Series[int] = Field(nullable=True)
    rows_updated: Series[int] = Field(nullable=True)
    execution_time: Series[Float32] = Field(nullable=True)


class RegistryDataContainer(DataContainer):
    """Registry data container."""

    def __init__(self, lf: LazyFrame) -> None:
        super().__init__(
            DataContainerConfig(
                name="registry",
                model=RegistryItem,
                lf=lf,
                kind="non-relational",
                primary_key="uuid",
                unique_fields=["timestamp"],
            )
        )


class RegistryError(Exception):
    """Registry error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class Registry:
    """
    A registry is a collection of registry items which contains the logs of the actions performed
    by the core components.

    Attributes:
        repository: The repository to use to save the registry items.
        broker: The broker to use to identify the registry items.

    Examples:
        >>> registry = Registry(repository, broker.BINANCE)
    """

    def __init__(self, repository: Repository, broker: Broker) -> None:
        self._repository = repository
        self._broker = broker

    def add(
        self,
        action: str,
        log: str,
        rows_inserted: int,
        rows_updated: int,
        execution_time: float32,
    ) -> None:
        """
        Add a registry item to the registry.

        Args:
            action: The action performed.
            log: The log of the action performed.
            rows_inserted: The number of rows inserted.
            rows_updated: The number of rows updated.

        Examples:
            >>> registry = Registry(repository, broker)
            >>> registry.add("GET_BARS", "Bars fetched and upserted", 10, 0)
        """

        item = RegistryDataContainer(
            LazyFrame(
                {
                    "uuid": uuid.uuid4().hex,
                    "timestamp": datetime.now(tz=timezone.utc),
                    "action": action,
                    "broker": self._broker.value,
                    "log": log,
                    "rows_inserted": rows_inserted,
                    "rows_updated": rows_updated,
                    "execution_time": execution_time,
                }
            )
        )

        result = self._repository.save(item)

        if result.status == "error":
            message = f"Error adding registry item: {result.message}"
            raise RegistryError(message)

    def get(self, load_options: LoadOptions) -> DataFrame:
        """
        Get the registry for the given load options.

        Args:
            load_options: The load options.

        Returns:
            The registry for the given load options.

        Examples:
            >>> registry = Registry(repository, broker)
            >>> filters = [
            >>>     LoadFilters(field="action", operator="=", value="GET_BARS"),
            >>>     LoadFilters(field="broker", operator="=", value=Broker.BINANCE.value),
            >>> ]
            >>> registry.get(LoadOptions(name="registry", filters=filters))
        """
        return self._repository.load(load_options)
