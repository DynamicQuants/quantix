# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of an asset that can be traded by the broker."""

from enum import Enum
from typing import final

from core.utils.dataframe import (
    Categorical,
    DataContainer,
    DataContainerConfig,
    DataFrameBaseModel,
    Field,
    LazyFrame,
    Series,
)

from .broker import Broker


@final
class AssetClass(str, Enum):
    """
    This enum represents the different types of assets that can be traded by the broker. If any
    other asset class is needed, it can be added here.

    An equity is a security representing ownership of a company. A cryptocurrency is a digital
    currency that uses cryptography for security. A forex is a decentralized global market for
    trading currencies.
    """

    EQUITY = "Equity"
    CRYPTO = "Crypto"
    FOREX = "Forex"
    OPTION = "Option"
    FUTURE = "Future"


class AssetStatus(str, Enum):
    """
    Represents the status of an asset. This status can be 'Active' or 'Inactive' if the asset is
    available for trading or not in specific broker. Some brokers may have different status values.
    And sometimes the asset may be temporarily unavailable for trading by the exchange itself.
    """

    ACTIVE = "Active"
    INACTIVE = "Inactive"


@final
class AssetModel(DataFrameBaseModel):
    """
    Defines the data of an asset that can be traded by the broker.

    This model is used to store information about the asset such as the name, symbol, exchange,
    broker, asset class, and status. This information is used to fetch market data for the asset
    and to place trades.
    """

    broker: Series[Categorical] = Field(
        description="The broker that provides access to the asset",
        nullable=False,
        coerce=True,
        isin=[broker.value for broker in Broker],
    )

    name: Series[str] = Field(
        description="""The name of the asset. For example, for equity, this could be 'Apple Inc'
        for Apple Inc. or 'Bitcoin' for Bitcoin.""",
        unique=True,
    )

    symbol: Series[str] = Field(
        description="""A symbol that represents the asset. For example, for equity, this could be
        'AAPL' for Apple Inc or 'BTC' for Bitcoin.""",
        unique=True,
    )

    exchange: Series[str] = Field(
        description="""The exchange in which the asset is traded. For example, for equity, this
        could be 'NASDAQ' or 'NYSE'. Or for crypto, this could be 'Binance' or 'Coinbase'.""",
    )

    asset_class: Series[Categorical] = Field(
        description="""The type of asset. Can be 'equity', 'crypto', 'forex', etc.""",
        nullable=False,
        coerce=True,
        isin=[asset.value for asset in AssetClass],
    )

    tradable: Series[bool] = Field(
        description="""Indicates whether the asset is currently active and available for trading.""",
        nullable=False,
    )

    status: Series[Categorical] = Field(
        description="""The asset status in the broker.""",
        nullable=False,
        coerce=True,
        isin=[status.value for status in AssetStatus],
    )


@final
class AssetData(DataContainer):
    """
    Asset data container which contains a dataframe of assets and validates it using the Asset
    model. If the dataframe is not valid, a `DataFrameValidationError` exception is raised.
    """

    def __init__(self, lf: LazyFrame) -> None:
        super().__init__(
            DataContainerConfig(
                name="assets",
                model=AssetModel,
                lf=lf,
                kind="non-relational",
                primary_key="broker,symbol",
                unique_fields=[],
            )
        )
