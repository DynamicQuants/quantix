# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""Defines the data of an asset that can be traded by the broker."""

from enum import Enum
from typing import final

import patito as pt

from core.models.broker import Broker


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


class AssetStatus(str, Enum):
    """
    Represents the status of an asset. This status can be 'Active' or 'Inactive' if the asset is
    available for trading or not in specific broker. Some brokers may have different status values.
    And sometimes the asset may be temporarily unavailable for trading by the exchange itself.
    """

    ACTIVE = "Active"
    INACTIVE = "Inactive"


class Asset(pt.Model):
    """
    Defines the data of an asset that can be traded by the broker.

    This model is used to store information about the asset such as the name, symbol, exchange,
    broker, asset class, and status. This information is used to fetch market data for the asset
    and to place trades.
    """

    name: str = pt.Field(unique=True)
    """
    The name of the asset. For example, for equity, this could be 'Apple Inc' for Apple Inc.
    or 'Bitcoin' for Bitcoin.
    """

    symbol: str = pt.Field(unique=True)
    """
    A symbol that represents the asset. For example, for equity, this could be 'AAPL' for
    Apple Inc or 'BTC' for Bitcoin.
    """

    exchange: str
    """
    The exchange in which the asset is traded. For example, for equity, this could be 'NASDAQ'
    or 'NYSE'. Or for cryptocurrency, this could be 'Binance' or 'Coinbase'.
    """

    broker: Broker
    """
    The broker that provides access to the asset.
    """

    asset_class: AssetClass
    """
    The type of asset. Can be 'equity', 'crypto', or 'forex'.
    """

    tradable: bool
    """
    Indicates whether the asset is currently active and available for trading.
    """

    status: AssetStatus
    """
    The asset status in the broker.
    """


AssetDataFrame = pt.DataFrame[Asset]
"""
A DataFrame type that contains a list of assets. It validates using the Asset model.

For example:
.. code-block:: python
    data = Asset.examples(
        data={
            "name": ["Apple Inc", "Bitcoin"],
            "symbol": ["AAPL", "BTC"],
            "exchange": ["NASDAQ", "Binance"],
            "broker": ["Alpaca", "Binance"],
            "asset_class": ["Equity", "Crypto"],
            "tradable": [True, True],
            "status": ["Active", "Active"],
        }
    )
"""
