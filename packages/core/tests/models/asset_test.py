import pytest
from polars import DataFrame

from core.models.asset import Asset, AssetClass, AssetData, AssetStatus
from core.models.broker import Broker
from core.utils.dataframe import Boolean, DataFrameValidationError, LazyFrame, String


class TestAsset:
    @pytest.fixture
    def asset_data(self):
        return {
            "broker": [Broker.ALPACA.value, Broker.BINANCE.value],
            "name": ["Apple Inc", "Bitcoin"],
            "symbol": ["AAPL", "BTC"],
            "exchange": ["NASDAQ", "Binance"],
            "asset_class": [AssetClass.EQUITY.value, AssetClass.CRYPTO.value],
            "tradable": [True, True],
            "status": [AssetStatus.ACTIVE.value, AssetStatus.ACTIVE.value],
        }

    @pytest.fixture
    def sample_asset_lf(self, asset_data):
        return DataFrame(asset_data).lazy()

    def test_asset_class_enum(self):
        assert AssetClass.EQUITY.value == "Equity"
        assert AssetClass.CRYPTO.value == "Crypto"
        assert AssetClass.FOREX.value == "Forex"
        assert AssetClass.OPTION.value == "Option"
        assert AssetClass.FUTURE.value == "Future"

    def test_asset_status_enum(self):
        assert AssetStatus.ACTIVE.value == "Active"
        assert AssetStatus.INACTIVE.value == "Inactive"

    def test_asset_model_fields(self):
        assert set(Asset.to_schema().columns) == {
            "broker",
            "name",
            "symbol",
            "exchange",
            "asset_class",
            "tradable",
            "status",
        }

    def test_asset_dtypes(self, sample_asset_lf):
        asset_data = AssetData(sample_asset_lf)
        assert asset_data.dtypes() == {
            "broker": "String",
            "name": "String",
            "symbol": "String",
            "exchange": "String",
            "asset_class": "String",
            "tradable": "Bool",
            "status": "String",
        }
        assert asset_data.df().dtypes == [String, String, String, String, String, Boolean, String]

    def test_asset_data_schema(self, sample_asset_lf):
        asset_data = AssetData(sample_asset_lf)
        schema = asset_data.schema
        assert set(schema.columns) == {
            "broker",
            "name",
            "symbol",
            "exchange",
            "asset_class",
            "tradable",
            "status",
        }

    def test_asset_data_validation_success(self, sample_asset_lf):
        asset_data = AssetData(sample_asset_lf)
        df = asset_data.df()
        assert df.shape == (2, 7)
        assert set(df.columns) == {
            "broker",
            "name",
            "symbol",
            "exchange",
            "asset_class",
            "tradable",
            "status",
        }

    def test_asset_data_validation_failure(self, asset_data):
        invalid_data = asset_data.copy()
        invalid_data["broker"][0] = "InvalidBroker"
        invalid_data["asset_class"][0] = "InvalidAssetClass"
        lz = LazyFrame(invalid_data)

        with pytest.raises(DataFrameValidationError) as e:
            AssetData(lz)

        assert e.value.failure_cases.shape == (2, 6)
