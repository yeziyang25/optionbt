"""
tests/test_config_loader.py
===========================
Unit tests for config/config_loader.py — validates YAML parsing, config
validation, and CSV conversion without touching the database.
"""

import os
import sys
import datetime as dt

import pandas as pd
import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config_loader import to_dataframe_config, validate_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MINIMAL_CONFIG = {
    "strategy": {
        "name": "TestStrategy",
        "base_currency": "USD",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "initial_capital": 1000000,
        "rebalance_schedule": "monthly_third_friday",
        "holiday_calendar": "TSX",
    },
    "legs": [
        {
            "id": "cash",
            "type": "cash",
            "currency": "USD",
        },
        {
            "id": "equity1",
            "type": "equity",
            "ticker": "SPY US",
            "currency": "USD",
            "allocation": 1.0,
            "data_source": {"type": "file", "equity_file": "data/SPY US equity_pricing.csv"},
        },
        {
            "id": "option1",
            "type": "call_option",
            "underlying_ticker": "SPY US",
            "currency": "USD",
            "allocation": -1.0,
            "coverage": {"against": "equity1", "ratio": 1.0},
            "selection": {"method": "custom", "custom_file": "runs/custom_options_list/SPY US_0.0_option_list.csv"},
            "pricing": {"sell_price": "bid", "buy_price": "intrinsic"},
            "data_source": {"type": "file", "options_file": "data/SPY US_backtest_format_options.csv"},
        },
    ],
}


@pytest.fixture
def config_yaml(tmp_path):
    """Write a minimal valid YAML config and return the path."""
    path = tmp_path / "test_strategy.yaml"
    with open(path, "w") as f:
        yaml.dump(MINIMAL_CONFIG, f)
    return str(path)


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig:
    def test_valid_config_does_not_raise(self, config_yaml):
        validate_config(config_yaml)  # Should not raise

    def test_missing_strategy_key_raises(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("legs: []\n")
        with pytest.raises(ValueError, match="strategy"):
            validate_config(str(bad))

    def test_missing_cash_leg_raises(self, tmp_path):
        cfg = {k: v for k, v in MINIMAL_CONFIG.items()}
        cfg["legs"] = [l for l in MINIMAL_CONFIG["legs"] if l["id"] != "cash"]
        path = tmp_path / "no_cash.yaml"
        with open(path, "w") as f:
            yaml.dump(cfg, f)
        with pytest.raises(ValueError, match="cash"):
            validate_config(str(path))

    def test_option_missing_coverage_raises(self, tmp_path):
        import copy
        cfg = copy.deepcopy(MINIMAL_CONFIG)
        cfg["legs"][2].pop("coverage")
        path = tmp_path / "no_cov.yaml"
        with open(path, "w") as f:
            yaml.dump(cfg, f)
        with pytest.raises(ValueError, match="coverage"):
            validate_config(str(path))

    def test_option_referencing_unknown_leg_raises(self, tmp_path):
        import copy
        cfg = copy.deepcopy(MINIMAL_CONFIG)
        cfg["legs"][2]["coverage"]["against"] = "nonexistent_equity"
        path = tmp_path / "bad_against.yaml"
        with open(path, "w") as f:
            yaml.dump(cfg, f)
        with pytest.raises(ValueError, match="nonexistent_equity"):
            validate_config(str(path))


# ---------------------------------------------------------------------------
# to_dataframe_config
# ---------------------------------------------------------------------------

class TestToDataframeConfig:
    def test_returns_dataframe(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        assert isinstance(df, pd.DataFrame)

    def test_one_row_per_leg(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        assert len(df) == len(MINIMAL_CONFIG["legs"])

    def test_backtest_name_from_yaml(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        assert (df["backtest"] == "TestStrategy").all()

    def test_backtest_name_override(self, config_yaml):
        df = to_dataframe_config(config_yaml, backtest_name="OverrideName")
        assert (df["backtest"] == "OverrideName").all()

    def test_cash_leg_allocation_equals_initial_capital(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        cash_row = df[df["sec_id"] == "cash"].iloc[0]
        assert float(cash_row["allocation"]) == pytest.approx(1_000_000.0)

    def test_equity_allocation(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        eq_row = df[df["sec_id"] == "equity1"].iloc[0]
        assert float(eq_row["allocation"]) == pytest.approx(1.0)

    def test_option_allocation_sign(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        opt_row = df[df["sec_id"] == "option1"].iloc[0]
        # allocation: direction(-1) * ratio(1.0) = -1.0
        assert float(opt_row["allocation"]) < 0

    def test_option_w_against_field(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        opt_row = df[df["sec_id"] == "option1"].iloc[0]
        assert opt_row["option_w_against"] == "equity1"

    def test_sec_type_mapping(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        assert df[df["sec_id"] == "cash"].iloc[0]["sec_type"] == "cash"
        assert df[df["sec_id"] == "equity1"].iloc[0]["sec_type"] == "equity"
        assert df[df["sec_id"] == "option1"].iloc[0]["sec_type"] == "call option"

    def test_sell_price_filled(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        opt_row = df[df["sec_id"] == "option1"].iloc[0]
        assert opt_row["option_sell_to_open_price"] == "bid"

    def test_buy_price_filled(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        opt_row = df[df["sec_id"] == "option1"].iloc[0]
        assert opt_row["option_buy_to_close_price"] == "intrinsic"

    def test_custom_options_file_filled(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        opt_row = df[df["sec_id"] == "option1"].iloc[0]
        assert "SPY US_0.0_option_list.csv" in str(opt_row["custom_options_file"])

    def test_start_date_propagated(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        assert (df["start_date"] == "2024-01-01").all()

    def test_required_columns_present(self, config_yaml):
        df = to_dataframe_config(config_yaml)
        required = [
            "backtest", "sec_id", "sec_name", "sec_type", "currency",
            "allocation", "option_w_against", "start_date",
            "option_selection", "custom_options_file",
            "option_sell_to_open_price", "option_buy_to_close_price",
            "end_date",
        ]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Multi-leg strategy (basket with 2 equities + 2 options)
# ---------------------------------------------------------------------------

class TestMultiLegStrategy:
    @pytest.fixture
    def multi_leg_yaml(self, tmp_path):
        cfg = {
            "strategy": {
                "name": "MultiLeg",
                "base_currency": "USD",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "initial_capital": 5_000_000,
                "rebalance_schedule": "monthly_third_friday",
                "holiday_calendar": "TSX",
            },
            "legs": [
                {"id": "cash", "type": "cash", "currency": "USD"},
                {
                    "id": "equity1", "type": "equity", "ticker": "BCE CN",
                    "currency": "CAD", "allocation": 0.5,
                    "data_source": {"type": "database"},
                },
                {
                    "id": "equity2", "type": "equity", "ticker": "T CN",
                    "currency": "CAD", "allocation": 0.5,
                    "data_source": {"type": "database"},
                },
                {
                    "id": "option1", "type": "call_option",
                    "underlying_ticker": "BCE CN", "currency": "CAD",
                    "allocation": -0.5,
                    "coverage": {"against": "equity1", "ratio": 0.5},
                    "selection": {"method": "custom", "custom_file": "runs/custom_options_list/BCE CN_0_option_list.csv"},
                    "pricing": {"sell_price": "bid", "buy_price": "intrinsic"},
                    "data_source": {"type": "database"},
                },
                {
                    "id": "option2", "type": "call_option",
                    "underlying_ticker": "T CN", "currency": "CAD",
                    "allocation": -0.5,
                    "coverage": {"against": "equity2", "ratio": 0.5},
                    "selection": {"method": "custom", "custom_file": "runs/custom_options_list/T CN_0_option_list.csv"},
                    "pricing": {"sell_price": "bid", "buy_price": "intrinsic"},
                    "data_source": {"type": "database"},
                },
            ],
        }
        path = tmp_path / "multi.yaml"
        with open(path, "w") as f:
            yaml.dump(cfg, f)
        return str(path)

    def test_validate_passes(self, multi_leg_yaml):
        validate_config(multi_leg_yaml)

    def test_five_rows(self, multi_leg_yaml):
        df = to_dataframe_config(multi_leg_yaml)
        assert len(df) == 5

    def test_option1_against_equity1(self, multi_leg_yaml):
        df = to_dataframe_config(multi_leg_yaml)
        assert df[df["sec_id"] == "option1"].iloc[0]["option_w_against"] == "equity1"

    def test_option2_against_equity2(self, multi_leg_yaml):
        df = to_dataframe_config(multi_leg_yaml)
        assert df[df["sec_id"] == "option2"].iloc[0]["option_w_against"] == "equity2"

    def test_coverage_ratio_in_allocation(self, multi_leg_yaml):
        df = to_dataframe_config(multi_leg_yaml)
        # allocation = direction(-1) * ratio(0.5) = -0.5
        opt1 = df[df["sec_id"] == "option1"].iloc[0]
        assert float(opt1["allocation"]) == pytest.approx(-0.5)
