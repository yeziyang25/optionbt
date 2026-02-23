"""
tests/test_data_loader.py
=========================
Unit tests for data_loader.py — FileDataLoader and its CSV conventions.
"""

import os
import sys
import datetime as dt

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_loader import FileDataLoader


# ---------------------------------------------------------------------------
# Fixtures — build a small synthetic data directory
# ---------------------------------------------------------------------------

@pytest.fixture
def data_dir(tmp_path):
    """
    Create a minimal data directory with equity, options, dividends,
    FX rates and holiday files for tests.
    """
    eq_dir  = tmp_path / "equity"
    opt_dir = tmp_path / "options"
    dvd_dir = tmp_path / "dividends"
    cal_dir = tmp_path / "calendars"
    for d in (eq_dir, opt_dir, dvd_dir, cal_dir):
        d.mkdir()

    # Equity pricing
    (eq_dir / "SPY US equity_pricing.csv").write_text(
        "date,px_last\n"
        "2024-01-02,476.0\n"
        "2024-01-03,474.0\n"
        "2024-01-04,473.5\n"
    )

    # Option pricing (backtest_format)
    (opt_dir / "SPY US_backtest_format_options.csv").write_text(
        "ticker,date,side,value\n"
        "SPY US 01/19/24 C490,2024-01-02,px_bid,1.50\n"
        "SPY US 01/19/24 C490,2024-01-02,px_ask,1.70\n"
        "SPY US 01/19/24 C490,2024-01-03,px_bid,1.20\n"
        "SPY US 01/19/24 C490,2024-01-03,px_ask,1.40\n"
    )

    # Dividends
    (dvd_dir / "SPY US_dividends.csv").write_text(
        "ex_date,dvd_amount\n"
        "2024-03-15,1.75\n"
    )

    # FX rates
    fx_file = tmp_path / "fx_rates.csv"
    fx_file.write_text(
        "date,currency,rate\n"
        "2024-01-02,CAD,1.33\n"
        "2024-01-02,USD,1.00\n"
        "2024-01-03,CAD,1.34\n"
        "2024-01-03,USD,1.00\n"
    )

    # Holiday calendar
    (cal_dir / "tsx_holidays.csv").write_text(
        "date,name\n"
        "2024-01-01,New Year\n"
        "2024-12-25,Christmas\n"
    )

    return {
        "equity_dir": str(eq_dir),
        "options_dir": str(opt_dir),
        "dividends_dir": str(dvd_dir),
        "fx_file": str(fx_file),
        "holidays_dir": str(cal_dir),
    }


@pytest.fixture
def loader(data_dir):
    return FileDataLoader(**data_dir)


# ---------------------------------------------------------------------------
# get_equity_pricing
# ---------------------------------------------------------------------------

class TestGetEquityPricing:
    def test_returns_dict(self, loader):
        result = loader.get_equity_pricing("SPY US")
        assert isinstance(result, dict)

    def test_correct_values(self, loader):
        result = loader.get_equity_pricing("SPY US")
        assert result["2024-01-02"] == pytest.approx(476.0)
        assert result["2024-01-04"] == pytest.approx(473.5)

    def test_date_filter_start(self, loader):
        result = loader.get_equity_pricing("SPY US", start_date="2024-01-03")
        assert "2024-01-02" not in result
        assert "2024-01-03" in result

    def test_date_filter_end(self, loader):
        result = loader.get_equity_pricing("SPY US", end_date="2024-01-03")
        assert "2024-01-04" not in result

    def test_missing_file_raises(self, loader):
        with pytest.raises(FileNotFoundError):
            loader.get_equity_pricing("NONEXISTENT TICKER")


# ---------------------------------------------------------------------------
# get_option_pricing
# ---------------------------------------------------------------------------

class TestGetOptionPricing:
    def test_returns_dataframe(self, loader):
        result = loader.get_option_pricing("SPY US")
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, loader):
        result = loader.get_option_pricing("SPY US")
        for col in ("ticker", "date", "side", "value"):
            assert col in result.columns

    def test_filter_by_option_tickers(self, loader):
        result = loader.get_option_pricing(
            "SPY US",
            option_tickers=["SPY US 01/19/24 C490"],
        )
        assert not result.empty
        assert set(result["ticker"].unique()) == {"SPY US 01/19/24 C490"}

    def test_filter_strips_equity_suffix(self, loader):
        result = loader.get_option_pricing(
            "SPY US",
            option_tickers=["SPY US 01/19/24 C490 Equity"],
        )
        assert not result.empty

    def test_side_values(self, loader):
        result = loader.get_option_pricing("SPY US")
        assert set(result["side"].unique()) <= {"px_bid", "px_ask"}

    def test_missing_file_raises(self, loader):
        with pytest.raises(FileNotFoundError):
            loader.get_option_pricing("NONEXISTENT TICKER")


# ---------------------------------------------------------------------------
# get_dividends
# ---------------------------------------------------------------------------

class TestGetDividends:
    def test_returns_dict(self, loader):
        result = loader.get_dividends("SPY US")
        assert isinstance(result, dict)

    def test_correct_value(self, loader):
        result = loader.get_dividends("SPY US")
        assert result.get("2024-03-15") == pytest.approx(1.75)

    def test_missing_file_returns_empty_dict(self, loader):
        result = loader.get_dividends("NO DIVIDENDS TICKER")
        assert result == {}

    def test_no_dividends_dir_returns_empty(self, data_dir):
        ld = FileDataLoader(
            equity_dir=data_dir["equity_dir"],
            options_dir=data_dir["options_dir"],
            # dividends_dir omitted
        )
        assert ld.get_dividends("SPY US") == {}


# ---------------------------------------------------------------------------
# get_fx_rates
# ---------------------------------------------------------------------------

class TestGetFxRates:
    def test_returns_nested_dict(self, loader):
        result = loader.get_fx_rates()
        assert isinstance(result, dict)
        assert "CAD" in result
        assert "USD" in result

    def test_cad_value(self, loader):
        result = loader.get_fx_rates()
        assert result["CAD"]["2024-01-02"] == pytest.approx(1.33)

    def test_missing_fx_file_returns_empty(self, data_dir):
        ld = FileDataLoader(equity_dir=data_dir["equity_dir"])
        assert ld.get_fx_rates() == {}


# ---------------------------------------------------------------------------
# get_holidays
# ---------------------------------------------------------------------------

class TestGetHolidays:
    def test_returns_dict(self, loader):
        result = loader.get_holidays("TSX")
        assert isinstance(result, dict)

    def test_known_holiday(self, loader):
        result = loader.get_holidays("TSX")
        assert "2024-01-01" in result
        assert "2024-12-25" in result

    def test_missing_calendar_returns_empty(self, loader):
        result = loader.get_holidays("NONEXISTENT_CALENDAR")
        assert result == {}
