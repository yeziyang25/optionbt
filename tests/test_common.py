"""Tests for common utilities (option ticker parsing, DB abstractions)."""

import pytest
import pandas as pd
from datetime import date

from src.optionbt.utils.common import (
    extract_option_ticker,
    OptionTickerInfo,
    DbConnection,
)


# ---------------------------------------------------------------------------
# extract_option_ticker
# ---------------------------------------------------------------------------

class TestExtractOptionTicker:
    """Test the option ticker parser – mirrors common.extract_option_ticker."""

    def test_call_ticker(self):
        """Parse a standard call option ticker."""
        df = pd.DataFrame({"ticker": ["SPY CN 01/17/20 C325"]})
        info = extract_option_ticker(df, "ticker")
        assert info.underlying_ticker["SPY CN 01/17/20 C325"] == "SPY"
        assert info.expiry["SPY CN 01/17/20 C325"] == date(2020, 1, 17)
        assert info.strike["SPY CN 01/17/20 C325"] == 325.0
        assert info.option_type["SPY CN 01/17/20 C325"] == "C"

    def test_put_ticker(self):
        """Parse a put option ticker."""
        df = pd.DataFrame({"ticker": ["XIU CN 02/21/25 P30.5"]})
        info = extract_option_ticker(df, "ticker")
        assert info.option_type["XIU CN 02/21/25 P30.5"] == "P"
        assert info.strike["XIU CN 02/21/25 P30.5"] == 30.5
        assert info.expiry["XIU CN 02/21/25 P30.5"] == date(2025, 2, 21)

    def test_multiple_tickers(self):
        """Parse multiple tickers in one DataFrame."""
        df = pd.DataFrame({
            "ticker": [
                "SPY CN 01/17/20 C325",
                "SPY CN 01/17/20 P310",
                "SPY CN 02/21/20 C330",
            ]
        })
        info = extract_option_ticker(df, "ticker")
        assert len(info.expiry) == 3
        assert info.strike["SPY CN 02/21/20 C330"] == 330.0

    def test_duplicate_tickers(self):
        """Duplicates in the DataFrame should not cause errors."""
        df = pd.DataFrame({
            "ticker": [
                "SPY CN 01/17/20 C325",
                "SPY CN 01/17/20 C325",
            ]
        })
        info = extract_option_ticker(df, "ticker")
        assert len(info.expiry) == 1  # unique

    def test_missing_values(self):
        """NaN tickers should be silently skipped."""
        df = pd.DataFrame({"ticker": [None, "SPY CN 01/17/20 C325"]})
        info = extract_option_ticker(df, "ticker")
        assert len(info.expiry) == 1

    def test_unrecognised_format(self):
        """Non-matching ticker strings should be skipped without error."""
        df = pd.DataFrame({"ticker": ["SOME_RANDOM_STRING"]})
        info = extract_option_ticker(df, "ticker")
        assert len(info.expiry) == 0

    def test_underlying_with_slash(self):
        """Underlying tickers with special characters."""
        df = pd.DataFrame({"ticker": ["RCI/B CN 03/15/24 C50"]})
        info = extract_option_ticker(df, "ticker")
        assert info.underlying_ticker["RCI/B CN 03/15/24 C50"] == "RCI/B"
        assert info.strike["RCI/B CN 03/15/24 C50"] == 50.0


# ---------------------------------------------------------------------------
# DbConnection protocol
# ---------------------------------------------------------------------------

class TestDbConnection:
    """Test the abstract DbConnection protocol."""

    def test_list_to_sql_str(self):
        """list_to_sql_str should produce a SQL IN clause."""

        class DummyConn(DbConnection):
            def query_tbl(self, sql):
                return pd.DataFrame()

        conn = DummyConn()
        result = conn.list_to_sql_str(["SPY", "QQQ"])
        assert result == "('SPY', 'QQQ')"

    def test_list_to_sql_str_convert(self):

        class DummyConn(DbConnection):
            def query_tbl(self, sql):
                return pd.DataFrame()

        conn = DummyConn()
        result = conn.list_to_sql_str([1, 2, 3], convert_elements=True)
        assert result == "('1', '2', '3')"

    def test_insert_tbl_not_implemented(self):
        """Default insert_tbl should raise NotImplementedError."""

        class DummyConn(DbConnection):
            def query_tbl(self, sql):
                return pd.DataFrame()

        conn = DummyConn()
        with pytest.raises(NotImplementedError):
            conn.insert_tbl(pd.DataFrame())
