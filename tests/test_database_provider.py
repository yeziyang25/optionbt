"""Tests for DatabaseDataProvider."""

import pytest
import pandas as pd
from datetime import date
from unittest.mock import MagicMock

from src.optionbt.data.loader import DatabaseDataProvider


class FakeDbConnection:
    """Minimal DB connection stub matching common.db_connection() API."""

    def __init__(self, data: dict):
        """data: mapping of SQL fragment → DataFrame to return."""
        self._data = data

    def query_tbl(self, sql: str) -> pd.DataFrame:
        for fragment, df in self._data.items():
            if fragment in sql:
                return df.copy()
        return pd.DataFrame()


class TestDatabaseDataProviderEquity:
    """Test equity data loading from database."""

    def test_load_equity_basic(self):
        df = pd.DataFrame({
            "date": ["2024-01-02", "2024-01-03"],
            "close": [100.0, 101.0],
            "source": ["bloomberg", "bloomberg"],
        })
        conn = FakeDbConnection({"px_last": df})
        provider = DatabaseDataProvider(conn)

        result = provider.load_equity_data("SPY", date(2024, 1, 1), date(2024, 1, 31))
        assert len(result) == 2
        assert "close" in result.columns
        assert "date" in result.columns

    def test_load_equity_deduplicates_by_source(self):
        """When multiple sources exist for the same date, prefer bloomberg."""
        df = pd.DataFrame({
            "date": ["2024-01-02", "2024-01-02"],
            "close": [100.0, 99.5],
            "source": ["bloomberg", "solactive"],
        })
        conn = FakeDbConnection({"px_last": df})
        provider = DatabaseDataProvider(conn)

        result = provider.load_equity_data("SPY", date(2024, 1, 1), date(2024, 1, 31))
        assert len(result) == 1
        assert result.iloc[0]["close"] == 100.0  # bloomberg preferred

    def test_load_equity_missing_raises(self):
        conn = FakeDbConnection({"px_last": pd.DataFrame()})
        provider = DatabaseDataProvider(conn)
        with pytest.raises(FileNotFoundError):
            provider.load_equity_data("MISSING", date(2024, 1, 1), date(2024, 1, 31))


class TestDatabaseDataProviderOptions:
    """Test option data loading from database."""

    def test_load_option_data_basic(self):
        raw = pd.DataFrame({
            "ticker": [
                "SPY CN 01/19/24 C480",
                "SPY CN 01/19/24 C480",
            ],
            "date": ["2024-01-02", "2024-01-02"],
            "field": ["px_bid", "px_ask"],
            "value": [5.0, 5.2],
        })
        conn = FakeDbConnection({"px_ask": raw})
        provider = DatabaseDataProvider(conn)

        result = provider.load_option_data("SPY", date(2024, 1, 1), date(2024, 1, 31))
        assert len(result) >= 1
        assert "bid" in result.columns
        assert "ask" in result.columns
        assert "strike" in result.columns
        assert "call_put" in result.columns

    def test_load_option_data_filter_type(self):
        raw = pd.DataFrame({
            "ticker": [
                "SPY CN 01/19/24 C480",
                "SPY CN 01/19/24 C480",
                "SPY CN 01/19/24 P460",
                "SPY CN 01/19/24 P460",
            ],
            "date": ["2024-01-02"] * 4,
            "field": ["px_bid", "px_ask", "px_bid", "px_ask"],
            "value": [5.0, 5.2, 3.0, 3.2],
        })
        conn = FakeDbConnection({"px_ask": raw})
        provider = DatabaseDataProvider(conn)

        calls = provider.load_option_data(
            "SPY", date(2024, 1, 1), date(2024, 1, 31), option_type="call"
        )
        assert all(calls["call_put"] == "call")

    def test_load_option_data_missing_raises(self):
        conn = FakeDbConnection({"px_ask": pd.DataFrame()})
        provider = DatabaseDataProvider(conn)
        with pytest.raises(FileNotFoundError):
            provider.load_option_data("MISSING", date(2024, 1, 1), date(2024, 1, 31))


class TestDatabaseDataProviderDividends:
    """Test dividend loading."""

    def test_load_dividends(self):
        df = pd.DataFrame({
            "ex_date": ["2024-03-15", "2024-06-14"],
            "dvd_amount": [1.50, 1.60],
        })
        conn = FakeDbConnection({"dividends": df})
        provider = DatabaseDataProvider(conn)

        divs = provider.load_dividends("SPY")
        assert date(2024, 3, 15) in divs
        assert divs[date(2024, 3, 15)] == 1.50

    def test_load_dividends_empty(self):
        conn = FakeDbConnection({"dividends": pd.DataFrame()})
        provider = DatabaseDataProvider(conn)
        assert provider.load_dividends("NODVD") == {}
