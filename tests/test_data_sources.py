"""
tests/test_data_sources.py
==========================
Unit tests for the data_sources/ package:
  - data_sources.base.DataFetcher interface
  - data_sources.csv_loader.CsvDataFetcher
  - data_sources.tmx_fetcher._process_raw and TmxFetcher.get_option_chain cache
  - data_sources._extract_option_ticker fallback in securities.py
"""

import os
import sys
import datetime as dt

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_sources.base import DataFetcher
from data_sources.csv_loader import (
    CsvDataFetcher,
    _normalise_equity_df,
    _normalise_options_df,
    _normalise_dividends_df,
    _filter_date_range,
)
from data_sources.tmx_fetcher import _process_raw, select_option_for_roll


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_equity_csv(tmp_path, ticker: str, rows: list) -> str:
    path = tmp_path / f"{ticker} equity_pricing.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return str(tmp_path)


def _make_options_csv(tmp_path, ticker: str, rows: list) -> str:
    path = tmp_path / f"{ticker}_backtest_format_options.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return str(tmp_path)


def _make_raw_tmx_df(start_str: str) -> pd.DataFrame:
    """Return a minimal raw TMX-format DataFrame (same structure as web download)."""
    rows = [
        # Underlying price row (Class Symbol is NaN)
        {
            "Symbol": np.nan,
            "Class Symbol": np.nan,
            "Date": start_str,
            "Expiry Date": np.nan,
            "Strike Price": np.nan,
            "Call/Put": np.nan,
            "Bid Price": np.nan,
            "Ask Price": np.nan,
            "Last Price": 30.0,
        },
        # ATM call
        {
            "Symbol": "XIU24300C",
            "Class Symbol": "XIU",
            "Date": start_str,
            "Expiry Date": "2024-03-15",
            "Strike Price": 30.0,
            "Call/Put": 0,
            "Bid Price": 0.55,
            "Ask Price": 0.65,
            "Last Price": 0.60,
        },
        # OTM call
        {
            "Symbol": "XIU24310C",
            "Class Symbol": "XIU",
            "Date": start_str,
            "Expiry Date": "2024-03-15",
            "Strike Price": 31.0,
            "Call/Put": 0,
            "Bid Price": 0.20,
            "Ask Price": 0.30,
            "Last Price": 0.25,
        },
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# DataFetcher ABC
# ---------------------------------------------------------------------------

class TestDataFetcherABC:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            DataFetcher()

    def test_concrete_subclass_must_implement_get_equity_pricing(self):
        class Bad(DataFetcher):
            def get_option_pricing(self, ticker, opt_tickers=None):
                return pd.DataFrame()
        with pytest.raises(TypeError):
            Bad()

    def test_concrete_subclass_must_implement_get_option_pricing(self):
        class Bad(DataFetcher):
            def get_equity_pricing(self, ticker, start_date, end_date):
                return {}
        with pytest.raises(TypeError):
            Bad()

    def test_concrete_subclass_with_both_required_methods(self):
        class Good(DataFetcher):
            def get_equity_pricing(self, ticker, start_date, end_date):
                return {}
            def get_option_pricing(self, ticker, opt_tickers=None):
                return pd.DataFrame()
        obj = Good()
        assert obj.get_dividends("anything") == {}
        assert obj.get_holidays("TSX") == {}


# ---------------------------------------------------------------------------
# _normalise_equity_df
# ---------------------------------------------------------------------------

class TestNormaliseEquityDf:
    def test_standard_columns(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "px_last": [100.0]})
        result = _normalise_equity_df(df)
        assert list(result.columns) == ["date", "px_last"]

    def test_close_column_renamed(self):
        df = pd.DataFrame({"Date": ["2024-01-01"], "Close": [200.0]})
        result = _normalise_equity_df(df)
        assert "px_last" in result.columns

    def test_value_column_renamed(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "value": [150.0]})
        result = _normalise_equity_df(df)
        assert "px_last" in result.columns

    def test_date_format_normalised(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "px_last": [100.0]})
        result = _normalise_equity_df(df)
        assert result["date"].iloc[0] == "2024-01-01"

    def test_missing_price_column_raises(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "mystery_col": [100.0]})
        with pytest.raises(ValueError, match="price columns"):
            _normalise_equity_df(df)


# ---------------------------------------------------------------------------
# _normalise_options_df
# ---------------------------------------------------------------------------

class TestNormaliseOptionsDf:
    def test_standard_columns_pass_through(self):
        df = pd.DataFrame({
            "ticker": ["SPY CN 03/15/24 C30"],
            "date": ["2024-01-19"],
            "side": ["px_bid"],
            "value": [0.55],
        })
        result = _normalise_options_df(df)
        assert list(result.columns) == ["ticker", "date", "side", "value"]

    def test_wide_format_bid_ask_melted(self):
        df = pd.DataFrame({
            "ticker": ["SPY CN 03/15/24 C30"],
            "date": ["2024-01-19"],
            "px_bid": [0.55],
            "px_ask": [0.65],
        })
        result = _normalise_options_df(df)
        assert len(result) == 2
        assert set(result["side"].unique()) == {"px_bid", "px_ask"}

    def test_bid_ask_columns_renamed(self):
        df = pd.DataFrame({
            "ticker": ["SPY CN 03/15/24 C30"],
            "date": ["2024-01-19"],
            "bid": [0.55],
            "ask": [0.65],
        })
        result = _normalise_options_df(df)
        assert "side" in result.columns
        assert set(result["side"].unique()).issubset({"px_bid", "px_ask"})


# ---------------------------------------------------------------------------
# CsvDataFetcher
# ---------------------------------------------------------------------------

class TestCsvDataFetcherEquityPricing:
    @pytest.fixture
    def equity_dir(self, tmp_path):
        return _make_equity_csv(
            tmp_path,
            "SPY US",
            [
                {"date": "2024-01-02", "px_last": 476.0},
                {"date": "2024-01-03", "px_last": 474.0},
                {"date": "2024-01-04", "px_last": 473.5},
            ],
        )

    def test_returns_dict(self, equity_dir):
        f = CsvDataFetcher(equity_dir=equity_dir)
        result = f.get_equity_pricing("SPY US", "2024-01-01", "2024-12-31")
        assert isinstance(result, dict)

    def test_correct_values(self, equity_dir):
        f = CsvDataFetcher(equity_dir=equity_dir)
        result = f.get_equity_pricing("SPY US", "2024-01-01", "2024-12-31")
        assert result["2024-01-02"] == pytest.approx(476.0)

    def test_date_filter_applied(self, equity_dir):
        f = CsvDataFetcher(equity_dir=equity_dir)
        result = f.get_equity_pricing("SPY US", "2024-01-03", "2024-12-31")
        assert "2024-01-02" not in result
        assert "2024-01-03" in result

    def test_missing_ticker_raises(self, equity_dir):
        f = CsvDataFetcher(equity_dir=equity_dir)
        with pytest.raises(FileNotFoundError):
            f.get_equity_pricing("UNKNOWN", "2024-01-01", "2024-12-31")

    def test_no_dir_raises(self):
        f = CsvDataFetcher()
        with pytest.raises(FileNotFoundError, match="no equity_dir or equity_file"):
            f.get_equity_pricing("SPY US", "2024-01-01", "2024-12-31")


class TestCsvDataFetcherOptionPricing:
    @pytest.fixture
    def options_dir(self, tmp_path):
        return _make_options_csv(
            tmp_path,
            "SPY US",
            [
                {"ticker": "SPY US 01/19/24 C490", "date": "2024-01-02", "side": "px_bid", "value": 1.5},
                {"ticker": "SPY US 01/19/24 C490", "date": "2024-01-02", "side": "px_ask", "value": 1.7},
                {"ticker": "SPY US 01/19/24 C500", "date": "2024-01-02", "side": "px_bid", "value": 0.5},
                {"ticker": "SPY US 01/19/24 C500", "date": "2024-01-02", "side": "px_ask", "value": 0.6},
            ],
        )

    def test_returns_dataframe(self, options_dir):
        f = CsvDataFetcher(options_dir=options_dir)
        result = f.get_option_pricing("SPY US")
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, options_dir):
        f = CsvDataFetcher(options_dir=options_dir)
        result = f.get_option_pricing("SPY US")
        for col in ("ticker", "date", "side", "value"):
            assert col in result.columns

    def test_filter_by_opt_tickers(self, options_dir):
        f = CsvDataFetcher(options_dir=options_dir)
        result = f.get_option_pricing("SPY US", opt_tickers=["SPY US 01/19/24 C490"])
        assert set(result["ticker"].unique()) == {"SPY US 01/19/24 C490"}

    def test_filter_strips_equity_suffix(self, options_dir):
        f = CsvDataFetcher(options_dir=options_dir)
        result = f.get_option_pricing(
            "SPY US", opt_tickers=["SPY US 01/19/24 C490 Equity"]
        )
        assert not result.empty


class TestCsvDataFetcherDividends:
    @pytest.fixture
    def dividends_dir(self, tmp_path):
        path = tmp_path / "SPY US_dividends.csv"
        pd.DataFrame([{"ex_date": "2024-03-15", "dvd_amount": 1.75}]).to_csv(path, index=False)
        return str(tmp_path)

    def test_correct_value(self, dividends_dir):
        f = CsvDataFetcher(dividends_dir=dividends_dir)
        result = f.get_dividends("SPY US")
        assert result.get("2024-03-15") == pytest.approx(1.75)

    def test_missing_file_returns_empty(self, dividends_dir):
        f = CsvDataFetcher(dividends_dir=dividends_dir)
        assert f.get_dividends("NO_TICKER") == {}

    def test_no_dir_returns_empty(self):
        f = CsvDataFetcher()
        assert f.get_dividends("SPY US") == {}


# ---------------------------------------------------------------------------
# _process_raw (TMX data transformation — same logic as modify_data)
# ---------------------------------------------------------------------------

class TestProcessRaw:
    def test_returns_dataframe(self):
        raw = _make_raw_tmx_df("2024-01-19")
        start_dt = dt.datetime(2024, 1, 19)
        result = _process_raw(raw, start_dt, "call", pct_otm_limit=0.10, max_ttm_days=66)
        assert isinstance(result, pd.DataFrame)

    def test_required_columns_present(self):
        raw = _make_raw_tmx_df("2024-01-19")
        result = _process_raw(raw, dt.datetime(2024, 1, 19), "call", 0.10, 66)
        if not result.empty:
            for col in ("ticker", "date", "side", "value"):
                assert col in result.columns

    def test_side_values_correct(self):
        raw = _make_raw_tmx_df("2024-01-19")
        result = _process_raw(raw, dt.datetime(2024, 1, 19), "call", 0.10, 66)
        if not result.empty:
            assert set(result["side"].unique()).issubset({"px_bid", "px_ask"})

    def test_put_filter_returns_empty_for_call_only_data(self):
        raw = _make_raw_tmx_df("2024-01-19")
        result = _process_raw(raw, dt.datetime(2024, 1, 19), "put", 0.10, 66)
        assert result.empty

    def test_invalid_call_put_raises(self):
        raw = _make_raw_tmx_df("2024-01-19")
        with pytest.raises(ValueError, match="call.*put"):
            _process_raw(raw, dt.datetime(2024, 1, 19), "invalid", 0.10, 66)

    def test_ticker_format_contains_cn(self):
        raw = _make_raw_tmx_df("2024-01-19")
        result = _process_raw(raw, dt.datetime(2024, 1, 19), "call", 0.10, 66)
        if not result.empty:
            assert " CN " in result["ticker"].iloc[0]

    def test_no_trailing_dot_zero_in_ticker(self):
        """Strike of 30.0 should appear as '30' not '30.0' in the ticker."""
        raw = _make_raw_tmx_df("2024-01-19")
        result = _process_raw(raw, dt.datetime(2024, 1, 19), "call", 0.10, 66)
        if not result.empty:
            assert not any(t.endswith(".0") for t in result["ticker"])


# ---------------------------------------------------------------------------
# select_option_for_roll
# ---------------------------------------------------------------------------

class TestSelectOptionForRoll:
    @pytest.fixture
    def chain_df(self):
        """Minimal processed option chain with two strikes."""
        rows = []
        for strike, otm in [(30, 0.0), (31, 0.033)]:
            for side in ("px_bid", "px_ask"):
                rows.append({
                    "ticker": f"XIU CN 03/15/24 C{strike}",
                    "date": "2024-01-19",
                    "side": side,
                    "value": 0.55 if side == "px_bid" else 0.65,
                    "pct_otm": otm,
                })
        return pd.DataFrame(rows)

    def test_returns_string_ticker(self, chain_df):
        result = select_option_for_roll(
            chain_df,
            roll_date=dt.datetime(2024, 1, 19),
            target_expiry=dt.datetime(2024, 3, 15),
            pct_otm_target=0.0,
            strike_mode="abs_closest",
        )
        assert isinstance(result, str)

    def test_atm_selection_abs_closest(self, chain_df):
        result = select_option_for_roll(
            chain_df,
            roll_date=dt.datetime(2024, 1, 19),
            target_expiry=dt.datetime(2024, 3, 15),
            pct_otm_target=0.0,
            strike_mode="abs_closest",
        )
        assert "C30" in result

    def test_round_up_returns_otm(self, chain_df):
        result = select_option_for_roll(
            chain_df,
            roll_date=dt.datetime(2024, 1, 19),
            target_expiry=dt.datetime(2024, 3, 15),
            pct_otm_target=0.02,
            strike_mode="round_up",
        )
        # Only C31 is >= 2% OTM
        assert "C31" in result

    def test_no_data_returns_none(self, chain_df):
        result = select_option_for_roll(
            chain_df,
            roll_date=dt.datetime(2024, 2, 1),  # no data for this date
            target_expiry=dt.datetime(2024, 3, 15),
        )
        assert result is None


# ---------------------------------------------------------------------------
# _extract_option_ticker fallback in securities.py
# ---------------------------------------------------------------------------

class TestExtractOptionTickerFallback:
    def test_basic_parsing(self):
        from helper_functions.securities import _extract_option_ticker_local
        df = pd.DataFrame({"ticker": ["SPY US 09/19/25 C570", "RCI CN 04/17/20 C56"]})
        info = _extract_option_ticker_local(df, "ticker")
        assert info.expiry["SPY US 09/19/25 C570"] == dt.datetime(2025, 9, 19)
        assert info.strike["SPY US 09/19/25 C570"] == pytest.approx(570.0)
        assert info.option_type["SPY US 09/19/25 C570"] == "call"

    def test_put_option(self):
        from helper_functions.securities import _extract_option_ticker_local
        df = pd.DataFrame({"ticker": ["RCI CN 04/17/20 P50"]})
        info = _extract_option_ticker_local(df, "ticker")
        assert info.option_type["RCI CN 04/17/20 P50"] == "put"
        assert info.strike["RCI CN 04/17/20 P50"] == pytest.approx(50.0)

    def test_trailing_dot_zero_strike(self):
        from helper_functions.securities import _extract_option_ticker_local
        df = pd.DataFrame({"ticker": ["XIU CN 03/15/24 C30.0"]})
        info = _extract_option_ticker_local(df, "ticker")
        assert info.strike["XIU CN 03/15/24 C30.0"] == pytest.approx(30.0)

    def test_returns_option_ticker_info(self):
        from helper_functions.securities import _extract_option_ticker_local, _OptionTickerInfo
        df = pd.DataFrame({"ticker": ["SPY US 09/19/25 C570"]})
        info = _extract_option_ticker_local(df, "ticker")
        assert isinstance(info, _OptionTickerInfo)

    def test_dispatch_returns_local_when_common_none(self):
        """When im_prod is not available, _extract_option_ticker uses local fallback."""
        from helper_functions.securities import _extract_option_ticker, common
        if common is None:
            df = pd.DataFrame({"ticker": ["SPY US 09/19/25 C570"]})
            info = _extract_option_ticker(df, "ticker")
            assert info.strike.get("SPY US 09/19/25 C570") == pytest.approx(570.0)
