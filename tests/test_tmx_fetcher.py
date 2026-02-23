"""
tests/test_tmx_fetcher.py
=========================
Tests for the TMX option downloader:
  - modify_data (data transformation logic)
  - save_to_csv (standalone file persistence)
"""

import os
import sys
import datetime as dt

import pandas as pd
import pytest
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tmx_option_downloader import modify_data, save_to_csv


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_raw_tmx_df(start_dt: dt.datetime):
    """Build a minimal raw TMX HTML download DataFrame."""
    # TMX download includes header rows with underlying price (Class Symbol = NaN)
    # and option rows
    rows = [
        # Underlying price row (Class Symbol is NaN)
        {
            "Symbol": np.nan,
            "Class Symbol": np.nan,
            "Date": start_dt.strftime("%Y-%m-%d"),
            "Expiry Date": np.nan,
            "Strike Price": np.nan,
            "Call/Put": np.nan,
            "Bid Price": np.nan,
            "Ask Price": np.nan,
            "Last Price": 30.0,
        },
        # Option rows
        {
            "Symbol": "XIU24300C",
            "Class Symbol": "XIU",
            "Date": start_dt.strftime("%Y-%m-%d"),
            "Expiry Date": "2024-03-15",
            "Strike Price": 30.0,
            "Call/Put": 0,  # 0 = call
            "Bid Price": 0.55,
            "Ask Price": 0.65,
            "Last Price": 0.60,
        },
        {
            "Symbol": "XIU24310C",
            "Class Symbol": "XIU",
            "Date": start_dt.strftime("%Y-%m-%d"),
            "Expiry Date": "2024-03-15",
            "Strike Price": 31.0,
            "Call/Put": 0,  # 0 = call
            "Bid Price": 0.20,
            "Ask Price": 0.30,
            "Last Price": 0.25,
        },
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# modify_data
# ---------------------------------------------------------------------------

class TestModifyData:
    def test_returns_dataframe(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        result = modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="call")
        assert isinstance(result, pd.DataFrame)

    def test_required_columns_present(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        result = modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="call")
        if not result.empty:
            for col in ("ticker", "date", "side", "value"):
                assert col in result.columns

    def test_side_values_normalised(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        result = modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="call")
        if not result.empty:
            assert set(result["side"].unique()).issubset({"px_bid", "px_ask"})

    def test_put_filter(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        # All rows are calls (Call/Put == 0), so asking for puts should be empty
        result = modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="put")
        assert result.empty

    def test_invalid_call_put_raises(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        with pytest.raises(ValueError, match="call/put"):
            modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="invalid")

    def test_ticker_format(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        result = modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="call")
        if not result.empty:
            # Ticker should be like "XIU CN MM/DD/YY C30.0"
            sample = result["ticker"].iloc[0]
            assert " CN " in sample
            assert sample.count(" ") >= 3

    def test_underlying_price_populated(self):
        raw = _make_raw_tmx_df(dt.datetime(2024, 1, 19))
        result = modify_data(raw, dt.datetime(2024, 1, 19), pct_otm_limit=0.10, call_put="call")
        if not result.empty:
            assert "underlying_price" in result.columns
            assert (result["underlying_price"] > 0).all()


# ---------------------------------------------------------------------------
# save_to_csv
# ---------------------------------------------------------------------------

class TestSaveToCsv:
    def _make_sample_data(self):
        return pd.DataFrame({
            "ticker": ["XIU CN 03/15/24 C30", "XIU CN 03/15/24 C30"],
            "date": ["2024-01-19", "2024-01-19"],
            "side": ["px_bid", "px_ask"],
            "value": [0.55, 0.65],
        })

    def test_creates_file(self, tmp_path):
        df = self._make_sample_data()
        path = save_to_csv(df, str(tmp_path), "XIU")
        assert os.path.exists(path)

    def test_correct_filename(self, tmp_path):
        df = self._make_sample_data()
        path = save_to_csv(df, str(tmp_path), "XIU")
        assert path.endswith("XIU_backtest_format_options.csv")

    def test_saved_columns(self, tmp_path):
        df = self._make_sample_data()
        path = save_to_csv(df, str(tmp_path), "XIU")
        saved = pd.read_csv(path)
        for col in ("ticker", "date", "side", "value"):
            assert col in saved.columns

    def test_saved_row_count(self, tmp_path):
        df = self._make_sample_data()
        path = save_to_csv(df, str(tmp_path), "XIU")
        saved = pd.read_csv(path)
        assert len(saved) == 2

    def test_idempotent_append(self, tmp_path):
        """Calling save_to_csv twice with the same data must not duplicate rows."""
        df = self._make_sample_data()
        save_to_csv(df, str(tmp_path), "XIU")
        save_to_csv(df, str(tmp_path), "XIU")
        saved = pd.read_csv(tmp_path / "XIU_backtest_format_options.csv")
        assert len(saved) == 2  # still 2, not 4

    def test_append_new_rows(self, tmp_path):
        """New date range data is appended to an existing file."""
        df1 = self._make_sample_data()
        df2 = pd.DataFrame({
            "ticker": ["XIU CN 04/19/24 C31"],
            "date": ["2024-02-16"],
            "side": ["px_bid"],
            "value": [0.40],
        })
        save_to_csv(df1, str(tmp_path), "XIU")
        save_to_csv(df2, str(tmp_path), "XIU")
        saved = pd.read_csv(tmp_path / "XIU_backtest_format_options.csv")
        assert len(saved) == 3  # 2 original + 1 new

    def test_creates_output_dir(self, tmp_path):
        df = self._make_sample_data()
        new_dir = str(tmp_path / "nested" / "dir")
        save_to_csv(df, new_dir, "XIU")
        assert os.path.isdir(new_dir)
