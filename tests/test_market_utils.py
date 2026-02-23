"""
tests/test_market_utils.py
==========================
Unit tests for utils/market_utils.py — standalone calendar and option-ticker
utilities that must work without any database or internal IM library.
"""

import datetime as dt
import sys
import os

import pandas as pd
import pytest

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.market_utils import (
    workday,
    week_count,
    OptionTickerInfo,
    load_holidays_from_csv,
    total_return_calc,
)


# ---------------------------------------------------------------------------
# workday
# ---------------------------------------------------------------------------

class TestWorkday:
    def test_advance_one_business_day_monday(self):
        # Monday -> Tuesday
        monday = dt.datetime(2024, 1, 8)
        assert workday(monday, 1) == dt.datetime(2024, 1, 9)

    def test_advance_skips_weekend(self):
        # Friday -> next Monday
        friday = dt.datetime(2024, 1, 5)
        assert workday(friday, 1) == dt.datetime(2024, 1, 8)

    def test_advance_zero_days(self):
        d = dt.datetime(2024, 3, 15)
        assert workday(d, 0) == d

    def test_advance_skips_holiday(self):
        # Monday is a holiday; next business day is Tuesday
        monday = dt.datetime(2024, 1, 8)
        holidays = {monday.strftime("%Y-%m-%d"): "Holiday"}
        # Starting from Friday, advance 1 → skip Monday → Tuesday
        friday = dt.datetime(2024, 1, 5)
        result = workday(friday, 1, holidays)
        assert result == dt.datetime(2024, 1, 9)

    def test_go_backward(self):
        # Wednesday -> Tuesday (1 business day back)
        wednesday = dt.datetime(2024, 1, 10)
        assert workday(wednesday, -1) == dt.datetime(2024, 1, 9)

    def test_go_backward_skips_weekend(self):
        # Monday -> previous Friday
        monday = dt.datetime(2024, 1, 8)
        assert workday(monday, -1) == dt.datetime(2024, 1, 5)

    def test_advance_multiple_days(self):
        monday = dt.datetime(2024, 1, 8)
        # +5 business days should land on the next Monday
        assert workday(monday, 5) == dt.datetime(2024, 1, 15)


# ---------------------------------------------------------------------------
# week_count
# ---------------------------------------------------------------------------

class TestWeekCount:
    def test_returns_dict(self):
        result = week_count(dt.datetime(2024, 1, 1), years=1)
        assert isinstance(result, dict)

    def test_known_third_friday(self):
        # Jan 19 2024 is the third Friday of January 2024
        result = week_count(dt.datetime(2024, 1, 1), years=1)
        assert result.get("2024-01-19") == 3

    def test_first_friday(self):
        # Jan 5 2024 is the first Friday of January 2024
        result = week_count(dt.datetime(2024, 1, 1), years=1)
        assert result.get("2024-01-05") == 1

    def test_covers_requested_years(self):
        result = week_count(dt.datetime(2023, 1, 1), years=2)
        # Should cover at least 2024
        assert any(k.startswith("2024") for k in result)


# ---------------------------------------------------------------------------
# OptionTickerInfo
# ---------------------------------------------------------------------------

class TestOptionTickerInfo:
    def _make_df(self, tickers):
        return pd.DataFrame({"ticker": tickers})

    def test_call_option_xiu(self):
        df = self._make_df(["XIU CN 03/15/24 C30.5"])
        info = OptionTickerInfo(df, "ticker")
        assert info.expiry["XIU CN 03/15/24 C30.5"] == dt.date(2024, 3, 15)
        assert info.strike["XIU CN 03/15/24 C30.5"] == pytest.approx(30.5)
        assert info.option_type["XIU CN 03/15/24 C30.5"] == "call"
        assert info.currency["XIU CN 03/15/24 C30.5"] == "CAD"

    def test_put_option_spy(self):
        df = self._make_df(["SPY US 01/17/25 P450"])
        info = OptionTickerInfo(df, "ticker")
        assert info.expiry["SPY US 01/17/25 P450"] == dt.date(2025, 1, 17)
        assert info.strike["SPY US 01/17/25 P450"] == pytest.approx(450.0)
        assert info.option_type["SPY US 01/17/25 P450"] == "put"
        assert info.currency["SPY US 01/17/25 P450"] == "USD"

    def test_underlying_override_btcc(self):
        df = self._make_df(["BTCC CN 06/21/24 C5"])
        info = OptionTickerInfo(df, "ticker")
        assert info.underlying_ticker["BTCC CN 06/21/24 C5"] == "BTCC/B CN"

    def test_underlying_override_rci(self):
        df = self._make_df(["RCI CN 03/15/24 C45"])
        info = OptionTickerInfo(df, "ticker")
        assert info.underlying_ticker["RCI CN 03/15/24 C45"] == "RCI/B CN"

    def test_empty_dataframe(self):
        df = pd.DataFrame({"ticker": []})
        info = OptionTickerInfo(df, "ticker")
        assert info.expiry == {}
        assert info.strike == {}

    def test_malformed_ticker_skipped(self):
        df = self._make_df(["INVALID_TICKER", "SPY US 01/17/25 C450"])
        info = OptionTickerInfo(df, "ticker")
        assert "INVALID_TICKER" not in info.expiry
        assert "SPY US 01/17/25 C450" in info.expiry

    def test_multiple_tickers(self):
        tickers = [
            "XIU CN 03/15/24 C30",
            "XIU CN 04/19/24 C31",
            "SPY US 01/17/25 C450",
        ]
        df = self._make_df(tickers)
        info = OptionTickerInfo(df, "ticker")
        assert len(info.expiry) == 3


# ---------------------------------------------------------------------------
# load_holidays_from_csv
# ---------------------------------------------------------------------------

class TestLoadHolidaysFromCsv:
    def test_loads_correctly(self, tmp_path):
        csv = tmp_path / "holidays.csv"
        csv.write_text("date,name\n2024-01-01,New Year\n2024-07-04,Independence Day\n")
        result = load_holidays_from_csv(str(csv))
        assert result["2024-01-01"] == "New Year"
        assert result["2024-07-04"] == "Independence Day"

    def test_missing_date_column_raises(self, tmp_path):
        csv = tmp_path / "bad.csv"
        csv.write_text("holiday,name\n2024-01-01,New Year\n")
        with pytest.raises(ValueError, match="'date' column"):
            load_holidays_from_csv(str(csv))

    def test_date_format_normalised(self, tmp_path):
        csv = tmp_path / "holidays.csv"
        csv.write_text("date,name\n01/01/2024,New Year\n")
        result = load_holidays_from_csv(str(csv))
        assert "2024-01-01" in result


# ---------------------------------------------------------------------------
# total_return_calc
# ---------------------------------------------------------------------------

class TestTotalReturnCalc:
    def test_no_dividends(self):
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "price": [100.0, 101.0, 102.0],
            "dvd": [0, 0, 0],
        })
        result = total_return_calc(df, "price", "dvd")
        assert "total_return_price" in result.columns
        # With no dividends, total return == raw price
        assert result["total_return_price"].tolist() == pytest.approx([100.0, 101.0, 102.0])

    def test_with_dividend(self):
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "price": [100.0, 99.0, 100.0],
            "dvd": [0, 1.0, 0],
        })
        result = total_return_calc(df, "price", "dvd")
        # After dividend on day 2: price drops to 99 but TR should stay higher
        tr = result["total_return_price"].tolist()
        assert tr[2] > tr[1]  # total return grows even though raw price recovered

    def test_dividend_on_first_day_raises(self):
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "price": [100.0, 99.0],
            "dvd": [1.0, 0],
        })
        with pytest.raises(ValueError):
            total_return_calc(df, "price", "dvd")

    def test_empty_dvd_strings_handled(self):
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "price": [100.0, 101.0],
            "dvd": ["", ""],
        })
        result = total_return_calc(df, "price", "dvd")
        assert result["total_return_price"].tolist() == pytest.approx([100.0, 101.0])
