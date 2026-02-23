"""
data_loader.py
==============
Data loading abstraction for the option backtesting framework.

Two concrete backends are provided:

  FileDataLoader
    Loads all pricing data from local CSV files.  Completely standalone —
    no database or internal IM library required.

  DatabaseDataLoader
    Loads from the internal IM SQL database.  Wraps the existing
    common.db_connection() / data_library interface for backward
    compatibility.

Usage
-----
    # Standalone / CSV path
    from data_loader import FileDataLoader
    loader = FileDataLoader(
        equity_dir="data/equity",
        options_dir="data/options",
        dividends_dir="data/dividends",   # optional
        fx_file="data/fx_rates.csv",      # optional
        holidays_dir="data/calendars",    # optional
    )

    equity_px  = loader.get_equity_pricing("SPY US", "2020-01-01", "2025-01-01")
    option_df  = loader.get_option_pricing("SPY US")
    holidays   = loader.get_holidays("TSX")

    # Database path (production)
    from data_loader import DatabaseDataLoader
    loader = DatabaseDataLoader()
    equity_px = loader.get_equity_pricing("SPY US", "2020-01-01", "2025-01-01")

CSV file conventions
--------------------
Equity pricing
    <equity_dir>/<ticker> equity_pricing.csv
    Required columns: date, px_last
    Example filename:  "SPY US equity_pricing.csv"

Option pricing
    <options_dir>/<ticker>_backtest_format_options.csv
    Required columns: ticker, date, side (px_bid or px_ask), value
    Example filename:  "SPY US_backtest_format_options.csv"

Dividends (optional)
    <dividends_dir>/<ticker>_dividends.csv
    Required columns: ex_date, dvd_amount

FX rates (optional)
    <fx_file>  — a single CSV with columns: date, currency, rate
    Example:
        date,currency,rate
        2020-01-02,CAD,1.2980
        2020-01-02,USD,1.0000

Holiday calendars (optional)
    <holidays_dir>/tsx_holidays.csv   columns: date, name
    <holidays_dir>/nyse_holidays.csv  columns: date, name
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class DataLoader(ABC):
    """Base interface for all data loading backends."""

    @abstractmethod
    def get_equity_pricing(
        self,
        ticker: str,
        start_date: str = "",
        end_date: str = "",
    ) -> Dict[str, float]:
        """
        Return a {date_str: price} dict for the given equity ticker.

        Parameters
        ----------
        ticker     : Bloomberg-style ticker, e.g. "SPY US" or "XIU CN"
        start_date : ISO-format date string "YYYY-MM-DD" (inclusive)
        end_date   : ISO-format date string "YYYY-MM-DD" (inclusive)
        """

    @abstractmethod
    def get_option_pricing(
        self,
        ticker: str,
        option_tickers: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Return a DataFrame with columns [ticker, date, side, value].
        ``side`` values are 'px_bid' or 'px_ask'.

        Parameters
        ----------
        ticker         : Underlying ticker, used to locate the pricing file.
        option_tickers : If provided, filter to only these option contract
                         tickers (trailing " Equity" is stripped automatically).
        """

    @abstractmethod
    def get_dividends(self, ticker: str) -> Dict[str, float]:
        """Return a {ex_date_str: dvd_amount} dict for the given ticker."""

    @abstractmethod
    def get_fx_rates(self) -> Dict[str, Dict[str, float]]:
        """Return a {currency: {date_str: rate}} nested dict."""

    @abstractmethod
    def get_holidays(self, calendar: str = "TSX") -> Dict[str, str]:
        """Return a {date_str: holiday_name} dict."""


# ---------------------------------------------------------------------------
# File-based implementation
# ---------------------------------------------------------------------------

class FileDataLoader(DataLoader):
    """
    Loads all pricing data from local CSV files.

    Directory/file layout (all configurable):

        equity_dir/
            <ticker> equity_pricing.csv        date, px_last
        options_dir/
            <ticker>_backtest_format_options.csv   ticker, date, side, value
        dividends_dir/
            <ticker>_dividends.csv             ex_date, dvd_amount
        fx_file                                date, currency, rate
        holidays_dir/
            tsx_holidays.csv                   date, name
            nyse_holidays.csv                  date, name
    """

    EQUITY_SUFFIX = " equity_pricing.csv"
    OPTIONS_SUFFIX = "_backtest_format_options.csv"
    DIVIDENDS_SUFFIX = "_dividends.csv"

    def __init__(
        self,
        equity_dir: str = "",
        options_dir: str = "",
        dividends_dir: str = "",
        fx_file: str = "",
        holidays_dir: str = "",
    ):
        self.equity_dir = equity_dir
        self.options_dir = options_dir
        self.dividends_dir = dividends_dir
        self.fx_file = fx_file
        self.holidays_dir = holidays_dir

    # ------------------------------------------------------------------
    # Equity pricing
    # ------------------------------------------------------------------

    def get_equity_pricing(
        self,
        ticker: str,
        start_date: str = "",
        end_date: str = "",
    ) -> Dict[str, float]:
        path = os.path.join(self.equity_dir, ticker + self.EQUITY_SUFFIX)
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Equity pricing file not found: {path}\n"
                f"Expected: {os.path.abspath(path)}"
            )
        df = pd.read_csv(path)
        # Accept either (date, px_last) or the first two columns
        if "date" not in df.columns:
            df = df.rename(columns={df.columns[0]: "date"})
        if "px_last" not in df.columns:
            price_col = next((c for c in df.columns if c != "date"), None)
            if price_col is None:
                raise ValueError(
                    f"Cannot find a price column in {path}. "
                    f"Found columns: {list(df.columns)}"
                )
            df = df.rename(columns={price_col: "px_last"})

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["px_last"] = pd.to_numeric(df["px_last"], errors="coerce")
        if start_date:
            df = df[df["date"] >= start_date]
        if end_date:
            df = df[df["date"] <= end_date]
        df = df.dropna(subset=["px_last"])
        return dict(zip(df["date"], df["px_last"]))

    # ------------------------------------------------------------------
    # Option pricing
    # ------------------------------------------------------------------

    def get_option_pricing(
        self,
        ticker: str,
        option_tickers: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        path = os.path.join(self.options_dir, ticker + self.OPTIONS_SUFFIX)
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Option pricing file not found: {path}\n"
                f"Expected: {os.path.abspath(path)}"
            )
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Normalise 'field' -> 'side' if needed
        if "side" not in df.columns and "field" in df.columns:
            df = df.rename(columns={"field": "side"})

        if option_tickers is not None:
            # Strip trailing " Equity" from supplied tickers before filtering
            norm = [t.replace(" Equity", "") for t in option_tickers]
            df = df[df["ticker"].isin(norm)].reset_index(drop=True)

        return df

    # ------------------------------------------------------------------
    # Dividends
    # ------------------------------------------------------------------

    def get_dividends(self, ticker: str) -> Dict[str, float]:
        if not self.dividends_dir:
            return {}
        path = os.path.join(self.dividends_dir, ticker + self.DIVIDENDS_SUFFIX)
        if not os.path.exists(path):
            return {}
        df = pd.read_csv(path)
        df["ex_date"] = pd.to_datetime(df["ex_date"]).dt.strftime("%Y-%m-%d")
        df["dvd_amount"] = pd.to_numeric(df["dvd_amount"], errors="coerce").fillna(0)
        return dict(zip(df["ex_date"], df["dvd_amount"]))

    # ------------------------------------------------------------------
    # FX rates
    # ------------------------------------------------------------------

    def get_fx_rates(self) -> Dict[str, Dict[str, float]]:
        if not self.fx_file or not os.path.exists(self.fx_file):
            return {}
        df = pd.read_csv(self.fx_file)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
        result: Dict[str, Dict[str, float]] = {}
        for _, row in df.iterrows():
            ccy = str(row["currency"])
            result.setdefault(ccy, {})[row["date"]] = row["rate"]
        return result

    # ------------------------------------------------------------------
    # Holiday calendars
    # ------------------------------------------------------------------

    def get_holidays(self, calendar: str = "TSX") -> Dict[str, str]:
        filename = f"{calendar.lower()}_holidays.csv"
        path = (
            os.path.join(self.holidays_dir, filename)
            if self.holidays_dir
            else filename
        )
        if not os.path.exists(path):
            return {}
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        name_col = "name" if "name" in df.columns else df.columns[1] if len(df.columns) > 1 else None
        values = df[name_col] if name_col else "Holiday"
        return dict(zip(df["date"], values))


# ---------------------------------------------------------------------------
# Database-backed implementation
# ---------------------------------------------------------------------------

class DatabaseDataLoader(DataLoader):
    """
    Loads pricing data from the internal IM SQL database.

    This is a thin wrapper around the existing common/data_library interface
    and is kept for backward compatibility with the production environment.

    Requires the internal ``im_prod`` (or ``im_dev``) libraries to be on the
    Python path (typically via Z:\\ApolloGX).
    """

    def __init__(self):
        self._common = None
        self._data_library = None
        self._conn = None
        self._init_db()

    def _init_db(self):
        import sys
        # Try the internal im_prod path first, then the local common.py
        for attempt in ["im_prod", "local"]:
            try:
                if attempt == "im_prod":
                    sys.path.append("Z:\\ApolloGX")
                    import im_prod.std_lib.common as _common
                    import im_prod.std_lib.data_library as _dl
                else:
                    import common as _common
                    import data_library as _dl
                self._common = _common
                self._data_library = _dl
                self._conn = _common.db_connection()
                return
            except Exception:
                continue
        raise ImportError(
            "DatabaseDataLoader could not connect to the IM database. "
            "Ensure the im_prod libraries are on the Python path, or use "
            "FileDataLoader for standalone operation."
        )

    # ------------------------------------------------------------------
    # Equity pricing
    # ------------------------------------------------------------------

    def get_equity_pricing(
        self,
        ticker: str,
        start_date: str = "",
        end_date: str = "",
    ) -> Dict[str, float]:
        query = (
            f"SELECT [date] as date, [value] as px_last, source "
            f"FROM [dbo].[market_data] "
            f"WHERE ticker = '{ticker}' AND field = 'px_last'"
        )
        if start_date:
            query += f" AND [date] >= '{start_date}'"
        if end_date:
            query += f" AND [date] <= '{end_date}'"
        df = self._conn.query_tbl(query)
        df["px_last"] = pd.to_numeric(df["px_last"], errors="coerce")

        # Deduplicate by preferred source
        hierarchy = {"bloomberg": 1, "solactive": 2, "mellon": 3}
        df["_rank"] = df["source"].map(hierarchy).fillna(99)
        df = df.sort_values(["date", "_rank"]).drop_duplicates(subset="date")
        return dict(zip(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"), df["px_last"]))

    # ------------------------------------------------------------------
    # Option pricing
    # ------------------------------------------------------------------

    def get_option_pricing(
        self,
        ticker: str,
        option_tickers: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        if option_tickers is not None:
            params = (x.replace(" Equity", "") for x in option_tickers)
            tickers_str = "', '".join(params)
            query = (
                f"SELECT [ticker], [date], [field] as side, [value] "
                f"FROM [dbo].[market_data] "
                f"WHERE ticker IN ('{tickers_str}') "
                f"AND (field = 'px_ask' OR field = 'px_bid')"
            )
        else:
            query = (
                f"SELECT [ticker], [date], [field] as side, [value] "
                f"FROM [dbo].[market_data] "
                f"WHERE ticker LIKE '{ticker} CN ___/__/__ C%' "
                f"AND (field = 'px_ask' OR field = 'px_bid')"
            )
        return self._conn.query_tbl(query)

    # ------------------------------------------------------------------
    # Dividends
    # ------------------------------------------------------------------

    def get_dividends(self, ticker: str) -> Dict[str, float]:
        query = (
            f"SELECT ticker, ex_date, payable_date, dvd_amount "
            f"FROM dividends WHERE ticker='{ticker.upper()}'"
        )
        df = self._conn.query_tbl(query)
        if not df.empty:
            df["ex_date"] = pd.to_datetime(df["ex_date"]).dt.strftime("%Y-%m-%d")
            df["dvd_amount"] = pd.to_numeric(df["dvd_amount"], errors="coerce").fillna(0)
            return dict(zip(df["ex_date"], df["dvd_amount"]))
        return {}

    # ------------------------------------------------------------------
    # FX rates
    # ------------------------------------------------------------------

    def get_fx_rates(self) -> Dict[str, Dict[str, float]]:
        return self._data_library.fx_rates()

    # ------------------------------------------------------------------
    # Holiday calendars
    # ------------------------------------------------------------------

    def get_holidays(self, calendar: str = "TSX") -> Dict[str, str]:
        cal = calendar.upper()
        if cal == "TSX":
            return self._data_library.tsx_holidays()
        if cal in ("NYSE", "US"):
            return self._data_library.nyse_holidays()
        return {}
