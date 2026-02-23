"""
data_sources/db_fetcher.py
--------------------------
Database-backed DataFetcher that wraps the existing ``im_prod`` / ``im_dev``
SQL infrastructure.

This class is a thin adapter: it delegates to the same SQL queries already
used in ``helper_functions/securities.py`` but exposes them through the
standard :class:`DataFetcher` interface so they can be swapped out for
:class:`CsvDataFetcher` or :class:`TmxFetcher` without changing the engine.

Requires ``im_prod`` (or ``im_dev``) to be importable.  If the package is
not available an ``ImportError`` is raised at construction time with a
helpful message.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

import pandas as pd

from .base import DataFetcher


class DbDataFetcher(DataFetcher):
    """
    Fetch market data from the internal IM SQL database.

    Parameters
    ----------
    sec_type : str
        Security type string used to build the equity-price query
        (e.g. ``"equity"`` or ``"call option"``).
    """

    def __init__(self, sec_type: str = "equity") -> None:
        self._sec_type = sec_type
        self._conn = self._connect()

    # ------------------------------------------------------------------
    # DataFetcher interface
    # ------------------------------------------------------------------

    def get_equity_pricing(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        query = (
            f"SELECT [date] as date, [value] as px_last, source "
            f"FROM [dbo].[market_data] "
            f"WHERE ticker = '{ticker}' AND field = 'px_last' "
            f"AND [date] >= '{start_date}' AND [date] <= '{end_date}';"
        )
        df = self._conn.query_tbl(query)
        if df.empty:
            return {}
        df["px_last"] = df["px_last"].astype(float)

        # Prefer bloomberg > solactive > mellon when multiple sources exist
        hierarchy = {"bloomberg": 1, "solactive": 2, "mellon": 3}
        df["ranking"] = df["source"].map(hierarchy)
        df = df.sort_values(["date", "ranking"]).drop_duplicates(subset="date")
        return dict(
            zip(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"), df["px_last"])
        )

    def get_option_pricing(
        self,
        ticker: str,
        opt_tickers: Optional[list] = None,
    ) -> pd.DataFrame:
        if opt_tickers:
            params = (x.replace(" Equity", "") for x in opt_tickers)
            tickers = "', '".join(params)
            query = (
                f"SELECT [ticker], [date], [field] as side, [value] "
                f"FROM [dbo].[market_data] "
                f"WHERE ticker IN ('{tickers}') "
                f"AND (field = 'px_ask' OR field = 'px_bid')"
            )
        else:
            query = (
                f"SELECT [ticker], [date], [field] as side, [value] "
                f"FROM [dbo].[market_data] "
                f"WHERE ticker LIKE '{ticker + ' CN'}___/__/__ C%' "
                f"AND (field = 'px_ask' OR field = 'px_bid')"
            )
        return self._conn.query_tbl(query)

    def get_dividends(self, ticker: str) -> dict:
        query = (
            f"SELECT ticker, ex_date, payable_date, dvd_amount "
            f"FROM dividends WHERE ticker='{ticker.upper()}';"
        )
        df = self._conn.query_tbl(query)
        if df.empty:
            return {}
        df["dvd_amount"] = df["dvd_amount"].astype(float)
        return dict(
            zip(
                pd.to_datetime(df["ex_date"]).dt.strftime("%Y-%m-%d"),
                df["dvd_amount"],
            )
        )

    def get_holidays(self, calendar: str = "TSX") -> dict:
        import importlib
        dl = self._get_data_library()
        if calendar.upper() == "NYSE":
            return dl.nyse_holidays()
        return dl.tsx_holidays()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _connect():
        """Import im_prod (or im_dev) and return a db_connection object."""
        sys.path.append("Z:\\ApolloGX")
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        if "\\im_dev\\" in cur_dir:
            try:
                import im_dev.std_lib.common as common
                return common.db_connection()
            except ImportError:
                pass
        try:
            import im_prod.std_lib.common as common
            return common.db_connection()
        except ImportError as exc:
            raise ImportError(
                "DbDataFetcher requires 'im_prod' (or 'im_dev') to be installed. "
                "Use CsvDataFetcher for file-based backtesting without a DB connection."
            ) from exc

    @staticmethod
    def _get_data_library():
        sys.path.append("Z:\\ApolloGX")
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        if "\\im_dev\\" in cur_dir:
            try:
                import im_dev.std_lib.data_library as dl
                return dl
            except ImportError:
                pass
        import im_prod.std_lib.data_library as dl
        return dl
