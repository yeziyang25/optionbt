"""
data_sources/base.py
--------------------
Abstract base class for all data fetchers.

Concrete implementations:
  - CsvDataFetcher  (data_sources/csv_loader.py)  — file-based, no DB required
  - TmxFetcher      (data_sources/tmx_fetcher.py) — live TMX web download
  - DbDataFetcher   (data_sources/db_fetcher.py)  — internal SQL database (im_prod)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class DataFetcher(ABC):
    """
    Common interface for all market-data backends.

    Method names match what ``helper_functions/securities.py`` calls on the
    ``data_loader`` parameter so that any implementation is a drop-in
    replacement.
    """

    # ------------------------------------------------------------------
    # Required
    # ------------------------------------------------------------------

    @abstractmethod
    def get_equity_pricing(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        """
        Return daily closing prices for *ticker* between *start_date* and
        *end_date* (both inclusive, ``YYYY-MM-DD``).

        Returns
        -------
        dict
            Mapping ``date_str → float``.
        """

    @abstractmethod
    def get_option_pricing(
        self,
        ticker: str,
        opt_tickers: Optional[list] = None,
    ) -> pd.DataFrame:
        """
        Return the option bid/ask chain for *ticker*.

        Parameters
        ----------
        ticker : str
            Underlying name used to look up the option chain.
        opt_tickers : list, optional
            When provided, return data only for this explicit list of
            option contract tickers (used for custom option lists).

        Returns
        -------
        pd.DataFrame
            Columns: ``ticker``, ``date``, ``side`` (``px_bid`` / ``px_ask``),
            ``value`` (float).
        """

    # ------------------------------------------------------------------
    # Optional (default implementations return empty structures)
    # ------------------------------------------------------------------

    def get_dividends(self, ticker: str) -> dict:
        """
        Return ex-dividend schedule for *ticker*.

        Returns
        -------
        dict
            Mapping ``ex_date_str (YYYY-MM-DD) → dividend_amount (float)``.
        """
        return {}

    def get_holidays(self, calendar: str = "TSX") -> dict:
        """
        Return non-trading days for *calendar* (``"TSX"`` or ``"NYSE"``).

        Returns
        -------
        dict
            Mapping ``date_str (YYYY-MM-DD) → holiday_name (str)``.
        """
        return {}
