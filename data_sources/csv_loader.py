"""
data_sources/csv_loader.py
--------------------------
File-based DataFetcher implementation.

Loads equity prices, option chains, and dividends from local CSV files so
that the backtesting engine can run **without** a database connection.

Expected file layouts
---------------------
Equity pricing CSV (equity_file or ``{equity_dir}/{ticker} equity_pricing.csv``):
    date,px_last
    2020-01-02,329.11
    ...

Option chain CSV (options_file or ``{options_dir}/{ticker}_backtest_format_options.csv``):
    ticker,date,side,value
    "SPY US 01/17/25 C570",2024-12-20,px_bid,12.50
    "SPY US 01/17/25 C570",2024-12-20,px_ask,12.80
    ...

Dividend CSV (dividend_file or ``{dividends_dir}/{ticker}_dividends.csv``) — optional:
    ex_date,dvd_amount
    2020-03-20,1.25
    ...

Usage
-----
    from data_sources.csv_loader import CsvDataFetcher

    # Directory-based: file names are derived from the ticker
    fetcher = CsvDataFetcher(
        equity_dir="data/equity",
        options_dir="data/options",
        dividends_dir="data/dividends",   # optional
    )

    # File-based: supply explicit paths for a specific security
    fetcher = CsvDataFetcher(
        equity_file="data/equity/SPY US equity_pricing.csv",
        options_file="data/options/SPY US_backtest_format_options.csv",
        dividend_file="data/dividends/SPY US_dividends.csv",
    )
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from .base import DataFetcher


class CsvDataFetcher(DataFetcher):
    """
    Load market data from local CSV files.

    Parameters
    ----------
    equity_dir : str, optional
        Directory containing ``<ticker> equity_pricing.csv`` files.
    options_dir : str, optional
        Directory containing ``<ticker>_backtest_format_options.csv`` files.
    dividends_dir : str, optional
        Directory containing ``<ticker>_dividends.csv`` files.
    equity_file : str, optional
        Explicit path to a single equity pricing file (overrides directory).
    options_file : str, optional
        Explicit path to a single option-chain file (overrides directory).
    dividend_file : str, optional
        Explicit path to a single dividend file (overrides directory).
    """

    def __init__(
        self,
        equity_dir: Optional[str] = None,
        options_dir: Optional[str] = None,
        dividends_dir: Optional[str] = None,
        equity_file: Optional[str] = None,
        options_file: Optional[str] = None,
        dividend_file: Optional[str] = None,
    ) -> None:
        self._equity_dir = equity_dir
        self._options_dir = options_dir
        self._dividends_dir = dividends_dir
        self._equity_file = equity_file
        self._options_file = options_file
        self._dividend_file = dividend_file

    # ------------------------------------------------------------------
    # DataFetcher interface
    # ------------------------------------------------------------------

    def get_equity_pricing(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        """
        Load closing prices from CSV and return ``{date_str: float}``.

        Filters to [start_date, end_date] when both are provided.
        """
        path = self._resolve_equity_path(ticker)
        df = pd.read_csv(path)
        df = _normalise_equity_df(df)
        df = _filter_date_range(df, "date", start_date, end_date)
        return dict(zip(df["date"], df["px_last"].astype(float)))

    def get_option_pricing(
        self,
        ticker: str,
        opt_tickers: Optional[list] = None,
    ) -> pd.DataFrame:
        """
        Load option bid/ask chain from CSV.

        When *opt_tickers* is provided, only rows whose ``ticker`` column is
        in that list are returned (used with custom option lists).

        Returns a DataFrame with columns: ``ticker``, ``date``, ``side``,
        ``value``.
        """
        path = self._resolve_options_path(ticker)
        df = pd.read_csv(path)
        df = _normalise_options_df(df)
        if opt_tickers:
            # Strip Bloomberg " Equity" suffix for matching
            clean = [t.replace(" Equity", "") for t in opt_tickers]
            df = df[df["ticker"].isin(clean)]
        return df.reset_index(drop=True)

    def get_dividends(self, ticker: str) -> dict:
        """
        Load dividend schedule from CSV and return ``{ex_date_str: amount}``.
        Returns empty dict when no dividend file is available.
        """
        try:
            path = self._resolve_dividends_path(ticker)
        except FileNotFoundError:
            return {}
        df = pd.read_csv(path)
        df = _normalise_dividends_df(df)
        return dict(
            zip(
                pd.to_datetime(df["ex_date"]).dt.strftime("%Y-%m-%d"),
                df["dvd_amount"].astype(float),
            )
        )

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def _resolve_equity_path(self, ticker: str) -> str:
        if self._equity_file:
            path = self._equity_file
        elif self._equity_dir:
            path = os.path.join(self._equity_dir, f"{ticker} equity_pricing.csv")
        else:
            raise FileNotFoundError(
                f"CsvDataFetcher: no equity_dir or equity_file configured "
                f"(cannot load prices for '{ticker}')."
            )
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"CsvDataFetcher: equity pricing file not found: {path}"
            )
        return path

    def _resolve_options_path(self, ticker: str) -> str:
        if self._options_file:
            path = self._options_file
        elif self._options_dir:
            path = os.path.join(
                self._options_dir, f"{ticker}_backtest_format_options.csv"
            )
        else:
            raise FileNotFoundError(
                f"CsvDataFetcher: no options_dir or options_file configured "
                f"(cannot load option chain for '{ticker}')."
            )
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"CsvDataFetcher: option chain file not found: {path}"
            )
        return path

    def _resolve_dividends_path(self, ticker: str) -> str:
        if self._dividend_file:
            path = self._dividend_file
        elif self._dividends_dir:
            path = os.path.join(self._dividends_dir, f"{ticker}_dividends.csv")
        else:
            raise FileNotFoundError("no dividends_dir or dividend_file configured")
        if not os.path.exists(path):
            raise FileNotFoundError(f"dividend file not found: {path}")
        return path


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _normalise_equity_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accept equity CSVs with varied column names and normalise to
    ``date`` + ``px_last``.
    """
    df = df.copy()
    # Accept 'Date', 'date', 'DATE'
    col_map = {c.lower(): c for c in df.columns}
    if "date" in col_map:
        df = df.rename(columns={col_map["date"]: "date"})
    # Accept 'px_last', 'px_Last', 'close', 'PX_LAST', 'value'
    for candidate in ("px_last", "px_Last", "PX_LAST", "close", "Close", "value"):
        if candidate in df.columns:
            df = df.rename(columns={candidate: "px_last"})
            break
    if "date" not in df.columns or "px_last" not in df.columns:
        raise ValueError(
            f"Equity CSV must have 'date' and price columns; "
            f"found: {df.columns.tolist()}"
        )
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df[["date", "px_last"]]


def _normalise_options_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accept option-chain CSVs with varied column names and normalise to
    ``ticker``, ``date``, ``side``, ``value``.

    Also handles the Bloomberg-style format where bid/ask are separate columns
    (``px_bid``, ``px_ask``) and produces a melted long-form DataFrame.
    """
    df = df.copy()
    col_lower = {c.lower(): c for c in df.columns}

    # Rename canonical columns
    for want, candidates in {
        "ticker": ["ticker", "Ticker"],
        "date":   ["date", "Date", "DATE"],
        "side":   ["side", "Side", "field", "Field"],
        "value":  ["value", "Value", "price", "Price"],
    }.items():
        for cand in candidates:
            if cand in df.columns and want not in df.columns:
                df = df.rename(columns={cand: want})

    # If side/value are missing but px_bid/px_ask columns exist → melt
    if "side" not in df.columns or "value" not in df.columns:
        bid_col = next((c for c in df.columns if c.lower() in ("px_bid", "bid", "bid_price")), None)
        ask_col = next((c for c in df.columns if c.lower() in ("px_ask", "ask", "ask_price")), None)
        if bid_col and ask_col:
            id_vars = [c for c in df.columns if c not in (bid_col, ask_col)]
            df = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=[bid_col, ask_col],
                var_name="side",
                value_name="value",
            )
            df["side"] = df["side"].map({bid_col: "px_bid", ask_col: "px_ask"})

    required = {"ticker", "date", "side", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Option-chain CSV is missing columns: {missing}. "
            f"Found: {df.columns.tolist()}"
        )

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df[["ticker", "date", "side", "value"]]


def _normalise_dividends_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for want, candidates in {
        "ex_date": ["ex_date", "Ex_Date", "exdate", "date", "Date"],
        "dvd_amount": ["dvd_amount", "dividend", "Dividend", "amount", "Amount", "value"],
    }.items():
        for cand in candidates:
            if cand in df.columns and want not in df.columns:
                df = df.rename(columns={cand: want})
    return df


def _filter_date_range(
    df: pd.DataFrame,
    date_col: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> pd.DataFrame:
    if start_date:
        df = df[df[date_col] >= start_date]
    if end_date:
        df = df[df[date_col] <= end_date]
    return df
