"""
data_sources/tmx_fetcher.py
---------------------------
Standalone TMX (Montréal Exchange) option-data downloader.

This module **does not depend on** ``im_prod`` / ``im_dev``.  It uses the
public TMX historical-data endpoint directly via ``requests`` + ``pandas``.

The downloaded data is returned in the same DataFrame format used by the
rest of the backtesting engine (``ticker``, ``date``, ``side``, ``value``)
so it can be dropped in anywhere a ``DataFetcher`` is expected.

Typical workflow
----------------
1. Call :func:`fetch_period` to download a single roll-period window.
2. Optionally filter the result with :func:`select_option_for_roll` to pick
   the closest-OTM contract on each roll date.
3. Save as CSV (``df.to_csv(...)``), then use :class:`CsvDataFetcher` for
   backtesting.

Or use :class:`TmxFetcher` as a ``DataFetcher`` directly — ``get_option_chain``
will download on-demand (cached per session).

Example
-------
    from data_sources.tmx_fetcher import TmxFetcher

    fetcher = TmxFetcher(pct_otm_limit=0.05)
    chain = fetcher.get_option_chain(
        "XIU",
        start_date="2024-01-19",
        end_date="2024-02-16",
    )
    print(chain.head())
"""

from __future__ import annotations

import datetime as dt
import io
import time
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import requests

from .base import DataFetcher

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TMX_URL = (
    "https://www.m-x.ca/en/trading/data/historical"
    "?symbol={symbol}&from={start}&to={end}&dnld=1#quotes"
)
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; OptionBacktest/1.0; "
        "+https://github.com/yeziyang25/optionbt)"
    )
}
_MAX_RETRIES = 3
_RETRY_DELAY_SEC = 2


# ---------------------------------------------------------------------------
# Public DataFetcher implementation
# ---------------------------------------------------------------------------

class TmxFetcher(DataFetcher):
    """
    Download option-chain data from the TMX (Montréal Exchange) website.

    Parameters
    ----------
    call_put : str
        ``"call"`` or ``"put"`` (default: ``"call"``).
    pct_otm_limit : float
        Maximum OTM fraction to retain.  Options deeper OTM than this are
        dropped.  Default: ``0.05`` (5 %).
    max_ttm_days : int
        Maximum days to expiry retained in the output.  Default: ``66``.
    timeout : int
        HTTP request timeout in seconds.  Default: ``30``.
    """

    def __init__(
        self,
        call_put: str = "call",
        pct_otm_limit: float = 0.05,
        max_ttm_days: int = 66,
        timeout: int = 30,
    ) -> None:
        self.call_put = call_put.lower()
        self.pct_otm_limit = pct_otm_limit
        self.max_ttm_days = max_ttm_days
        self.timeout = timeout
        self._cache: dict = {}  # (ticker, start, end) → DataFrame

    # ------------------------------------------------------------------
    # DataFetcher interface
    # ------------------------------------------------------------------

    def get_equity_prices(self, ticker: str, start_date: str, end_date: str) -> dict:
        """
        TMX provides underlying prices embedded in the option-chain download.
        Retrieve and return them as ``{date_str: float}``.
        """
        raw = _download_raw(ticker, start_date, end_date, self.timeout)
        if raw.empty:
            return {}
        underlying = raw[raw["Class Symbol"].isnull()]
        return dict(
            zip(
                pd.to_datetime(underlying["Date"]).dt.strftime("%Y-%m-%d"),
                pd.to_numeric(underlying["Last Price"], errors="coerce"),
            )
        )

    def get_option_chain(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Download and return the option chain for *ticker* between *start_date*
        and *end_date*.

        Returns an empty DataFrame when the download fails or no data exists.
        """
        key = (ticker, start_date, end_date)
        if key in self._cache:
            return self._cache[key]

        raw = _download_raw(ticker, start_date or "", end_date or "", self.timeout)
        if raw.empty:
            return pd.DataFrame(columns=["ticker", "date", "side", "value"])

        start_dt = (
            dt.datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        )
        result = _process_raw(
            raw,
            start_dt=start_dt,
            call_put=self.call_put,
            pct_otm_limit=self.pct_otm_limit,
            max_ttm_days=self.max_ttm_days,
        )
        self._cache[key] = result
        return result


# ---------------------------------------------------------------------------
# Standalone functional API
# ---------------------------------------------------------------------------

def fetch_period(
    ticker: str,
    date_range: Tuple[dt.datetime, dt.datetime],
    call_put: str = "call",
    pct_otm_limit: float = 0.05,
    max_ttm_days: int = 66,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Download and process TMX option data for one roll-period window.

    Parameters
    ----------
    ticker : str
        TMX symbol (e.g. ``"XIU"``, ``"SPY"``).
    date_range : tuple[datetime, datetime]
        ``(start_dt, end_dt)`` of the period.
    call_put : str
        ``"call"`` or ``"put"``.
    pct_otm_limit : float
        Maximum OTM fraction (0.05 = 5 %).
    max_ttm_days : int
        Maximum days-to-expiry retained.
    timeout : int
        HTTP timeout in seconds.

    Returns
    -------
    pd.DataFrame
        Columns: ``ticker``, ``date``, ``side``, ``value``, ``Strike Price``,
        ``Call/Put``, ``pct_otm``.
        Returns an empty DataFrame when the download fails.
    """
    start_dt, end_dt = date_range
    raw = _download_raw(ticker, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"), timeout)
    if raw.empty:
        print(
            f"[TMX] No data for {ticker} "
            f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}"
        )
        return pd.DataFrame()
    return _process_raw(raw, start_dt, call_put, pct_otm_limit, max_ttm_days)


def select_option_for_roll(
    df: pd.DataFrame,
    roll_date: dt.datetime,
    target_expiry: dt.datetime,
    pct_otm_target: float = 0.0,
    strike_mode: str = "abs_closest",
) -> Optional[str]:
    """
    From a processed option-chain DataFrame, select the best contract for a
    given roll date, target expiry, and OTM target.

    Parameters
    ----------
    df : pd.DataFrame
        Output of :func:`fetch_period`.
    roll_date : datetime
        The date on which we are selecting the option.
    target_expiry : datetime
        Desired expiry (usually the next roll date).
    pct_otm_target : float
        Target OTM level (0.0 = ATM, 0.02 = 2 % OTM).
    strike_mode : str
        ``"round_up"``  — nearest strike at or above target OTM.
        ``"round_down"`` — nearest strike at or below target OTM.
        ``"abs_closest"`` — closest strike by absolute OTM distance.

    Returns
    -------
    str or None
        Ticker of the selected option contract, or ``None`` if not found.
    """
    expiry_str = target_expiry.strftime("%m/%d/%y")
    roll_str = roll_date.strftime("%Y-%m-%d")

    df_day = df[
        (pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d") == roll_str)
        & (df["ticker"].str.contains(expiry_str, na=False))
        & (df["side"] == "px_bid")
    ].copy()

    if df_day.empty:
        return None

    if strike_mode == "round_up":
        df_sel = df_day[df_day["pct_otm"] >= pct_otm_target]
        if df_sel.empty:
            df_sel = df_day
        return df_sel.loc[(df_sel["pct_otm"] - pct_otm_target).abs().idxmin(), "ticker"]
    elif strike_mode == "round_down":
        df_sel = df_day[
            (df_day["pct_otm"] <= pct_otm_target) & (df_day["pct_otm"] >= 0)
        ]
        if df_sel.empty:
            df_sel = df_day
        return df_sel.loc[(df_sel["pct_otm"] - pct_otm_target).abs().idxmin(), "ticker"]
    else:  # abs_closest
        df_sel = df_day[df_day["pct_otm"] >= 0]
        if df_sel.empty:
            df_sel = df_day
        return df_sel.loc[(df_sel["pct_otm"] - pct_otm_target).abs().idxmin(), "ticker"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _download_raw(ticker: str, start: str, end: str, timeout: int) -> pd.DataFrame:
    """
    HTTP GET the TMX historical CSV and return a raw DataFrame.
    Retries up to ``_MAX_RETRIES`` times on failure.
    """
    url = _TMX_URL.format(symbol=ticker.lower(), start=start, end=end)
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=timeout)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            if not df.empty:
                return df
            return pd.DataFrame()
        except Exception as exc:
            print(f"[TMX] Attempt {attempt}/{_MAX_RETRIES} failed for {ticker}: {exc}")
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY_SEC)
    return pd.DataFrame()


def _process_raw(
    df: pd.DataFrame,
    start_dt: Optional[dt.datetime],
    call_put: str,
    pct_otm_limit: float,
    max_ttm_days: int,
) -> pd.DataFrame:
    """
    Transform the raw TMX CSV into the standard option-chain format.

    Raw columns (from TMX download):
        Date, Symbol, Class Symbol, Expiry Date, Strike Price,
        Call/Put (0=call, 1=put), Bid Price, Ask Price, Last Price, …
    """
    # Separate underlying prices (rows where Class Symbol is NaN)
    underlying = df[df["Class Symbol"].isnull()]
    underlying_prices = dict(
        zip(underlying["Date"], pd.to_numeric(underlying["Last Price"], errors="coerce"))
    )

    # Keep only option rows
    df = df[~df["Symbol"].isnull()].copy()
    df["Class Symbol"] = df["Class Symbol"].fillna("NA")

    # Filter call / put
    if call_put == "call":
        df = df[df["Call/Put"] == 0].reset_index(drop=True)
    elif call_put == "put":
        df = df[df["Call/Put"] == 1].reset_index(drop=True)
    else:
        raise ValueError(f"call_put must be 'call' or 'put', got '{call_put}'")

    if df.empty:
        return pd.DataFrame(columns=["ticker", "date", "side", "value"])

    # Build standard ticker format: "<CLASS> CN MM/DD/YY C<STRIKE>"
    # Format strike as integer when it has no fractional part (e.g. 30.0 → "30")
    def _fmt_strike(s):
        return str(int(s)) if s == int(s) else str(round(s, 2))

    df["_strike_str"] = df["Strike Price"].apply(_fmt_strike)
    df["ticker"] = (
        df["Class Symbol"].astype(str)
        + " CN "
        + pd.to_datetime(df["Expiry Date"]).dt.strftime("%m/%d/%y")
        + " "
        + np.where(df["Call/Put"] == 1, "P", "C")
        + df["_strike_str"]
    )
    df = df.drop(columns=["_strike_str"])

    # Melt bid/ask into long form
    df_long = pd.melt(
        df,
        id_vars=["ticker", "Date", "Strike Price", "Call/Put"],
        value_vars=["Bid Price", "Ask Price"],
    )
    df_long["side"] = np.where(df_long["variable"] == "Bid Price", "px_bid", "px_ask")
    df_long = df_long[["ticker", "Date", "side", "value", "Strike Price", "Call/Put"]].rename(
        columns={"Date": "date"}
    )

    # Attach underlying price
    df_long["underlying_price"] = df_long["date"].map(underlying_prices)

    # OTM filter
    df_long["pct_otm"] = np.where(
        df_long["Call/Put"] == 1,
        df_long["underlying_price"] / df_long["Strike Price"] - 1,
        df_long["Strike Price"] / df_long["underlying_price"] - 1,
    )

    if start_dt is not None:
        df_start = df_long[
            pd.to_datetime(df_long["date"]).dt.strftime("%Y-%m-%d")
            == start_dt.strftime("%Y-%m-%d")
        ]
        df_start = df_start[
            (df_start["pct_otm"] <= pct_otm_limit) & (df_start["pct_otm"] >= -0.005)
        ]
        option_universe = df_start["ticker"].unique().tolist()
        if not option_universe:
            return pd.DataFrame(columns=["ticker", "date", "side", "value"])
        df_long = df_long[df_long["ticker"].isin(option_universe)]

    # TTM filter
    df_long["expiration_date"] = pd.to_datetime(
        df_long["ticker"].str.split().str[-2], format="%m/%d/%y"
    )
    df_long["TTM"] = (df_long["expiration_date"] - pd.to_datetime(df_long["date"])).dt.days
    df_long = df_long[(df_long["TTM"] < max_ttm_days) & (df_long["value"] != 0)]

    df_long["date"] = pd.to_datetime(df_long["date"]).dt.strftime("%Y-%m-%d")
    return df_long.reset_index(drop=True)
