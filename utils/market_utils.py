"""
utils/market_utils.py
=====================
Standalone market-calendar and option-ticker utilities.

These provide self-contained implementations of the helper functions that the
existing backtesting code normally imports from the internal IM library
(``common.workday``, ``common.week_count``, ``common.extract_option_ticker``).

They are used automatically as fallbacks when the internal library is not
available, and are the primary implementation for the standalone / CSV path.

Public API
----------
workday(date, n, holidays)        — advance n business days
week_count(start_date, n_years)   — date -> week-of-month mapping
OptionTickerInfo(df, col)         — parse option ticker fields
load_holidays_from_csv(path)      — load {date_str: name} dict from CSV
total_return_calc(df, price, dvd) — compute total-return price series
"""

import calendar as _calendar
import datetime as dt
from typing import Dict, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Business-day arithmetic
# ---------------------------------------------------------------------------

def workday(date, n: int, holidays: Dict[str, str] = {}) -> dt.datetime:
    """
    Advance *n* business days from *date*, skipping weekends and any dates
    present in *holidays* (keys are "YYYY-MM-DD" strings, values are ignored).

    Parameters
    ----------
    date     : datetime or date
    n        : number of business days to advance; negative goes backward
    holidays : {date_str: any} mapping of non-trading days

    Returns
    -------
    datetime (same type as *date*)
    """
    if isinstance(date, dt.date) and not isinstance(date, dt.datetime):
        date = dt.datetime(date.year, date.month, date.day)

    step = 1 if n >= 0 else -1
    remaining = abs(n)
    d = date
    while remaining > 0:
        d += dt.timedelta(days=step)
        if d.weekday() < 5 and holidays.get(d.strftime("%Y-%m-%d")) is None:
            remaining -= 1
    return d


# ---------------------------------------------------------------------------
# Week-of-month mapping  (mirrors common.week_count)
# ---------------------------------------------------------------------------

def week_count(start_date: dt.datetime, years: int = 2) -> Dict[str, int]:
    """
    Return a dict mapping every calendar date in the range to the week-of-month
    number (1 = first week, 2 = second week, …).

    The first day of each week is **Sunday** (matching the internal library).
    This is used by :func:`helper_functions.rebalance_dates.option_dates` to
    identify third-Friday option expiry dates.

    Parameters
    ----------
    start_date : starting datetime (only year is used; computation begins Jan 1)
    years      : number of years to cover (start_year … start_year + years)

    Returns
    -------
    dict  {date_str: week_number}
    """
    result: Dict[str, int] = {}
    _calendar.setfirstweekday(_calendar.SUNDAY)
    for iy in range(years):
        for month in range(1, 13):
            month_cal = _calendar.monthcalendar(start_date.year + iy, month)
            week_num = 1
            for week in month_cal:
                if week[5] != 0:  # Friday slot non-zero => this week has a Friday
                    for day in week:
                        if day > 0:
                            key = dt.datetime(
                                start_date.year + iy, month, day
                            ).strftime("%Y-%m-%d")
                            result[key] = week_num
                    week_num += 1
    return result


# ---------------------------------------------------------------------------
# Option ticker parsing  (mirrors common.extract_option_ticker)
# ---------------------------------------------------------------------------

class OptionTickerInfo:
    """
    Parse option ticker strings of the form::

        "<UNDERLYING> <EXCHANGE> <MM/DD/YY> <C|P><STRIKE>"

    Examples::

        "XIU CN 03/15/24 C30.5"
        "SPY US 01/17/25 C450"
        "BTCC/B CN 06/20/25 P5.5"

    Attributes
    ----------
    expiry : dict  {ticker: date}
    strike : dict  {ticker: float}
    option_type : dict  {ticker: 'call' | 'put'}
    underlying_ticker : dict  {ticker: underlying_ticker_str}
    currency : dict  {ticker: 'CAD' | 'USD'}

    Notes
    -----
    An ``_UNDERLYING_OVERRIDE`` map handles tickers where the option root
    differs from the equity ticker (e.g. "BTCC CN" options trade against
    "BTCC/B CN").
    """

    _UNDERLYING_OVERRIDE = {
        "BTCC CN": "BTCC/B CN",
        "RCI CN": "RCI/B CN",
        "SPX US": "SPX Index",
        "SPXW US": "SPX Index",
        "NDX US": "NDX Index",
        "NDXP US": "NDX Index",
    }

    def __init__(self, df: pd.DataFrame, col: str):
        self.expiry: Dict[str, dt.date] = {}
        self.strike: Dict[str, float] = {}
        self.option_type: Dict[str, str] = {}
        self.underlying_ticker: Dict[str, str] = {}
        self.currency: Dict[str, str] = {}

        if df.empty:
            return

        for ticker in df[col].dropna().unique():
            try:
                parts = str(ticker).split(" ")
                # parts[0] = root (may have leading digit for adjusted tickers)
                # parts[1] = exchange (CN / US / etc.)
                # parts[2] = expiry MM/DD/YY
                # parts[3] = C<strike> or P<strike>
                expiry = dt.datetime.strptime(parts[2], "%m/%d/%y").date()
                opt_char = parts[3][0].upper()
                strike = float(parts[3][1:])

                # Derive underlying ticker (strip leading digit for adjustments)
                root = parts[0][1:] if parts[0][0].isdigit() else parts[0]
                raw_underlying = f"{root} {parts[1]}"
                underlying = self._UNDERLYING_OVERRIDE.get(raw_underlying, raw_underlying)
                # Fix known TRP adjustment
                underlying = underlying.replace("TRP1 CN", "TRP CN")

                self.expiry[ticker] = expiry
                self.strike[ticker] = strike
                self.option_type[ticker] = "call" if opt_char == "C" else "put"
                self.underlying_ticker[ticker] = underlying
                self.currency[ticker] = "CAD" if " CN " in ticker else "USD"
            except (IndexError, ValueError):
                # Skip malformed tickers silently
                pass


# ---------------------------------------------------------------------------
# Holiday calendar loader
# ---------------------------------------------------------------------------

def load_holidays_from_csv(filepath: str) -> Dict[str, str]:
    """
    Load a holiday calendar from a CSV file.

    The file must have at minimum a ``date`` column.  An optional ``name``
    column will be used as the value; otherwise the value is ``"Holiday"``.

    Parameters
    ----------
    filepath : str
        Path to the CSV file.

    Returns
    -------
    dict  {date_str "YYYY-MM-DD": holiday_name}
    """
    df = pd.read_csv(filepath)
    if "date" not in df.columns:
        raise ValueError(f"Holiday CSV must contain a 'date' column: {filepath}")
    date_strs = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    names = df["name"] if "name" in df.columns else "Holiday"
    return dict(zip(date_strs, names))


# ---------------------------------------------------------------------------
# Total-return price calculation  (mirrors common.total_return_calc)
# ---------------------------------------------------------------------------

def total_return_calc(
    input_data: pd.DataFrame,
    price_col: str,
    dvd_col: str,
) -> pd.DataFrame:
    """
    Calculate a total-return price series from a raw price and dividend
    schedule.  Dividends are assumed to be reinvested on the ex-date.

    Parameters
    ----------
    input_data : pd.DataFrame
        Must contain columns named *price_col* and *dvd_col*, sorted by date
        ascending.
    price_col : str
        Name of the column containing end-of-day prices.
    dvd_col : str
        Name of the column containing dividend amounts (empty string / NaN
        means no dividend on that date).

    Returns
    -------
    pd.DataFrame  — copy of *input_data* with an added ``total_return_price``
    column and a ``dvd_reinvestment`` column.
    """
    data = input_data.copy(deep=True).reset_index(drop=True)
    data[dvd_col] = data[dvd_col].fillna(0)
    data[dvd_col] = np.where(data[dvd_col] == "", 0, data[dvd_col])
    data[dvd_col] = pd.to_numeric(data[dvd_col], errors="coerce").fillna(0)
    data["dvd_reinvestment"] = 0.0

    for i in data.index:
        if i == 0:
            if float(data.at[i, dvd_col]) > 0:
                raise ValueError(
                    "There cannot be a dividend on the first day; "
                    "total-return reinvestment cannot be calculated."
                )
            data.at[i, "dvd_reinvestment"] = 0.0
        else:
            px_prev = float(data.at[i - 1, price_col])
            dvd_today = float(data.at[i, dvd_col])
            reinv_prev = float(data.at[i - 1, "dvd_reinvestment"])
            if dvd_today > 0 and abs(px_prev - dvd_today) > 1e-9:
                daily_return = float(data.at[i, price_col]) / (px_prev - dvd_today)
                data.at[i, "dvd_reinvestment"] = (reinv_prev + dvd_today) * daily_return
            elif px_prev != 0:
                daily_return = float(data.at[i, price_col]) / px_prev
                data.at[i, "dvd_reinvestment"] = reinv_prev * daily_return
            else:
                data.at[i, "dvd_reinvestment"] = reinv_prev

    data["total_return_price"] = (
        data["dvd_reinvestment"] + data[price_col].astype(float)
    )
    return data
