"""Common utilities ported from the legacy ``im_prod.std_lib.common`` module.

This module re-implements the key helpers that the old backtesting workflow
relied on (``extract_option_ticker``, ``db_connection`` protocol) so that
both legacy and new code paths can share the same logic without requiring
the proprietary ``im_prod`` / ``im_dev`` packages.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd
import re


# ---------------------------------------------------------------------------
# Option ticker parsing (mirrors common.extract_option_ticker)
# ---------------------------------------------------------------------------

class OptionTickerInfo:
    """Parsed components of a set of option tickers.

    Instances are returned by :func:`extract_option_ticker` and expose
    dictionaries that map each raw ticker string to its parsed fields –
    exactly the same interface the legacy ``common.extract_option_ticker``
    provided::

        info = extract_option_ticker(df, "ticker")
        df["expiry"]  = df["ticker"].map(info.expiry)
        df["strike"]  = df["ticker"].map(info.strike)
    """

    def __init__(self):
        self.expiry: Dict[str, date] = {}
        self.strike: Dict[str, float] = {}
        self.option_type: Dict[str, str] = {}
        self.underlying_ticker: Dict[str, str] = {}


def extract_option_ticker(
    df: pd.DataFrame,
    ticker_col: str = "ticker",
) -> OptionTickerInfo:
    """Parse option tickers in a DataFrame into structured components.

    This is a drop-in replacement for ``common.extract_option_ticker(df, col)``
    from the legacy ``im_prod.std_lib.common`` library.

    Supported ticker formats:

    * ``"UNDERLYING EXCHANGE MM/DD/YY CSTRIKE"``
      e.g. ``"SPY CN 01/17/20 C325"``
    * ``"UNDERLYING EXCHANGE MM/DD/YY PSTRIKE"``
      e.g. ``"XIU CN 02/21/25 P30.5"``

    Args:
        df: DataFrame containing option tickers.
        ticker_col: Name of the column holding the tickers.

    Returns:
        An :class:`OptionTickerInfo` with ``expiry``, ``strike``,
        ``option_type``, and ``underlying_ticker`` dictionaries.
    """
    info = OptionTickerInfo()

    # Pattern: UNDERLYING EXCHANGE MM/DD/YY {C|P}STRIKE
    pattern = re.compile(
        r"^(.+?)\s+(\S+)\s+(\d{2}/\d{2}/\d{2})\s+([CP])(.+)$"
    )

    for ticker in df[ticker_col].dropna().unique():
        m = pattern.match(str(ticker))
        if m:
            underlying = m.group(1)
            # exchange = m.group(2)  # not stored but parsed
            expiry_str = m.group(3)
            opt_type = m.group(4)
            strike_str = m.group(5)

            info.underlying_ticker[ticker] = underlying
            info.expiry[ticker] = datetime.strptime(expiry_str, "%m/%d/%y").date()
            info.option_type[ticker] = opt_type
            try:
                info.strike[ticker] = float(strike_str)
            except ValueError:
                import warnings
                warnings.warn(
                    f"Could not parse strike '{strike_str}' from ticker '{ticker}', defaulting to 0.0"
                )
                info.strike[ticker] = 0.0
        # If the format doesn't match, silently skip – the caller can
        # handle missing keys.

    return info


# ---------------------------------------------------------------------------
# SQL sanitisation helper
# ---------------------------------------------------------------------------

def _sanitise_sql_param(value: str) -> str:
    """Escape single quotes in a SQL parameter to prevent injection."""
    return str(value).replace("'", "''")


# ---------------------------------------------------------------------------
# Database connection protocol
# ---------------------------------------------------------------------------

class DbConnection(ABC):
    """Abstract database connection matching the legacy ``common.db_connection()`` API.

    Concrete implementations just need to provide :meth:`query_tbl` (and
    optionally :meth:`insert_tbl`).  This allows the new framework's
    :class:`~optionbt.data.loader.DatabaseDataProvider` and
    :class:`~optionbt.utils.dates.HolidayCalendar` to work with *any*
    database back-end without importing ``im_prod.std_lib.common``.

    If the proprietary library *is* available you can pass its connection
    object directly – any object with a ``query_tbl`` method is accepted.
    """

    @abstractmethod
    def query_tbl(self, sql: str) -> pd.DataFrame:
        """Execute *sql* and return the result as a DataFrame."""
        ...

    def insert_tbl(self, df: pd.DataFrame) -> None:
        """Insert *df* into the database (optional)."""
        raise NotImplementedError

    def list_to_sql_str(
        self, items: List[str], convert_elements: bool = False,
    ) -> str:
        """Convert a Python list to a SQL ``IN (…)`` clause string."""
        if convert_elements:
            items = [str(i) for i in items]
        quoted = ", ".join(f"'{_sanitise_sql_param(i)}'" for i in items)
        return f"({quoted})"
