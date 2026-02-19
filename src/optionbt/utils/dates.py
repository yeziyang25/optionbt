"""Date utility functions for backtesting.

Provides business day calculations, holiday-aware scheduling, and option
expiration date generation.  The helpers here bridge the gap between the
legacy ``common.workday`` / ``common.week_count`` / ``data_library.*_holidays``
utilities and the new modular framework so that both CSV-based and
database-backed workflows can share the same date logic.
"""

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Union


# ---------------------------------------------------------------------------
# Holiday calendar
# ---------------------------------------------------------------------------

class HolidayCalendar:
    """Manage exchange holidays from multiple sources.

    The legacy code stores holidays as ``Dict[str, int]`` (date-string → 1).
    The new ``BacktestEngine`` expects ``List[date]``.  ``HolidayCalendar``
    accepts *both* formats and exposes helpers that work with either
    convention, making it easy to plug data from ``data_library.tsx_holidays()``
    or a plain CSV file into the new framework.

    Parameters
    ----------
    holidays : dict, list, or set, optional
        Initial holidays.  Accepts:
        * ``Dict[str, int]`` – legacy format (``{"2024-01-01": 1, …}``)
        * ``List[date]`` or ``Set[date]`` – new framework format
    """

    def __init__(
        self,
        holidays: Optional[Union[Dict[str, int], List[date], Set[date]]] = None,
    ):
        self._dates: Set[date] = set()
        if holidays is not None:
            self.add(holidays)

    # -- mutators -----------------------------------------------------------

    def add(
        self,
        holidays: Union[Dict[str, int], List[date], Set[date], date],
    ) -> None:
        """Add holidays (accepts legacy dict, list/set of dates, or a single date)."""
        if isinstance(holidays, dict):
            for key in holidays:
                self._dates.add(self._parse_date(key))
        elif isinstance(holidays, (list, set)):
            for item in holidays:
                if isinstance(item, date):
                    self._dates.add(item)
                else:
                    self._dates.add(self._parse_date(item))
        elif isinstance(holidays, date):
            self._dates.add(holidays)

    # -- queries ------------------------------------------------------------

    def is_holiday(self, d: date) -> bool:
        return d in self._dates

    def is_business_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self._dates

    # -- export helpers (bridge old ↔ new) ----------------------------------

    def to_list(self) -> List[date]:
        """Return holidays as ``List[date]`` (new framework convention)."""
        return sorted(self._dates)

    def to_dict(self) -> Dict[str, int]:
        """Return holidays as ``Dict[str, int]`` (legacy convention)."""
        return {d.strftime("%Y-%m-%d"): 1 for d in sorted(self._dates)}

    # -- factory methods ----------------------------------------------------

    @classmethod
    def from_csv(cls, filepath: str, date_column: str = "date") -> "HolidayCalendar":
        """Load holidays from a CSV file.

        The CSV must contain at least a date column.  Each row is treated as
        a holiday.  This mirrors the pattern used by
        ``data_library.tsx_holidays()`` but reads from a local file instead
        of a database.
        """
        import pandas as pd

        df = pd.read_csv(filepath)
        dates = pd.to_datetime(df[date_column]).dt.date.tolist()
        return cls(holidays=dates)

    @classmethod
    def from_db(cls, db_conn, holiday_type: str) -> "HolidayCalendar":
        """Load holidays from a database connection.

        This mirrors the legacy ``data_library.tsx_holidays()`` /
        ``nyse_holidays()`` pattern::

            conn = common.db_connection()
            cal = HolidayCalendar.from_db(conn, "tsx trading")

        Parameters
        ----------
        db_conn :
            Any object exposing a ``query_tbl(sql)`` method that returns a
            ``pandas.DataFrame`` (compatible with ``common.db_connection()``).
        holiday_type : str
            Value to filter on, e.g. ``"tsx trading"``, ``"nyse trading"``.
        """
        import pandas as pd

        query = f"SELECT * FROM holidays WHERE [holiday_type] = '{holiday_type}'"
        df = db_conn.query_tbl(query)
        dates = pd.to_datetime(df["date"]).dt.date.tolist()
        return cls(holidays=dates)

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _parse_date(val) -> date:
        if isinstance(val, date) and not isinstance(val, datetime):
            return val
        if isinstance(val, datetime):
            return val.date()
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()

    def __contains__(self, item: date) -> bool:
        return item in self._dates

    def __len__(self) -> int:
        return len(self._dates)

    def __repr__(self) -> str:
        return f"HolidayCalendar({len(self._dates)} holidays)"


# ---------------------------------------------------------------------------
# Normalise holidays argument
# ---------------------------------------------------------------------------

def _normalise_holidays(holidays) -> Set[date]:
    """Convert various holiday representations to ``Set[date]``.

    Accepts ``None``, ``HolidayCalendar``, ``List[date]``, ``Dict[str,int]``.
    """
    if holidays is None:
        return set()
    if isinstance(holidays, HolidayCalendar):
        return set(holidays.to_list())
    if isinstance(holidays, dict):
        return {HolidayCalendar._parse_date(k) for k in holidays}
    if isinstance(holidays, (list, set)):
        return {d if isinstance(d, date) else HolidayCalendar._parse_date(d) for d in holidays}
    return set()


# ---------------------------------------------------------------------------
# Business day helpers (mirrors common.workday)
# ---------------------------------------------------------------------------

def get_business_days(
    start_date: date,
    end_date: date,
    holidays=None,
) -> List[date]:
    """
    Get all business days between start and end dates.

    Args:
        start_date: Start date
        end_date: End date
        holidays: Holidays to exclude.  Accepts ``List[date]``,
                  ``Dict[str,int]``, or ``HolidayCalendar``.

    Returns:
        List of business days (excluding weekends and holidays)
    """
    hols = _normalise_holidays(holidays)
    business_days = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5 and current not in hols:
            business_days.append(current)
        current += timedelta(days=1)
    return business_days


def workday(d: date, offset: int, holidays=None) -> date:
    """Advance *d* by *offset* business days, skipping weekends and holidays.

    This is a direct equivalent of ``common.workday(d, offset, holidays)``
    from the legacy ``im_prod.std_lib.common`` module.

    Args:
        d: Starting date (``date`` or ``datetime``).
        offset: Number of business days to move (positive = forward,
                negative = backward).
        holidays: Holidays to skip.  Accepts ``List[date]``,
                  ``Dict[str,int]``, or ``HolidayCalendar``.

    Returns:
        The resulting business day.
    """
    if isinstance(d, datetime):
        d = d.date()
    hols = _normalise_holidays(holidays)
    step = 1 if offset >= 0 else -1
    remaining = abs(offset)
    current = d
    while remaining > 0:
        current += timedelta(days=step)
        if current.weekday() < 5 and current not in hols:
            remaining -= 1
    return current


# ---------------------------------------------------------------------------
# Week-of-month helper (mirrors common.week_count)
# ---------------------------------------------------------------------------

def week_count(start_date: date, num_years: int = 1) -> Dict[str, int]:
    """Return a mapping of *date-string → week-of-month* for every day.

    This mirrors ``common.week_count(start_date, num_years)`` used by the
    legacy ``rebalance_dates.option_dates()`` to identify the "3rd week of
    the month" for standard monthly option expiry.

    Args:
        start_date: The first date to include.
        num_years: Number of calendar years to cover from *start_date*.

    Returns:
        ``Dict[str, int]`` mapping ``"YYYY-MM-DD"`` to the ISO-style week
        number within the month (1-based).
    """
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    end_date = date(start_date.year + num_years, start_date.month, start_date.day) - timedelta(days=1)
    result: Dict[str, int] = {}
    current = start_date
    while current <= end_date:
        # Week of month: which week does this day fall into?
        day = current.day
        week_num = (day - 1) // 7 + 1
        result[current.strftime("%Y-%m-%d")] = week_num
        current += timedelta(days=1)
    return result


# ---------------------------------------------------------------------------
# Third Friday helpers
# ---------------------------------------------------------------------------

def get_third_friday(year: int, month: int) -> date:
    """
    Get the third Friday of a given month.

    Args:
        year: Year
        month: Month (1-12)

    Returns:
        Date of third Friday
    """
    first_day = date(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(days=14)


def get_third_friday_adjusted(year: int, month: int, holidays=None) -> date:
    """Get the third Friday of a month, adjusted for exchange holidays.

    If the third Friday is a holiday the expiration moves to the preceding
    Thursday – matching the behaviour of the legacy
    ``rebalance_dates.option_dates()`` helper.

    Args:
        year: Year
        month: Month (1-12)
        holidays: Holidays.  Accepts ``List[date]``, ``Dict[str,int]``,
                  or ``HolidayCalendar``.

    Returns:
        Adjusted expiration date.
    """
    hols = _normalise_holidays(holidays)
    friday = get_third_friday(year, month)
    if friday in hols:
        return friday - timedelta(days=1)  # Thursday
    return friday


# ---------------------------------------------------------------------------
# Option expiration schedule
# ---------------------------------------------------------------------------

def get_option_expiration_dates(
    start_date: date,
    end_date: date,
    frequency: str = "monthly",
    holidays=None,
) -> List[date]:
    """
    Generate option expiration dates.

    Args:
        start_date: Start date
        end_date: End date
        frequency: ``"weekly"``, ``"monthly"``, or ``"quarterly"``
        holidays: Optional holidays for adjustment.  When provided,
                  monthly/quarterly third-Friday dates that fall on a
                  holiday are shifted to the preceding Thursday.

    Returns:
        Sorted list of expiration dates
    """
    hols = _normalise_holidays(holidays)
    expirations: List[date] = []

    if frequency == "monthly":
        current_date = start_date
        while current_date <= end_date:
            friday = get_third_friday(current_date.year, current_date.month)
            exp = friday - timedelta(days=1) if friday in hols else friday
            if start_date <= exp <= end_date:
                expirations.append(exp)
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)

    elif frequency == "weekly":
        current = start_date
        while current <= end_date:
            if current.weekday() == 4:  # Friday
                if current in hols:
                    adjusted = current - timedelta(days=1)
                    if start_date <= adjusted <= end_date:
                        expirations.append(adjusted)
                else:
                    expirations.append(current)
            current += timedelta(days=1)

    elif frequency == "quarterly":
        current_date = start_date
        quarterly_months = [3, 6, 9, 12]
        while current_date <= end_date:
            if current_date.month in quarterly_months:
                friday = get_third_friday(current_date.year, current_date.month)
                exp = friday - timedelta(days=1) if friday in hols else friday
                if start_date <= exp <= end_date and exp not in expirations:
                    expirations.append(exp)
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)

    return sorted(expirations)


def days_between(date1: date, date2: date) -> int:
    """Calculate number of days between two dates."""
    return abs((date2 - date1).days)
