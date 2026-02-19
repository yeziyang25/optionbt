"""Tests for date utilities including legacy-compatible helpers."""

import pytest
from datetime import date, datetime, timedelta

from src.optionbt.utils.dates import (
    get_business_days,
    get_third_friday,
    get_third_friday_adjusted,
    get_option_expiration_dates,
    days_between,
    workday,
    week_count,
    HolidayCalendar,
)


# ---------------------------------------------------------------------------
# HolidayCalendar
# ---------------------------------------------------------------------------

class TestHolidayCalendar:
    """Test HolidayCalendar class."""

    def test_from_list(self):
        """Create calendar from a list of dates."""
        hols = [date(2024, 1, 1), date(2024, 12, 25)]
        cal = HolidayCalendar(holidays=hols)
        assert len(cal) == 2
        assert cal.is_holiday(date(2024, 1, 1))
        assert not cal.is_holiday(date(2024, 1, 2))

    def test_from_legacy_dict(self):
        """Create calendar from legacy Dict[str, int] format."""
        hols = {"2024-01-01": 1, "2024-12-25": 1}
        cal = HolidayCalendar(holidays=hols)
        assert len(cal) == 2
        assert cal.is_holiday(date(2024, 1, 1))

    def test_to_list(self):
        """Export as sorted list of dates."""
        cal = HolidayCalendar([date(2024, 12, 25), date(2024, 1, 1)])
        assert cal.to_list() == [date(2024, 1, 1), date(2024, 12, 25)]

    def test_to_dict(self):
        """Export as legacy dict format."""
        cal = HolidayCalendar([date(2024, 1, 1)])
        d = cal.to_dict()
        assert d == {"2024-01-01": 1}

    def test_contains(self):
        """Supports ``in`` operator."""
        cal = HolidayCalendar([date(2024, 7, 4)])
        assert date(2024, 7, 4) in cal
        assert date(2024, 7, 5) not in cal

    def test_is_business_day(self):
        """is_business_day should exclude weekends and holidays."""
        cal = HolidayCalendar([date(2024, 1, 1)])  # Monday holiday
        assert not cal.is_business_day(date(2024, 1, 1))      # holiday
        assert not cal.is_business_day(date(2024, 1, 6))      # Saturday
        assert cal.is_business_day(date(2024, 1, 2))           # Tuesday

    def test_add_single_date(self):
        """Add a single date after construction."""
        cal = HolidayCalendar()
        cal.add(date(2024, 5, 1))
        assert date(2024, 5, 1) in cal

    def test_empty_calendar(self):
        cal = HolidayCalendar()
        assert len(cal) == 0
        assert not cal.is_holiday(date(2024, 1, 1))


# ---------------------------------------------------------------------------
# workday
# ---------------------------------------------------------------------------

class TestWorkday:
    """Test the workday() function – equivalent to common.workday."""

    def test_forward_no_holidays(self):
        """Advance one business day (Mon→Tue)."""
        assert workday(date(2024, 1, 8), 1) == date(2024, 1, 9)

    def test_forward_over_weekend(self):
        """Advance one business day from Friday → Monday."""
        assert workday(date(2024, 1, 5), 1) == date(2024, 1, 8)

    def test_forward_multiple(self):
        """Advance 5 business days (Mon → next Mon)."""
        assert workday(date(2024, 1, 8), 5) == date(2024, 1, 15)

    def test_forward_skips_holiday(self):
        """Advance should skip holidays."""
        hols = [date(2024, 1, 9)]  # Tuesday
        result = workday(date(2024, 1, 8), 1, hols)
        assert result == date(2024, 1, 10)  # Wednesday

    def test_backward(self):
        """Move backward one business day (Tue→Mon)."""
        assert workday(date(2024, 1, 9), -1) == date(2024, 1, 8)

    def test_backward_over_weekend(self):
        """Move backward from Monday → Friday."""
        assert workday(date(2024, 1, 8), -1) == date(2024, 1, 5)

    def test_with_legacy_holiday_dict(self):
        """Accept legacy Dict[str, int] holiday format."""
        hols = {"2024-01-09": 1}
        result = workday(date(2024, 1, 8), 1, hols)
        assert result == date(2024, 1, 10)

    def test_with_holiday_calendar(self):
        """Accept HolidayCalendar object."""
        cal = HolidayCalendar([date(2024, 1, 9)])
        result = workday(date(2024, 1, 8), 1, cal)
        assert result == date(2024, 1, 10)

    def test_zero_offset(self):
        """Zero offset returns the same date."""
        assert workday(date(2024, 1, 8), 0) == date(2024, 1, 8)

    def test_accepts_datetime(self):
        """workday should accept datetime objects as well."""
        dt = datetime(2024, 1, 8, 10, 30)
        result = workday(dt, 1)
        assert result == date(2024, 1, 9)


# ---------------------------------------------------------------------------
# week_count
# ---------------------------------------------------------------------------

class TestWeekCount:
    """Test the week_count() helper – equivalent to common.week_count."""

    def test_first_day_of_month(self):
        """1st of the month is always in week 1."""
        wc = week_count(date(2024, 1, 1), 1)
        assert wc["2024-01-01"] == 1

    def test_third_week(self):
        """Days 15-21 should be in week 3."""
        wc = week_count(date(2024, 1, 1), 1)
        assert wc["2024-01-15"] == 3
        assert wc["2024-01-21"] == 3

    def test_span_multiple_months(self):
        """Should cover multiple months within the year span."""
        wc = week_count(date(2024, 1, 1), 1)
        assert "2024-06-15" in wc
        assert "2024-12-31" in wc


# ---------------------------------------------------------------------------
# get_third_friday / get_third_friday_adjusted
# ---------------------------------------------------------------------------

class TestThirdFriday:
    """Test third Friday helpers."""

    def test_january_2024(self):
        assert get_third_friday(2024, 1) == date(2024, 1, 19)

    def test_march_2024(self):
        assert get_third_friday(2024, 3) == date(2024, 3, 15)

    def test_adjusted_no_holiday(self):
        """No adjustment when Friday is not a holiday."""
        result = get_third_friday_adjusted(2024, 1, holidays=[])
        assert result == date(2024, 1, 19)

    def test_adjusted_holiday_on_friday(self):
        """Shift to Thursday when Friday is a holiday."""
        holidays = [date(2024, 1, 19)]
        result = get_third_friday_adjusted(2024, 1, holidays=holidays)
        assert result == date(2024, 1, 18)

    def test_adjusted_with_legacy_dict(self):
        """Accept legacy dict holiday format."""
        holidays = {"2024-01-19": 1}
        result = get_third_friday_adjusted(2024, 1, holidays=holidays)
        assert result == date(2024, 1, 18)


# ---------------------------------------------------------------------------
# get_option_expiration_dates (with holidays)
# ---------------------------------------------------------------------------

class TestOptionExpirationDates:
    """Test option expiration date generation."""

    def test_monthly_basic(self):
        """Monthly expirations are third Fridays."""
        dates = get_option_expiration_dates(
            date(2024, 1, 1), date(2024, 3, 31), "monthly"
        )
        assert date(2024, 1, 19) in dates
        assert date(2024, 2, 16) in dates
        assert date(2024, 3, 15) in dates

    def test_monthly_with_holiday_adjustment(self):
        """When third Friday is a holiday, shift to Thursday."""
        holidays = [date(2024, 1, 19)]
        dates = get_option_expiration_dates(
            date(2024, 1, 1), date(2024, 3, 31), "monthly",
            holidays=holidays,
        )
        assert date(2024, 1, 18) in dates  # adjusted
        assert date(2024, 2, 16) in dates  # unaffected

    def test_weekly(self):
        """Weekly expirations are every Friday."""
        dates = get_option_expiration_dates(
            date(2024, 1, 1), date(2024, 1, 14), "weekly"
        )
        assert date(2024, 1, 5) in dates
        assert date(2024, 1, 12) in dates

    def test_quarterly(self):
        """Quarterly = third Friday of Mar/Jun/Sep/Dec."""
        dates = get_option_expiration_dates(
            date(2024, 1, 1), date(2024, 12, 31), "quarterly"
        )
        assert date(2024, 3, 15) in dates
        assert date(2024, 6, 21) in dates
        assert date(2024, 9, 20) in dates
        assert date(2024, 12, 20) in dates

    def test_quarterly_with_holiday(self):
        holidays = [date(2024, 3, 15)]
        dates = get_option_expiration_dates(
            date(2024, 1, 1), date(2024, 6, 30), "quarterly",
            holidays=holidays,
        )
        assert date(2024, 3, 14) in dates  # adjusted
        assert date(2024, 6, 21) in dates  # unaffected


# ---------------------------------------------------------------------------
# get_business_days (enhanced)
# ---------------------------------------------------------------------------

class TestGetBusinessDays:
    """Test enhanced get_business_days with multiple holiday formats."""

    def test_with_list(self):
        """Standard list of dates."""
        days = get_business_days(
            date(2024, 1, 1), date(2024, 1, 5),
            holidays=[date(2024, 1, 1)],
        )
        assert date(2024, 1, 1) not in days
        assert date(2024, 1, 2) in days

    def test_with_legacy_dict(self):
        """Accept legacy Dict[str, int]."""
        days = get_business_days(
            date(2024, 1, 1), date(2024, 1, 5),
            holidays={"2024-01-01": 1},
        )
        assert date(2024, 1, 1) not in days

    def test_with_holiday_calendar(self):
        cal = HolidayCalendar([date(2024, 1, 1)])
        days = get_business_days(date(2024, 1, 1), date(2024, 1, 5), holidays=cal)
        assert date(2024, 1, 1) not in days

    def test_excludes_weekends(self):
        days = get_business_days(date(2024, 1, 1), date(2024, 1, 7))
        assert date(2024, 1, 6) not in days  # Saturday
        assert date(2024, 1, 7) not in days  # Sunday


# ---------------------------------------------------------------------------
# days_between
# ---------------------------------------------------------------------------

class TestDaysBetween:

    def test_same_date(self):
        assert days_between(date(2024, 1, 1), date(2024, 1, 1)) == 0

    def test_positive_diff(self):
        assert days_between(date(2024, 1, 1), date(2024, 1, 10)) == 9
