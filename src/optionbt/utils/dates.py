"""Date utility functions for backtesting."""

from datetime import date, timedelta
from typing import List


def get_business_days(
    start_date: date,
    end_date: date,
    holidays: List[date] = None
) -> List[date]:
    """
    Get all business days between start and end dates.
    
    Args:
        start_date: Start date
        end_date: End date
        holidays: List of holidays to exclude
        
    Returns:
        List of business days (excluding weekends and holidays)
    """
    if holidays is None:
        holidays = []
    
    business_days = []
    current = start_date
    
    while current <= end_date:
        # Skip weekends (5 = Saturday, 6 = Sunday)
        if current.weekday() < 5 and current not in holidays:
            business_days.append(current)
        current += timedelta(days=1)
    
    return business_days


def get_third_friday(year: int, month: int) -> date:
    """
    Get the third Friday of a given month.
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        Date of third Friday
    """
    # Start with first day of month
    first_day = date(year, month, 1)
    
    # Find first Friday
    # 4 = Friday in weekday()
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    
    # Third Friday is 14 days after first Friday
    third_friday = first_friday + timedelta(days=14)
    
    return third_friday


def get_option_expiration_dates(
    start_date: date,
    end_date: date,
    frequency: str = "monthly"
) -> List[date]:
    """
    Generate option expiration dates.
    
    Args:
        start_date: Start date
        end_date: End date
        frequency: "weekly", "monthly", or "quarterly"
        
    Returns:
        List of expiration dates
    """
    expirations = []
    
    if frequency == "monthly":
        # Third Friday of each month
        current_date = start_date
        while current_date <= end_date:
            third_friday = get_third_friday(current_date.year, current_date.month)
            if start_date <= third_friday <= end_date:
                expirations.append(third_friday)
            
            # Move to next month
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)
    
    elif frequency == "weekly":
        # Every Friday
        current = start_date
        while current <= end_date:
            if current.weekday() == 4:  # Friday
                expirations.append(current)
            current += timedelta(days=1)
    
    elif frequency == "quarterly":
        # Third Friday of March, June, September, December
        current_date = start_date
        quarterly_months = [3, 6, 9, 12]
        
        while current_date <= end_date:
            if current_date.month in quarterly_months:
                third_friday = get_third_friday(current_date.year, current_date.month)
                if start_date <= third_friday <= end_date and third_friday not in expirations:
                    expirations.append(third_friday)
            
            # Move to next month
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)
    
    return sorted(expirations)


def days_between(date1: date, date2: date) -> int:
    """Calculate number of days between two dates."""
    return abs((date2 - date1).days)
