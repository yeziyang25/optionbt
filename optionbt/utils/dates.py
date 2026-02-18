"""Utility functions for date handling in backtests."""

import pandas as pd
import datetime as dt
from typing import List, Optional


def get_third_friday(year: int, month: int) -> dt.datetime:
    """Get the third Friday of a given month.
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        datetime object for third Friday
    """
    # First day of the month
    first_day = dt.datetime(year, month, 1)
    
    # Find first Friday
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + dt.timedelta(days=days_until_friday)
    
    # Third Friday is 14 days after first Friday
    third_friday = first_friday + dt.timedelta(days=14)
    
    return third_friday


def get_option_dates(
    start_date: dt.datetime,
    end_date: dt.datetime,
    dtm: int = 6,
    holidays: Optional[List[dt.datetime]] = None
) -> List[dt.datetime]:
    """Generate option roll dates based on DTM (days to maturity).
    
    Args:
        start_date: Start date for backtest
        end_date: End date for backtest
        dtm: Days to maturity parameter (determines roll frequency)
        holidays: List of holidays to avoid
        
    Returns:
        List of option roll dates
    """
    if holidays is None:
        holidays = []
    
    roll_dates = []
    current_date = start_date
    
    while current_date <= end_date:
        # Add DTM days
        next_date = current_date + dt.timedelta(days=dtm)
        
        # Find next Friday
        days_until_friday = (4 - next_date.weekday()) % 7
        roll_date = next_date + dt.timedelta(days=days_until_friday)
        
        # Adjust for holidays
        while roll_date in holidays:
            roll_date += dt.timedelta(days=1)
        
        if roll_date <= end_date:
            roll_dates.append(roll_date)
        
        current_date = roll_date + dt.timedelta(days=1)
    
    return roll_dates


def get_equity_rebalance_dates(
    start_date: dt.datetime,
    end_date: dt.datetime,
    rule: str,
    option_dates: Optional[List[dt.datetime]] = None
) -> List[dt.datetime]:
    """Generate equity rebalance dates based on rule.
    
    Args:
        start_date: Start date
        end_date: End date
        rule: Rebalance rule ('Q'=Quarterly, 'S'=Semi-Annual, 'A'=Annual, 'O'=Option dates)
        option_dates: Option roll dates (required if rule='O')
        
    Returns:
        List of rebalance dates
    """
    if rule == 'O':
        # Same as option roll schedule
        return option_dates if option_dates else []
    
    rebal_dates = []
    
    if rule == 'Q':
        # Quarterly: 3rd Friday of Mar/Jun/Sep/Dec
        months = [3, 6, 9, 12]
    elif rule == 'S':
        # Semi-Annual: 3rd Friday of Mar/Sep
        months = [3, 9]
    elif rule == 'A':
        # Annual: 3rd Friday of Dec
        months = [12]
    else:
        return []
    
    current_year = start_date.year
    end_year = end_date.year
    
    for year in range(current_year, end_year + 1):
        for month in months:
            rebal_date = get_third_friday(year, month)
            if start_date <= rebal_date <= end_date:
                rebal_dates.append(rebal_date)
    
    return sorted(rebal_dates)


def get_tsx_holidays() -> List[dt.datetime]:
    """Get list of TSX holidays.
    
    Returns:
        List of TSX holiday dates
    """
    # This is a simplified list - should be extended for production use
    holidays = []
    
    # Add major Canadian holidays for common years
    years = range(2018, 2027)
    for year in years:
        # New Year's Day
        holidays.append(dt.datetime(year, 1, 1))
        
        # Good Friday (approximate - needs proper calculation)
        # Canada Day
        holidays.append(dt.datetime(year, 7, 1))
        
        # Labour Day (first Monday of September)
        first_day = dt.datetime(year, 9, 1)
        days_to_monday = (7 - first_day.weekday()) % 7
        labour_day = first_day + dt.timedelta(days=days_to_monday)
        holidays.append(labour_day)
        
        # Thanksgiving (second Monday of October)
        first_day = dt.datetime(year, 10, 1)
        days_to_monday = (7 - first_day.weekday()) % 7
        thanksgiving = first_day + dt.timedelta(days=days_to_monday + 7)
        holidays.append(thanksgiving)
        
        # Christmas
        holidays.append(dt.datetime(year, 12, 25))
        holidays.append(dt.datetime(year, 12, 26))
    
    return holidays


def get_us_holidays() -> List[dt.datetime]:
    """Get list of US market holidays.
    
    Returns:
        List of US holiday dates
    """
    # Simplified list - should be extended for production use
    holidays = []
    
    years = range(2018, 2027)
    for year in years:
        # New Year's Day
        holidays.append(dt.datetime(year, 1, 1))
        
        # Independence Day
        holidays.append(dt.datetime(year, 7, 4))
        
        # Thanksgiving (4th Thursday of November)
        first_day = dt.datetime(year, 11, 1)
        days_to_thursday = (3 - first_day.weekday()) % 7
        thanksgiving = first_day + dt.timedelta(days=days_to_thursday + 21)
        holidays.append(thanksgiving)
        
        # Christmas
        holidays.append(dt.datetime(year, 12, 25))
    
    return holidays
