"""
optionbt - A clean, modular framework for options strategy backtesting.

This package provides tools for backtesting covered calls, call spreads,
and other option overlay strategies with flexible configuration and
multiple data source support.
"""

__version__ = "2.0.0"
__author__ = "optionbt contributors"

from src.optionbt.engine.backtest import BacktestEngine
from src.optionbt.core.portfolio import Portfolio
from src.optionbt.core.strategy import Strategy
from src.optionbt.utils.dates import HolidayCalendar, workday, week_count
from src.optionbt.utils.common import extract_option_ticker, DbConnection
from src.optionbt.data.loader import DatabaseDataProvider

__all__ = [
    "BacktestEngine",
    "Portfolio",
    "Strategy",
    "HolidayCalendar",
    "workday",
    "week_count",
    "extract_option_ticker",
    "DbConnection",
    "DatabaseDataProvider",
]
