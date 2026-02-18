"""
Backtest engine for running option strategy backtests.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional
import pandas as pd
from tqdm import tqdm

from src.optionbt.core.portfolio import Portfolio
from src.optionbt.core.security import Equity, Option, Cash
from src.optionbt.core.strategy import Strategy
from src.optionbt.utils.dates import get_business_days, get_option_expiration_dates


class BacktestEngine:
    """
    Main backtest engine for option strategies.
    
    Executes a backtest by:
    1. Iterating through each trading day
    2. Checking for roll/rebalance dates
    3. Executing strategy logic
    4. Recording portfolio values
    """
    
    def __init__(
        self,
        portfolio: Portfolio,
        strategy: Strategy,
        equity: Equity,
        options: List[Option],
        start_date: date,
        end_date: date,
        roll_dates: Optional[List[date]] = None,
        holidays: Optional[List[date]] = None
    ):
        """
        Initialize backtest engine.
        
        Args:
            portfolio: Portfolio to run backtest on
            strategy: Strategy to execute
            equity: Underlying equity security
            options: List of available options
            start_date: Backtest start date
            end_date: Backtest end date
            roll_dates: Optional list of option roll dates
            holidays: Optional list of holidays to exclude
        """
        self.portfolio = portfolio
        self.strategy = strategy
        self.equity = equity
        self.options = options
        self.start_date = start_date
        self.end_date = end_date
        self.holidays = holidays or []
        
        # Generate roll dates if not provided
        if roll_dates is None:
            self.roll_dates = self._generate_roll_dates()
        else:
            self.roll_dates = roll_dates
        
        # Track current option position
        self.current_option: Optional[Option] = None
        self.current_option_ticker: Optional[str] = None
        
        # Trading days
        self.trading_days = get_business_days(start_date, end_date, self.holidays)
    
    def _generate_roll_dates(self) -> List[date]:
        """Generate option roll dates based on strategy configuration."""
        from src.optionbt.core.strategy import RollFrequency
        
        freq = self.strategy.config.roll_frequency
        
        if freq == RollFrequency.MONTHLY:
            # Third Friday of each month
            return get_option_expiration_dates(
                self.start_date,
                self.end_date,
                "monthly"
            )
        elif freq == RollFrequency.WEEKLY:
            # Weekly Friday expiries
            return get_option_expiration_dates(
                self.start_date,
                self.end_date,
                "weekly"
            )
        elif freq == RollFrequency.QUARTERLY:
            # March, June, September, December third Fridays
            return get_option_expiration_dates(
                self.start_date,
                self.end_date,
                "quarterly"
            )
        else:
            # Default to monthly
            return get_option_expiration_dates(
                self.start_date,
                self.end_date,
                "monthly"
            )
    
    def run(self, verbose: bool = True) -> pd.DataFrame:
        """
        Run the backtest.
        
        Args:
            verbose: Whether to show progress bar
            
        Returns:
            DataFrame with daily portfolio performance
        """
        # Initialize: Buy equity on day 1
        self._initialize_positions(self.trading_days[0])
        
        # Iterate through trading days
        iterator = tqdm(self.trading_days, desc="Running backtest") if verbose else self.trading_days
        
        for current_date in iterator:
            # Check if we should roll options
            if current_date in self.roll_dates:
                self._roll_options(current_date)
            
            # Check if option expired (close if needed)
            if self.current_option and self.current_option.is_expired(current_date):
                self._close_option(current_date)
            
            # Record daily value
            self._record_daily_metrics(current_date)
        
        # Return performance DataFrame
        return self.portfolio.get_performance_dataframe()
    
    def _initialize_positions(self, init_date: date) -> None:
        """Initialize the portfolio with equity position on first day."""
        # Calculate how much equity to buy
        equity_price = self.equity.get_price(init_date)
        equity_value = self.portfolio.initial_capital
        equity_shares = equity_value / equity_price
        
        # Buy equity
        self.portfolio.add_position(
            security=self.equity,
            quantity=equity_shares,
            price=equity_price,
            trade_date=init_date
        )
    
    def _roll_options(self, roll_date: date) -> None:
        """Roll options: close existing, open new."""
        # Close existing option if any
        if self.current_option_ticker:
            self._close_option(roll_date)
        
        # Get equity position size
        equity_position = self.portfolio.get_position(self.equity.ticker)
        if not equity_position:
            return
        
        # Get available options for this date
        available_options = self._get_available_options(roll_date)
        if not available_options:
            return
        
        # Select option using strategy
        equity_price = self.equity.get_price(roll_date)
        selected_option = self.strategy.select_option(
            available_options=available_options,
            current_date=roll_date,
            underlying_price=equity_price,
            equity_position_size=equity_position.quantity
        )
        
        if not selected_option:
            return
        
        # Calculate position size
        contracts = self.strategy.calculate_position_size(
            equity_position_size=equity_position.quantity,
            option=selected_option,
            current_date=roll_date
        )
        
        # Sell option (negative quantity for short position)
        option_price = self._get_option_trade_price(selected_option, roll_date, is_sell=True)
        
        self.portfolio.add_position(
            security=selected_option,
            quantity=-contracts,  # Negative for short
            price=option_price,
            trade_date=roll_date
        )
        
        self.current_option = selected_option
        self.current_option_ticker = selected_option.ticker
    
    def _close_option(self, close_date: date) -> None:
        """Close the current option position."""
        if not self.current_option_ticker:
            return
        
        option_position = self.portfolio.get_position(self.current_option_ticker)
        if not option_position:
            return
        
        # Buy back option (opposite of sell)
        option_price = self._get_option_trade_price(
            self.current_option,
            close_date,
            is_sell=False
        )
        
        self.portfolio.close_position(
            ticker=self.current_option_ticker,
            price=option_price,
            trade_date=close_date
        )
        
        self.current_option = None
        self.current_option_ticker = None
    
    def _get_available_options(self, trade_date: date) -> List[Option]:
        """Get options available for trading on a specific date."""
        available = []
        for option in self.options:
            # Option must have pricing data for this date
            try:
                _ = option.get_mid(trade_date)
                # Option must not be expired
                if not option.is_expired(trade_date):
                    available.append(option)
            except (ValueError, KeyError):
                continue
        return available
    
    def _get_option_trade_price(
        self,
        option: Option,
        trade_date: date,
        is_sell: bool
    ) -> float:
        """
        Get the price to use for option trade.
        
        Args:
            option: Option to trade
            trade_date: Date of trade
            is_sell: True if selling (use bid), False if buying (use ask)
        """
        if self.strategy.config.use_bid_ask:
            # Use bid when selling, ask when buying
            return option.get_bid(trade_date) if is_sell else option.get_ask(trade_date)
        else:
            # Use mid price
            return option.get_mid(trade_date)
    
    def _record_daily_metrics(self, current_date: date) -> None:
        """Record daily portfolio metrics."""
        # Calculate any additional metrics
        additional_metrics = {}
        
        # Track option position details if any
        if self.current_option_ticker:
            option_pos = self.portfolio.get_position(self.current_option_ticker)
            if option_pos:
                try:
                    option_value = option_pos.market_value(current_date)
                    additional_metrics["option_value"] = option_value
                    additional_metrics["option_ticker"] = self.current_option_ticker
                except (ValueError, KeyError):
                    pass
        
        # Record daily value
        self.portfolio.record_daily_value(current_date, additional_metrics)
    
    def get_results(self) -> Dict[str, pd.DataFrame]:
        """
        Get backtest results.
        
        Returns:
            Dictionary with 'performance' and 'trades' DataFrames
        """
        return {
            "performance": self.portfolio.get_performance_dataframe(),
            "trades": self.portfolio.get_trade_dataframe(),
            "summary": self.portfolio.summary_stats()
        }
