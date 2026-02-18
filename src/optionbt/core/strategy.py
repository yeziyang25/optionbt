"""
Strategy definitions for option backtesting.

Strategies define the rules for selecting and rolling options.
"""

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional, List
import pandas as pd

from src.optionbt.core.security import Option, OptionType


class RollFrequency(Enum):
    """Frequency for rolling options."""
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    CUSTOM = "custom"


@dataclass
class StrategyConfig:
    """Configuration for a strategy."""
    
    name: str
    strategy_type: str  # "covered_call", "cash_secured_put", "call_spread", etc.
    
    # Option selection parameters
    moneyness: float = 0.0  # % OTM/ITM (0.0 = ATM, 0.05 = 5% OTM for calls)
    roll_frequency: RollFrequency = RollFrequency.MONTHLY
    coverage_ratio: float = 1.0  # What fraction of equity to cover with options
    
    # Advanced parameters
    target_yield: Optional[float] = None  # Target annualized yield (e.g., 0.15 for 15%)
    days_to_expiration: int = 30  # Target DTE for option selection
    rebalance_rule: str = "O"  # O=option roll, Q=quarterly, S=semi-annual, A=annual
    
    # Pricing parameters
    use_bid_ask: bool = True  # Use bid/ask for trades vs mid
    
    def __post_init__(self):
        """Validate configuration."""
        if self.coverage_ratio < 0 or self.coverage_ratio > 2:
            raise ValueError("coverage_ratio must be between 0 and 2")
        
        if self.target_yield is not None and self.target_yield < 0:
            raise ValueError("target_yield must be positive or None")


class Strategy:
    """
    Base class for option strategies.
    
    Subclasses implement specific strategy logic for selecting and rolling options.
    """
    
    def __init__(self, config: StrategyConfig):
        self.config = config
    
    def select_option(
        self,
        available_options: List[Option],
        current_date: date,
        underlying_price: float,
        equity_position_size: float
    ) -> Optional[Option]:
        """
        Select the best option from available options.
        
        Args:
            available_options: List of available options to choose from
            current_date: Current date
            underlying_price: Current price of underlying
            equity_position_size: Size of equity position (in shares)
            
        Returns:
            Selected option or None if no suitable option found
        """
        raise NotImplementedError("Subclasses must implement select_option")
    
    def should_roll(
        self,
        current_date: date,
        option_expiration: date,
        roll_dates: List[date]
    ) -> bool:
        """
        Determine if option should be rolled.
        
        Args:
            current_date: Current date
            option_expiration: Expiration date of current option
            roll_dates: List of predefined roll dates
            
        Returns:
            True if option should be rolled
        """
        # Default: roll on predefined roll dates
        return current_date in roll_dates
    
    def calculate_position_size(
        self,
        equity_position_size: float,
        option: Option,
        current_date: date
    ) -> float:
        """
        Calculate number of option contracts to trade.
        
        Args:
            equity_position_size: Size of equity position in shares
            option: The option to trade
            current_date: Current date
            
        Returns:
            Number of contracts (can be fractional for backtesting)
        """
        # Default: coverage_ratio * equity_position / 100 (contract size)
        return (self.config.coverage_ratio * equity_position_size) / 100.0
    
    def calculate_target_allocation(
        self,
        underlying_price: float,
        option_mid_price: float,
        equity_value: float,
        target_yield: float,
        days_to_expiration: int
    ) -> float:
        """
        Calculate the option allocation needed to achieve target yield.
        
        Args:
            underlying_price: Price of underlying equity
            option_mid_price: Mid price of option
            equity_value: Total value of equity position
            target_yield: Target annualized yield
            days_to_expiration: Days until option expiration
            
        Returns:
            Coverage ratio needed to achieve target yield
        """
        if days_to_expiration == 0:
            return 0.0
        
        # Annualize the DTE
        years = days_to_expiration / 365.0
        
        # Calculate required premium to achieve target yield
        required_premium = equity_value * target_yield * years
        
        # Calculate number of contracts needed
        premium_per_contract = option_mid_price * 100  # Contract size
        
        if premium_per_contract == 0:
            return 0.0
        
        contracts_needed = required_premium / premium_per_contract
        
        # Convert to coverage ratio (contracts * 100 / equity_shares)
        equity_shares = equity_value / underlying_price
        coverage_ratio = (contracts_needed * 100.0) / equity_shares if equity_shares > 0 else 0.0
        
        return coverage_ratio
    
    def __repr__(self) -> str:
        return f"Strategy({self.config.name}, type={self.config.strategy_type})"


class CoveredCallStrategy(Strategy):
    """
    Covered call strategy: sell call options against long equity position.
    """
    
    def __init__(self, config: StrategyConfig):
        super().__init__(config)
        if config.strategy_type != "covered_call":
            config.strategy_type = "covered_call"
    
    def select_option(
        self,
        available_options: List[Option],
        current_date: date,
        underlying_price: float,
        equity_position_size: float
    ) -> Optional[Option]:
        """
        Select call option closest to target moneyness.
        
        For covered calls, positive moneyness means OTM.
        """
        # Filter for call options that haven't expired
        calls = [
            opt for opt in available_options
            if opt.option_type == OptionType.CALL and not opt.is_expired(current_date)
        ]
        
        if not calls:
            return None
        
        # If target yield is specified, select based on yield
        if self.config.target_yield is not None:
            return self._select_by_target_yield(calls, current_date, underlying_price)
        
        # Otherwise, select by moneyness
        target_strike = underlying_price * (1 + self.config.moneyness)
        
        # Find option closest to target strike
        best_option = min(
            calls,
            key=lambda opt: abs(opt.strike - target_strike)
        )
        
        return best_option
    
    def _select_by_target_yield(
        self,
        options: List[Option],
        current_date: date,
        underlying_price: float
    ) -> Optional[Option]:
        """Select option that best achieves target yield."""
        best_option = None
        best_yield = 0.0
        
        for option in options:
            try:
                mid_price = option.get_mid(current_date)
                dte = (option.expiration - current_date).days
                
                if dte <= 0:
                    continue
                
                # Calculate annualized yield this option would provide
                premium_yield = (mid_price / underlying_price) * (365.0 / dte)
                
                # Find option closest to target yield
                if best_option is None or abs(premium_yield - self.config.target_yield) < abs(best_yield - self.config.target_yield):
                    best_option = option
                    best_yield = premium_yield
            except (ValueError, KeyError):
                continue
        
        return best_option


class CallSpreadStrategy(Strategy):
    """
    Call spread strategy: buy lower strike call, sell higher strike call.
    """
    
    def __init__(self, config: StrategyConfig, long_moneyness: float = -0.2):
        """
        Args:
            config: Strategy configuration
            long_moneyness: Moneyness for long call (negative = ITM)
        """
        super().__init__(config)
        self.long_moneyness = long_moneyness
        if config.strategy_type != "call_spread":
            config.strategy_type = "call_spread"
    
    def select_option_pair(
        self,
        available_options: List[Option],
        current_date: date,
        underlying_price: float
    ) -> tuple[Optional[Option], Optional[Option]]:
        """
        Select both legs of the call spread.
        
        Returns:
            (long_call, short_call) tuple
        """
        calls = [
            opt for opt in available_options
            if opt.option_type == OptionType.CALL and not opt.is_expired(current_date)
        ]
        
        if len(calls) < 2:
            return None, None
        
        # Select long call (ITM)
        long_strike = underlying_price * (1 + self.long_moneyness)
        long_call = min(calls, key=lambda opt: abs(opt.strike - long_strike))
        
        # Select short call (OTM or ATM)
        short_strike = underlying_price * (1 + self.config.moneyness)
        short_call = min(calls, key=lambda opt: abs(opt.strike - short_strike))
        
        return long_call, short_call
