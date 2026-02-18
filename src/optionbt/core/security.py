"""
Security classes representing different asset types in the portfolio.
"""

from datetime import date
from enum import Enum
from typing import Optional


class SecurityType(Enum):
    """Types of securities supported in the backtest."""
    CASH = "cash"
    EQUITY = "equity"
    CALL_OPTION = "call_option"
    PUT_OPTION = "put_option"


class Security:
    """Base class for all securities."""
    
    def __init__(self, ticker: str, security_type: SecurityType, currency: str = "USD"):
        self.ticker = ticker
        self.security_type = security_type
        self.currency = currency
    
    def get_price(self, pricing_date: date) -> float:
        """Get the security price on a given date."""
        raise NotImplementedError("Subclasses must implement get_price")


class Cash(Security):
    """Cash security - always priced at 1.0."""
    
    def __init__(self, currency: str = "USD"):
        super().__init__(
            ticker="CASH",
            security_type=SecurityType.CASH,
            currency=currency
        )
    
    def get_price(self, pricing_date: date) -> float:
        """Cash is always worth 1.0 per unit."""
        return 1.0


class Equity(Security):
    """Equity security with price history."""
    
    def __init__(
        self,
        ticker: str,
        currency: str = "USD",
        name: Optional[str] = None,
        price_data: Optional[dict] = None
    ):
        super().__init__(
            ticker=ticker,
            security_type=SecurityType.EQUITY,
            currency=currency
        )
        self.name = name or ticker
        self._price_data = price_data or {}
    
    def set_price_data(self, price_data: dict):
        """Set the price history for this equity."""
        self._price_data = price_data
    
    def get_price(self, pricing_date: date) -> float:
        """Get equity price on a specific date."""
        if pricing_date not in self._price_data:
            raise ValueError(
                f"No price data available for {self.ticker} on {pricing_date}"
            )
        return self._price_data[pricing_date]


class OptionType(Enum):
    """Type of option."""
    CALL = "call"
    PUT = "put"


class Option(Security):
    """Option security with strike, expiration, and pricing."""
    
    def __init__(
        self,
        ticker: str,
        underlying_ticker: str,
        strike: float,
        expiration: date,
        option_type: OptionType,
        currency: str = "USD",
        bid_data: Optional[dict] = None,
        ask_data: Optional[dict] = None,
        underlying_price_data: Optional[dict] = None
    ):
        sec_type = (
            SecurityType.CALL_OPTION
            if option_type == OptionType.CALL
            else SecurityType.PUT_OPTION
        )
        super().__init__(
            ticker=ticker,
            security_type=sec_type,
            currency=currency
        )
        self.underlying_ticker = underlying_ticker
        self.strike = strike
        self.expiration = expiration
        self.option_type = option_type
        self._bid_data = bid_data or {}
        self._ask_data = ask_data or {}
        self._underlying_price_data = underlying_price_data or {}
    
    def set_pricing_data(
        self,
        bid_data: dict,
        ask_data: dict,
        underlying_price_data: dict
    ):
        """Set the pricing data for this option."""
        self._bid_data = bid_data
        self._ask_data = ask_data
        self._underlying_price_data = underlying_price_data
    
    def get_price(self, pricing_date: date, price_type: str = "mid") -> float:
        """
        Get option price on a specific date.
        
        Args:
            pricing_date: Date to get price for
            price_type: "bid", "ask", or "mid" (default)
        """
        # Check if option has expired - use intrinsic value
        if self.is_expired(pricing_date) or pricing_date == self.expiration:
            # Calculate intrinsic value
            try:
                underlying = self.get_underlying_price(pricing_date)
            except (ValueError, KeyError):
                # No underlying price either, assume 0
                return 0.0
            
            if self.option_type == OptionType.CALL:
                return max(0, underlying - self.strike)
            else:  # PUT
                return max(0, self.strike - underlying)
        
        if pricing_date not in self._bid_data or pricing_date not in self._ask_data:
            raise ValueError(
                f"No price data available for {self.ticker} on {pricing_date}"
            )
        
        bid = self._bid_data[pricing_date]
        ask = self._ask_data[pricing_date]
        
        if price_type == "bid":
            return bid
        elif price_type == "ask":
            return ask
        else:  # mid
            return (bid + ask) / 2.0
    
    def get_bid(self, pricing_date: date) -> float:
        """Get bid price."""
        return self.get_price(pricing_date, "bid")
    
    def get_ask(self, pricing_date: date) -> float:
        """Get ask price."""
        return self.get_price(pricing_date, "ask")
    
    def get_mid(self, pricing_date: date) -> float:
        """Get mid price."""
        return self.get_price(pricing_date, "mid")
    
    def get_underlying_price(self, pricing_date: date) -> float:
        """Get underlying price on a specific date."""
        if pricing_date not in self._underlying_price_data:
            raise ValueError(
                f"No underlying price data for {self.ticker} on {pricing_date}"
            )
        return self._underlying_price_data[pricing_date]
    
    def moneyness(self, pricing_date: date) -> float:
        """
        Calculate moneyness (strike / underlying_price - 1).
        
        Returns:
            Positive values = OTM for calls, ITM for puts
            Negative values = ITM for calls, OTM for puts
        """
        underlying_price = self.get_underlying_price(pricing_date)
        return (self.strike / underlying_price) - 1.0
    
    def is_expired(self, current_date: date) -> bool:
        """Check if option has expired."""
        return current_date > self.expiration

