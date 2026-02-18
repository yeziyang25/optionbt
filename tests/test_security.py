"""Tests for core domain models."""

import pytest
from datetime import date
from src.optionbt.core.security import (
    Security,
    Cash,
    Equity,
    Option,
    OptionType,
    SecurityType
)


class TestCash:
    """Test Cash security."""
    
    def test_cash_always_one(self):
        """Cash should always be priced at 1.0."""
        cash = Cash("USD")
        assert cash.get_price(date(2020, 1, 1)) == 1.0
        assert cash.get_price(date(2024, 12, 31)) == 1.0
        assert cash.currency == "USD"


class TestEquity:
    """Test Equity security."""
    
    def test_equity_creation(self):
        """Test creating an equity."""
        equity = Equity("SPY", currency="USD", name="SPDR S&P 500")
        assert equity.ticker == "SPY"
        assert equity.name == "SPDR S&P 500"
        assert equity.security_type == SecurityType.EQUITY
    
    def test_equity_pricing(self):
        """Test equity pricing."""
        price_data = {
            date(2020, 1, 1): 100.0,
            date(2020, 1, 2): 101.0,
            date(2020, 1, 3): 99.0
        }
        equity = Equity("SPY", price_data=price_data)
        
        assert equity.get_price(date(2020, 1, 1)) == 100.0
        assert equity.get_price(date(2020, 1, 2)) == 101.0
        assert equity.get_price(date(2020, 1, 3)) == 99.0
    
    def test_equity_missing_price(self):
        """Test error when price is missing."""
        equity = Equity("SPY")
        with pytest.raises(ValueError):
            equity.get_price(date(2020, 1, 1))


class TestOption:
    """Test Option security."""
    
    def test_option_creation(self):
        """Test creating an option."""
        option = Option(
            ticker="SPY_320_20200117_call",
            underlying_ticker="SPY",
            strike=320.0,
            expiration=date(2020, 1, 17),
            option_type=OptionType.CALL
        )
        
        assert option.ticker == "SPY_320_20200117_call"
        assert option.strike == 320.0
        assert option.option_type == OptionType.CALL
        assert option.security_type == SecurityType.CALL_OPTION
    
    def test_option_pricing(self):
        """Test option pricing."""
        bid_data = {date(2020, 1, 1): 5.0}
        ask_data = {date(2020, 1, 1): 5.2}
        
        option = Option(
            ticker="SPY_320_20200117_call",
            underlying_ticker="SPY",
            strike=320.0,
            expiration=date(2020, 1, 17),
            option_type=OptionType.CALL,
            bid_data=bid_data,
            ask_data=ask_data
        )
        
        assert option.get_bid(date(2020, 1, 1)) == 5.0
        assert option.get_ask(date(2020, 1, 1)) == 5.2
        assert option.get_mid(date(2020, 1, 1)) == 5.1
    
    def test_option_moneyness(self):
        """Test moneyness calculation."""
        underlying_data = {date(2020, 1, 1): 325.0}
        
        # ATM option
        option_atm = Option(
            ticker="test",
            underlying_ticker="SPY",
            strike=325.0,
            expiration=date(2020, 1, 17),
            option_type=OptionType.CALL,
            underlying_price_data=underlying_data
        )
        assert abs(option_atm.moneyness(date(2020, 1, 1))) < 0.001
        
        # 5% OTM call
        option_otm = Option(
            ticker="test",
            underlying_ticker="SPY",
            strike=341.25,  # 5% above 325
            expiration=date(2020, 1, 17),
            option_type=OptionType.CALL,
            underlying_price_data=underlying_data
        )
        assert abs(option_otm.moneyness(date(2020, 1, 1)) - 0.05) < 0.001
    
    def test_option_expiration(self):
        """Test expiration check."""
        option = Option(
            ticker="test",
            underlying_ticker="SPY",
            strike=320.0,
            expiration=date(2020, 1, 17),
            option_type=OptionType.CALL
        )
        
        assert not option.is_expired(date(2020, 1, 16))
        assert not option.is_expired(date(2020, 1, 17))
        assert option.is_expired(date(2020, 1, 18))
