"""Tests for portfolio management."""

import pytest
from datetime import date
from src.optionbt.core.portfolio import Portfolio, Position
from src.optionbt.core.security import Equity


class TestPosition:
    """Test Position class."""
    
    def test_position_creation(self):
        """Test creating a position."""
        equity = Equity("SPY", price_data={date(2020, 1, 1): 325.0})
        position = Position(
            security=equity,
            quantity=100.0,
            entry_date=date(2020, 1, 1),
            entry_price=325.0
        )
        
        assert position.quantity == 100.0
        assert position.entry_price == 325.0
    
    def test_position_market_value(self):
        """Test market value calculation."""
        price_data = {
            date(2020, 1, 1): 325.0,
            date(2020, 1, 2): 330.0
        }
        equity = Equity("SPY", price_data=price_data)
        position = Position(
            security=equity,
            quantity=100.0,
            entry_date=date(2020, 1, 1),
            entry_price=325.0
        )
        
        # Initial value
        assert position.market_value(date(2020, 1, 1)) == 32500.0
        
        # After price increase
        assert position.market_value(date(2020, 1, 2)) == 33000.0
    
    def test_position_unrealized_pnl(self):
        """Test unrealized P&L calculation."""
        price_data = {
            date(2020, 1, 1): 325.0,
            date(2020, 1, 2): 330.0
        }
        equity = Equity("SPY", price_data=price_data)
        position = Position(
            security=equity,
            quantity=100.0,
            entry_date=date(2020, 1, 1),
            entry_price=325.0
        )
        
        # Price went up $5 per share * 100 shares = $500 profit
        assert position.unrealized_pnl(date(2020, 1, 2)) == 500.0


class TestPortfolio:
    """Test Portfolio class."""
    
    def test_portfolio_creation(self):
        """Test creating a portfolio."""
        portfolio = Portfolio("Test Portfolio", initial_capital=1_000_000)
        
        assert portfolio.name == "Test Portfolio"
        assert portfolio.initial_capital == 1_000_000
        assert portfolio.cash == 1_000_000
        assert len(portfolio.positions) == 0
    
    def test_add_position(self):
        """Test adding a position."""
        portfolio = Portfolio("Test", initial_capital=100_000)
        equity = Equity("SPY", price_data={date(2020, 1, 1): 325.0})
        
        # Buy 100 shares at $325
        portfolio.add_position(
            security=equity,
            quantity=100.0,
            price=325.0,
            trade_date=date(2020, 1, 1)
        )
        
        # Check position was added
        assert "SPY" in portfolio.positions
        assert portfolio.positions["SPY"].quantity == 100.0
        
        # Check cash was reduced
        assert portfolio.cash == 100_000 - 32_500
    
    def test_insufficient_cash(self):
        """Test that buying with insufficient cash raises error."""
        portfolio = Portfolio("Test", initial_capital=10_000)
        equity = Equity("SPY")
        
        # Try to buy $100,000 worth with only $10,000 cash
        with pytest.raises(ValueError, match="Insufficient cash"):
            portfolio.add_position(
                security=equity,
                quantity=100.0,
                price=1000.0,
                trade_date=date(2020, 1, 1)
            )
    
    def test_close_position(self):
        """Test closing a position."""
        portfolio = Portfolio("Test", initial_capital=100_000)
        price_data = {
            date(2020, 1, 1): 325.0,
            date(2020, 1, 2): 330.0
        }
        equity = Equity("SPY", price_data=price_data)
        
        # Buy
        portfolio.add_position(
            security=equity,
            quantity=100.0,
            price=325.0,
            trade_date=date(2020, 1, 1)
        )
        
        initial_cash = portfolio.cash
        
        # Sell
        realized_pnl = portfolio.close_position(
            ticker="SPY",
            price=330.0,
            trade_date=date(2020, 1, 2)
        )
        
        # Check position removed
        assert "SPY" not in portfolio.positions
        
        # Check realized P&L (100 shares * $5 gain = $500)
        assert realized_pnl == 500.0
        
        # Check cash increased by sale proceeds
        assert portfolio.cash == initial_cash + 33_000
    
    def test_total_market_value(self):
        """Test total market value calculation."""
        portfolio = Portfolio("Test", initial_capital=100_000)
        price_data = {
            date(2020, 1, 1): 325.0,
            date(2020, 1, 2): 330.0
        }
        equity = Equity("SPY", price_data=price_data)
        
        # Buy 100 shares at $325
        portfolio.add_position(
            security=equity,
            quantity=100.0,
            price=325.0,
            trade_date=date(2020, 1, 1)
        )
        
        # NAV should be: cash + position value
        # cash = 100,000 - 32,500 = 67,500
        # position = 100 * 330 = 33,000
        # total = 100,500
        nav = portfolio.total_market_value(date(2020, 1, 2))
        assert nav == 100_500
    
    def test_record_daily_value(self):
        """Test recording daily values."""
        portfolio = Portfolio("Test", initial_capital=100_000)
        price_data = {date(2020, 1, 1): 325.0}
        equity = Equity("SPY", price_data=price_data)
        
        portfolio.add_position(
            security=equity,
            quantity=100.0,
            price=325.0,
            trade_date=date(2020, 1, 1)
        )
        
        portfolio.record_daily_value(date(2020, 1, 1))
        
        assert len(portfolio.daily_values) == 1
        assert portfolio.daily_values[0]["date"] == date(2020, 1, 1)
        assert portfolio.daily_values[0]["nav"] == 100_000
        assert portfolio.daily_values[0]["return"] == 0.0
