"""
Portfolio and Position classes for managing holdings and tracking performance.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional
import pandas as pd

from src.optionbt.core.security import Security


@dataclass
class Position:
    """Represents a position in a security."""
    
    security: Security
    quantity: float
    entry_date: date
    entry_price: float
    
    def market_value(self, current_date: date, price: Optional[float] = None) -> float:
        """
        Calculate current market value of the position.
        
        Args:
            current_date: Date to value the position
            price: Optional price override (otherwise fetched from security)
        """
        if price is None:
            price = self.security.get_price(current_date)
        return self.quantity * price
    
    def unrealized_pnl(self, current_date: date) -> float:
        """Calculate unrealized P&L."""
        current_price = self.security.get_price(current_date)
        return self.quantity * (current_price - self.entry_price)
    
    def __repr__(self) -> str:
        return (
            f"Position({self.security.ticker}, "
            f"qty={self.quantity:.2f}, "
            f"entry=${self.entry_price:.2f})"
        )


@dataclass
class Portfolio:
    """
    Portfolio managing multiple positions and cash.
    
    Tracks positions, executes trades, and calculates performance metrics.
    """
    
    name: str
    initial_capital: float
    positions: Dict[str, Position] = field(default_factory=dict)
    cash: float = 0.0
    trade_history: List[dict] = field(default_factory=list)
    daily_values: List[dict] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize cash to initial capital if not set."""
        if self.cash == 0.0:
            self.cash = self.initial_capital
    
    def add_position(
        self,
        security: Security,
        quantity: float,
        price: float,
        trade_date: date,
        record_trade: bool = True
    ) -> None:
        """
        Add or increase a position.
        
        Args:
            security: Security to buy
            quantity: Number of shares/contracts (positive for long, negative for short)
            price: Entry price
            trade_date: Date of trade
            record_trade: Whether to record in trade history
        """
        cost = quantity * price
        
        # Check if we have enough cash (for buys)
        if quantity > 0 and cost > self.cash:
            raise ValueError(
                f"Insufficient cash. Need ${cost:.2f}, have ${self.cash:.2f}"
            )
        
        # Update cash
        self.cash -= cost
        
        # Add or update position
        if security.ticker in self.positions:
            # Average cost for existing position
            existing_pos = self.positions[security.ticker]
            total_qty = existing_pos.quantity + quantity
            if total_qty != 0:
                avg_price = (
                    (existing_pos.quantity * existing_pos.entry_price + cost)
                    / total_qty
                )
                existing_pos.quantity = total_qty
                existing_pos.entry_price = avg_price
            else:
                # Position closed
                del self.positions[security.ticker]
        else:
            self.positions[security.ticker] = Position(
                security=security,
                quantity=quantity,
                entry_date=trade_date,
                entry_price=price
            )
        
        # Record trade
        if record_trade:
            self.trade_history.append({
                "date": trade_date,
                "ticker": security.ticker,
                "quantity": quantity,
                "price": price,
                "value": cost,
                "type": "BUY" if quantity > 0 else "SELL"
            })
    
    def close_position(
        self,
        ticker: str,
        price: float,
        trade_date: date,
        record_trade: bool = True
    ) -> Optional[float]:
        """
        Close an entire position.
        
        Args:
            ticker: Ticker symbol to close
            price: Exit price
            trade_date: Date of trade
            record_trade: Whether to record in trade history
            
        Returns:
            Realized P&L from closing the position
        """
        if ticker not in self.positions:
            return None
        
        position = self.positions[ticker]
        proceeds = -position.quantity * price  # Negative quantity means we sell
        realized_pnl = proceeds + position.quantity * position.entry_price
        
        # Update cash
        self.cash += proceeds
        
        # Record trade
        if record_trade:
            self.trade_history.append({
                "date": trade_date,
                "ticker": ticker,
                "quantity": -position.quantity,
                "price": price,
                "value": proceeds,
                "type": "CLOSE",
                "realized_pnl": realized_pnl
            })
        
        # Remove position
        del self.positions[ticker]
        
        return realized_pnl
    
    def get_position(self, ticker: str) -> Optional[Position]:
        """Get a position by ticker."""
        return self.positions.get(ticker)
    
    def total_market_value(self, current_date: date) -> float:
        """Calculate total portfolio market value including cash."""
        positions_value = sum(
            pos.market_value(current_date) for pos in self.positions.values()
        )
        return self.cash + positions_value
    
    def net_asset_value(self, current_date: date) -> float:
        """Alias for total_market_value."""
        return self.total_market_value(current_date)
    
    def record_daily_value(
        self,
        current_date: date,
        additional_metrics: Optional[dict] = None
    ) -> None:
        """
        Record daily portfolio value and metrics.
        
        Args:
            current_date: Date to record
            additional_metrics: Optional dict of additional metrics to record
        """
        nav = self.total_market_value(current_date)
        
        record = {
            "date": current_date,
            "nav": nav,
            "cash": self.cash,
            "return": (nav / self.initial_capital) - 1.0,
            "num_positions": len(self.positions)
        }
        
        if additional_metrics:
            record.update(additional_metrics)
        
        self.daily_values.append(record)
    
    def get_performance_dataframe(self) -> pd.DataFrame:
        """Get daily performance as a DataFrame."""
        df = pd.DataFrame(self.daily_values)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            
            # Calculate cumulative return
            df["cumulative_return"] = df["return"]
            
            # Calculate daily returns
            df["daily_return"] = df["nav"].pct_change()
            
            # Calculate drawdown
            df["peak"] = df["nav"].cummax()
            df["drawdown"] = (df["nav"] / df["peak"]) - 1.0
        
        return df
    
    def get_trade_dataframe(self) -> pd.DataFrame:
        """Get trade history as a DataFrame."""
        df = pd.DataFrame(self.trade_history)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
        return df
    
    def summary_stats(self) -> dict:
        """Calculate summary statistics for the portfolio."""
        df = self.get_performance_dataframe()
        
        if df.empty:
            return {}
        
        total_return = df["return"].iloc[-1]
        num_days = len(df)
        num_years = num_days / 252.0  # Approximate trading days per year
        
        annualized_return = (1 + total_return) ** (1 / num_years) - 1 if num_years > 0 else 0
        
        daily_returns = df["daily_return"].dropna()
        volatility = daily_returns.std() * (252 ** 0.5) if len(daily_returns) > 1 else 0
        
        sharpe = annualized_return / volatility if volatility > 0 else 0
        
        max_drawdown = df["drawdown"].min()
        
        return {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "volatility": volatility,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_drawdown,
            "num_trades": len(self.trade_history),
            "final_nav": df["nav"].iloc[-1],
            "num_days": num_days
        }
    
    def __repr__(self) -> str:
        return (
            f"Portfolio('{self.name}', "
            f"cash=${self.cash:,.2f}, "
            f"positions={len(self.positions)})"
        )
