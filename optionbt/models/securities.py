"""Security classes for different asset types."""

import pandas as pd
import datetime as dt
from typing import Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class SecurityConfig:
    """Configuration for a security in the portfolio."""
    sec_id: str
    sec_name: str
    sec_type: str  # 'cash', 'equity', 'call option', 'put option', 'fx_fwd'
    currency: str
    allocation: Optional[float] = None
    option_w_against: Optional[str] = None  # Which equity this option is written against
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
    # Option-specific fields
    option_selection: Optional[str] = None
    custom_options_file: Optional[str] = None
    option_sell_to_open_price: str = 'bid'
    option_buy_to_close_price: str = 'intrinsic'
    pct_otm: Optional[float] = None
    dtm: Optional[int] = None
    
    # Target yield
    target_yield: float = -1
    
    @classmethod
    def from_config_row(cls, row: pd.Series) -> 'SecurityConfig':
        """Create SecurityConfig from portfolio config CSV row."""
        return cls(
            sec_id=str(row.get('sec_id', '')),
            sec_name=str(row.get('sec_name', 'NA')),
            sec_type=str(row.get('sec_type', '')),
            currency=str(row.get('currency', 'USD')),
            allocation=float(row['allocation']) if pd.notna(row.get('allocation')) else None,
            option_w_against=str(row['option_w_against']) if pd.notna(row.get('option_w_against')) else None,
            start_date=str(row['start_date']) if pd.notna(row.get('start_date')) else None,
            end_date=str(row['end_date']) if pd.notna(row.get('end_date')) else None,
            option_selection=str(row['option_selection']) if pd.notna(row.get('option_selection')) else None,
            custom_options_file=str(row['custom_options_file']) if pd.notna(row.get('custom_options_file')) else None,
            option_sell_to_open_price=str(row.get('option_sell_to_open_price', 'bid')),
            option_buy_to_close_price=str(row.get('option_buy_to_close_price', 'intrinsic')),
            pct_otm=float(row['pct_otm']) if pd.notna(row.get('pct_otm')) else None,
            dtm=int(row['DTM']) if pd.notna(row.get('DTM')) else None,
            target_yield=float(row.get('target_yield', -1))
        )


class Security:
    """Base class for all securities."""
    
    def __init__(
        self,
        date: dt.datetime,
        config: SecurityConfig,
        prior_state: Optional[Dict[str, Any]] = None
    ):
        """Initialize security.
        
        Args:
            date: Current date
            config: Security configuration
            prior_state: State from previous day
        """
        self.date = date
        self.config = config
        self.sec_id = config.sec_id
        self.sec_name = config.sec_name
        self.sec_type = config.sec_type
        self.currency = config.currency
        
        # Position tracking
        self.open_qty = prior_state.get('close_qty', 0) if prior_state else 0
        self.close_qty = self.open_qty
        self.open_wt = prior_state.get('close_wt', 0) if prior_state else 0
        self.close_wt = self.open_wt
        
        # Pricing
        self.eod_price = 0.0
        self.bid = 0.0
        self.ask = 0.0
        
        # FX rate
        self.fx = prior_state.get('fx', 1.0) if prior_state else 1.0
        
        # Cash flows
        self.cash_inflow = 0.0
        self.cash_outflow = 0.0
    
    def get_mid_price(self) -> float:
        """Get mid price from bid/ask."""
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.eod_price
    
    def get_market_value(self) -> float:
        """Get market value in base currency."""
        return self.close_qty * self.eod_price * self.fx
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for output."""
        return {
            'date': self.date,
            'sec_id': self.sec_id,
            'sec_name': self.sec_name,
            'sec_type': self.sec_type,
            'currency': self.currency,
            'open_qty': self.open_qty,
            'close_qty': self.close_qty,
            'eod_price': self.eod_price,
            'bid': self.bid,
            'ask': self.ask,
            'fx': self.fx,
            'market_value': self.get_market_value(),
            'cash_inflow': self.cash_inflow,
            'cash_outflow': self.cash_outflow
        }


class Cash(Security):
    """Cash security."""
    
    def __init__(self, date: dt.datetime, config: SecurityConfig, prior_state: Optional[Dict] = None):
        super().__init__(date, config, prior_state)
        self.eod_price = 1.0
        self.bid = 1.0
        self.ask = 1.0
        self.fx = 1.0  # Cash is always in base currency


class Equity(Security):
    """Equity security."""
    
    def __init__(
        self,
        date: dt.datetime,
        config: SecurityConfig,
        pricing_data: Dict[str, float],
        dividend_data: Dict[str, float],
        fx_rate: float = 1.0,
        prior_state: Optional[Dict] = None
    ):
        super().__init__(date, config, prior_state)
        
        date_str = date.strftime('%Y-%m-%d')
        
        # Set pricing
        self.eod_price = pricing_data.get(date_str, prior_state.get('eod_price', 0) if prior_state else 0)
        self.bid = self.eod_price
        self.ask = self.eod_price
        
        # Set FX rate
        self.fx = fx_rate
        
        # Set dividend
        self.dvd_rate = dividend_data.get(date_str, 0)


class Option(Security):
    """Option security (call or put)."""
    
    def __init__(
        self,
        date: dt.datetime,
        config: SecurityConfig,
        option_ticker: str,
        option_pricing: Dict[str, Dict[str, float]],
        underlying_price: float,
        expiry_date: Optional[dt.datetime] = None,
        strike: Optional[float] = None,
        fx_rate: float = 1.0,
        prior_state: Optional[Dict] = None,
        eod_pricing_method: str = 'mid'
    ):
        super().__init__(date, config, prior_state)
        
        self.sec_ticker = option_ticker
        self.option_underlying_price = underlying_price
        self.expiry = expiry_date
        self.strike = strike
        self.fx = fx_rate
        
        date_str = date.strftime('%Y-%m-%d')
        
        # Get option pricing for this ticker
        ticker_data = option_pricing.get(option_ticker, {})
        
        self.bid = ticker_data.get('bid', {}).get(date_str, 0)
        self.ask = ticker_data.get('ask', {}).get(date_str, 0)
        
        # Calculate mid price
        mid = self.get_mid_price() if self.bid > 0 and self.ask > 0 else 0
        
        # Set EOD price based on method
        if eod_pricing_method == 'mid':
            self.eod_price = mid
        elif eod_pricing_method == 'bid':
            self.eod_price = self.bid
        elif eod_pricing_method == 'ask':
            self.eod_price = self.ask
        elif eod_pricing_method.startswith('mid_'):
            # mid_1.5 means bid + 1.5 * (ask - bid) / 2
            multiplier = float(eod_pricing_method.split('_')[1])
            if self.bid > 0 and self.ask > 0:
                self.eod_price = self.bid + multiplier * (self.ask - self.bid) / 2
            else:
                self.eod_price = mid
        else:
            self.eod_price = mid
        
        # Calculate moneyness if we have strike and underlying price
        if strike and underlying_price:
            self.moneyness = underlying_price / strike
        else:
            self.moneyness = None
    
    def get_intrinsic_value(self) -> float:
        """Calculate intrinsic value of option."""
        if not self.strike or not self.option_underlying_price:
            return 0
        
        if 'call' in self.sec_type.lower():
            return max(0, self.option_underlying_price - self.strike)
        else:  # put
            return max(0, self.strike - self.option_underlying_price)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for output."""
        result = super().to_dict()
        result.update({
            'sec_ticker': self.sec_ticker,
            'opt_u_price': self.option_underlying_price,
            'expiry': self.expiry,
            'strike': self.strike,
            'moneyness': self.moneyness
        })
        return result


class FXForward(Security):
    """FX Forward contract."""
    
    def __init__(
        self,
        date: dt.datetime,
        config: SecurityConfig,
        fx_rate: float,
        prior_state: Optional[Dict] = None
    ):
        super().__init__(date, config, prior_state)
        
        # FX forward ticker
        if prior_state and 'sec_ticker' in prior_state:
            self.sec_ticker = prior_state['sec_ticker']
        else:
            self.sec_ticker = f"{config.sec_name}_{date.strftime('%Y%m%d')}"
        
        self.fx = fx_rate
        self.eod_price = fx_rate
        self.bid = fx_rate
        self.ask = fx_rate
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for output."""
        result = super().to_dict()
        result['sec_ticker'] = self.sec_ticker
        return result
