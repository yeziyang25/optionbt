"""Main backtest engine for options strategies."""

import pandas as pd
import datetime as dt
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from .securities import Security, Cash, Equity, Option, FXForward, SecurityConfig
from ..data.loader import DataLoader
from ..utils.dates import get_option_dates, get_equity_rebalance_dates
from ..config import get_config


class BacktestEngine:
    """Main backtest engine for running portfolio backtests."""
    
    def __init__(
        self,
        portfolio_config: pd.DataFrame,
        start_date: dt.datetime,
        end_date: dt.datetime,
        holidays: List[dt.datetime],
        reinvest_premium: bool = True,
        use_database: bool = False
    ):
        """Initialize backtest engine.
        
        Args:
            portfolio_config: DataFrame with portfolio configuration
            start_date: Backtest start date
            end_date: Backtest end date
            holidays: List of market holidays
            reinvest_premium: Whether to reinvest option premiums
            use_database: Whether to use database for data (vs CSV files)
        """
        self.config = get_config()
        self.portfolio_config = portfolio_config
        self.start_date = start_date
        self.end_date = end_date
        self.holidays = holidays
        self.reinvest_premium = reinvest_premium
        
        # Initialize data loader
        self.data_loader = DataLoader(use_database=use_database)
        
        # Parse portfolio securities
        self.securities_config = self._parse_portfolio_config()
        
        # Determine rebalance dates
        self.option_rebalance_dates = self._get_option_rebalance_dates()
        self.equity_rebalance_dates = self._get_equity_rebalance_dates()
        
        # Load market data
        self.market_data = self._load_market_data()
        
        # State tracking
        self.portfolio_state = {}
        self.daily_records = []
    
    def _parse_portfolio_config(self) -> Dict[str, SecurityConfig]:
        """Parse portfolio configuration into SecurityConfig objects."""
        securities = {}
        for _, row in self.portfolio_config.iterrows():
            config = SecurityConfig.from_config_row(row)
            securities[config.sec_id] = config
        return securities
    
    def _get_option_rebalance_dates(self) -> List[dt.datetime]:
        """Get option rebalance dates based on DTM parameter."""
        # Get DTM from first option in config
        dtm = 6  # default
        for config in self.securities_config.values():
            if 'option' in config.sec_type.lower() and config.dtm:
                dtm = config.dtm
                break
        
        return get_option_dates(self.start_date, self.end_date, dtm, self.holidays)
    
    def _get_equity_rebalance_dates(self) -> List[dt.datetime]:
        """Get equity rebalance dates based on rebal_rule."""
        # Get rebal_rule from config
        rebal_rule = 'O'  # default to option dates
        for _, row in self.portfolio_config.iterrows():
            if pd.notna(row.get('rebal_rule')):
                rebal_rule = str(row['rebal_rule'])
                break
        
        return get_equity_rebalance_dates(
            self.start_date,
            self.end_date,
            rebal_rule,
            self.option_rebalance_dates
        )
    
    def _load_market_data(self) -> Dict:
        """Load all required market data."""
        data = {
            'equities': {},
            'options': {},
            'dividends': {},
            'fx': {}
        }
        
        start_str = self.start_date.strftime('%Y-%m-%d')
        end_str = self.end_date.strftime('%Y-%m-%d')
        
        # Load equity and option data
        for sec_id, config in self.securities_config.items():
            if config.sec_type == 'equity':
                try:
                    df = self.data_loader.load_equity_prices(
                        config.sec_name,
                        start_str,
                        end_str
                    )
                    data['equities'][config.sec_name] = dict(
                        zip(pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d'), df['px_last'])
                    )
                    
                    # Load dividends
                    div_df = self.data_loader.load_dividends(config.sec_name)
                    if not div_df.empty:
                        data['dividends'][config.sec_name] = dict(
                            zip(pd.to_datetime(div_df['date']).dt.strftime('%Y-%m-%d'), div_df['amount'])
                        )
                    else:
                        data['dividends'][config.sec_name] = {}
                except Exception as e:
                    print(f"Warning: Could not load equity data for {config.sec_name}: {e}")
                    data['equities'][config.sec_name] = {}
                    data['dividends'][config.sec_name] = {}
            
            elif 'option' in config.sec_type.lower():
                try:
                    df = self.data_loader.load_option_prices(
                        config.sec_name,
                        start_str,
                        end_str
                    )
                    # Group by ticker
                    for ticker in df['ticker'].unique():
                        ticker_data = df[df['ticker'] == ticker]
                        if ticker not in data['options']:
                            data['options'][ticker] = {}
                        
                        for field in ['bid', 'ask', 'px_last']:
                            field_data = ticker_data[ticker_data['field'] == field]
                            if not field_data.empty:
                                data['options'][ticker][field] = dict(
                                    zip(
                                        pd.to_datetime(field_data['date']).dt.strftime('%Y-%m-%d'),
                                        field_data['value']
                                    )
                                )
                except Exception as e:
                    print(f"Warning: Could not load option data for {config.sec_name}: {e}")
        
        return data
    
    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Run the backtest.
        
        Returns:
            Tuple of (aggregate_df, detailed_df)
        """
        # Generate all business days between start and end
        business_days = pd.bdate_range(
            start=self.start_date,
            end=self.end_date,
            freq='B'
        )
        
        # Filter out holidays
        business_days = [d for d in business_days if d.to_pydatetime() not in self.holidays]
        
        # Run backtest day by day
        for current_date in business_days:
            current_date = current_date.to_pydatetime()
            self._process_day(current_date)
        
        # Convert daily records to DataFrames
        detailed_df = pd.DataFrame(self.daily_records)
        aggregate_df = self._create_aggregate_report(detailed_df)
        
        return aggregate_df, detailed_df
    
    def _process_day(self, date: dt.datetime):
        """Process a single day in the backtest.
        
        Args:
            date: Current date
        """
        is_rebalance_day = date in self.option_rebalance_dates
        
        # Update all securities with current market data
        current_securities = {}
        
        for sec_id, config in self.securities_config.items():
            prior_state = self.portfolio_state.get(sec_id)
            
            if config.sec_type == 'cash':
                security = Cash(date, config, prior_state)
            
            elif config.sec_type == 'equity':
                pricing_data = self.market_data['equities'].get(config.sec_name, {})
                dividend_data = self.market_data['dividends'].get(config.sec_name, {})
                fx_rate = 1.0  # TODO: Load actual FX rates
                
                security = Equity(date, config, pricing_data, dividend_data, fx_rate, prior_state)
            
            elif 'option' in config.sec_type.lower():
                # For now, simplified option handling
                # In full implementation, would select specific option contract
                option_ticker = f"{config.sec_name}_PLACEHOLDER"
                option_pricing = self.market_data['options']
                
                # Get underlying price
                underlying_ticker = config.sec_name
                underlying_pricing = self.market_data['equities'].get(underlying_ticker, {})
                date_str = date.strftime('%Y-%m-%d')
                underlying_price = underlying_pricing.get(date_str, 0)
                
                security = Option(
                    date, config, option_ticker, option_pricing,
                    underlying_price, None, None, 1.0, prior_state
                )
            
            elif config.sec_type == 'fx_fwd':
                fx_rate = 1.0  # TODO: Load actual FX rates
                security = FXForward(date, config, fx_rate, prior_state)
            
            else:
                continue
            
            current_securities[sec_id] = security
        
        # Handle rebalancing if needed
        if is_rebalance_day:
            self._rebalance_portfolio(date, current_securities)
        
        # Record daily state
        for sec_id, security in current_securities.items():
            self.daily_records.append(security.to_dict())
        
        # Update state
        for sec_id, security in current_securities.items():
            self.portfolio_state[sec_id] = {
                'close_qty': security.close_qty,
                'close_wt': security.close_wt,
                'eod_price': security.eod_price,
                'fx': security.fx,
                'sec_ticker': getattr(security, 'sec_ticker', None)
            }
    
    def _rebalance_portfolio(self, date: dt.datetime, securities: Dict[str, Security]):
        """Rebalance portfolio on a rebalance date.
        
        Args:
            date: Current date
            securities: Current securities
        """
        # Simplified rebalancing logic
        # Full implementation would handle:
        # - Closing expired options
        # - Opening new option positions
        # - Rebalancing equity positions
        # - Calculating target allocations
        pass
    
    def _create_aggregate_report(self, detailed_df: pd.DataFrame) -> pd.DataFrame:
        """Create aggregate portfolio-level report.
        
        Args:
            detailed_df: Detailed security-level data
            
        Returns:
            Aggregate DataFrame
        """
        if detailed_df.empty:
            return pd.DataFrame()
        
        # Group by date and sum market values
        aggregate = detailed_df.groupby('date').agg({
            'market_value': 'sum',
            'cash_inflow': 'sum',
            'cash_outflow': 'sum'
        }).reset_index()
        
        # Calculate returns
        aggregate['portfolio_value'] = aggregate['market_value']
        aggregate['daily_return'] = aggregate['portfolio_value'].pct_change()
        aggregate['cumulative_return'] = (1 + aggregate['daily_return']).cumprod() - 1
        
        # Calculate drawdown
        aggregate['cumulative_max'] = aggregate['portfolio_value'].cummax()
        aggregate['drawdown'] = (aggregate['portfolio_value'] - aggregate['cumulative_max']) / aggregate['cumulative_max']
        
        return aggregate
