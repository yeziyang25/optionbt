"""Data loader for market data (equity, options, FX)."""

import pandas as pd
import os
from pathlib import Path
from typing import Dict, List, Optional
from .database import DatabaseConnection
from ..config import get_config


class DataLoader:
    """Load market data from database or CSV files."""
    
    def __init__(self, use_database: bool = False):
        """Initialize data loader.
        
        Args:
            use_database: If True, try to load from database. Otherwise use CSV files.
        """
        self.config = get_config()
        self.use_database = use_database and self.config.database_enabled
        self.db = None
        
        if self.use_database:
            try:
                self.db = DatabaseConnection()
            except Exception as e:
                print(f"Warning: Could not connect to database: {e}")
                print("Falling back to CSV files")
                self.use_database = False
    
    def load_equity_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Load equity prices for a ticker.
        
        Args:
            ticker: Stock ticker (e.g., 'SPY US')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with columns: date, px_last
        """
        if self.use_database:
            return self._load_equity_from_db(ticker, start_date, end_date)
        else:
            return self._load_equity_from_csv(ticker)
    
    def _load_equity_from_db(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Load equity prices from database."""
        query = """
            SELECT [date] as date, [value] as px_last, source
            FROM [dbo].[market_data]
            WHERE ticker = ? 
                AND field = 'px_last'
                AND [date] >= ? 
                AND [date] <= ?
            ORDER BY [date]
        """
        
        # Use parameterized query to prevent SQL injection
        import pandas as pd
        df = pd.read_sql(query, self.db.conn, params=(ticker, start_date, end_date))
        
        if df.empty:
            raise ValueError(f"No data found for {ticker}")
        
        # Handle multiple sources - prioritize bloomberg > solactive > mellon
        source_priority = {"bloomberg": 1, "solactive": 2, "mellon": 3}
        df["ranking"] = df["source"].map(source_priority)
        df = df.sort_values(["date", "ranking"]).drop_duplicates(subset="date")
        df['px_last'] = df['px_last'].astype(float)
        
        return df[['date', 'px_last']]
    
    def _load_equity_from_csv(self, ticker: str) -> pd.DataFrame:
        """Load equity prices from CSV file."""
        data_dir = self.config.data_dir / 'adhoc_pricing' / 'equity'
        csv_file = data_dir / f"{ticker} equity_pricing.csv"
        
        if not csv_file.exists():
            # Try alternative path
            csv_file = self.config.data_dir / 'raw_data' / f"{ticker}_prices.csv"
        
        if not csv_file.exists():
            raise FileNotFoundError(
                f"Could not find equity pricing file for {ticker}. "
                f"Expected at: {csv_file}"
            )
        
        df = pd.read_csv(csv_file)
        
        # Ensure required columns exist
        if 'date' not in df.columns or 'px_last' not in df.columns:
            raise ValueError(f"CSV file must contain 'date' and 'px_last' columns")
        
        df['date'] = pd.to_datetime(df['date'])
        df['px_last'] = df['px_last'].astype(float)
        
        return df[['date', 'px_last']]
    
    def load_option_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Load option chain prices.
        
        Args:
            ticker: Underlying ticker (e.g., 'SPY US')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with columns: date, ticker, field, value
        """
        if self.use_database:
            return self._load_options_from_db(ticker, start_date, end_date)
        else:
            return self._load_options_from_csv(ticker)
    
    def _load_options_from_db(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Load option prices from database."""
        # For LIKE queries with %, we need to append the % in the parameter
        ticker_pattern = f"{ticker}%"
        query = """
            SELECT [date], [ticker], [field], [value]
            FROM [dbo].[market_data]
            WHERE [ticker] LIKE ?
                AND [date] >= ?
                AND [date] <= ?
            ORDER BY [date], [ticker]
        """
        
        # Use parameterized query to prevent SQL injection
        import pandas as pd
        df = pd.read_sql(query, self.db.conn, params=(ticker_pattern, start_date, end_date))
        df['value'] = df['value'].astype(float)
        
        return df
    
    def _load_options_from_csv(self, ticker: str) -> pd.DataFrame:
        """Load option prices from CSV file."""
        data_dir = self.config.data_dir / 'adhoc_pricing' / 'options'
        csv_file = data_dir / f"{ticker}_backtest_format_options.csv"
        
        if not csv_file.exists():
            # Try alternative naming
            csv_file = data_dir / f"{ticker}_options.csv"
        
        if not csv_file.exists():
            raise FileNotFoundError(
                f"Could not find option pricing file for {ticker}. "
                f"Expected at: {csv_file}"
            )
        
        df = pd.read_csv(csv_file)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = df['value'].astype(float)
        
        return df
    
    def load_dividends(self, ticker: str) -> pd.DataFrame:
        """Load dividend schedule for a ticker.
        
        Args:
            ticker: Stock ticker
            
        Returns:
            DataFrame with columns: date, amount
        """
        if self.use_database:
            return self._load_dividends_from_db(ticker)
        else:
            return self._load_dividends_from_csv(ticker)
    
    def _load_dividends_from_db(self, ticker: str) -> pd.DataFrame:
        """Load dividends from database."""
        query = """
            SELECT [date], [value] as amount
            FROM [dbo].[market_data]
            WHERE ticker = ? 
                AND field = 'dvd_amt'
            ORDER BY [date]
        """
        
        # Use parameterized query to prevent SQL injection
        import pandas as pd
        df = pd.read_sql(query, self.db.conn, params=(ticker,))
        df['amount'] = df['amount'].astype(float)
        
        return df
    
    def _load_dividends_from_csv(self, ticker: str) -> pd.DataFrame:
        """Load dividends from CSV file."""
        data_dir = self.config.data_dir / 'dividends'
        csv_file = data_dir / f"{ticker}_dividends.csv"
        
        if not csv_file.exists():
            # Return empty DataFrame if no dividend file exists
            return pd.DataFrame(columns=['date', 'amount'])
        
        df = pd.read_csv(csv_file)
        df['date'] = pd.to_datetime(df['date'])
        df['amount'] = df['amount'].astype(float)
        
        return df
    
    def load_fx_rates(
        self,
        currency_pair: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Load FX rates.
        
        Args:
            currency_pair: Currency pair (e.g., 'USDCAD')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with columns: date, rate
        """
        if self.use_database:
            return self._load_fx_from_db(currency_pair, start_date, end_date)
        else:
            return self._load_fx_from_csv(currency_pair)
    
    def _load_fx_from_db(
        self,
        currency_pair: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Load FX rates from database."""
        query = """
            SELECT [date], [value] as rate
            FROM [dbo].[market_data]
            WHERE ticker = ? 
                AND [date] >= ? 
                AND [date] <= ?
            ORDER BY [date]
        """
        
        # Use parameterized query to prevent SQL injection
        import pandas as pd
        df = pd.read_sql(query, self.db.conn, params=(currency_pair, start_date, end_date))
        df['rate'] = df['rate'].astype(float)
        
        return df
    
    def _load_fx_from_csv(self, currency_pair: str) -> pd.DataFrame:
        """Load FX rates from CSV file."""
        data_dir = self.config.data_dir / 'fx'
        csv_file = data_dir / f"{currency_pair}.csv"
        
        if not csv_file.exists():
            # Return DataFrame with 1.0 exchange rate if not found
            return pd.DataFrame(columns=['date', 'rate'])
        
        df = pd.read_csv(csv_file)
        df['date'] = pd.to_datetime(df['date'])
        df['rate'] = df['rate'].astype(float)
        
        return df
    
    def close(self):
        """Close database connection if open."""
        if self.db:
            self.db.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
