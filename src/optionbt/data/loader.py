"""
Data loader classes for loading market data from various sources.

Includes:
* :class:`CSVDataProvider` – load equity / option data from CSV files.
* :class:`DatabaseDataProvider` – load data via a DB connection that
  follows the legacy ``common.db_connection()`` interface (any object
  with a ``query_tbl(sql) -> DataFrame`` method).
"""

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

from src.optionbt.core.security import Equity, Option, OptionType


class DataProvider(ABC):
    """Abstract base class for data providers."""
    
    @abstractmethod
    def load_equity_data(
        self,
        ticker: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Load equity price data.
        
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        pass
    
    @abstractmethod
    def load_option_data(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        option_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load option chain data.
        
        Returns:
            DataFrame with columns: date, strike, expiration, call_put, bid, ask, underlying_price
        """
        pass


class CSVDataProvider(DataProvider):
    """Load data from CSV files."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize CSV data provider.
        
        Args:
            data_dir: Directory containing data files
        """
        self.data_dir = Path(data_dir)
    
    def load_equity_data(
        self,
        ticker: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """Load equity data from CSV file."""
        file_path = self.data_dir / "equity" / f"{ticker}.csv"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Equity data file not found: {file_path}")
        
        df = pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"])
        
        # Filter by date range
        mask = (df["date"] >= pd.Timestamp(start_date)) & (df["date"] <= pd.Timestamp(end_date))
        df = df[mask].copy()
        
        # Ensure required columns exist
        required_cols = ["date", "close"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column '{col}' in {file_path}")
        
        return df.sort_values("date").reset_index(drop=True)
    
    def load_option_data(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        option_type: Optional[str] = None
    ) -> pd.DataFrame:
        """Load option data from CSV file."""
        file_path = self.data_dir / "options" / f"{ticker}_options.csv"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Option data file not found: {file_path}")
        
        df = pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"])
        df["expiration"] = pd.to_datetime(df["expiration"])
        
        # Filter by date range
        mask = (df["date"] >= pd.Timestamp(start_date)) & (df["date"] <= pd.Timestamp(end_date))
        df = df[mask].copy()
        
        # Filter by option type if specified
        if option_type:
            df = df[df["call_put"].str.lower() == option_type.lower()]
        
        # Ensure required columns exist
        required_cols = ["date", "strike", "expiration", "call_put", "bid", "ask"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column '{col}' in {file_path}")
        
        return df.sort_values(["date", "expiration", "strike"]).reset_index(drop=True)


class DataLoader:
    """
    High-level data loader that creates Security objects from data.
    """
    
    def __init__(self, provider: DataProvider):
        """
        Initialize data loader.
        
        Args:
            provider: Data provider instance
        """
        self.provider = provider
    
    def load_equity(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        currency: str = "USD"
    ) -> Equity:
        """
        Load equity security with price data.
        
        Args:
            ticker: Equity ticker symbol
            start_date: Start date for data
            end_date: End date for data
            currency: Currency of the equity
            
        Returns:
            Equity object with price data loaded
        """
        df = self.provider.load_equity_data(ticker, start_date, end_date)
        
        # Convert to price dictionary {date: price}
        price_data = {}
        for _, row in df.iterrows():
            price_data[row["date"].date()] = row["close"]
        
        equity = Equity(
            ticker=ticker,
            currency=currency,
            price_data=price_data
        )
        
        return equity
    
    def load_options(
        self,
        ticker: str,
        underlying_ticker: str,
        start_date: date,
        end_date: date,
        option_type: Optional[str] = None,
        currency: str = "USD"
    ) -> List[Option]:
        """
        Load option securities with pricing data.
        
        Args:
            ticker: Option ticker/identifier
            underlying_ticker: Ticker of underlying equity
            start_date: Start date for data
            end_date: End date for data
            option_type: Filter by "call" or "put"
            currency: Currency of the options
            
        Returns:
            List of Option objects with pricing data loaded
        """
        df = self.provider.load_option_data(ticker, start_date, end_date, option_type)
        
        # Group by unique option (strike, expiration, call_put)
        options = []
        grouped = df.groupby(["strike", "expiration", "call_put"])
        
        for (strike, expiration, call_put), group in grouped:
            # Create pricing dictionaries
            bid_data = {}
            ask_data = {}
            underlying_price_data = {}
            
            for _, row in group.iterrows():
                row_date = row["date"].date()
                bid_data[row_date] = row["bid"]
                ask_data[row_date] = row["ask"]
                if "underlying_price" in row:
                    underlying_price_data[row_date] = row["underlying_price"]
            
            # Create option object
            opt_type = OptionType.CALL if call_put.lower() == "call" else OptionType.PUT
            exp_date = expiration.date() if hasattr(expiration, 'date') else expiration
            
            option = Option(
                ticker=f"{ticker}_{strike}_{exp_date}_{call_put}",
                underlying_ticker=underlying_ticker,
                strike=float(strike),
                expiration=exp_date,
                option_type=opt_type,
                currency=currency,
                bid_data=bid_data,
                ask_data=ask_data,
                underlying_price_data=underlying_price_data
            )
            
            options.append(option)
        
        return options
    
    def load_option_chain(
        self,
        ticker: str,
        underlying_ticker: str,
        trade_date: date,
        start_date: date,
        end_date: date,
        currency: str = "USD"
    ) -> List[Option]:
        """
        Load all available options for a specific trade date.
        
        This loads the full option chain available on trade_date.
        """
        return self.load_options(
            ticker=ticker,
            underlying_ticker=underlying_ticker,
            start_date=start_date,
            end_date=end_date,
            currency=currency
        )


class DatabaseDataProvider(DataProvider):
    """Load data from a database using the legacy ``common.db_connection()`` API.

    This provider accepts any connection object that exposes a
    ``query_tbl(sql) -> pd.DataFrame`` method – the same interface used by
    ``im_prod.std_lib.common.db_connection()``.  This makes it possible to
    plug the old database workflow directly into the new framework::

        from im_prod.std_lib.common import db_connection
        conn = db_connection()
        provider = DatabaseDataProvider(conn)

    If the proprietary library is *not* available you can implement a thin
    wrapper around any SQL engine (see :class:`~optionbt.utils.common.DbConnection`).

    The default SQL queries match the ``market_data`` table schema used in the
    legacy codebase.  Override the query builder methods to adapt to a
    different schema.
    """

    def __init__(self, db_conn, source_hierarchy: Optional[List[str]] = None):
        """
        Args:
            db_conn: Connection object with a ``query_tbl(sql)`` method.
            source_hierarchy: Ordered list of data sources to prefer when
                deduplicating (first = highest priority).  Defaults to
                ``["bloomberg", "solactive", "mellon"]``.
        """
        self._conn = db_conn
        self._source_hierarchy = source_hierarchy or [
            "bloomberg", "solactive", "mellon",
        ]

    # -- DataProvider interface ---------------------------------------------

    def load_equity_data(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Load equity price data from the ``market_data`` table."""
        query = (
            f"SELECT [date], CAST([value] AS FLOAT) AS close, source "
            f"FROM market_data "
            f"WHERE ticker = '{ticker}' AND field = 'px_last' "
            f"AND [date] >= '{start_date}' AND [date] <= '{end_date}';"
        )
        df = self._conn.query_tbl(query)
        if df.empty:
            raise FileNotFoundError(
                f"No equity data found in database for {ticker}"
            )
        df["date"] = pd.to_datetime(df["date"])
        df["close"] = df["close"].astype(float)

        # Deduplicate by source priority (mirrors helper_functions/securities.py)
        hierarchy = {s: i for i, s in enumerate(self._source_hierarchy)}
        df["_rank"] = df["source"].map(hierarchy).fillna(len(hierarchy))
        df = (
            df.sort_values(["date", "_rank"])
            .drop_duplicates(subset="date", keep="first")
            .drop(columns=["_rank", "source"])
        )
        return df.sort_values("date").reset_index(drop=True)

    def load_option_data(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        option_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load option chain data from the ``market_data`` table.

        The legacy schema stores bid / ask as separate rows (field =
        ``px_bid`` / ``px_ask``).  This method pivots them into the
        ``bid`` / ``ask`` columns expected by the framework.
        """
        query = (
            f"SELECT [ticker], [date], [field], CAST([value] AS FLOAT) AS value "
            f"FROM market_data "
            f"WHERE ticker LIKE '{ticker}%' "
            f"AND (field = 'px_ask' OR field = 'px_bid') "
            f"AND [date] >= '{start_date}' AND [date] <= '{end_date}';"
        )
        raw = self._conn.query_tbl(query)
        if raw.empty:
            raise FileNotFoundError(
                f"No option data found in database for {ticker}"
            )

        raw["date"] = pd.to_datetime(raw["date"])
        raw["value"] = raw["value"].astype(float)

        # Parse option ticker components
        from src.optionbt.utils.common import extract_option_ticker

        info = extract_option_ticker(raw, "ticker")

        raw["strike"] = raw["ticker"].map(info.strike)
        raw["expiration"] = pd.to_datetime(
            raw["ticker"].map(
                {k: v.strftime("%Y-%m-%d") for k, v in info.expiry.items()}
            )
        )
        raw["call_put"] = raw["ticker"].map(info.option_type).map(
            {"C": "call", "P": "put"}
        )

        # Pivot bid/ask into separate columns
        pivoted = raw.pivot_table(
            index=["ticker", "date", "strike", "expiration", "call_put"],
            columns="field",
            values="value",
            aggfunc="first",
        ).reset_index()
        pivoted = pivoted.rename(
            columns={"px_bid": "bid", "px_ask": "ask"}
        )

        # Filter by option type if specified
        if option_type:
            pivoted = pivoted[pivoted["call_put"].str.lower() == option_type.lower()]

        required_cols = ["date", "strike", "expiration", "call_put", "bid", "ask"]
        for col in required_cols:
            if col not in pivoted.columns:
                raise ValueError(f"Missing required column '{col}' after pivot")

        return pivoted.sort_values(
            ["date", "expiration", "strike"]
        ).reset_index(drop=True)

    # -- convenience --------------------------------------------------------

    def load_dividends(self, ticker: str) -> Dict[date, float]:
        """Load dividend schedule from the ``dividends`` table.

        Returns a ``{date: amount}`` dictionary matching the convention
        used by ``helper_functions/securities.py``.
        """
        query = (
            f"SELECT ex_date, CAST(dvd_amount AS FLOAT) AS dvd_amount "
            f"FROM dividends WHERE ticker = '{ticker.upper()}';"
        )
        df = self._conn.query_tbl(query)
        if df.empty:
            return {}
        df["ex_date"] = pd.to_datetime(df["ex_date"]).dt.date
        df["dvd_amount"] = df["dvd_amount"].astype(float)
        return dict(zip(df["ex_date"], df["dvd_amount"]))
