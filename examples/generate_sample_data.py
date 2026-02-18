"""
Generate sample data for testing the backtest framework.

This script creates synthetic equity and option data that can be used
to test the backtest framework without needing real market data.
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path


def generate_sample_equity_data(
    ticker: str,
    start_date: date,
    end_date: date,
    initial_price: float = 300.0,
    volatility: float = 0.20,
    drift: float = 0.10
) -> pd.DataFrame:
    """
    Generate synthetic equity price data.
    
    Uses geometric Brownian motion to simulate realistic price movements.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date
        end_date: End date
        initial_price: Starting price
        volatility: Annual volatility (e.g., 0.20 for 20%)
        drift: Annual drift/return (e.g., 0.10 for 10%)
    
    Returns:
        DataFrame with columns: date, open, high, low, close, volume
    """
    # Generate business days
    dates = pd.bdate_range(start=start_date, end=end_date)
    n_days = len(dates)
    
    # Daily parameters
    dt = 1/252  # One trading day
    daily_vol = volatility * np.sqrt(dt)
    daily_drift = (drift - 0.5 * volatility**2) * dt
    
    # Generate returns using GBM
    random_returns = np.random.normal(daily_drift, daily_vol, n_days)
    
    # Calculate prices
    price_multipliers = np.exp(random_returns)
    prices = initial_price * np.cumprod(price_multipliers)
    
    # Add some intraday variation for OHLC
    daily_range = 0.01  # 1% average daily range
    
    df = pd.DataFrame({
        'date': dates,
        'close': prices,
    })
    
    # Generate OHLC from close
    df['open'] = df['close'] * (1 + np.random.uniform(-daily_range/2, daily_range/2, n_days))
    df['high'] = df[['open', 'close']].max(axis=1) * (1 + np.random.uniform(0, daily_range, n_days))
    df['low'] = df[['open', 'close']].min(axis=1) * (1 - np.random.uniform(0, daily_range, n_days))
    df['volume'] = np.random.randint(50_000_000, 150_000_000, n_days)
    
    # Reorder columns
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    
    return df


def black_scholes_call_price(S, K, T, r, sigma):
    """
    Calculate Black-Scholes call option price.
    
    Args:
        S: Stock price
        K: Strike price
        T: Time to expiration (in years)
        r: Risk-free rate
        sigma: Volatility
    """
    from scipy.stats import norm
    
    if T <= 0:
        return max(0, S - K)
    
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    
    call_price = S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)
    return call_price


def generate_sample_option_data(
    ticker: str,
    equity_df: pd.DataFrame,
    strike_range: tuple = (0.9, 1.1),
    n_strikes: int = 11,
    expiry_type: str = "monthly",
    volatility: float = 0.25,
    risk_free_rate: float = 0.05,
    bid_ask_spread: float = 0.05
) -> pd.DataFrame:
    """
    Generate synthetic option chain data.
    
    Args:
        ticker: Underlying ticker
        equity_df: Equity price DataFrame
        strike_range: Tuple of (min_mult, max_mult) for strike generation
        n_strikes: Number of strikes to generate
        expiry_type: "monthly" or "weekly"
        volatility: Implied volatility
        risk_free_rate: Risk-free rate
        bid_ask_spread: Spread as fraction of mid price (e.g., 0.05 = 5%)
    
    Returns:
        DataFrame with columns: date, strike, expiration, call_put, bid, ask, underlying_price
    """
    try:
        from scipy.stats import norm
    except ImportError:
        print("Warning: scipy not installed. Installing...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'scipy'])
        from scipy.stats import norm
    
    records = []
    
    for _, row in equity_df.iterrows():
        current_date = pd.Timestamp(row['date']).date()
        underlying_price = row['close']
        
        # Determine next expiration date
        if expiry_type == "monthly":
            # Third Friday of current or next month
            current_month_third_friday = get_third_friday(current_date.year, current_date.month)
            if current_date < current_month_third_friday:
                expiration = current_month_third_friday
            else:
                # Next month
                if current_date.month == 12:
                    expiration = get_third_friday(current_date.year + 1, 1)
                else:
                    expiration = get_third_friday(current_date.year, current_date.month + 1)
        else:
            # Next Friday
            days_ahead = 4 - current_date.weekday()  # 4 = Friday
            if days_ahead <= 0:
                days_ahead += 7
            expiration = current_date + timedelta(days=days_ahead)
        
        # Generate strikes around current price
        strikes = np.linspace(
            underlying_price * strike_range[0],
            underlying_price * strike_range[1],
            n_strikes
        )
        
        # Time to expiration in years
        dte = (expiration - current_date).days
        T = dte / 365.0
        
        # Generate option prices for each strike
        for strike in strikes:
            # Calculate theoretical price using Black-Scholes
            if T > 0:
                call_mid = black_scholes_call_price(
                    underlying_price, strike, T, risk_free_rate, volatility
                )
            else:
                call_mid = max(0, underlying_price - strike)
            
            # Add bid-ask spread
            spread = call_mid * bid_ask_spread
            call_bid = max(0, call_mid - spread/2)
            call_ask = call_mid + spread/2
            
            records.append({
                'date': current_date,
                'strike': round(strike, 2),
                'expiration': expiration,
                'call_put': 'call',
                'bid': round(call_bid, 2),
                'ask': round(call_ask, 2),
                'underlying_price': round(underlying_price, 2)
            })
    
    df = pd.DataFrame(records)
    return df


def get_third_friday(year: int, month: int) -> date:
    """Get the third Friday of a given month."""
    first_day = date(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    third_friday = first_friday + timedelta(days=14)
    return third_friday


def generate_all_sample_data(
    ticker: str = "SPY",
    start_date: date = date(2020, 1, 1),
    end_date: date = date(2024, 12, 31),
    output_dir: str = "data"
):
    """
    Generate complete sample dataset for a ticker.
    
    Args:
        ticker: Ticker symbol
        start_date: Start date
        end_date: End date
        output_dir: Output directory
    """
    print(f"Generating sample data for {ticker}...")
    print(f"Period: {start_date} to {end_date}")
    
    # Create output directories
    equity_dir = Path(output_dir) / "equity"
    options_dir = Path(output_dir) / "options"
    equity_dir.mkdir(parents=True, exist_ok=True)
    options_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate equity data
    print("\nGenerating equity data...")
    equity_df = generate_sample_equity_data(ticker, start_date, end_date)
    equity_file = equity_dir / f"{ticker}.csv"
    equity_df.to_csv(equity_file, index=False)
    print(f"✓ Saved {len(equity_df)} days of equity data to {equity_file}")
    
    # Generate option data
    print("\nGenerating option data...")
    option_df = generate_sample_option_data(ticker, equity_df)
    option_file = options_dir / f"{ticker}_options.csv"
    option_df.to_csv(option_file, index=False)
    print(f"✓ Saved {len(option_df)} option quotes to {option_file}")
    
    print(f"\n✓ Sample data generation complete!")
    print(f"\nYou can now run backtests using:")
    print(f"  python examples/simple_backtest.py")
    print(f"  python -m optionbt.cli run --config configs/spy_covered_call.yaml")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample backtest data")
    parser.add_argument("--ticker", default="SPY", help="Ticker symbol")
    parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2024-12-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="data", help="Output directory")
    
    args = parser.parse_args()
    
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    
    generate_all_sample_data(args.ticker, start, end, args.output)
