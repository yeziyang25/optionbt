# Data Requirements for Option Backtest Framework

This document outlines the data requirements for running backtests with the new option backtest framework.

## Overview

The framework is designed to be **data source agnostic**, meaning you can use data from any provider (Bloomberg, CSV files, database, custom API, etc.). This document describes the required data format and how to provide it.

## Required Data Types

### 1. Equity Price Data

Historical daily equity prices for the underlying securities.

**Required Columns:**
- `date` (datetime): Trading date
- `close` (float): Closing price

**Optional Columns:**
- `open` (float): Opening price
- `high` (float): Daily high
- `low` (float): Daily low
- `volume` (int): Trading volume

**Example Format (CSV):**
```csv
date,open,high,low,close,volume
2020-01-02,324.87,325.25,323.34,324.87,123456
2020-01-03,323.54,325.15,322.30,324.68,234567
2020-01-06,322.25,326.20,321.80,325.94,345678
```

**File Location:** `data/equity/{TICKER}.csv`

### 2. Option Chain Data

Historical option pricing data including strikes, expirations, and bid/ask prices.

**Required Columns:**
- `date` (datetime): Trading date
- `strike` (float): Strike price
- `expiration` (datetime): Option expiration date
- `call_put` (string): "call" or "put"
- `bid` (float): Bid price
- `ask` (float): Ask price

**Optional Columns:**
- `underlying_price` (float): Underlying equity price on that date
- `volume` (int): Option contract volume
- `open_interest` (int): Open interest
- `implied_volatility` (float): Implied volatility

**Example Format (CSV):**
```csv
date,strike,expiration,call_put,bid,ask,underlying_price
2020-01-02,320,2020-01-17,call,5.10,5.20,324.87
2020-01-02,325,2020-01-17,call,2.80,2.90,324.87
2020-01-02,330,2020-01-17,call,1.10,1.20,324.87
2020-01-02,320,2020-01-17,put,0.15,0.25,324.87
2020-01-03,320,2020-01-17,call,4.90,5.00,324.68
```

**File Location:** `data/options/{TICKER}_options.csv`

### 3. Holiday Calendar (Optional)

List of market holidays to exclude from trading days.

**Format:** List of dates

**Example:**
```python
holidays = [
    date(2020, 1, 1),   # New Year's Day
    date(2020, 1, 20),  # MLK Day
    date(2020, 2, 17),  # Presidents' Day
    # ...
]
```

## Data Requirements by Strategy Type

### Covered Call Strategy
- **Equity data:** Full price history for underlying
- **Option data:** Call options with multiple strikes and expirations
- **Recommended:** At least 3-5 strikes around current price (ATM, OTM)
- **Expiration frequency:** Match roll frequency (weekly, monthly, etc.)

### Call Spread Strategy
- **Equity data:** Full price history
- **Option data:** Call options with multiple strikes
- **Recommended:** Wide range of strikes (ITM to OTM)
- **Special:** Need both long and short strikes available

### Target Yield Strategy
- **Equity data:** Full price history
- **Option data:** Dense strike coverage to find yield target
- **Recommended:** At least 10+ strikes across 80%-120% of current price

## Data Coverage Requirements

### Minimum Data Requirements

For a backtest from **2020-01-01** to **2024-12-31**:

1. **Equity Data:**
   - Every trading day in the period
   - Approximately 1,260 data points (252 trading days/year × 5 years)

2. **Option Data:**
   - For **monthly rolls:** ~60 expiration cycles × 10 strikes = 600 unique options
   - For **weekly rolls:** ~260 expiration cycles × 10 strikes = 2,600 unique options
   - Each option needs pricing data for all days it's tradeable

### Recommended Data Coverage

- **Strike range:** 80% to 120% of underlying price
- **Number of strikes:** 10-15 per expiration
- **Bid-ask spread:** Should reflect realistic market conditions
- **Data frequency:** Daily (intraday not currently supported)

## Data Sources

### Option 1: Generate Sample Data (For Testing)

Use the provided sample data generator:

```bash
python examples/generate_sample_data.py --ticker SPY --start 2020-01-01 --end 2024-12-31
```

This creates synthetic but realistic data for testing.

### Option 2: Bloomberg Terminal

If you have Bloomberg access, export data:

```python
import pandas as pd
from xbbg import blp

# Equity data
equity_df = blp.bdh(
    tickers='SPY US Equity',
    flds=['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'VOLUME'],
    start_date='2020-01-01',
    end_date='2024-12-31'
)

# Option data (requires option ticker list)
# See legacy code in data_download/ for examples
```

### Option 3: CSV Export from Existing Database

If you have the legacy system running with populated database:

```python
# See MIGRATION.md for export scripts
from im_prod.std_lib import common
import pandas as pd

conn = common.db_connection()

# Export equity
query = """
    SELECT date, open, high, low, close, volume
    FROM equity_prices
    WHERE ticker = 'SPY'
    ORDER BY date
"""
equity_df = pd.read_sql(query, conn)
equity_df.to_csv('data/equity/SPY.csv', index=False)

# Export options
query = """
    SELECT date, strike, expiration, call_put, bid, ask, underlying_price
    FROM option_prices
    WHERE underlying = 'SPY'
    ORDER BY date, expiration, strike
"""
options_df = pd.read_sql(query, conn)
options_df.to_csv('data/options/SPY_options.csv', index=False)
```

### Option 4: Custom Data Provider

Implement your own data provider:

```python
from optionbt.data.loader import DataProvider
import pandas as pd

class MyCustomProvider(DataProvider):
    def load_equity_data(self, ticker, start_date, end_date):
        # Your custom logic here
        # Return DataFrame with required columns
        return df
    
    def load_option_data(self, ticker, start_date, end_date, option_type=None):
        # Your custom logic here
        # Return DataFrame with required columns
        return df

# Use it
from optionbt.data.loader import DataLoader
provider = MyCustomProvider()
loader = DataLoader(provider)
```

## Data Quality Checks

Before running backtests, verify your data:

### 1. Completeness
- No missing dates in equity data
- Option data covers all roll dates
- All required columns present

### 2. Consistency
- Bid < Ask for all options
- Option prices non-negative
- Underlying prices in option data match equity data

### 3. Reasonableness
- No extreme outliers (check for data errors)
- Bid-ask spreads realistic (typically 1-10% for most options)
- Expiration dates are valid option expiry dates (usually 3rd Friday)

## Example: Preparing SPY Data

For a SPY covered call backtest (2020-2024):

**Step 1:** Identify requirements
- Strategy: Monthly covered calls at 2% OTM
- Roll frequency: Monthly (12 rolls/year × 5 years = 60 rolls)
- Need: 60 monthly expirations × ~10 strikes = ~600 unique options

**Step 2:** Export or generate data
```bash
# Option A: Generate synthetic data
python examples/generate_sample_data.py --ticker SPY

# Option B: Export from Bloomberg/database
# (see examples above)
```

**Step 3:** Verify data
```python
import pandas as pd

# Check equity data
equity = pd.read_csv('data/equity/SPY.csv')
print(f"Equity data: {len(equity)} days")
print(f"Date range: {equity['date'].min()} to {equity['date'].max()}")

# Check option data
options = pd.read_csv('data/options/SPY_options.csv')
print(f"Option data: {len(options)} rows")
print(f"Unique options: {options.groupby(['strike', 'expiration', 'call_put']).ngroups}")
print(f"Expirations: {options['expiration'].nunique()}")
```

**Step 4:** Run backtest
```bash
python -m optionbt.cli run --config configs/spy_covered_call.yaml
```

## Common Data Issues

### Issue: "No price data available for {ticker} on {date}"
**Cause:** Missing data in equity or option CSV
**Solution:** Ensure data covers all trading days, check for gaps

### Issue: "Missing required column 'X'"
**Cause:** CSV file missing required column
**Solution:** Add the missing column or adjust data export

### Issue: "No options available for trading on {date}"
**Cause:** No options with pricing data on that date
**Solution:** Ensure option chain has data for all roll dates

## Need Help?

1. **Test with sample data first:** Use `generate_sample_data.py` to create test data
2. **Check examples:** See `examples/simple_backtest.py` for a working example
3. **Verify format:** Compare your CSV files to the examples above
4. **Custom provider:** If you have a unique data source, implement a custom `DataProvider`

## Summary

Minimum required data:
- ✅ Equity daily prices (date, close)
- ✅ Option chain data (date, strike, expiration, call_put, bid, ask)
- ✅ Data coverage for entire backtest period

That's it! The framework handles the rest.
