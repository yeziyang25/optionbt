# Migration Guide: Legacy to New Option Backtest Framework

This guide helps you migrate from the legacy option backtest system to the new, cleaner framework.

## Overview of Changes

### What's Different?

| Aspect | Legacy System | New System |
|--------|--------------|------------|
| **Structure** | Scattered files, mixed concerns | Clean modular architecture with src layout |
| **Configuration** | CSV files with complex structure | Simple YAML configuration files |
| **Dependencies** | Proprietary (ApolloGX, im_dev) + Bloomberg | Standard Python packages, data-agnostic |
| **Data** | Hard-coded Bloomberg/database calls | Pluggable data providers (CSV, custom) |
| **Paths** | Windows-only (Z:\\) | Cross-platform |
| **Models** | 4 separate backtest variants | Single unified engine with strategies |
| **Entry Points** | Multiple run*.py files | Single CLI + programmatic API |

## Migration Steps

### 1. Understanding the Mapping

#### Legacy Components → New Components

**Legacy:**
```
run_targetpay.py                    → optionbt CLI or BacktestEngine
models/btbuilder_customized.py     → engine/backtest.py
helper_functions/securities.py      → core/security.py + data/loader.py
runs/portfolio_configs.csv          → configs/*.yaml
runs/create_custom_options_*.py     → Handled by data loader + strategy
```

#### Configuration Migration

**Legacy CSV config:**
```csv
backtest,sec_id,allocation,pct_otm,DTM,rebal_rule,target_yield
SPY,equity1,1,0,6,O,-1
SPY,option1,-0.5,0,6,O,-1
```

**New YAML config:**
```yaml
backtest:
  name: "SPY"
  initial_capital: 10000000

strategy:
  type: "covered_call"
  moneyness: 0.0  # pct_otm
  coverage_ratio: 0.5  # -0.5 allocation
  roll_frequency: "weekly"  # DTM=6 ≈ weekly
  rebalance_rule: "O"
  target_yield: null  # -1 in legacy
```

### 2. Data Migration

#### From Bloomberg/Database to CSV

The new system uses a simple CSV format that you can populate from any source.

**Equity Data Format** (`data/equity/{TICKER}.csv`):
```csv
date,open,high,low,close,volume
2020-01-02,324.87,325.25,323.34,324.87,123456
2020-01-03,323.54,325.15,322.30,324.68,234567
```

Minimum required: `date, close`

**Option Data Format** (`data/options/{TICKER}_options.csv`):
```csv
date,strike,expiration,call_put,bid,ask,underlying_price
2020-01-02,320,2020-01-17,call,5.10,5.20,324.87
2020-01-02,325,2020-01-17,call,2.80,2.90,324.87
```

Required columns: `date, strike, expiration, call_put, bid, ask`

#### Exporting from Legacy System

If you have data in the legacy database format, export it:

```python
# Example export script
import pandas as pd
from im_prod.std_lib import common

conn = common.db_connection()

# Export equity data
query = "SELECT date, close FROM equity_prices WHERE ticker = 'SPY'"
df = pd.read_sql(query, conn)
df.to_csv("data/equity/SPY.csv", index=False)

# Export option data
query = """
SELECT date, strike, expiration, call_put, bid, ask, underlying_price
FROM option_prices
WHERE underlying = 'SPY'
"""
df = pd.read_sql(query, conn)
df.to_csv("data/options/SPY_options.csv", index=False)
```

### 3. Running Backtests

#### Legacy Way:
```python
# Edit run_targetpay.py line 164
backtestname = "SPY"
# Run the file
python run_targetpay.py
```

#### New Way (CLI):
```bash
# Create config once
python -m optionbt.cli run --config configs/spy_covered_call.yaml
```

#### New Way (Programmatic):
```python
from datetime import date
from optionbt.core.portfolio import Portfolio
from optionbt.core.strategy import CoveredCallStrategy, StrategyConfig
from optionbt.data.loader import DataLoader, CSVDataProvider
from optionbt.engine.backtest import BacktestEngine

# Load data
provider = CSVDataProvider("data")
loader = DataLoader(provider)
equity = loader.load_equity("SPY", date(2020,1,1), date(2024,12,31))
options = loader.load_options("SPY", "SPY", date(2020,1,1), date(2024,12,31))

# Create portfolio and strategy
portfolio = Portfolio("SPY CC", initial_capital=10_000_000)
config = StrategyConfig(
    name="Monthly CC",
    strategy_type="covered_call",
    moneyness=0.02,
    coverage_ratio=0.5
)
strategy = CoveredCallStrategy(config)

# Run backtest
engine = BacktestEngine(portfolio, strategy, equity, options,
                        date(2020,1,1), date(2024,12,31))
results = engine.run()
```

### 4. Output Files Comparison

| Legacy Output | New Output | Notes |
|--------------|-----------|-------|
| `{name}_aggregate_{model}.csv` | `{name}_performance.csv` | Daily portfolio values |
| `{name}_detailed_{model}.csv` | Included in performance | Position-level tracking |
| `{name}_period_return_{model}.csv` | Computed from performance | Period returns |
| `{name}_option_list_{model}.csv` | `{name}_trades.csv` | Trade history |
| PNG plots | Same (optional) | Via visualization utils |

### 5. Advanced Features

#### Target Yield Strategy

**Legacy:**
```python
target_yield = 0.15  # in portfolio_configs.csv
```

**New:**
```yaml
strategy:
  target_yield: 0.15  # Automatically adjusts coverage
```

#### Custom Rebalance Rules

**Legacy:**
```
rebal_rule: Q  # Quarterly
```

**New:**
```yaml
strategy:
  rebalance_rule: "Q"  # Q, S, A, or O
```

#### Call Spreads

**Legacy:** Manual calculation of allocations

**New:**
```python
from optionbt.core.strategy import CallSpreadStrategy

strategy = CallSpreadStrategy(
    config=StrategyConfig(...),
    long_moneyness=-0.20  # 20% ITM long call
)
```

### 6. Extending the Framework

#### Custom Data Provider

```python
from optionbt.data.loader import DataProvider

class BloombergProvider(DataProvider):
    def load_equity_data(self, ticker, start_date, end_date):
        # Your Bloomberg API calls
        return df
    
    def load_option_data(self, ticker, start_date, end_date, option_type=None):
        # Your Bloomberg option data
        return df
```

#### Custom Strategy

```python
from optionbt.core.strategy import Strategy

class MyCustomStrategy(Strategy):
    def select_option(self, available_options, current_date, 
                     underlying_price, equity_position_size):
        # Your custom logic
        return selected_option
```

## Common Migration Issues

### Issue: "Can't find im_dev module"
**Solution:** The new system doesn't use proprietary libraries. Use CSV data or create a custom provider.

### Issue: "Invalid path Z:\\..."
**Solution:** New system uses relative paths. Data goes in `data/` directory.

### Issue: "Different results from legacy"
**Possible causes:**
1. Bid/ask handling - check `use_bid_ask` setting
2. Date calculation - verify roll dates match
3. Premium reinvestment - legacy had this optional, new system tracks separately

### Issue: "Missing option data"
**Solution:** Ensure your CSV has data for all required dates and strikes. Check expiration dates match roll dates.

## Getting Help

- Check `examples/` directory for working examples
- Review the main README.md for API documentation
- Run the simple example: `python examples/simple_backtest.py`

## Benefits of New System

✅ **Cleaner code** - Modular, testable, maintainable
✅ **Faster development** - Easy to add new strategies
✅ **Portable** - Works on any OS, no proprietary dependencies  
✅ **Flexible** - Plug in any data source
✅ **Modern** - Type hints, proper Python packaging
✅ **Documented** - Clear examples and API docs
