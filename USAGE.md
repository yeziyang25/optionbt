# Usage Guide

This guide provides detailed instructions for using the refactored option backtest framework.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [Data Setup](#data-setup)
4. [Running Backtests](#running-backtests)
5. [Output Files](#output-files)
6. [Advanced Usage](#advanced-usage)

## Quick Start

### Minimal Example

1. Ensure you have a backtest configured in `runs/portfolio_configs.csv`
2. Run the backtest:

```bash
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10
```

That's it! Results will be in `output/{timestamp}_SPY/`

## Configuration

### Portfolio Configuration

Edit `runs/portfolio_configs.csv` to define your backtest:

```csv
backtest,sec_id,sec_name,sec_type,currency,allocation,option_w_against,start_date,...
SPY,cash,Cash,cash,USD,10000000,,2018-01-03,...
SPY,equity1,SPY US,equity,USD,1,,,,...
SPY,option1,SPY US,call option,USD,-0.5,equity1,,,...
```

#### Column Definitions

- **backtest**: Unique identifier for this backtest
- **sec_id**: Security ID (cash, equity1, option1, etc.)
- **sec_name**: Ticker symbol (e.g., "SPY US", "QQQ US")
- **sec_type**: Type of security
  - `cash`: Cash position
  - `equity`: Stock/ETF
  - `call option`: Call option
  - `put option`: Put option
  - `fx_fwd`: FX forward contract
- **currency**: USD, CAD, etc.
- **allocation**: 
  - For cash: Initial capital (e.g., 10000000)
  - For equity: Percentage of portfolio (e.g., 1 = 100%)
  - For options: Coverage ratio (e.g., -0.5 = write calls on 50% of equity)
- **option_w_against**: For options, which equity they're written against (e.g., "equity1")
- **start_date**: Backtest start date (YYYY-MM-DD or M/D/YYYY)

### System Configuration (Optional)

Create `config.yaml` for system-wide settings:

```yaml
# Database configuration
database:
  enabled: false  # Set to true if using database
  server: "${DB_SERVER}"
  database: "${DB_NAME}"
  username: "${DB_USERNAME}"
  password: "${DB_PASSWORD}"

# Directory paths
paths:
  data_dir: "data_download"
  output_dir: "output"
  config_dir: "runs"

# Backtest defaults
backtest:
  base_currency: "USD"
  reinvest_premium: true
  eod_pricing_method: "mid"
```

Use with:
```bash
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --config config.yaml
```

## Data Setup

### Option 1: CSV Files (Recommended for Testing)

Create the following directory structure:

```
data_download/
├── adhoc_pricing/
│   ├── equity/
│   │   ├── SPY US equity_pricing.csv
│   │   └── QQQ US equity_pricing.csv
│   └── options/
│       ├── SPY US_backtest_format_options.csv
│       └── QQQ US_backtest_format_options.csv
├── dividends/
│   ├── SPY US_dividends.csv
│   └── QQQ US_dividends.csv
└── fx/
    └── USDCAD.csv
```

#### Equity Pricing Format

File: `data_download/adhoc_pricing/equity/SPY US equity_pricing.csv`

```csv
date,px_last
2020-01-02,324.87
2020-01-03,325.12
2020-01-06,323.87
...
```

#### Option Pricing Format

File: `data_download/adhoc_pricing/options/SPY US_backtest_format_options.csv`

```csv
date,ticker,field,value
2020-01-02,SPY US 01/31/20 C330,bid,2.15
2020-01-02,SPY US 01/31/20 C330,ask,2.25
2020-01-02,SPY US 01/31/20 C330,px_last,2.20
2020-01-02,SPY US 01/31/20 C335,bid,1.05
2020-01-02,SPY US 01/31/20 C335,ask,1.15
...
```

Fields required: `bid`, `ask`, and optionally `px_last`

#### Dividend Format

File: `data_download/dividends/SPY US_dividends.csv`

```csv
date,amount
2020-03-20,1.57
2020-06-19,1.46
2020-09-18,1.43
...
```

#### FX Rate Format

File: `data_download/fx/USDCAD.csv`

```csv
date,rate
2020-01-02,1.2987
2020-01-03,1.3012
...
```

### Option 2: Database

1. Set up environment variables:
```bash
export DB_SERVER="your-server.database.windows.net"
export DB_NAME="market-data-db"
export DB_USERNAME="your-username"
export DB_PASSWORD="your-password"
```

2. Create `config.yaml`:
```yaml
database:
  enabled: true
```

3. Run with database flag:
```bash
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --use-database --config config.yaml
```

## Running Backtests

### Basic Commands

```bash
# Simple backtest
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10

# Specify output directory
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --output-dir my_results

# Canadian market (TSX holidays)
python -m optionbt.backtest --backtest XIU --end-date 2025-09-10 --market CA

# Don't reinvest premiums
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --no-reinvest
```

### Command Line Options

```
--backtest BACKTEST        Name of backtest from portfolio_configs.csv (required)
--end-date END_DATE        End date (YYYY-MM-DD format) (required)
--config CONFIG            Path to config YAML file (optional)
--output-dir OUTPUT_DIR    Output directory (default: ./output)
--use-database             Use database instead of CSV files
--market {US,CA,TSX}       Market for holidays (default: US)
--no-reinvest              Don't reinvest option premiums
```

### Example Workflows

#### Testing a New Strategy

```bash
# 1. Create CSV files with sample data (just a few months)
# 2. Add configuration to portfolio_configs.csv
# 3. Run short backtest
python -m optionbt.backtest --backtest TEST --end-date 2020-03-31

# 4. Review output
ls output/*/TEST_*
```

#### Running Production Backtest

```bash
# 1. Ensure database is configured
# 2. Run full backtest
python -m optionbt.backtest \
  --backtest SPY \
  --end-date 2025-09-10 \
  --use-database \
  --config config.yaml \
  --output-dir production_results

# 3. Archive results
tar -czf backtest_$(date +%Y%m%d).tar.gz production_results/
```

## Output Files

Each backtest creates a timestamped directory with the following files:

### 1. Configuration File
`{backtest}_configuration.csv`

Records the exact configuration used for this backtest run.

### 2. Aggregate Report
`{backtest}_aggregate.csv`

Portfolio-level daily summary:

```csv
date,market_value,cash_inflow,cash_outflow,portfolio_value,daily_return,cumulative_return,drawdown
2020-01-03,10324870.0,0,0,10324870.0,0.0,0.0,0.0
2020-01-06,10298450.0,0,0,10298450.0,-0.00256,-0.00256,-0.00256
...
```

Columns:
- **date**: Trading date
- **market_value**: Total portfolio value
- **cash_inflow**: Premium received from option sales
- **cash_outflow**: Cash paid for option buybacks
- **portfolio_value**: Net portfolio value
- **daily_return**: Daily percentage return
- **cumulative_return**: Cumulative return since start
- **drawdown**: Drawdown from peak

### 3. Detailed Report
`{backtest}_detailed.csv`

Security-level daily positions:

```csv
date,sec_id,sec_name,sec_type,currency,open_qty,close_qty,eod_price,bid,ask,fx,market_value,cash_inflow,cash_outflow
2020-01-03,cash,Cash,cash,USD,10000000,10000000,1.0,1.0,1.0,1.0,10000000,0,0
2020-01-03,equity1,SPY US,equity,USD,0,30802.3,324.87,324.87,324.87,1.0,10006787.2,0,0
...
```

### Interpreting Results

```python
import pandas as pd

# Load aggregate results
df = pd.read_csv('output/{timestamp}_SPY/SPY_aggregate.csv')

# Calculate annualized return
total_days = (df['date'].max() - df['date'].min()).days
total_return = df['cumulative_return'].iloc[-1]
annualized_return = (1 + total_return) ** (365 / total_days) - 1

# Calculate Sharpe ratio
daily_returns = df['daily_return'].dropna()
sharpe = daily_returns.mean() / daily_returns.std() * (252 ** 0.5)

print(f"Annualized Return: {annualized_return:.2%}")
print(f"Sharpe Ratio: {sharpe:.2f}")
print(f"Max Drawdown: {df['drawdown'].min():.2%}")
```

## Advanced Usage

### Batch Processing Multiple Backtests

```bash
#!/bin/bash
# run_all_backtests.sh

BACKTESTS=("SPY" "QQQ" "RNCC")
END_DATE="2025-09-10"

for bt in "${BACKTESTS[@]}"; do
  echo "Running $bt..."
  python -m optionbt.backtest --backtest $bt --end-date $END_DATE
done
```

### Programmatic Usage

```python
from optionbt.backtest import run_backtest
import datetime as dt
from pathlib import Path

# Run backtest programmatically
run_backtest(
    backtest_name="SPY",
    end_date=dt.datetime(2025, 9, 10),
    output_dir=Path("my_output"),
    config_file="config.yaml",
    use_database=False,
    market="US",
    reinvest_premium=True
)
```

### Custom Data Loaders

```python
from optionbt.data.loader import DataLoader
import pandas as pd

# Create custom data loader
loader = DataLoader(use_database=False)

# Load specific data
equity_prices = loader.load_equity_prices("SPY US", "2020-01-01", "2020-12-31")
option_prices = loader.load_option_prices("SPY US", "2020-01-01", "2020-12-31")
dividends = loader.load_dividends("SPY US")

# Process data
print(f"Loaded {len(equity_prices)} price points")
print(f"Loaded {len(option_prices)} option data points")
```

## Tips and Best Practices

### 1. Start Small
Test with a short date range (1-3 months) before running multi-year backtests.

### 2. Verify Data Quality
Check your input CSV files have complete data for the backtest period.

### 3. Monitor Output
Review the aggregate report after each run to ensure results make sense.

### 4. Use Version Control
Keep your `portfolio_configs.csv` in version control to track strategy changes.

### 5. Document Assumptions
Add comments in your config files explaining strategy rationale.

### 6. Compare Results
When migrating from legacy code, run parallel backtests to verify consistency.

## Troubleshooting

See [MIGRATION.md](MIGRATION.md#troubleshooting) for common issues and solutions.
