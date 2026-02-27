# Migration Guide: Legacy to Refactored Code

This guide helps you transition from the legacy option backtest codebase to the new refactored version.

## Key Changes

### 1. Project Structure

**Legacy:**
- Flat structure with multiple run scripts (`run.py`, `run.customized.py`, `run_weekly.py`, etc.)
- Hardcoded paths to `Z:\ApolloGX\im_dev\` and `Z:\IPS\python\`
- Dependencies on external modules via `sys.path.append`

**New:**
- Clean package structure under `optionbt/`
- Single entry point: `python -m optionbt.backtest`
- Configurable paths via `config.yaml` or environment variables
- No hardcoded dependencies on external modules

### 2. Configuration

**Legacy:**
```python
# In run.py or run_targetpay.py
backtestname = "RNCC"
end_date = dt.datetime(2025, 9, 10)
run_backtest(backtestname, end_date, "btbuilder", data_library.tsx_holidays())
```

**New:**
```bash
# Via command line
python -m optionbt.backtest --backtest RNCC --end-date 2025-09-10 --market CA

# Or with config file
python -m optionbt.backtest --backtest RNCC --end-date 2025-09-10 --config config.yaml
```

### 3. Database Connection

**Legacy:**
```python
# In common.py - hardcoded credentials
server = 'hemi-ips-sql-srv.database.windows.net'
username = 'ips_login'
password = 'BxigZoHCdWdvnp*MtX3V!Uia'  # ⚠️ Security issue!
```

**New:**
```yaml
# In config.yaml - credentials from environment
database:
  enabled: true
  server: ${DB_SERVER}
  username: ${DB_USERNAME}
  password: ${DB_PASSWORD}
```

Or set environment variables:
```bash
export DB_SERVER="your-server.database.windows.net"
export DB_USERNAME="your-username"
export DB_PASSWORD="your-password"
```

### 4. Data Loading

**Legacy:**
```python
# Requires Bloomberg connection and internal database
import im_dev.std_lib.common as common
import im_dev.std_lib.data_library as data_library
```

**New:**
```python
# Flexible data loading from database OR CSV files
from optionbt.data.loader import DataLoader

# Use database
loader = DataLoader(use_database=True)

# Or use CSV files (no database required)
loader = DataLoader(use_database=False)
```

### 5. Portfolio Configuration

**No Change Required!**

The `runs/portfolio_configs.csv` file format remains the same. You can use your existing configurations without modification.

## Step-by-Step Migration

### Step 1: Update Dependencies

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. (Optional) If using database:
```bash
pip install pyodbc
```

### Step 2: Configure Data Sources

**Option A: Use CSV files (Recommended for testing)**

1. Create data directory structure:
```
data_download/
  adhoc_pricing/
    equity/
      SPY US equity_pricing.csv
    options/
      SPY US_backtest_format_options.csv
  dividends/
    SPY US_dividends.csv
```

2. Format your CSV files:

Equity pricing CSV:
```csv
date,px_last
2020-01-02,324.87
2020-01-03,325.12
...
```

Option pricing CSV:
```csv
date,ticker,field,value
2020-01-02,SPY US 01/31/20 C330,bid,2.15
2020-01-02,SPY US 01/31/20 C330,ask,2.25
...
```

**Option B: Use Database**

1. Create `config.yaml`:
```yaml
database:
  enabled: true
  server: "your-server.database.windows.net"
  database: "market-data-db"
  username: "${DB_USERNAME}"
  password: "${DB_PASSWORD}"
```

2. Set environment variables:
```bash
export DB_USERNAME="your-username"
export DB_PASSWORD="your-password"
```

### Step 3: Run Your First Backtest

```bash
# Using CSV files
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10

# Using database
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --use-database --config config.yaml
```

### Step 4: Verify Output

The new code generates the same output files as before:
- `{backtest}_configuration.csv` - Portfolio configuration
- `{backtest}_aggregate.csv` - Portfolio-level returns
- `{backtest}_detailed.csv` - Security-level positions

Output location: `output/{timestamp}_{backtest}/`

## Feature Comparison

| Feature | Legacy | New | Status |
|---------|--------|-----|--------|
| Portfolio configuration | ✅ | ✅ | Same format |
| Equity holdings | ✅ | ✅ | Fully compatible |
| Call options | ✅ | 🚧 | Basic support |
| Put options | ✅ | 🚧 | Basic support |
| Call spreads | ✅ | 🚧 | Planned |
| Target yield | ✅ | 🚧 | Planned |
| Custom option lists | ✅ | 🚧 | Planned |
| FX forwards | ✅ | ✅ | Basic support |
| Premium reinvestment | ✅ | 🚧 | Planned |
| Database connectivity | ✅ | ✅ | Improved security |
| CSV file support | ⚠️ | ✅ | Better support |
| Bloomberg downloads | ✅ | 🚧 | Separate module |

Legend:
- ✅ Fully implemented
- 🚧 In progress / partial
- ⚠️ Limited support

## Known Limitations (Current Version)

The refactored code is currently a **foundation** with basic functionality. The following features from the legacy code are not yet fully implemented:

1. **Option Selection Logic**: Custom option selection based on moneyness, DTM
2. **Rebalancing**: Full rebalancing logic for options and equities
3. **Target Yield**: Calculating coverage ratio to achieve target premium yield
4. **Premium Reinvestment**: Automatic reinvestment of option premiums
5. **Call Spreads**: Advanced multi-leg option strategies
6. **Custom Option Lists**: Loading pre-defined option contracts from CSV

These features will be added incrementally. The current version provides:
- Clean, modular architecture
- Secure configuration management
- Flexible data loading
- Clear CLI interface
- Foundation for adding missing features

## Troubleshooting

### "No module named 'pyodbc'"

**Solution**: Only required if using database. Install with:
```bash
pip install pyodbc
```

Or use CSV files instead:
```bash
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10
# (don't use --use-database flag)
```

### "Portfolio config file not found"

**Solution**: Ensure you're running from the project root directory where `runs/portfolio_configs.csv` exists.

### "No data found for {ticker}"

**Solution**: 
1. Check your CSV files are in the correct location
2. Verify the ticker name matches exactly
3. Check date range covers your backtest period

## Getting Help

If you encounter issues during migration:

1. Check this guide first
2. Review the README.md for usage examples
3. Examine the example config files
4. Check the code comments for detailed documentation

## Next Steps

After successfully migrating:

1. Review your backtest outputs to verify accuracy
2. Create a `config.yaml` for your environment
3. Set up environment variables for sensitive data
4. Consider contributing improvements back to the codebase
