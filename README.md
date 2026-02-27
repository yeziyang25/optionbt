# Option Backtest Framework

A Python framework for backtesting covered-call ETFs and options overlay strategies.

## Overview

This framework allows you to:
- Backtest options strategies (covered calls, puts, spreads)
- Evaluate historical performance of options overlay strategies
- Generate detailed performance reports and analytics
- Support both US and Canadian markets

## Installation

### Prerequisites
- Python 3.8 or higher
- (Optional) Bloomberg Terminal with API access for live data downloads

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd optionbt
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure database connection (optional):
```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your database credentials
```

## Quick Start

### Running a Backtest

The simplest way to run a backtest is using the main entry point:

```bash
python -m optionbt.backtest --backtest SPY --end-date 2025-09-10
```

### Configuration

Backtest configurations are stored in `runs/portfolio_configs.csv`. Each backtest requires:
- `backtest`: Unique identifier for the backtest
- `sec_id`: Security identifier (cash, equity1, option1, etc.)
- `sec_name`: Security ticker symbol
- `sec_type`: Type of security (cash, equity, call option, put option, fx_fwd)
- `currency`: Currency (USD, CAD)
- `allocation`: Initial allocation or position size
- `start_date`: Backtest start date

### Example Configuration

```csv
backtest,sec_id,sec_name,sec_type,currency,allocation,...
SPY,cash,Cash,cash,USD,10000000,...
SPY,equity1,SPY US,equity,USD,1,...
SPY,option1,SPY US,call option,USD,-0.5,...
```

## Project Structure

```
optionbt/
├── optionbt/               # Main package
│   ├── __init__.py
│   ├── backtest.py        # Main backtest entry point
│   ├── config.py          # Configuration management
│   ├── models/            # Backtest models
│   │   ├── backtest_engine.py
│   │   └── securities.py
│   ├── data/              # Data management
│   │   ├── loader.py
│   │   └── database.py
│   └── utils/             # Utility functions
│       ├── dates.py
│       └── helpers.py
├── runs/                  # Backtest configurations
│   └── portfolio_configs.csv
├── data_download/         # Data download scripts
├── tests/                 # Unit tests
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Features

### Supported Strategies
- Covered call writing
- Put writing
- Call spreads
- Collar strategies
- Multi-asset portfolios

### Key Features
- Configurable rebalancing schedules (weekly, monthly, quarterly, annual)
- Target yield optimization
- Premium reinvestment options
- FX hedging support
- Detailed performance attribution

## Data Requirements

The framework requires market data for:
- Underlying equity prices
- Option prices (bid/ask)
- Dividend schedules
- FX rates (for multi-currency portfolios)

### Data Sources
1. **Database** (recommended): Connect to SQL database with market data
2. **CSV files**: Load data from CSV files in `data_download/raw_data/`
3. **Bloomberg** (optional): Download data directly using Bloomberg API

## Advanced Usage

### Custom Option Selection

You can specify custom option lists for precise backtesting:

```csv
backtest,sec_id,option_selection,custom_options_file
SPY,option1,custom,runs/custom_options_list/SPY_US_0.0_option_list.csv
```

### Target Yield Strategy

Set a target annualized yield for option writing:

```csv
backtest,target_yield
QQQ,0.15
```

### Call Spread Construction

To create call spreads, use multiple option positions with different strikes:

```csv
SPY,option1,call option,-0.53,equity1  # Short ATM calls
SPY,option2,call option,0.1,equity1   # Long OTM calls
```

## Output

Backtests generate the following reports in the `output/` directory:

1. **Aggregate Report** (`*_aggregate_*.csv`): Portfolio-level daily returns
2. **Detailed Report** (`*_detailed_*.csv`): Security-level holdings
3. **Period Return Report** (`*_period_return_*.csv`): Performance attribution
4. **Option List** (`*_option_list_*.csv`): Options traded during backtest

## Configuration Reference

See `Backtest Guidelines.docx` for detailed configuration options and internal logic.

## License

[Add license information]

## Support

For questions or issues, please contact [contact information].
