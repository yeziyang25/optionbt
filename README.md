# Option Backtest Framework

A clean, modular framework for backtesting covered call and options overlay strategies.

## Overview

This framework allows you to backtest various option strategies including:
- Covered calls
- Call spreads
- Put writing
- Custom option overlay strategies

## Key Features

- **Modular Design**: Clean separation between data, strategy logic, and execution
- **Flexible Configuration**: Easy-to-use YAML configuration files
- **Multiple Strategies**: Support for various option strategies with customizable parameters
- **Data Agnostic**: Works with any data source (Bloomberg, CSV files, etc.)
- **Comprehensive Output**: Detailed performance metrics and visualizations

## Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Basic Usage

1. **Prepare your data**: Place equity and option data in the `data/` directory
2. **Configure your backtest**: Edit or create a config file in `configs/`
3. **Run the backtest**: 

```bash
python -m optionbt run --config configs/my_strategy.yaml
```

## Project Structure

```
optionbt/
├── src/
│   └── optionbt/
│       ├── __init__.py
│       ├── core/              # Core domain models
│       │   ├── portfolio.py
│       │   ├── security.py
│       │   └── strategy.py
│       ├── data/              # Data providers and loaders
│       │   ├── loader.py
│       │   └── providers/
│       ├── engine/            # Backtest execution engine
│       │   ├── backtest.py
│       │   └── calculator.py
│       ├── strategies/        # Pre-built strategies
│       │   ├── covered_call.py
│       │   └── call_spread.py
│       └── utils/             # Utilities
│           ├── dates.py
│           └── visualization.py
├── configs/                   # Configuration files
├── data/                      # Sample data
├── tests/                     # Test suite
└── examples/                  # Example scripts and notebooks
```

## Configuration

Backtests are configured using YAML files. Example:

```yaml
backtest:
  name: "SPY_Covered_Call"
  start_date: "2020-01-01"
  end_date: "2024-12-31"
  initial_capital: 1000000

strategy:
  type: "covered_call"
  parameters:
    coverage_ratio: 0.5
    moneyness: 0.0  # ATM
    roll_frequency: "monthly"
    
positions:
  - ticker: "SPY"
    allocation: 1.0
    option_ticker: "SPY"
```

## Data Format

### Equity Data
CSV file with columns: `date, open, high, low, close, volume`

### Option Data
CSV file with columns: `date, strike, expiration, call_put, bid, ask, underlying_price`

## Examples

See the `examples/` directory for:
- Simple covered call strategy
- Call spread construction
- Multi-asset portfolio
- Custom rebalancing rules

## Migration from Legacy System

If migrating from the legacy backtest system, see [MIGRATION.md](MIGRATION.md) for a detailed guide.

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
black src/ tests/
flake8 src/ tests/
```

## License

MIT License

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.
