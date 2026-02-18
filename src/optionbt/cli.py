"""
Command-line interface for running backtests.

Usage:
    python -m optionbt.cli run --config configs/my_strategy.yaml
    python -m optionbt.cli run --config configs/my_strategy.yaml --output output/
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
import yaml

from src.optionbt.core.portfolio import Portfolio
from src.optionbt.core.strategy import (
    CoveredCallStrategy,
    StrategyConfig,
    RollFrequency
)
from src.optionbt.data.loader import DataLoader, CSVDataProvider
from src.optionbt.engine.backtest import BacktestEngine
from src.optionbt.utils.visualization import plot_performance, create_summary_table


def load_config(config_path: str) -> dict:
    """Load YAML configuration file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def parse_date(date_str: str) -> datetime.date:
    """Parse date string to date object."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def run_backtest(config_path: str, output_dir: str = None) -> None:
    """
    Run backtest from configuration file.
    
    Args:
        config_path: Path to YAML configuration file
        output_dir: Optional output directory override
    """
    # Load configuration
    print(f"Loading configuration from {config_path}...")
    config = load_config(config_path)
    
    # Extract configuration
    backtest_config = config.get("backtest", {})
    strategy_config_dict = config.get("strategy", {})
    data_config = config.get("data", {})
    output_config = config.get("output", {})
    
    # Parse dates
    start_date = parse_date(backtest_config["start_date"])
    end_date = parse_date(backtest_config["end_date"])
    
    # Setup output directory
    if output_dir is None:
        output_dir = output_config.get("output_dir", "output")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 60)
    print(f"BACKTEST: {backtest_config['name']}")
    print("=" * 60)
    print(f"Period: {start_date} to {end_date}")
    print(f"Initial Capital: ${backtest_config['initial_capital']:,}")
    print()
    
    # Load data
    print("Loading data...")
    provider = CSVDataProvider(data_dir=data_config.get("data_dir", "data"))
    loader = DataLoader(provider)
    
    equity_ticker = data_config["equity"]["ticker"]
    option_ticker = data_config["options"]["ticker"]
    
    try:
        equity = loader.load_equity(equity_ticker, start_date, end_date)
        
        # Determine option type from strategy
        option_type = "call" if "call" in strategy_config_dict["type"] else None
        options = loader.load_options(
            option_ticker,
            equity_ticker,
            start_date,
            end_date,
            option_type=option_type
        )
        
        print(f"✓ Loaded equity data: {len(equity._price_data)} days")
        print(f"✓ Loaded option data: {len(options)} unique options")
    except FileNotFoundError as e:
        print(f"✗ Error: {e}")
        return
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        return
    
    print()
    
    # Create portfolio
    portfolio = Portfolio(
        name=backtest_config["name"],
        initial_capital=backtest_config["initial_capital"]
    )
    
    # Create strategy
    print("Creating strategy...")
    
    # Parse roll frequency
    freq_str = strategy_config_dict.get("roll_frequency", "monthly")
    roll_freq = RollFrequency[freq_str.upper()]
    
    strategy_config = StrategyConfig(
        name=strategy_config_dict["name"],
        strategy_type=strategy_config_dict["type"],
        moneyness=strategy_config_dict.get("moneyness", 0.0),
        roll_frequency=roll_freq,
        coverage_ratio=strategy_config_dict.get("coverage_ratio", 1.0),
        target_yield=strategy_config_dict.get("target_yield"),
        days_to_expiration=strategy_config_dict.get("days_to_expiration", 30),
        rebalance_rule=strategy_config_dict.get("rebalance_rule", "O"),
        use_bid_ask=strategy_config_dict.get("use_bid_ask", True)
    )
    
    strategy = CoveredCallStrategy(strategy_config)
    print(f"✓ Strategy: {strategy_config.name}")
    print()
    
    # Run backtest
    print("Running backtest...")
    engine = BacktestEngine(
        portfolio=portfolio,
        strategy=strategy,
        equity=equity,
        options=options,
        start_date=start_date,
        end_date=end_date
    )
    
    performance_df = engine.run(verbose=True)
    print()
    
    # Display results
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    
    summary = portfolio.summary_stats()
    summary_table = create_summary_table(summary)
    print(summary_table.to_string(index=False))
    print()
    
    # Save results
    if output_config.get("save_results", True):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = backtest_config["name"].replace(" ", "_")
        
        perf_file = Path(output_dir) / f"{base_name}_{timestamp}_performance.csv"
        performance_df.to_csv(perf_file, index=False)
        print(f"✓ Performance saved to {perf_file}")
        
        if output_config.get("save_trades", True):
            trades_df = portfolio.get_trade_dataframe()
            trades_file = Path(output_dir) / f"{base_name}_{timestamp}_trades.csv"
            trades_df.to_csv(trades_file, index=False)
            print(f"✓ Trades saved to {trades_file}")
    
    # Generate plots
    if output_config.get("generate_plots", False):
        print("\nGenerating performance plot...")
        try:
            plot_performance(performance_df, title=backtest_config["name"])
        except Exception as e:
            print(f"Could not generate plot: {e}")
    
    print("\n" + "=" * 60)
    print("BACKTEST COMPLETE")
    print("=" * 60)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Option Backtest Framework CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run a backtest")
    run_parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to YAML configuration file"
    )
    run_parser.add_argument(
        "--output",
        "-o",
        help="Output directory for results"
    )
    
    args = parser.parse_args()
    
    if args.command == "run":
        run_backtest(args.config, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
