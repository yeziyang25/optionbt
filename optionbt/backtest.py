"""Main entry point for running backtests via command line."""

import argparse
import pandas as pd
import datetime as dt
import sys
from pathlib import Path

from .models.backtest_engine import BacktestEngine
from .utils.dates import get_tsx_holidays, get_us_holidays
from .config import get_config


def load_portfolio_config(backtest_name: str, config_dir: Path) -> pd.DataFrame:
    """Load portfolio configuration for a specific backtest.
    
    Args:
        backtest_name: Name of the backtest to run
        config_dir: Directory containing portfolio_configs.csv
        
    Returns:
        DataFrame with portfolio configuration
    """
    config_file = config_dir / 'portfolio_configs.csv'
    
    if not config_file.exists():
        raise FileNotFoundError(f"Portfolio config file not found: {config_file}")
    
    df_config = pd.read_csv(config_file, delimiter=',')
    df_config = df_config[df_config['backtest'] == backtest_name].reset_index(drop=True)
    
    if df_config.empty:
        raise ValueError(f"No configuration found for backtest: {backtest_name}")
    
    # Fill NaN values
    df_config['sec_name'] = df_config['sec_name'].fillna('NA')
    
    # Parse dates
    df_config["start_date"] = pd.to_datetime(df_config["start_date"]).dt.strftime("%Y-%m-%d")
    
    return df_config


def get_holidays(market: str = 'US') -> list:
    """Get market holidays.
    
    Args:
        market: Market code ('US' or 'CA')
        
    Returns:
        List of holiday dates
    """
    if market.upper() in ['CA', 'TSX']:
        return get_tsx_holidays()
    else:
        return get_us_holidays()


def run_backtest(
    backtest_name: str,
    end_date: dt.datetime,
    output_dir: Path,
    config_file: str = None,
    use_database: bool = False,
    market: str = 'US',
    reinvest_premium: bool = True
):
    """Run a backtest.
    
    Args:
        backtest_name: Name of backtest from portfolio_configs.csv
        end_date: End date for backtest
        output_dir: Directory to save outputs
        config_file: Optional path to config YAML file
        use_database: Whether to use database for market data
        market: Market code for holidays ('US' or 'CA')
        reinvest_premium: Whether to reinvest option premiums
    """
    # Initialize config
    config = get_config(config_file)
    
    # Create output directory
    timestamp = dt.datetime.now()
    save_location = output_dir / f"{timestamp.strftime('%Y%m%d_%H%M')}_{backtest_name}"
    save_location.mkdir(parents=True, exist_ok=True)
    
    print(f"Running backtest: {backtest_name}")
    print(f"Output directory: {save_location}")
    
    # Load portfolio configuration
    df_config = load_portfolio_config(backtest_name, config.config_dir)
    
    # Get start date from config
    start_date_str = df_config[df_config['sec_id'] == 'cash']['start_date'].values[0]
    start_date = dt.datetime.strptime(start_date_str, "%Y-%m-%d")
    
    print(f"Backtest period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Get holidays
    holidays = get_holidays(market)
    
    # Initialize backtest engine
    print("Initializing backtest engine...")
    engine = BacktestEngine(
        portfolio_config=df_config,
        start_date=start_date,
        end_date=end_date,
        holidays=holidays,
        reinvest_premium=reinvest_premium,
        use_database=use_database
    )
    
    # Run backtest
    print("Running backtest...")
    try:
        aggregate_df, detailed_df = engine.run()
        
        # Save outputs
        print("Saving results...")
        
        # Save configuration
        config_output = save_location / f"{backtest_name}_configuration.csv"
        df_config.to_csv(config_output, index=False)
        
        # Save aggregate report
        aggregate_output = save_location / f"{backtest_name}_aggregate.csv"
        aggregate_df.to_csv(aggregate_output, index=False)
        
        # Save detailed report
        detailed_output = save_location / f"{backtest_name}_detailed.csv"
        detailed_df.to_csv(detailed_output, index=False)
        
        print(f"\nBacktest complete!")
        print(f"Results saved to: {save_location}")
        
        # Print summary statistics
        if not aggregate_df.empty:
            final_value = aggregate_df['portfolio_value'].iloc[-1]
            total_return = aggregate_df['cumulative_return'].iloc[-1]
            max_drawdown = aggregate_df['drawdown'].min()
            
            print(f"\nSummary:")
            print(f"  Final Portfolio Value: ${final_value:,.2f}")
            print(f"  Total Return: {total_return:.2%}")
            print(f"  Max Drawdown: {max_drawdown:.2%}")
        
    except Exception as e:
        print(f"Error running backtest: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Run option backtest',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run SPY backtest through September 2025
  python -m optionbt.backtest --backtest SPY --end-date 2025-09-10
  
  # Run with custom config file
  python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --config config.yaml
  
  # Run using database for market data
  python -m optionbt.backtest --backtest SPY --end-date 2025-09-10 --use-database
        """
    )
    
    parser.add_argument(
        '--backtest',
        required=True,
        help='Name of backtest from portfolio_configs.csv'
    )
    
    parser.add_argument(
        '--end-date',
        required=True,
        help='End date for backtest (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--config',
        help='Path to config YAML file (optional)'
    )
    
    parser.add_argument(
        '--output-dir',
        help='Output directory for results (default: ./output)',
        default='output'
    )
    
    parser.add_argument(
        '--use-database',
        action='store_true',
        help='Use database for market data instead of CSV files'
    )
    
    parser.add_argument(
        '--market',
        choices=['US', 'CA', 'TSX'],
        default='US',
        help='Market for holiday calendar (default: US)'
    )
    
    parser.add_argument(
        '--no-reinvest',
        action='store_true',
        help='Do not reinvest option premiums'
    )
    
    args = parser.parse_args()
    
    # Parse end date
    try:
        end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid end date format: {args.end_date}")
        print("Expected format: YYYY-MM-DD")
        sys.exit(1)
    
    # Run backtest
    run_backtest(
        backtest_name=args.backtest,
        end_date=end_date,
        output_dir=Path(args.output_dir),
        config_file=args.config,
        use_database=args.use_database,
        market=args.market,
        reinvest_premium=not args.no_reinvest
    )


if __name__ == '__main__':
    main()
