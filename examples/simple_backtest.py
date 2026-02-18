"""
Simple example of running a covered call backtest.

This example demonstrates the basic workflow:
1. Load data
2. Create portfolio and strategy
3. Run backtest
4. View results
"""

from datetime import date
from src.optionbt.core.portfolio import Portfolio
from src.optionbt.core.strategy import CoveredCallStrategy, StrategyConfig, RollFrequency
from src.optionbt.data.loader import DataLoader, CSVDataProvider
from src.optionbt.engine.backtest import BacktestEngine
from src.optionbt.utils.visualization import plot_performance, create_summary_table


def main():
    """Run a simple covered call backtest example."""
    
    # Configuration
    ticker = "SPY"
    start_date = date(2020, 1, 1)
    end_date = date(2024, 12, 31)
    initial_capital = 10_000_000
    
    print("=" * 60)
    print("COVERED CALL BACKTEST EXAMPLE")
    print("=" * 60)
    print(f"Ticker: {ticker}")
    print(f"Period: {start_date} to {end_date}")
    print(f"Initial Capital: ${initial_capital:,}")
    print()
    
    # 1. Load data
    print("Loading data...")
    provider = CSVDataProvider(data_dir="data")
    loader = DataLoader(provider)
    
    try:
        equity = loader.load_equity(ticker, start_date, end_date)
        options = loader.load_options(ticker, ticker, start_date, end_date, option_type="call")
        print(f"✓ Loaded equity data: {len(equity._price_data)} days")
        print(f"✓ Loaded option data: {len(options)} unique options")
    except FileNotFoundError as e:
        print(f"✗ Error loading data: {e}")
        print("\nTo run this example, you need to provide data files:")
        print("  - data/equity/SPY.csv (columns: date, close)")
        print("  - data/options/SPY_options.csv (columns: date, strike, expiration, call_put, bid, ask)")
        return
    
    print()
    
    # 2. Create portfolio
    print("Creating portfolio...")
    portfolio = Portfolio(
        name=f"{ticker} Covered Call",
        initial_capital=initial_capital
    )
    print(f"✓ Portfolio created with ${initial_capital:,} cash")
    print()
    
    # 3. Create strategy
    print("Creating strategy...")
    strategy_config = StrategyConfig(
        name="Monthly 2% OTM Covered Call",
        strategy_type="covered_call",
        moneyness=0.02,  # 2% OTM
        roll_frequency=RollFrequency.MONTHLY,
        coverage_ratio=0.5,  # 50% coverage
        use_bid_ask=True
    )
    strategy = CoveredCallStrategy(strategy_config)
    print(f"✓ Strategy: {strategy_config.name}")
    print(f"  - Moneyness: {strategy_config.moneyness * 100}% OTM")
    print(f"  - Coverage: {strategy_config.coverage_ratio * 100}%")
    print(f"  - Frequency: {strategy_config.roll_frequency.value}")
    print()
    
    # 4. Run backtest
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
    
    # 5. Display results
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    
    summary = portfolio.summary_stats()
    summary_table = create_summary_table(summary)
    print(summary_table.to_string(index=False))
    print()
    
    # Show last few days
    print("\nLast 5 days:")
    print(performance_df[["date", "nav", "cumulative_return", "drawdown"]].tail().to_string(index=False))
    print()
    
    # Plot performance
    print("Generating performance plot...")
    try:
        plot_performance(performance_df, title=f"{ticker} Covered Call Strategy")
    except Exception as e:
        print(f"Could not generate plot: {e}")
    
    # Save results
    output_file = f"output/{ticker}_backtest_results.csv"
    performance_df.to_csv(output_file, index=False)
    print(f"\n✓ Results saved to {output_file}")
    
    trades_file = f"output/{ticker}_trades.csv"
    trades_df = portfolio.get_trade_dataframe()
    trades_df.to_csv(trades_file, index=False)
    print(f"✓ Trades saved to {trades_file}")
    print()
    
    print("=" * 60)
    print("BACKTEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
