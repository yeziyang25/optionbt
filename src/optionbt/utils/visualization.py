"""Visualization utilities for backtest results."""

import matplotlib.pyplot as plt
import pandas as pd
from typing import Optional, List


def plot_performance(
    performance_df: pd.DataFrame,
    title: str = "Portfolio Performance",
    figsize: tuple = (12, 6)
) -> None:
    """
    Plot portfolio performance over time.
    
    Args:
        performance_df: DataFrame from Portfolio.get_performance_dataframe()
        title: Plot title
        figsize: Figure size
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize)
    
    # Cumulative return
    ax1.plot(performance_df["date"], performance_df["cumulative_return"] * 100, linewidth=2)
    ax1.set_title(f"{title} - Cumulative Return")
    ax1.set_ylabel("Return (%)")
    ax1.grid(True, alpha=0.3)
    
    # Drawdown
    ax2.fill_between(
        performance_df["date"],
        performance_df["drawdown"] * 100,
        0,
        alpha=0.3,
        color="red"
    )
    ax2.set_title("Drawdown")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Drawdown (%)")
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()


def plot_comparison(
    strategy_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    strategy_name: str = "Strategy",
    benchmark_name: str = "Benchmark",
    figsize: tuple = (12, 6)
) -> None:
    """
    Plot strategy performance vs benchmark.
    
    Args:
        strategy_df: Performance DataFrame for strategy
        benchmark_df: Performance DataFrame for benchmark
        strategy_name: Name of strategy
        benchmark_name: Name of benchmark
        figsize: Figure size
    """
    plt.figure(figsize=figsize)
    
    plt.plot(
        strategy_df["date"],
        strategy_df["cumulative_return"] * 100,
        label=strategy_name,
        linewidth=2
    )
    plt.plot(
        benchmark_df["date"],
        benchmark_df["cumulative_return"] * 100,
        label=benchmark_name,
        linewidth=2,
        linestyle="--"
    )
    
    plt.title("Strategy vs Benchmark")
    plt.xlabel("Date")
    plt.ylabel("Cumulative Return (%)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def create_summary_table(summary_stats: dict) -> pd.DataFrame:
    """
    Create a formatted summary table from summary stats.
    
    Args:
        summary_stats: Dictionary from Portfolio.summary_stats()
        
    Returns:
        Formatted DataFrame
    """
    data = {
        "Metric": [
            "Total Return",
            "Annualized Return",
            "Volatility",
            "Sharpe Ratio",
            "Max Drawdown",
            "Number of Trades",
            "Final NAV",
            "Number of Days"
        ],
        "Value": [
            f"{summary_stats.get('total_return', 0) * 100:.2f}%",
            f"{summary_stats.get('annualized_return', 0) * 100:.2f}%",
            f"{summary_stats.get('volatility', 0) * 100:.2f}%",
            f"{summary_stats.get('sharpe_ratio', 0):.2f}",
            f"{summary_stats.get('max_drawdown', 0) * 100:.2f}%",
            f"{summary_stats.get('num_trades', 0)}",
            f"${summary_stats.get('final_nav', 0):,.2f}",
            f"{summary_stats.get('num_days', 0)}"
        ]
    }
    
    return pd.DataFrame(data)
