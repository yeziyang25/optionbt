"""
run_v2.py
---------
Clean entry point for the option backtesting framework.

Accepts a YAML strategy configuration file (see config/strategy_config_template.yaml)
instead of the legacy portfolio_configs.csv, so strategies can be authored in a
human-readable, version-controllable format.

Usage
-----
    python run_v2.py --config config/example_covered_call.yaml

    # Override end date from command line
    python run_v2.py --config config/my_strategy.yaml --end-date 2025-06-20

    # Use file-based data sources (no database required)
    python run_v2.py --config config/my_strategy.yaml \\
        --equity-dir data/equity --options-dir data/options

The script writes all outputs to ``output/<timestamp>_<username>/``.
"""

import argparse
import datetime as dt
import os
import sys
import warnings

warnings.filterwarnings("ignore")

# Ensure the project root is on the path regardless of the working directory
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run an option backtest defined by a YAML config file."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the YAML strategy config (e.g. config/example_covered_call.yaml).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Override the end date from the config (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(_HERE, "output"),
        help="Root folder for output files (default: ./output).",
    )
    parser.add_argument(
        "--equity-dir",
        default=None,
        help="Directory with equity pricing CSV files. Activates file-based data sourcing.",
    )
    parser.add_argument(
        "--options-dir",
        default=None,
        help="Directory with option chain CSV files. Activates file-based data sourcing.",
    )
    parser.add_argument(
        "--dividends-dir",
        default=None,
        help="Directory with dividend CSV files (optional).",
    )
    return parser.parse_args()


def _build_data_loader(args, strategy: dict):
    """
    Construct the appropriate DataFetcher based on CLI arguments and the
    strategy's data_source configuration block.
    """
    # CLI flags override the YAML data_source
    if args.equity_dir or args.options_dir:
        from data_sources.csv_loader import CsvDataFetcher
        return CsvDataFetcher(
            equity_dir=args.equity_dir,
            options_dir=args.options_dir,
            dividends_dir=args.dividends_dir,
        )

    ds_cfg = strategy.get("data_source", {})
    source_type = ds_cfg.get("type", "database")

    if source_type == "file":
        from data_sources.csv_loader import CsvDataFetcher
        return CsvDataFetcher(
            equity_dir=ds_cfg.get("equity_dir"),
            options_dir=ds_cfg.get("options_dir"),
            dividends_dir=ds_cfg.get("dividends_dir"),
        )

    if source_type == "tmx":
        from data_sources.tmx_fetcher import TmxFetcher
        return TmxFetcher(
            call_put=ds_cfg.get("call_put", "call"),
            pct_otm_limit=float(ds_cfg.get("pct_otm_limit", 0.05)),
        )

    # Default: database — return None so security_data uses the legacy DB path
    return None


def _get_holidays(strategy: dict, data_loader) -> dict:
    """Resolve the holiday calendar from the data loader or internal library."""
    calendar = strategy.get("holiday_calendar", "TSX").upper()

    if data_loader is not None and hasattr(data_loader, "get_holidays"):
        holidays = data_loader.get_holidays(calendar)
        if holidays:
            return holidays

    # Try im_prod / im_dev
    try:
        sys.path.append("Z:\\ApolloGX")
        if "\\im_dev\\" in _HERE:
            import im_dev.std_lib.data_library as _dl  # type: ignore
        else:
            import im_prod.std_lib.data_library as _dl  # type: ignore
        return _dl.tsx_holidays() if calendar == "TSX" else _dl.nyse_holidays()
    except ImportError:
        pass

    # Try local common.py (repo root)
    try:
        import common as _c  # type: ignore
        return _c.tsx_holidays() if calendar == "TSX" else _c.nyse_holidays()
    except Exception:
        pass

    print(
        f"[WARNING] Could not load {calendar} holiday calendar. "
        "Weekend skipping will still work but named holidays may not be excluded."
    )
    return {}


def main():
    args = _parse_args()

    # ------------------------------------------------------------------
    # 1. Load strategy config
    # ------------------------------------------------------------------
    from config.config_loader import _load_yaml, _validate, to_dataframe_config

    yaml_path = os.path.abspath(args.config)
    if not os.path.exists(yaml_path):
        print(f"[ERROR] Config file not found: {yaml_path}")
        sys.exit(1)

    cfg = _load_yaml(yaml_path)
    errors = _validate(cfg)
    if errors:
        print("[ERROR] Strategy config validation failed:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    strategy = cfg["strategy"]
    name = strategy["name"]

    start_date = dt.datetime.strptime(strategy["start_date"], "%Y-%m-%d")
    end_date_cfg = dt.datetime.strptime(strategy["end_date"], "%Y-%m-%d")
    end_date = (
        dt.datetime.strptime(args.end_date, "%Y-%m-%d")
        if args.end_date
        else end_date_cfg
    )

    model = strategy.get("model", "btbuilder")
    reinvest_premium = bool(strategy.get("reinvest_premium", True))

    # ------------------------------------------------------------------
    # 2. Build data loader
    # ------------------------------------------------------------------
    data_loader = _build_data_loader(args, strategy)

    # ------------------------------------------------------------------
    # 3. Resolve holidays
    # ------------------------------------------------------------------
    holidays = _get_holidays(strategy, data_loader)

    # ------------------------------------------------------------------
    # 4. Build rebalance dates
    # ------------------------------------------------------------------
    rebal_file = strategy.get("rebalance_dates_file")
    import pandas as pd
    if rebal_file and os.path.exists(rebal_file):
        import pandas as pd
        opt_rebalance_dates = [
            d.date() for d in pd.to_datetime(pd.read_csv(rebal_file).iloc[:, 0])
        ]
    else:
        schedule = strategy.get("rebalance_schedule", "monthly_third_friday")
        if schedule == "weekly_friday":
            from helper_functions.rebalance_dates import weekly_option_dates
            opt_rebalance_dates = weekly_option_dates(start_date, holidays, end_date)
        else:
            from helper_functions.rebalance_dates import option_dates
            opt_rebalance_dates = option_dates(start_date, holidays, end_date)

    # ------------------------------------------------------------------
    # 5. Build portfolio dict from YAML config
    # ------------------------------------------------------------------
    df_config = to_dataframe_config(yaml_path, backtest_name=name)
    df_config["start_date"] = start_date.strftime("%Y-%m-%d")

    from helper_functions.securities import security_data

    portfolio = {}
    for _, row in df_config.iterrows():
        # Resolve per-leg data loader from YAML legs block
        leg_loader = data_loader
        for leg in cfg.get("legs", []):
            if leg.get("id") == row["sec_id"]:
                leg_ds = leg.get("data_source", {})
                leg_type = leg_ds.get("type", strategy.get("data_source", {}).get("type", "database"))
                if leg_type == "file":
                    from data_sources.csv_loader import CsvDataFetcher
                    leg_loader = CsvDataFetcher(
                        equity_file=leg_ds.get("equity_file"),
                        options_file=leg_ds.get("options_file"),
                        dividend_file=leg_ds.get("dividend_file"),
                    )
                break

        portfolio[row["sec_id"]] = security_data(
            row,
            cur_dir=_HERE,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            opt_rebal_dates=opt_rebalance_dates,
            data_loader=leg_loader,
        )

    # ------------------------------------------------------------------
    # 6. Set up output directory
    # ------------------------------------------------------------------
    timestamp = dt.datetime.now()
    username = os.getenv("username") or os.getenv("USER") or "user"
    save_location = os.path.join(
        args.output_dir, f"{timestamp.strftime('%Y%m%d_%H%M')}_{username}"
    )
    os.makedirs(save_location, exist_ok=True)

    # Save the config used for this run
    df_config.to_csv(
        os.path.join(save_location, f"{name}_{model}_configurations.csv"), index=False
    )
    print(f"[INFO] Running '{name}' | model={model} | {start_date.date()} → {end_date.date()}")
    print(f"[INFO] Output → {save_location}")

    # ------------------------------------------------------------------
    # 7. Run backtest
    # ------------------------------------------------------------------
    if model == "btbuilder":
        import models.btbuilder as btbuilder
        aggregate, detailed = btbuilder.run_portfolio_backtest(
            portfolio, start_date, end_date, opt_rebalance_dates, holidays,
            reinvest_premium=reinvest_premium,
        )
        aggregate.to_csv(os.path.join(save_location, f"{name}_aggregate_{model}.csv"), index=False)
        detailed.to_csv(os.path.join(save_location, f"{name}_detailed_{model}.csv"), index=False)
        p_cf = btbuilder.cashflow_period_report(detailed, opt_rebalance_dates)
        p_cf.to_csv(os.path.join(save_location, f"{name}_period_return_{model}.csv"), index=False)
        df_opt_list = detailed[
            (detailed["sec_id"].str.contains("option")) & (detailed["open_qty"] == 0)
        ].reset_index(drop=True)[["date", "sec_ticker", "bid", "ask", "opt_u_price"]]
        df_opt_list.to_csv(os.path.join(save_location, f"{name}_option_list_{model}.csv"), index=False)

    elif model == "btbuilder_weekly":
        import models.btbuilder_weekly as btbuilder_weekly
        aggregate, detailed = btbuilder_weekly.run_portfolio_backtest(
            portfolio, start_date, end_date, opt_rebalance_dates, holidays,
            reinvest_premium=reinvest_premium,
        )
        aggregate.to_csv(os.path.join(save_location, f"{name}_aggregate_{model}.csv"), index=False)
        detailed.to_csv(os.path.join(save_location, f"{name}_detailed_{model}.csv"), index=False)

    elif model == "cboe":
        import models.cboe as cboe
        aggregate, detailed = cboe.build_backtest(
            portfolio, start_date, end_date, opt_rebalance_dates, holidays
        )
        aggregate.to_csv(os.path.join(save_location, f"{name}_aggregate_{model}.csv"), index=False)
        detailed.to_csv(os.path.join(save_location, f"{name}_detailed_{model}.csv"), index=False)

    else:
        print(f"[ERROR] Unknown model: '{model}'. Choose from btbuilder, btbuilder_weekly, cboe.")
        sys.exit(1)

    print(f"[INFO] Done. Results saved to {save_location}")


if __name__ == "__main__":
    main()
