"""
config/config_loader.py
=======================
Loads a YAML strategy configuration file and produces either:

  1. A portfolio dict compatible with btbuilder.run_portfolio_backtest() by
     building security_data objects for each leg.
  2. A pandas DataFrame in the legacy portfolio_configs.csv format, so the
     strategy can be run with the existing engine unchanged.

The YAML format is defined (with full comments) in strategy_config_template.yaml.

Usage
-----
    from config.config_loader import load_strategy, to_dataframe_config

    # Option A – portfolio dict (for use with DataLoader, no DB required):
    portfolio, start_date, end_date, base_currency = load_strategy(
        "config/my_strategy.yaml",
        data_loader=FileDataLoader(equity_dir="data/equity", options_dir="data/options"),
    )

    # Option B – convert to legacy CSV format and run with existing engine:
    df_config = to_dataframe_config("config/my_strategy.yaml")
    df_config.to_csv("runs/portfolio_configs.csv", index=False)
"""

import os
import datetime as dt
from typing import Dict, Optional, Tuple

import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def load_strategy(
    yaml_path: str,
    data_loader=None,
) -> Tuple[Dict, dt.datetime, dt.datetime, str]:
    """
    Load a YAML strategy file and return a portfolio dict + metadata.

    The portfolio dict maps sec_id -> security_data-compatible object.
    If *data_loader* is None the function tries to use the database backend;
    when neither is available it raises an ImportError with a helpful message.

    Returns
    -------
    portfolio : dict
        Keys are sec_ids from the YAML legs (e.g. 'cash', 'equity1', 'option1').
        Values are security_data instances ready for the backtesting engine.
    start_date : datetime
    end_date   : datetime
    base_currency : str
    """
    cfg = _load_yaml(yaml_path)
    strategy = cfg["strategy"]
    legs = cfg["legs"]

    start_date = dt.datetime.strptime(strategy["start_date"], "%Y-%m-%d")
    end_date = dt.datetime.strptime(strategy["end_date"], "%Y-%m-%d")
    base_currency = strategy["base_currency"]
    initial_capital = float(strategy.get("initial_capital", 10_000_000))

    # Resolve rebalance dates (needed by security_data for option selection)
    opt_rebal_dates = _build_rebal_dates(strategy, data_loader)

    # Resolve the data_loader: per-strategy default or per-leg override
    default_loader = _resolve_loader(strategy.get("data_source", {}), data_loader)

    # Import security_data here to avoid circular-import issues at module load
    from helper_functions.securities import security_data as SecurityData

    portfolio: Dict = {}
    for leg in legs:
        sec_id = leg["id"]
        row = _leg_to_config_row(leg, strategy, initial_capital)
        leg_loader = _resolve_loader(leg.get("data_source", {}), default_loader)

        # cur_dir is used only for the legacy bs_flag CSV path; we pass the
        # project root so relative paths in custom_options_file resolve properly
        proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        portfolio[sec_id] = SecurityData(
            row,
            cur_dir=proj_root,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            opt_rebal_dates=opt_rebal_dates,
            data_loader=leg_loader,
        )

    return portfolio, start_date, end_date, base_currency


def to_dataframe_config(yaml_path: str, backtest_name: Optional[str] = None) -> pd.DataFrame:
    """
    Convert a YAML strategy config to the legacy ``portfolio_configs.csv``
    row format.  This lets you author strategies in YAML and feed them
    into the existing btbuilder engine without any engine changes.

    Parameters
    ----------
    yaml_path : str
        Path to the YAML file.
    backtest_name : str, optional
        Override the backtest name stored in the YAML.

    Returns
    -------
    pd.DataFrame  — one row per leg, with all columns expected by the engine.
    """
    cfg = _load_yaml(yaml_path)
    strategy = cfg["strategy"]
    legs = cfg["legs"]
    name = backtest_name or strategy["name"]

    rows = []
    for leg in legs:
        row = _leg_to_config_row(leg, strategy, float(strategy.get("initial_capital", 10_000_000)))
        row["backtest"] = name
        rows.append(row)

    col_order = [
        "backtest", "sec_id", "sec_name", "sec_type", "currency",
        "allocation", "option_w_against", "start_date",
        "option_selection", "custom_options_file",
        "option_sell_to_open_price", "option_buy_to_close_price",
        "end_date", "DTM", "rebal_rule", "pct_otm", "target_yield",
    ]
    df = pd.DataFrame(rows)
    for col in col_order:
        if col not in df.columns:
            df[col] = None
    return df[col_order]


def validate_config(yaml_path: str) -> None:
    """
    Validate a YAML strategy config file and raise ValueError with a
    descriptive message if anything is wrong.
    """
    cfg = _load_yaml(yaml_path)
    errors = _validate(cfg)
    if errors:
        raise ValueError("Strategy config validation failed:\n" + "\n".join(f"  - {e}" for e in errors))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: str) -> dict:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def _validate(cfg: dict) -> list:
    errors = []

    if "strategy" not in cfg:
        errors.append("Missing top-level 'strategy' key")
        return errors  # can't continue without strategy block

    s = cfg["strategy"]
    for required in ("name", "base_currency", "start_date", "end_date"):
        if required not in s:
            errors.append(f"strategy.{required} is required")

    if "legs" not in cfg or not cfg["legs"]:
        errors.append("At least one leg must be defined under 'legs'")
        return errors

    leg_ids = {leg["id"] for leg in cfg["legs"]}
    has_cash = any(leg.get("type") == "cash" for leg in cfg["legs"])
    if not has_cash:
        errors.append("A 'cash' leg is required")

    for leg in cfg["legs"]:
        lid = leg.get("id", "<unknown>")
        if "id" not in leg:
            errors.append("Every leg must have an 'id' field")
        if "type" not in leg:
            errors.append(f"Leg '{lid}' is missing a 'type' field")
        if leg.get("type") in ("call_option", "put_option"):
            cov = leg.get("coverage", {})
            against = cov.get("against")
            if not against:
                errors.append(f"Option leg '{lid}' must specify coverage.against")
            elif against not in leg_ids:
                errors.append(
                    f"Option leg '{lid}' references unknown equity leg '{against}'"
                )
    return errors


def _leg_to_config_row(leg: dict, strategy: dict, initial_capital: float) -> dict:
    """
    Convert a single YAML leg dict to a flat dict matching portfolio_configs columns.
    """
    leg_type = leg["type"]
    sec_type = _map_sec_type(leg_type)
    coverage = leg.get("coverage", {})
    selection = leg.get("selection", {})
    pricing = leg.get("pricing", {})

    # Allocation: cash uses initial_capital as allocation amount;
    # equity uses the fractional weight; options use direction * coverage_ratio
    if leg_type == "cash":
        allocation = initial_capital
    elif leg_type == "equity":
        allocation = leg.get("allocation", 1.0)
    else:  # option
        raw_alloc = leg.get("allocation")
        if raw_alloc is None:
            raise ValueError(
                f"Option leg '{leg.get('id', '<unknown>')}' must specify 'allocation'. "
                "Use a negative value to sell-to-open (e.g. -1.0) or positive to buy-to-open."
            )
        direction = -1 if float(raw_alloc) < 0 else 1
        ratio = float(coverage.get("ratio", 1.0))
        allocation = direction * ratio

    # Resolve sec_name
    if leg_type == "cash":
        sec_name = "Cash"
    elif leg_type == "equity":
        sec_name = leg.get("ticker", "")
    else:
        sec_name = leg.get("underlying_ticker", leg.get("ticker", ""))

    return {
        "sec_id": leg["id"],
        "sec_name": sec_name,
        "sec_type": sec_type,
        "currency": leg.get("currency", strategy.get("base_currency", "USD")),
        "allocation": allocation,
        "option_w_against": coverage.get("against", None),
        "start_date": strategy["start_date"],
        "end_date": strategy.get("end_date"),
        "option_selection": "custom" if selection.get("method") == "custom" else selection.get("method"),
        "custom_options_file": selection.get("custom_file"),
        "option_sell_to_open_price": pricing.get("sell_price"),
        "option_buy_to_close_price": pricing.get("buy_price"),
        "DTM": strategy.get("DTM"),
        "rebal_rule": _map_rebal_rule(strategy.get("rebalance_schedule", "monthly_third_friday")),
        "pct_otm": selection.get("pct_otm"),
        "target_yield": strategy.get("target_yield"),
    }


def _map_sec_type(leg_type: str) -> str:
    mapping = {
        "cash": "cash",
        "equity": "equity",
        "call_option": "call option",
        "put_option": "put option",
    }
    return mapping.get(leg_type, leg_type)


def _map_rebal_rule(schedule: str) -> str:
    mapping = {
        "monthly_third_friday": "S",
        "weekly_friday": "W",
        "quarterly": "Q",
        "custom_file": "S",
    }
    return mapping.get(schedule, "S")


def _resolve_loader(data_source: dict, explicit_loader):
    """
    Return a DataLoader instance based on the data_source config block or
    the explicitly supplied loader.  The explicit loader always wins.
    """
    if explicit_loader is not None:
        return explicit_loader

    source_type = data_source.get("type", "database")
    if source_type == "file":
        from data_loader import FileDataLoader
        return FileDataLoader(
            equity_dir=data_source.get("equity_dir", ""),
            options_dir=data_source.get("options_dir", ""),
            dividends_dir=data_source.get("dividends_dir", ""),
            fx_file=data_source.get("fx_file", ""),
            holidays_dir=data_source.get("holidays_dir", ""),
        )
    # "database" or unspecified — return None so security_data uses the
    # existing DB path (backward compatible)
    return None


def _build_rebal_dates(strategy: dict, data_loader) -> list:
    """
    Build the list of rebalance dates from the strategy config.
    Falls back to the standard third-Friday rule when no explicit file is given.
    """
    rebal_file = strategy.get("rebalance_dates_file")
    if rebal_file and os.path.exists(rebal_file):
        df = pd.read_csv(rebal_file)
        return [d.date() for d in pd.to_datetime(df.iloc[:, 0])]

    # Build from schedule
    schedule = strategy.get("rebalance_schedule", "monthly_third_friday")
    start_date = dt.datetime.strptime(strategy["start_date"], "%Y-%m-%d")
    end_date = dt.datetime.strptime(strategy["end_date"], "%Y-%m-%d")

    holidays = _get_holidays(strategy, data_loader)

    if schedule == "weekly_friday":
        from helper_functions.rebalance_dates import weekly_option_dates
        return weekly_option_dates(start_date, holidays, end_date)

    # Default: monthly third-Friday
    from helper_functions.rebalance_dates import option_dates
    return option_dates(start_date, holidays, end_date)


def _get_holidays(strategy: dict, data_loader) -> dict:
    """Resolve the holiday dict from the strategy config or data_loader."""
    holidays_file = strategy.get("holidays_file")
    if holidays_file and os.path.exists(holidays_file):
        from utils.market_utils import load_holidays_from_csv
        return load_holidays_from_csv(holidays_file)

    if data_loader is not None:
        cal = strategy.get("holiday_calendar", "TSX")
        return data_loader.get_holidays(cal)

    # Try the internal library if available
    try:
        import sys
        sys.path.append("Z:\\ApolloGX")
        import im_prod.std_lib.data_library as _dl
        cal = strategy.get("holiday_calendar", "TSX").upper()
        return _dl.tsx_holidays() if cal == "TSX" else _dl.nyse_holidays()
    except ImportError:
        pass

    # Try the local common.py
    try:
        import common as _common
        cal = strategy.get("holiday_calendar", "TSX").upper()
        return _common.tsx_holidays() if cal == "TSX" else _common.nyse_holidays()
    except Exception:
        pass

    return {}
