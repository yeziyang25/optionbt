import pandas as pd
import datetime as dt
import os
import sys
import numpy as np
import requests
from io import BytesIO
from typing import List, Optional, Tuple, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper_functions.rebalance_dates import option_dates

cur_dir = os.path.dirname(__file__)

# ---------------------------------------------------------------------------
# Optional internal-library imports (production DB path).
# The download / save-to-CSV path works without them.
# ---------------------------------------------------------------------------
_common = None
_data_library = None
try:
    sys.path.append("Z:\\ApolloGX\\im_prod\\std_lib")
    import common as _common          # type: ignore
    import data_library as _data_library  # type: ignore
except Exception:
    pass

MAX_DOWNLOAD_ATTEMPTS = 8  # Number of retry attempts for TMX data downloads

market_data_tbl_columns = {
    "gx_id": str, "date": str, "ticker": str, "field": str,
    "value": float, "currency": str, "source": str, "script_source": str,
}

def fetch_data(ticker:str, date_ranges:Tuple[dt.datetime, dt.datetime], call_put:str, pct_otm_limit:float=0.05) -> Dict[str, pd.DataFrame]:
    """
    Fetch option data for given tickers and date ranges

    arguments:
        tickers: List of ticker symbols.
        date_ranges: start and end date ranges.
    returns:
        Dictionary of DataFrames containing option data for each ticker.
    """
    start_dt = date_ranges[0]
    end_dt = date_ranges[1]
    df_download = download_html(ticker, start_dt, end_dt)
    if not df_download.empty:
        df_download = modify_data(df_download, start_dt, pct_otm_limit, call_put)
        return df_download
    else:
        print(f"No data available for {ticker} between: {start_dt.strftime('%Y-%m-%d')} and {end_dt.strftime('%Y-%m-%d')}")
        return pd.DataFrame()

def download_html(ticker: str, start_date: dt.datetime, end_date: dt.datetime) -> pd.DataFrame:
    """
    Download option data from the TMX (Montreal Exchange) website.

    The download is attempted via the public historical-data URL and falls
    back gracefully on network or parsing errors.  No internal library or
    database connection is required.

    Arguments
    ---------
    ticker     : TMX root ticker (e.g. "XIU", "SPY")
    start_date : period start (datetime)
    end_date   : period end   (datetime)

    Returns
    -------
    pd.DataFrame — raw HTML table, or empty DataFrame on failure.
    """
    url = (
        f"https://www.m-x.ca/en/trading/data/historical"
        f"?symbol={ticker.lower()}"
        f"&from={start_date.strftime('%Y-%m-%d')}"
        f"&to={end_date.strftime('%Y-%m-%d')}"
        f"&dnld=1#quotes"
    )
    for attempt in range(MAX_DOWNLOAD_ATTEMPTS):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(BytesIO(resp.content))
            print(
                f"Downloaded data for {ticker} "
                f"from {start_date.strftime('%Y-%m-%d')} "
                f"to {end_date.strftime('%Y-%m-%d')}"
            )
            return df
        except Exception as exc:
            print(
                f"Attempt {attempt + 1}/{MAX_DOWNLOAD_ATTEMPTS} failed for {ticker} "
                f"({start_date.strftime('%Y-%m-%d')} – {end_date.strftime('%Y-%m-%d')}): {exc}"
            )
    return pd.DataFrame()

def modify_data(df: pd.DataFrame, start_dt:dt.datetime, pct_otm_limit:float, call_put:str) -> pd.DataFrame:
    """
    Modify the downloaded option data to prepare for upload
    """
    underlying_prices = df[df["Class Symbol"].isnull()]
    underlying_prices_dict = dict(zip(underlying_prices["Date"], underlying_prices["Last Price"]))
    df = df[~df['Symbol'].isnull()]
    df['Class Symbol'] = df['Class Symbol'].fillna('NA')

    if call_put.lower() == "call":
        df = df[df['Call/Put'] == 0].reset_index(drop=True)
    elif call_put.lower() == "put":
        df = df[df['Call/Put'] == 1].reset_index(drop=True)
    else:
        raise ValueError("Do not recognise call/put option type")

    # Add ticker column
    df['ticker'] = (df['Class Symbol'] + ' CN ' + 
                    pd.to_datetime(df['Expiry Date']).dt.strftime('%m/%d/%y') + ' ' + 
                    np.where(df['Call/Put'] == 1, 'P', 'C') + 
                    round(df['Strike Price'], 2).astype(str))

    df_new = pd.melt(df, id_vars=['ticker', 'Date', 'Strike Price', 'Call/Put'], value_vars=['Bid Price', 'Ask Price'])
    df_new['side'] = np.where(df_new['variable'] == 'Bid Price', 'px_bid', 'px_ask')
    df_new = df_new[['ticker', 'Date', 'side', 'value', "Strike Price", "Call/Put"]].rename(columns={'Date': 'date'})
    df_new["underlying_price"] = df_new["date"].map(underlying_prices_dict)

    #remove ITM and way OTM
    df_new["pct_otm"] = np.where(df_new["Call/Put"] == 1,
                                 df_new["underlying_price"]/df_new["Strike Price"]-1,
                                 df_new["Strike Price"]/df_new["underlying_price"]-1)

    # identify the tickers in scope. These are the options that fall within the pct OTM range on the start_dt, or the rebalance date.
    df_start_dt = df_new.copy(deep=True)
    df_start_dt = df_start_dt.loc[pd.to_datetime(df_start_dt.date).dt.strftime("%Y-%m-%d") == start_dt.strftime("%Y-%m-%d")]
    df_start_dt = df_start_dt[(df_start_dt["pct_otm"] <= pct_otm_limit) & (df_start_dt["pct_otm"] >= -0.005)]
    option_universe = np.unique(df_start_dt["ticker"]).tolist()
    if option_universe == []:
        return pd.DataFrame()
    else:
        df_new = df_new[df_new["ticker"].isin(option_universe)]

        # Calculate and filter by time to maturity
        df_new['expiration_date'] = pd.to_datetime(df_new['ticker'].str.split().str[-2], format='%m/%d/%y')
        df_new['TTM'] = (df_new['expiration_date'] - pd.to_datetime(df_new['date'])).dt.days
        df_new = df_new[(df_new['TTM'] < 66)]
        df_new = df_new[df_new['value'] != 0]

        df_new.loc[:, 'ticker'] = df_new.loc[:, 'ticker'].apply(lambda x: x if x[-2:] != ".0" else x[:-2])

        return df_new

def remove_existing_records(upload_data: pd.DataFrame) -> None:
    """
    Remove duplicate records from the database before uploading new TMX data.
    Requires a live database connection (``_common`` must be importable).
    Skipped silently when running in standalone/file mode.
    """
    if _common is None:
        print("Skipping remove_existing_records — no database connection available.")
        return

    upload_data_min = upload_data["date"].min()
    upload_data_max = upload_data["date"].max()

    conn = _common.db_connection()
    str_sql = (
        f"SELECT * FROM market_data "
        f"WHERE ticker IN ('{"','".join(np.unique(upload_data['ticker']).tolist())}') "
        f"AND date>='{upload_data_min}' AND date<='{upload_data_max}' "
        f"AND source IN ('TMX', 'tmx');"
    )
    qry_data = conn.query_tbl(str_sql)

    upload_data["key"] = (
        upload_data["ticker"] + "_"
        + pd.to_datetime(upload_data["date"]).dt.strftime("%Y%m%d")
    )
    qry_data["key"] = (
        qry_data["ticker"] + "_"
        + pd.to_datetime(qry_data["date"]).dt.strftime("%Y%m%d")
    )
    qry_data["remove_indicator"] = qry_data["key"].map(
        dict(zip(upload_data["key"], "1"))
    )

    if not qry_data.empty:
        del_str_sql = (
            f"DELETE FROM market_data WHERE ID IN "
            f"('{"','".join(qry_data['id'].astype(str).tolist())}');"
        )
        conn.cursor.execute(del_str_sql)
        conn.cursor.commit()
        print("Deleted Existing Records")
        print(qry_data)


def upload_data(df: pd.DataFrame) -> None:
    """
    Upload the processed option data to the database.
    Requires a live database connection (``_common`` must be importable).
    Use :func:`save_to_csv` for the standalone / file-based workflow.
    """
    if _common is None:
        raise RuntimeError(
            "upload_data() requires a database connection. "
            "Use save_to_csv() to persist data locally instead."
        )
    conn = _common.db_connection()
    df_upload = pd.DataFrame(columns=market_data_tbl_columns)
    for col in df_upload.columns:
        if col in df.columns:
            df_upload[col] = df[col]
        else:
            df_upload[col] = None
    df_upload['field'] = df['side']
    df_upload['source'] = 'tmx'
    df_upload['script_source'] = os.path.abspath(__file__)
    df_upload['currency'] = 'CAD' if df_upload['ticker'].iloc[0].split(' ')[1] == 'CN' else 'USD'
    df_upload['date'] = pd.to_datetime(df_upload['date']).dt.strftime("%Y-%m-%d")
    df_upload = df_upload.drop_duplicates().reset_index(drop=True)
    conn.insert_data(df_upload, 'market_data')


def save_to_csv(df: pd.DataFrame, output_dir: str, ticker: str) -> str:
    """
    Persist downloaded option data to a local CSV file instead of uploading
    to the database.  This is the recommended path for standalone operation.

    The output file is written in the format expected by
    :class:`data_loader.FileDataLoader` and the existing ``bs_flag`` CSV
    loading path in :class:`helper_functions.securities.security_data`.

    Output file
    -----------
    ``<output_dir>/<ticker>_backtest_format_options.csv``

    Columns: ``ticker``, ``date``, ``side``, ``value``

    Parameters
    ----------
    df         : DataFrame returned by :func:`fetch_data`
    output_dir : Directory where the CSV will be written (created if needed)
    ticker     : Underlying ticker symbol (used to name the output file)

    Returns
    -------
    str — path of the written file
    """
    os.makedirs(output_dir, exist_ok=True)
    out_cols = ["ticker", "date", "side", "value"]
    df_out = df[out_cols].copy()
    df_out["date"] = pd.to_datetime(df_out["date"]).dt.strftime("%Y-%m-%d")
    df_out = df_out.drop_duplicates().reset_index(drop=True)

    out_path = os.path.join(output_dir, f"{ticker}_backtest_format_options.csv")

    if os.path.exists(out_path):
        # Append only rows that are not already present (idempotent)
        existing = pd.read_csv(out_path)
        existing["date"] = pd.to_datetime(existing["date"]).dt.strftime("%Y-%m-%d")
        existing["_key"] = existing["ticker"] + "_" + existing["date"] + "_" + existing["side"]
        df_out["_key"] = df_out["ticker"] + "_" + df_out["date"] + "_" + df_out["side"]
        df_out = df_out[~df_out["_key"].isin(existing["_key"])].drop(columns=["_key"])
        existing = existing.drop(columns=["_key"])
        df_combined = pd.concat([existing, df_out], ignore_index=True)
        df_combined.to_csv(out_path, index=False)
        print(f"Appended {len(df_out)} new rows to {out_path}")
    else:
        df_out.to_csv(out_path, index=False)
        print(f"Saved {len(df_out)} rows to {out_path}")

    return out_path

if __name__ == '__main__':
    # ------------------------------------------------------------------
    # Standalone example — downloads from TMX and saves to local CSV.
    # No database connection is required.
    #
    # To upload to the database instead, replace save_to_csv() calls with:
    #   remove_existing_records(all_data)
    #   upload_data(all_data)
    # (requires the internal IM library and a live DB connection)
    # ------------------------------------------------------------------
    import argparse

    parser = argparse.ArgumentParser(description="Download TMX option data to CSV")
    parser.add_argument("--tickers", nargs="+", default=["BTCC"],
                        help="TMX root ticker(s), e.g. XIU SPY")
    parser.add_argument("--start", default="2021-01-02",
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31",
                        help="End date YYYY-MM-DD")
    parser.add_argument("--call-put", default="call", choices=["call", "put"],
                        help="Option type to download")
    parser.add_argument("--pct-otm-limit", type=float, default=0.35,
                        help="Maximum pct-OTM filter on the selection date")
    parser.add_argument("--output-dir", default="data/options",
                        help="Directory to write output CSV files")
    parser.add_argument("--holidays", default=None,
                        help="Path to a holidays CSV (date, name). "
                             "If omitted and a DB is available, TSX holidays are fetched.")
    args = parser.parse_args()

    # Build holiday dict
    if args.holidays:
        from utils.market_utils import load_holidays_from_csv
        holidays = load_holidays_from_csv(args.holidays)
    elif _data_library is not None:
        try:
            holidays = _data_library.tsx_holidays()
        except Exception:
            holidays = {}
    else:
        holidays = {}

    start_dt = dt.datetime.strptime(args.start, "%Y-%m-%d")
    end_dt = dt.datetime.strptime(args.end, "%Y-%m-%d")
    opt_roll_dates = option_dates(start_dt, holidays, end_dt)

    date_ranges = [
        (opt_roll_dates[i], opt_roll_dates[i + 1])
        for i in range(len(opt_roll_dates) - 1)
    ]

    for ticker in args.tickers:
        print(f"\n=== Fetching {ticker} ===")
        for d_elm in date_ranges:
            all_data = fetch_data(ticker, d_elm, args.call_put,
                                  pct_otm_limit=args.pct_otm_limit)
            if not all_data.empty:
                save_to_csv(all_data, args.output_dir, ticker)
                print(f"Saved data for {ticker} ({d_elm[0]} – {d_elm[1]})")