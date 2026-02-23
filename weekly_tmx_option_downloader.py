import pandas as pd
import datetime as dt
import os
import sys
import numpy as np
from typing import List, Tuple, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper_functions.rebalance_dates_customized import option_dates_customized

cur_dir = os.path.dirname(__file__)
sys.path.append('Z:\\ApolloGX\\im_prod\\std_lib')
import common
import data_library

market_data_tbl_columns = {"gx_id":str, "date":str, "ticker":str, "field":str, "value":float, "currency":str, "source":str, "script_source":str}

holidays = data_library.tsx_holidays()

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

def download_html(ticker: str, start_date:str, end_date:str) -> Tuple[pd.DataFrame, str]:
    """
    Download option data from TMX website.

    arguments:
        ticker: Ticker symbol.
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.

    returns:
        Tuple of DataFrame with downloaded data and the end date used.
    """
    download_html = f"https://www.m-x.ca/en/trading/data/historical?symbol={ticker.lower()}&from={start_date.strftime('%Y-%m-%d')}&to={end_date.strftime('%Y-%m-%d')}&dnld=1#quotes"
    current_att = 0
    while current_att < 8:
        try:
            df_download = common.download_from_url(download_html)
            print(f"Downloaded data for {ticker} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            return df_download
        except:
            print(f"Failed to download data for {ticker} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
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

def remove_existing_records(upload_data:pd.DataFrame):
    # removing existing duplicated records
    upload_data_min = upload_data["date"].min()
    upload_data_max = upload_data["date"].max()

    conn = common.db_connection()
    str_sql = (f"""SELECT * 
                    FROM market_data 
                    WHERE ticker IN ('{"','".join(np.unique(upload_data["ticker"]).tolist())}') and date>='{upload_data_min}' and date<='{upload_data_max}' and source IN ('TMX', 'tmx');""")
    qry_data = conn.query_tbl(str_sql)

    #find existing data that matches ticker_YYYYMMDD and remove them
    upload_data["key"] = upload_data["ticker"] + str("_") + pd.to_datetime(upload_data["date"]).dt.strftime("%Y%m%d")
    qry_data["key"] = qry_data["ticker"] + str("_") + pd.to_datetime(qry_data["date"]).dt.strftime("%Y%m%d")
    qry_data["remove_indicator"] = qry_data["key"].map(dict(zip(upload_data["key"], "1")))
    # qry_data = qry_data[qry_data["remove_indicator"]=="1"]

    if not qry_data.empty:
        del_str_sql = (f"""DELETE FROM market_data WHERE ID IN ('{"','".join(qry_data["id"].astype(str).tolist())}');""")
        conn.cursor.execute(del_str_sql)
        conn.cursor.commit()
        print("Deleted Existing Records")
        print(qry_data)

def upload_data(df: pd.DataFrame) -> None:
    """
    Upload the processed option data to the database.
    """
    conn = common.db_connection()
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
    print(df_upload)

def filter_and_upload(df: pd.DataFrame, d_elm: tuple, pct_otm_target, strike_mode):
    """
    filter options data only relevant to rolling
    """

    start_date = pd.to_datetime(d_elm[0])
    expiry_target = pd.to_datetime(d_elm[1])
    fallback_expiry = expiry_target + dt.timedelta(days=7)

    df_start = df[pd.to_datetime(df["date"]).dt.date == start_date.date()].copy()
    if df_start.empty:
        print(f"No rows found for start date {start_date.date()}")
        return 

    expiry_str = expiry_target.strftime("%m/%d/%y")
    fallback_str = fallback_expiry.strftime("%m/%d/%y")
    
    # If Friday is a holiday → move to Thursday
    if expiry_target.weekday() == 4 and expiry_str in holidays:
        expiry_target -= dt.timedelta(days=1)
    
    # If Thursday (weekday=3) and NOT a holiday → move to Friday
    elif expiry_target.weekday() == 3 and expiry_str not in holidays:
        expiry_target += dt.timedelta(days=1)
    
    df_selected = df_start[df_start["ticker"].str.contains(expiry_str, na=False)]

    if not df_selected.empty:
        df_target_date = df[df["ticker"].str.contains(expiry_str, na=False)].copy()
        if strike_mode == "round_up":
            df_above = df_selected[df_selected["pct_otm"]>=pct_otm_target]
            if df_above.empty:
                df_above = df_selected.copy()
            df_above["diff"] = (df_above["pct_otm"] - pct_otm_target).abs()
            df_closest = df_above.loc[df_above["diff"].idxmin()].to_frame().T
        elif strike_mode == "round_down":
            df_below = df_selected[(df_selected["pct_otm"]<=pct_otm_target) & (df_selected["pct_otm"]>=0)]
            if df_below.empty:
                df_below = df_selected.copy()
            df_below["diff"] = (pct_otm_target - df_below["pct_otm"]).abs()
            df_closest = df_below.loc[df_below["diff"].idxmin()].to_frame().T
        elif strike_mode == "abs_closest":
            df_abs = df_selected[df_selected["pct_otm"]>=0]
            df_abs["diff"] = (pct_otm_target - df_selected["pct_otm"]).abs()
            df_closest = df_abs.loc[df_abs["diff"].idxmin()].to_frame().T
        selected_ticker = df_closest["ticker"].iloc[0]
        df_tobeupload = df_target_date[df_target_date["ticker"] == selected_ticker].copy()
        print(f"Found {len(df_selected)} rows for expiry {expiry_str}")
    else:
        df_selected = df_start[df_start["ticker"].str.contains(fallback_str, na=False)]
        if not df_selected.empty:
            df_target_date = df[df["ticker"].str.contains(expiry_str, na=False)].copy()
            df_above = df_selected[df_selected["pct_otm"]>=pct_otm_target]
            if df_above.empty:
                df_above = df_selected.copy()
            df_above["diff"] = (df_above["pct_otm"] - pct_otm_target).abs()
            df_closest = df_above.loc[df_above["diff"].idxmin()].to_frame().T
            selected_ticker = df_closest["ticker"].iloc[0]
            df_tobeupload = df_target_date[df_target_date["ticker"] == selected_ticker].copy()
            print(f"No expiry {expiry_str} found. Using fallback {fallback_str} ({len(df_selected)} rows).")
        else:
            print(f"No tickers found for expiry {expiry_str} or fallback {fallback_str}.")
            return
    if not df_tobeupload.empty:
        existing_records = remove_existing_records(df_tobeupload)
        upload_data(df_tobeupload)
        print("Successfully Uploaded Data")

def run_weekly_tmx_download(
    ticker_lis: list,
    start_date: dt.datetime,
    end_date: dt.datetime,
    DTM: int,
    pct_otm_target: float,
    strike_mode: str, #round_up, round_down, abs_closest
    call_put: str = "call",
    pct_otm_limit: float = 0.05
):

    opt_roll_dates = option_dates_customized(
        start_date,
        data_library.tsx_holidays(),
        end_date,
        DTM
    )

    date_ranges = [(opt_roll_dates[i], opt_roll_dates[i + 1]) for i in range(len(opt_roll_dates) - 1)]

    for ticker in ticker_lis:
        print(f"\nProcessing {ticker}...")
        for d_elm in date_ranges:
            all_data = fetch_data(ticker, d_elm, call_put, pct_otm_limit=pct_otm_limit)
            print(f"Successfully generated Dataframe for: {ticker}")
            filter_and_upload(all_data, d_elm, pct_otm_target, strike_mode)


if __name__ == "__main__":
    run_weekly_tmx_download(
        ticker_lis=["XIU"],
        start_date=dt.datetime(2025, 1, 1),
        end_date=dt.datetime(2025, 12, 31),
        DTM = 3,
        pct_otm_target = 0.01,
        strike_mode = "abs_closest", 
        call_put="call",
        pct_otm_limit=0.05
    )
