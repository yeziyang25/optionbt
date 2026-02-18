import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import numpy as np
from typing import List, Tuple, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper_functions.rebalance_dates import option_dates

cur_dir = os.path.dirname(__file__)
sys.path.append('Z:\\ApolloGX\\im_dev\\std_lib')
import common

market_data_tbl_columns = {"gx_id":str, "date":str, "ticker":str, "field":str, "value":float, "currency":str, "source":str, "script_source":str}

def fetch_data(tickers: List[str], date_ranges: List[Tuple[str, str]]) -> Dict[str, pd.DataFrame]:
    """
    Fetch option data for given tickers and date ranges

    arguments:
        tickers: List of ticker symbols.
        date_ranges: List of date ranges in format ('YYYYMMDD', 'YYYYMMDD').
    returns:
        Dictionary of DataFrames containing option data for each ticker.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    # Ensure tickers and date_ranges are lists
    tickers = [tickers] if not isinstance(tickers, list) else tickers
    date_ranges = [date_ranges] if not isinstance(date_ranges, list) else date_ranges
    all_data = {}
    for ticker, date_range in zip(tickers, date_ranges):
        start_date, end_date = [datetime.strptime(date, '%Y%m%d') for date in date_range]
        all_data[ticker] = pd.DataFrame()

        while start_date < end_date:
            chunk_end_date = min(end_date, start_date + timedelta(days=180))
            print(f"Starting {ticker} for {start_date.date()}")
            df_download, new_end_date = download_html(
                ticker,
                start_date.strftime('%Y-%m-%d'),
                chunk_end_date.strftime('%Y-%m-%d')
            )
            df_download = modify_data(df_download)
            all_data[ticker] = pd.concat([all_data[ticker], df_download], ignore_index=True)
            start_date = datetime.strptime(new_end_date, '%Y-%m-%d') + timedelta(days=1)
        all_data[ticker] = all_data[ticker].drop_duplicates().reset_index(drop=True)
    return all_data

def download_html(ticker: str, start_date: str, end_date: str) -> Tuple[pd.DataFrame, str]:
    """
    Download option data from TMX website.

    arguments:
        ticker: Ticker symbol.
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.

    returns:
        Tuple of DataFrame with downloaded data and the end date used.
    """
    download_html = f"https://www.m-x.ca/en/trading/data/historical?symbol={ticker.lower()}&from={start_date}&to={end_date}&dnld=1#quotes"
    current_att = 0
    while current_att < 8:
        try:
            df_download = common.download_from_url(download_html)
            print(f"Downloaded data for {ticker} from {start_date} to {end_date}")
            return df_download, (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=-current_att*7)).strftime('%Y-%m-%d')
        except:
            print(f"Failed to download data for {ticker} from {start_date} to {end_date}")
            current_att += 1
            start_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=-7)).strftime('%Y-%m-%d')
            if current_att == 8:
                raise Exception(f"Failed to download data for {ticker} from {start_date} to {end_date}")
    return df_download, end_date

def modify_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modify the downloaded option data to prepare for upload
    """
    df = df[~df['Symbol'].isnull()]
    df['Class Symbol'] = df['Class Symbol'].fillna('NA')
    df = df[df['Call/Put'] == 0].reset_index(drop=True)

    # Add ticker column
    df['ticker'] = (df['Class Symbol'] + ' CN ' + 
                    pd.to_datetime(df['Expiry Date']).dt.strftime('%m/%d/%y') + ' ' + 
                    np.where(df['Call/Put'] == 1, 'P', 'C') + 
                    round(df['Strike Price'], 2).astype(str))

    df_new = pd.melt(df, id_vars=['ticker', 'Date', 'Strike Price'], value_vars=['Bid Price', 'Ask Price'])
    df_new['side'] = np.where(df_new['variable'] == 'Bid Price', 'bid', 'ask')
    df_new = df_new[['ticker', 'Date', 'side', 'value', "Strike Price"]].rename(columns={'Date': 'date'})

    # Calculate and filter by time to maturity
    df_new['expiration_date'] = pd.to_datetime(df_new['ticker'].str.split().str[-2], format='%m/%d/%y')
    df_new['TTM'] = (df_new['expiration_date'] - pd.to_datetime(df_new['date'])).dt.days
    df_new = df_new[(df_new['TTM'] < 66)]

    df_new = df_new[df_new['value'] != 0]

    df_new.loc[:, 'ticker'] = df_new.loc[:, 'ticker'].apply(lambda x: x if x[-2:] != ".0" else x[:-2])

    return df_new

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
    df_upload['field'] = np.where(df['side'] == 'bid','px_bid','px_ask')
    df_upload['source'] = 'TMX'
    df_upload['script_source'] = os.path.abspath(__file__)
    df_upload = df_upload.reindex(columns=market_data_tbl_columns.keys())
    df_upload['currency'] = 'CAD' if df_upload['ticker'].iloc[0].split(' ')[1] == 'CN' else 'USD'
    df_upload['date'] = pd.to_datetime(df_upload['date'])
    df_upload = df_upload.drop_duplicates().reset_index(drop=True)
    conn.insert_data(df_upload, 'market_data', )

if __name__ == '__main__':
    tickers = ['BMO']
    date_ranges = ('20220101', '20240501')
    all_data = fetch_data(tickers, date_ranges)
    for ticker in tickers:
        upload_data(all_data[ticker])