import pandas as pd
import numpy as np
import datetime as dt
import os
import sys
sys.path.append('Z:\\ApolloGX')


import im_prod.std_lib.data_library as data_library
import im_prod.std_lib.bloomberg_session as bs
import im_prod.std_lib.common as common
from im_prod.std_lib.security_master import GXigon
from blp import blp
import win32com.client as win32


import warnings
warnings.filterwarnings('ignore')


option_pricing_loc = "Z:\\IPS\\python\\backtest\\data\\option_pricing\\"
market_data_tbl_columns = {"gx_id":str, "date":str, "ticker":str, "field":str, "value":float, "currency":str, "source":str, "script_source":str}

conn = common.db_connection()

def latest_date_pull(ticker):
    result = conn.query_tbl(f"""SELECT MAX(date) as latest_date FROM market_data WHERE ticker = '{ticker}'""")
    if result.iloc[0,0] is None:
        return None
    else:
        return result.iloc[0,0]




for file in os.listdir(option_pricing_loc):
    if file.endswith(".csv"):
        df = pd.read_csv(option_pricing_loc + file)
        print("Loading: " + file)
        #ticker = df['ticker'].iloc[0].split(' ')[0] + ' ' + df['ticker'].iloc[0].split(' ')[1]
        df['ticker'] = df['ticker'].str.replace(' Equity', '')
        df['gx_id'] = np.nan
        df['field'] = np.where(df['side'] == 'bid','px_bid','px_ask')
        df = df.drop(columns=['side'])
        df['currency'] = 'CAD' if df['ticker'].iloc[0].split(' ')[1] == 'CN' else 'USD'
        df['source'] = 'bloomberg'
        df['script_source'] = os.path.abspath(__file__)
        df = df.reindex(columns=['gx_id', 'date', 'ticker', 'field', 'value', 'currency', 'source', 'script_source'])
        df['date'] = pd.to_datetime(df['date'])
        latest_date = latest_date_pull(df['ticker'].iloc[0])

        filtered_dfs = []
        for ticker in df['ticker'].unique():
            ticker_df = df[df['ticker'] == ticker]
            latest_date = latest_date_pull(ticker)
            if latest_date:
                ticker_df = ticker_df[ticker_df['date']>latest_date]
            if not ticker_df.empty:
                filtered_dfs.append(ticker_df)
        if filtered_dfs:
            filtered_df = pd.concat(filtered_dfs, ignore_index=True)
            conn.insert_tbl(filtered_df)
        else:
            print('Data for file: '+file+' already up to date in database')









