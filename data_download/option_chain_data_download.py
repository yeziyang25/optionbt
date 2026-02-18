import os.path

import pandas as pd
import numpy as np
import datetime as dt
import calendar
import sys
sys.path.append('Z:\\ApolloGX')
import im_prod.std_lib.common as common
import im_prod.std_lib.data_library as data_library
from im_prod.std_lib.bloomberg_session import *

"""
Downloads the bid/ask prices for the option chains identified by a prior file. Prices are for the rebalance / option selection date only
"""

def upload_data(df_upload: pd.DataFrame, script_source:str) -> None:
    """
    Upload the processed option data to the database.
    """
    conn = common.db_connection()
    df_upload['source'] = 'bloomberg'
    df_upload['currency'] = 'CAD' if df_upload['ticker'].iloc[0].split(' ')[1] == 'CN' else 'USD'
    df_upload['date'] = pd.to_datetime(df_upload['date']).dt.strftime("%Y-%m-%d")
    df_upload['script_source'] = script_source

    unique_tickers = np.unique(df_upload['ticker']).tolist()
    unique_dates = np.unique(df_upload['date']).tolist()
    df_upload["ticker"] = df_upload["ticker"].str.replace(r"\s+Equity$", "", regex=True)
    conn.insert_data(df_upload, 'market_data', f"""SELECT * FROM market_data WHERE source='bloomberg' and field IN ('px_bid', 'px_ask') and ticker IN ('{"','".join(unique_tickers)}') and [date] IN ('{"','".join(unique_dates)}');""")


def clean_data(_data):
    output = pd.DataFrame(columns=['ticker', 'date', 'fld', 'value'])
    for ticker, grouped_data in _data.items():
        for fld, time_data in grouped_data.items():
            if str(time_data) == '#N/A Invalid Security':
                pass
            else:
                for key, val in time_data.items():
                    df = pd.DataFrame.from_dict({"ticker": ticker, "date":key, "fld": fld, "value": [val]})
                    if output.empty:
                        output = df
                    else:
                        output = pd.concat([output, df])
    if output.empty:
        return pd.DataFrame(columns=['ticker', 'date', 'px_bid', 'px_ask'])
    else:
        return pd.pivot_table(output, values='value', index=['ticker', 'date'], columns=['fld'], aggfunc="sum").reset_index()

def download_option_chain_rebal_price(df:pd.DataFrame) -> pd.DataFrame:
    dates_list = np.unique(df['rebal_date']).tolist()
    output = pd.DataFrame()
    for d in dates_list:
        print(f"Part (2/4): Downloading option chain rebalance price for {d}")
        bdp = BDP_Session()
        d_dt = dt.datetime.strptime(str(d), "%Y-%m-%d")
        sub_df = df[df['rebal_date']==d]
        data = bdp.bdh_request(np.unique(sub_df['ticker']).tolist(), ['px_bid', 'px_ask'], start_date=d_dt, end_date=d_dt)
        data2 = clean_data(data)
        data2 = data2[(~data2['px_bid'].isnull()) & (~data2['px_ask'].isnull())]

        if output.empty:
            output = data2
        else:
            output = pd.concat([output, data2])

    df_price = df.groupby(by=['rebal_date'], group_keys=True)[['underlying_price']].apply(max).reset_index()
    price_dict = dict(zip(df_price.rebal_date, df_price.underlying_price))
    output['underlying_price'] = output['date'].map(price_dict)
    return output

def download_option_chain_all_prices(df:pd.DataFrame, script_source:str):
    df_agg= df.groupby(by=['ticker'], group_keys=True)[['date']].apply(min).reset_index()
    opt_cls = common.extract_option_ticker(df_agg, "ticker")
    df_agg['expiry'] = df_agg["ticker"].map(opt_cls.expiry)

    for row in df_agg.itertuples():
        print(f"Part (4/4): Downloading option chain all prices for {row.ticker}")
        bdp = BDP_Session()
        data = bdp.bdh_request([row.ticker], ['px_bid', 'px_ask'], start_date=dt.datetime.strptime(str(row.date), "%Y-%m-%d"), end_date=dt.datetime.strptime(str(row.expiry), "%Y-%m-%d"))

        output_col = ["date", "ticker", "field", "value"]
        output_data = []
        for k, v in data.items():
            for k2, v2 in v.items():
                for k3, v3 in v2.items():
                    output_data += [[k3, k, k2, v3]]

        df_output = pd.DataFrame(data=output_data, columns=output_col)
        upload_data(df_output, script_source)