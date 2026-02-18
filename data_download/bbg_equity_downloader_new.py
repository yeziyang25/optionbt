import pandas as pd
import numpy as np
import datetime as dt
import inspect

import sys
sys.path.append('Z:\\ApolloGX')
import im_prod.std_lib.common as common
from im_prod.std_lib.bloomberg_session import *
from im_prod.std_lib.security_master import GXigon

TABLE_NAME = "market_data"
FIELD_NAME = "px_last"

def load_data_database(download_list:list, sm, script_source:str, end_dt_input:dt.datetime=None):
    """
    This functions pulls data from bloomberg for a select list of tickers
    :param download_list: ticker must follow the convention: "[ticker] [exch code]" i.e. "AAPL US"
    :return:
    """

    conn = common.db_connection()

    if end_dt_input is None:
        end_dt = common.workday(dt.datetime.now(), -1)
    else:
        end_dt = end_dt_input

    for sec in download_list:
        database_ticker = sec.replace(" Equity", "") if " Equity" in sec else sec # use this format for the database

        latest_date = pd.to_datetime(conn.query_tbl(f"SELECT MAX(date) as max_date FROM {TABLE_NAME} WHERE ticker = '{database_ticker}' and field='px_last' and source = 'bloomberg';").iloc[0, 0])
        #latest_date = dt.datetime(2020, 1, 1)
        if latest_date is None:
            _start_dt = dt.datetime(2018, 1, 1)
        else:
            _start_dt = latest_date

        if latest_date is None or (end_dt-latest_date).days > 0:

            #currency download
            bdp = BDP_Session()  # establish bloomberg connection
            currency_data = bdp.bdp_request([sec], ["crncy"])
            currency = currency_data.get(sec).get("crncy")

            row = pd.DataFrame(data=[[database_ticker, currency]], columns=["ticker", "currency"]).loc[0]
            _gx_id = sm.find_gx_id(row, {"currency": "currency", "ticker": 'ticker'})

            if not _gx_id is None:
                #historic data download
                bdp = BDP_Session()# establish bloomberg connection
                data = bdp.bdh_request([sec], [FIELD_NAME], start_date=_start_dt, end_date=end_dt)
                data_df = pd.DataFrame.from_dict(data.get(sec)).reset_index()

                #build dataframe for export
                export_mkt_data = pd.DataFrame(columns=["gx_id", "date", "ticker", "field", "value", "currency", "source", "script_source"])
                export_mkt_data["date"] = data_df["index"]
                export_mkt_data["ticker"] = database_ticker
                export_mkt_data["field"] = FIELD_NAME
                export_mkt_data["value"] = data_df[FIELD_NAME]
                export_mkt_data["currency"] = currency
                export_mkt_data["source"] = "bloomberg"
                export_mkt_data["script_source"] = script_source
                export_mkt_data["gx_id"] = _gx_id

                #insert data
                if not export_mkt_data.empty:
                    conn.insert_data(export_mkt_data, TABLE_NAME)
                    print(f"Successfully loaded equity data for: {sec}")
            else:
                print(f"Cannot find a corresponding gx_id for: {sec}")

if __name__ == '__main__':
    sec_list = ["XIU CN Equity"] #use full bloomberg ticker. i.e. AAPL US Equity, XBT Curncy, SPX Index
    sm = GXigon()
    load_data_database(sec_list, sm, inspect.getfile(lambda: None), dt.datetime(2025, 12, 31))