import pandas as pd
import numpy as np
import datetime as dt
import inspect

import sys
import os
sys.path.append('Z:\\ApolloGX')
import im_prod.std_lib.common as common
from im_prod.std_lib.bloomberg_session import *
from im_prod.std_lib.security_master import GXigon
cur_dir = os.path.dirname(__file__)


TABLE_NAME = "market_data"
FIELD_NAME = "px_last"
equity_save_loc = f"{cur_dir}\\adhoc_pricing\\equity\\"

#the ad hoc function is here in order to download equity data without loading to the database for stocks that have undergone splits/consolidations (don't want to load in case we have data but unadjusted)
def ad_hoc_equity_download(download_list:list, script_source:str, end_dt_input:dt.datetime=None):
    if end_dt_input is None:
        end_dt = common.workday(dt.datetime.now(), -1)
    else:
        end_dt = end_dt_input
    _start_dt = dt.datetime(2018, 1, 3)

    for sec in download_list:
        _ticker = sec + " Equity" if not "Index" in sec else sec
        # currency download
        bdp = BDP_Session()  # establish bloomberg connection
        currency_data = bdp.bdp_request([_ticker], ["crncy"])
        currency = currency_data.get(_ticker).get("crncy")

        bdp = BDP_Session()  # establish bloomberg connection
        data = bdp.bdh_request([_ticker], [FIELD_NAME], start_date=_start_dt, end_date=end_dt)
        data_df = pd.DataFrame.from_dict(data.get(_ticker)).reset_index()

        data_df = data_df.rename(columns={'index': 'date'})
        # # build dataframe for export -- this makes a database compatible table, not necassary when doing ad hoc
        # export_mkt_data = pd.DataFrame(
        #     columns=["gx_id", "date", "ticker", "field", "value", "currency", "source", "script_source"])
        # export_mkt_data["date"] = data_df["index"]
        # export_mkt_data["ticker"] = sec
        # export_mkt_data["field"] = FIELD_NAME
        # export_mkt_data["value"] = data_df[FIELD_NAME]
        # export_mkt_data["currency"] = currency
        # export_mkt_data["source"] = "bloomberg"
        # export_mkt_data["script_source"] = script_source

        # insert data
        if not data_df.empty:
            data_df.to_csv(equity_save_loc + sec + " equity_pricing.csv", index=False)
            print(f"Successfully downloaded equity data for: {sec}")




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
        query = f"""
           SELECT 
               CASE 
                   WHEN COUNT(*) > 0 THEN MAX(date) 
                   ELSE NULL 
               END as max_date 
           FROM {TABLE_NAME} 
           WHERE ticker = '{sec}' and field='px_last' and source = 'bloomberg'
           """

        latest_date = conn.query_tbl(query).iloc[0,0]
        #latest_date = pd.to_datetime(conn.query_tbl(f"SELECT MAX(date) as max_date FROM {TABLE_NAME} WHERE ticker = '{sec}' and field='px_last' and source = 'bloomberg';").iloc[0, 0])

        if latest_date is None:
            _start_dt = dt.datetime(2018, 1, 3)
        else:
            _start_dt = latest_date

        if latest_date is None or (latest_date-end_dt).days < 0:
            _ticker = sec + " Equity" if not "Index" in sec else sec

            #currency download
            bdp = BDP_Session()  # establish bloomberg connection
            currency_data = bdp.bdp_request([_ticker], ["crncy"])
            currency = currency_data.get(_ticker).get("crncy")

            row = pd.DataFrame(data=[[sec, currency]], columns=["ticker", "currency"]).loc[0]
            _gx_id = sm.find_gx_id(row, {"currency": "currency", "ticker": 'ticker'})

            if not _gx_id is None:
                #historic data download
                bdp = BDP_Session()# establish bloomberg connection
                data = bdp.bdh_request([_ticker], [FIELD_NAME], start_date=_start_dt, end_date=end_dt)
                data_df = pd.DataFrame.from_dict(data.get(_ticker)).reset_index()

                #build dataframe for export
                export_mkt_data = pd.DataFrame(columns=["gx_id", "date", "ticker", "field", "value", "currency", "source", "script_source"])
                export_mkt_data["date"] = data_df["index"]
                export_mkt_data["ticker"] = sec
                export_mkt_data["field"] = FIELD_NAME
                export_mkt_data["value"] = data_df[FIELD_NAME]
                export_mkt_data["currency"] = currency
                export_mkt_data["source"] = "bloomberg"
                export_mkt_data["script_source"] = script_source
                export_mkt_data["gx_id"] = _gx_id

                #insert data
                if not export_mkt_data.empty:
                    export_mkt_data.to_csv(equity_save_loc + sec + " pricing.csv", index=False)
                    conn.insert_data(export_mkt_data, TABLE_NAME)
                    print(f"Successfully loaded equity data for: {sec}")
            else:
                # historic data download
                bdp = BDP_Session()  # establish bloomberg connection
                data = bdp.bdh_request([_ticker], [FIELD_NAME], start_date=_start_dt, end_date=end_dt)
                data_df = pd.DataFrame.from_dict(data.get(_ticker)).reset_index()

                # build dataframe for export
                export_mkt_data = pd.DataFrame(
                    columns=["gx_id", "date", "ticker", "field", "value", "currency", "source", "script_source"])
                export_mkt_data["date"] = data_df["index"]
                export_mkt_data["ticker"] = sec
                export_mkt_data["field"] = FIELD_NAME
                export_mkt_data["value"] = data_df[FIELD_NAME]
                export_mkt_data["currency"] = currency
                export_mkt_data["source"] = "bloomberg"
                export_mkt_data["script_source"] = script_source
                export_mkt_data["gx_id"] = _gx_id

                # insert data
                if not export_mkt_data.empty:
                    export_mkt_data.to_csv(equity_save_loc + sec + " equity_pricing.csv", index=False)
                    conn.insert_data(export_mkt_data, TABLE_NAME)
                    print(f"Successfully loaded equity data for: {sec}")
                print(f"Cannot find a corresponding gx_id for: {sec}")

if __name__ == '__main__':
    sec_list = ['ZEB CN']
    sm = GXigon()
    load_data_database(sec_list, sm, inspect.getfile(lambda: None))