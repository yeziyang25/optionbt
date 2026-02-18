import pandas as pd
import datetime as dt
import numpy as np
import math
import calendar
import os
import sys
sys.path.append(f"{os.path.dirname(os.path.dirname(__file__))}")
import helper_functions.rebalance_dates as rebal_dates

sys.path.append('Z:\\ApolloGX')
import im_prod.std_lib.common as common
import im_prod.std_lib.data_library as data_library
from im_prod.std_lib.bloomberg_session import *

def myround(x, base=5):
    return base * round(x/base)

def chains_given_expiry(expiry_dt:dt.datetime, min_pct_strike:float, max_pct_strike:float, underlying_security:str, underlying_price:float, call_put_factor:float, strike_interval:float):
    output = []
    min_strike = myround(underlying_price * (1 + np.sign(call_put_factor)*min_pct_strike), strike_interval)
    max_strike = myround(underlying_price * (1 + np.sign(call_put_factor)*max_pct_strike), strike_interval)

    call_put_str = {1:' C', -1: ' P'}

    for cur_strike_100 in range(int(min_strike*100), int(max_strike*100), int(np.sign(call_put_factor)*strike_interval*100)):
        cur_strike = cur_strike_100/100
    # while cur_strike <= max_strike:
        _ticker = underlying_security + str(' ') + expiry_dt.strftime('%m/%d/%y') + call_put_str.get(int(call_put_factor)) + str(cur_strike).replace('.0', '') + str(' Equity')
        output.append(_ticker)
        cur_strike += strike_interval * call_put_factor
    return output

def gather_option_chains(rebal_date:dt.datetime, max_expiry:dt.datetime, min_pct_strike:float, max_pct_strike:float, underlying_security:str, underlying_price:float, call_put_factor:float, strike_interval:float, expiry_dt:dt.datetime=None):
    ticker_list = []
    _d = rebal_date
    if not expiry_dt is None:
        chain_per_expiry_dt = chains_given_expiry(expiry_dt, min_pct_strike, max_pct_strike, underlying_security, underlying_price, call_put_factor, strike_interval)
        ticker_list += chain_per_expiry_dt
    else:
        while _d <= max_expiry:
            chain_per_expiry_dt = chains_given_expiry(_d, min_pct_strike, max_pct_strike, underlying_security, underlying_price, call_put_factor, strike_interval)
            ticker_list += chain_per_expiry_dt
            _d+=dt.timedelta(days=7)
    return ticker_list

def build_option_chain_universe(start_date:dt.datetime, end_date:dt.datetime, underlying_security:str, strike_interval:float, call_put:str, rebalance_dates:dict, min_pct_strike:float, max_pct_strike:float, bbg_ticker_format:str=' Equity', include_weekly:bool=False):
    print(f"Part (1/4): Build Option Chain Universe")
    call_put_factor = 1 if call_put.lower() == 'call' else -1

    bdp = BDP_Session()
    data = bdp.bdh_request([underlying_security + bbg_ticker_format], ['PX_LAST'], start_date=start_date, end_date=end_date)
    hist_price = data.get(underlying_security + bbg_ticker_format).get('PX_LAST')

    output_col = ["rebal_date", "ticker"]
    opt_chain = []
    # opt_chain = pd.DataFrame(columns=['trade_date', 'expiry', 'option'])
    for idx in range(len(rebalance_dates)-2):
        d = rebalance_dates[idx]
        if d < dt.datetime.now().date():
            underlying_price = hist_price.get(d)
            if include_weekly:
                expiry_dt = None
            else:
                expiry_dt = rebalance_dates[idx+1]

            opt_chain_on_rebal = gather_option_chains(d, rebalance_dates[idx+2], min_pct_strike, max_pct_strike, underlying_security, underlying_price, call_put_factor, strike_interval, expiry_dt)
            opt_chain += [[d, x] for x in opt_chain_on_rebal]


    df_opt_chain = pd.DataFrame(opt_chain, columns=output_col)
    opt_cls = common.extract_option_ticker(df_opt_chain, 'ticker')
    df_opt_chain['expiry'] = df_opt_chain['ticker'].map(opt_cls.expiry)
    df_opt_chain = df_opt_chain[df_opt_chain['expiry'].isin(rebalance_dates)]
    df_opt_chain['underlying_price'] = df_opt_chain['rebal_date'].map(hist_price)
    return df_opt_chain