import pandas as pd
import numpy as np
import datetime as dt
import helper_functions.rebalance_dates as rebal_dates
from helper_functions.securities import security_data
import models.cboe as cboe
import models.btbuilder_weekly as btbuilder

import os
cur_dir = os.path.dirname(__file__)
import sys
sys.path.append('Z:\\ApolloGX')
if "\\im_dev\\" in cur_dir:
    import im_dev.std_lib.common as common
    import im_dev.std_lib.data_library as data_library
else:
    import im_prod.std_lib.common as common
    import im_prod.std_lib.data_library as data_library

import warnings
warnings.filterwarnings("ignore") 



def run_backtest(backtestname:str, end_date:dt.datetime, model:str, holidays, timestamp:dt.datetime=dt.datetime.now(), save_folder:str=f"{cur_dir}\\output", optional_des:str=""):
    #establish save folders
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    save_location = f"{save_folder}\\{timestamp.strftime('%Y%m%d_%H%M')}_{os.getenv('username')}\\"
    if not os.path.exists(save_location):
        os.makedirs(save_location)

    df_config = pd.read_csv(f"{cur_dir}\\runs\\portfolio_configs.csv", delimiter=',')
    df_config = df_config[df_config['backtest'] == backtestname].reset_index(drop=True)
    df_config['sec_name'] = df_config['sec_name'].fillna('NA')
    df_config["start_date"] = pd.to_datetime(df_config["start_date"]).dt.strftime("%Y-%m-%d")
    start_date = dt.datetime.strptime(df_config[df_config['sec_id'] == str("cash")]['start_date'].values[0], "%Y-%m-%d")
    opt_rebalance_dates = rebal_dates.option_dates(start_date, holidays, end_date)

    portfolio = {}
    for idx, row in df_config.iterrows():
        portfolio.update({row['sec_id']: security_data(row, cur_dir=cur_dir, start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"), opt_rebal_dates=opt_rebalance_dates)})

    
    # Run backtest
    df_config.to_csv(f"{save_location}\\{backtestname}_{model}{optional_des}_configurations.csv", index=False)  # saves configurations for the run
    if model == 'btbuilder_weekly':
        aggregate, detailed = btbuilder.run_portfolio_backtest(portfolio, start_date, end_date, opt_rebalance_dates, holidays, reinvest_premium=True)
        aggregate.to_csv(f"{save_location}\\{backtestname}_aggregate_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        detailed.to_csv(f"{save_location}\\{backtestname}_detailed_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        p_cash_flow_report = btbuilder.cashflow_period_report(detailed, opt_rebalance_dates)
        p_cash_flow_report.to_csv(f"{save_location}\\{backtestname}_period_return_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        df_option_list = detailed[(detailed['sec_id'].str.contains('option')) & (detailed['open_qty']==0)].reset_index(drop=True)[['date', 'sec_ticker', 'bid', 'ask', 'opt_u_price']]
        df_option_list.to_csv(f"{save_location}\\{backtestname}_option_list_{model}{optional_des}.csv", index=False)  # saves daily constituent level report

    elif model == 'cboe':
        aggregate, detailed = cboe.build_backtest(portfolio, start_date, end_date, opt_rebalance_dates, holidays)
        aggregate.to_csv(f"{save_location}\\{backtestname}_aggregate_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        detailed.to_csv(f"{save_location}\\{backtestname}_detailed_{model}{optional_des}.csv", index=False)  # saves daily constituent level report

if __name__ == "__main__":                                        
    backtestname = "ZEB" #pick up from \\runs\\portfolio_configs.csv
    end_date = dt.datetime(2024, 8, 23)
    # run_backtest(backtestname, end_date, "btbuilder", data_library.tsx_holidays(), timestamp=dt.datetime.now(), optional_des=f"_30_5_OTM_Long_20_OTM")
    run_backtest(backtestname, end_date, "btbuilder_weekly", data_library.tsx_holidays(), timestamp=dt.datetime.now(), optional_des=f"_5_OTM")
   
