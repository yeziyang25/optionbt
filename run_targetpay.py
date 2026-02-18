import pandas as pd
import numpy as np
import datetime as dt
# import helper_functions.rebalance_dates_customized as rebal_dates
from helper_functions.securities import security_data
from runs.create_custom_options_customized_tenor import custom_option
from helper_functions.rebalance_dates_customized import option_dates_customized
import models.cboe as cboe
import models.btbuilder_customized as btbuilder
import im_prod.std_lib.common as common
import matplotlib.pyplot as plt
from im_prod.std_lib.bloomberg_session import *
from xbbg import blp 

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


conn = common.db_connection()

def build_custom_option_files(backtestname: str,
                              start_date: dt.datetime,
                              end_date: dt.datetime,
                              tenor: int,
                              holidays,
                              conn,
                              call_put: str,
                              pct_otm: float):
    """
    Build the custom option list + valid_option_dates for this backtest.
    """
    opt_rebal_dates = option_dates_customized(start_date, holidays, end_date, tenor)
    #backtestname_list = ["RCI", "BCE", "T"]
    #for backtestname in backtestname_list:
    opt_underlying_ticker = f"{backtestname} US"
    opt_cls = custom_option(opt_underlying_ticker, conn, opt_rebal_dates, call_put)
    
    opt_cls.generate_custom_list(pct_otm=pct_otm)
    
    runs_folder = os.path.join(cur_dir, "runs")
    opt_cls.save_custom_file(runs_folder, f"_{pct_otm}")
    
def run_backtest(backtestname:str, end_date:dt.datetime, model:str, tenor:int, equity_rebal_rule:str, target_yield: float, pct_otm: list, holidays, timestamp:dt.datetime=dt.datetime.now(), save_folder:str=f"{cur_dir}\\output", optional_des:str=""):
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
    #opt_rebalance_dates = rebal_dates.option_dates_customized(start_date, holidays, end_date, tenor)

    for moneyness in pct_otm:
        build_custom_option_files(
                backtestname=backtestname,
                start_date=start_date,
                end_date=end_date,
                tenor=tenor,
                holidays=holidays,
                conn=conn,
                call_put="call",   
                pct_otm=moneyness  
            )
        
    valid_dates_file = f"{cur_dir}\\runs\\custom_options_list\\{backtestname} US_valid_option_dates.csv"
    #valid_dates_file = f"{cur_dir}\\runs\\custom_options_list\\BCE CN_valid_option_dates.csv"
    if os.path.exists(valid_dates_file):
        opt_rebalance_dates = [d.date() for d in pd.to_datetime(pd.read_csv(valid_dates_file)["valid_option_dates"])]

 
    portfolio = {}
    for idx, row in df_config.iterrows():
        portfolio.update({row['sec_id']: security_data(row, cur_dir=cur_dir, start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"), opt_rebal_dates=opt_rebalance_dates)})

    # Run backtest
    df_config.to_csv(f"{save_location}\\{backtestname}_{model}{optional_des}_configurations.csv", index=False)  # saves configurations for the run
    if model == 'btbuilder':
        if target_yield == -1:
            aggregate, detailed = btbuilder.run_portfolio_backtest(portfolio, start_date, end_date, opt_rebalance_dates, holidays, equity_rebal_rule, target_yield, reinvest_premium=True)
        else:
            aggregate, detailed, coverage = btbuilder.run_portfolio_backtest(portfolio, start_date, end_date, opt_rebalance_dates, holidays, equity_rebal_rule, target_yield, reinvest_premium=True)
            coverage_ratio = pd.DataFrame(list(coverage.items()), columns = ["Roll Day", "Coverage Ratio"])
            coverage_ratio.to_csv(f"{save_location}\\{backtestname}_coverage_ratio_{model}{optional_des}.csv", index=False)
        aggregate.to_csv(f"{save_location}\\{backtestname}_aggregate_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        detailed.to_csv(f"{save_location}\\{backtestname}_detailed_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        p_cash_flow_report = btbuilder.cashflow_period_report(detailed, opt_rebalance_dates)
        p_cash_flow_report.to_csv(f"{save_location}\\{backtestname}_period_return_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        df_option_list = detailed[(detailed['sec_id'].str.contains('option')) & (detailed['open_qty']==0)].reset_index(drop=True)[['date', 'sec_ticker', 'bid', 'ask', 'opt_u_price']]
        df_option_list.to_csv(f"{save_location}\\{backtestname}_option_list_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        """
        # Flag all rows where eod_price is 0 or missing
        detailed["eod_zero_flag"] = np.where((detailed["eod_price"] == 0) | (detailed["eod_price"].isna()), 1, 0)

        detailed["eod_zero_flag_option"] = np.where(
            detailed["security_type"].str.contains("option", case=False) & (detailed["eod_price"] == 0),
            1, 0
        )
        flag_summary = (
            detailed.groupby("date")["eod_zero_flag_option"].sum().reset_index()
        )
        flag_summary.columns = ["date", "num_zero_priced_options"]

        flag_summary.to_csv(f"{save_location}\\{backtestname}_zero_price_flags.csv", index=False)
        """

        #Plot the cover called strategy return vs equity return
        equity_df = blp.bdh(f"{backtestname} US Equity", "PX_LAST", aggregate["date"].min(), aggregate["date"].max())
        equity_df.columns = equity_df.columns.get_level_values(-1)
        equity_df = equity_df.reset_index()
        equity_df.rename(columns={'index': 'date'}, inplace=True)
        equity_df["equity_return"] = equity_df["PX_LAST"] / equity_df["PX_LAST"].iloc[0] - 1

        aggregate = aggregate.copy()
        aggregate['date'] = pd.to_datetime(aggregate['date'], errors='coerce').dt.normalize()
        equity_df['date'] = pd.to_datetime(equity_df['date'], errors='coerce').dt.normalize()

        equity_df = equity_df.sort_values('date')
        first_px = equity_df['PX_LAST'].iloc[0]
        equity_df['equity_return'] = equity_df['PX_LAST'] / first_px - 1
        
        merged = pd.merge(aggregate, equity_df[['date', 'equity_return']], on='date', how='left')
        merged['equity_return'] = merged['equity_return'].ffill()
        
        plt.figure(figsize=(10, 6))
        plt.plot(merged["date"], merged["cumulative_return"], label="Portfolio Cumulative Return", color="darkorange", linewidth=2)
        plt.plot(merged["date"], merged["equity_return"], label="Equity Return", color="steelblue", linestyle="--")
        plt.title(f"{backtestname} covered call strategy vs Underlying Equity Performance")
        plt.xlabel("Date")
        plt.ylabel("Return (%)")
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.6)
                                                                                                                                            
        plt.savefig(f"{save_location}\\{backtestname}_cumulative_vs_equity.png", dpi=300, bbox_inches="tight")
        plt.show()
    
    elif model == 'cboe':
        aggregate, detailed = cboe.build_backtest(portfolio, start_date, end_date, opt_rebalance_dates, holidays)
        aggregate.to_csv(f"{save_location}\\{backtestname}_aggregate_{model}{optional_des}.csv", index=False)  # saves daily constituent level report
        detailed.to_csv(f"{save_location}\\{backtestname}_detailed_{model}{optional_des}.csv", index=False)  # saves daily constituent level report





if __name__ == "__main__":
    backtestname = "SPY" #pick up from \\runs\\portfolio_configs.csv. If it's US equity, change line 46, 83 'CN' into 'US'.
    df_config = pd.read_csv(f"{cur_dir}\\runs\\portfolio_configs.csv", delimiter=',')
    df_config = df_config[df_config['backtest'] == backtestname].reset_index(drop=True)
    df_config["end_date"] = pd.to_datetime(df_config["end_date"]).dt.strftime("%Y-%m-%d")
    end_date = dt.datetime.strptime(df_config[df_config['sec_id'] == str("cash")]['end_date'].values[0], "%Y-%m-%d")
    #end_date = dt.datetime.strptime(df_config.iloc[0]['end_date'])
    DTM = int(df_config.iloc[0]['DTM'])
    equity_rebal_rule = df_config.iloc[0]['rebal_rule']
    pct_otm = df_config['pct_otm'].dropna().tolist()
    target_yield = df_config.iloc[0]['target_yield']
    # run_backtest(backtestname, end_date, "btbuilder", data_library.tsx_holidays(), timestamp=dt.datetime.now(), optional_des=f"_30_5_OTM_Long_20_OTM")
    run_backtest(backtestname, end_date, "btbuilder", DTM, equity_rebal_rule, target_yield, pct_otm, data_library.tsx_holidays(), timestamp=dt.datetime.now(), optional_des=f"_0",) # "-1" means we don't have a target yield and will use the fixed coverage ratio in config