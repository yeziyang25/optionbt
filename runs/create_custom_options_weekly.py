import pandas as pd
import numpy as np
import datetime as dt
import os

cur_dir = os.path.dirname(__file__)
import sys
sys.path.append('Z:\\ApolloGX')
import im_prod.std_lib.common as common
import im_prod.std_lib.data_library as data_library

sys.path.append(f"{os.path.dirname(os.path.dirname(__file__))}")
from helper_functions.rebalance_dates import option_dates

# Define output directory relative to project root
OUTPUT_DIR = f"{cur_dir}\\custom_options_list"

class custom_option():
    def __init__(self, ticker:str, conn, rebal_dates, call_put):
        self.ticker = ticker
        self.opt_underlying_ticker = "ZEB CN"
        self.conn = conn
        self.rebal_dates_list = rebal_dates
        self.call_put = call_put

    def generate_custom_list(self, pct_otm:float=0):
        self.options_list = []
        date_list_str = [x.strftime("%Y-%m-%d") for x in self.rebal_dates_list]
        call_put_cond = "___/__/__ C%" if self.call_put.lower() == "call" else "___/__/__ P%"
        str_sql_option = (f"""SELECT * FROM market_data WHERE ticker LIKE ('{self.opt_underlying_ticker} %')  and ticker LIKE ('%{call_put_cond}%') and source='tmx' and field='px_bid' and [date] IN ('{"','".join(date_list_str)}');""")
        option_quotes = conn.query_tbl(str_sql_option)
        if not option_quotes.empty:
            option_quotes["date"] = pd.to_datetime(option_quotes["date"]).dt.strftime("%Y-%m-%d")

            str_sql_equity = (f"""SELECT * FROM market_data WHERE ticker = '{self.ticker}' and field='px_last' and [date] IN ('{"','".join(date_list_str)}');""")
            equity_prices = conn.query_tbl(str_sql_equity)
            equity_prices_dict = dict(zip(pd.to_datetime(equity_prices["date"]).dt.strftime("%Y-%m-%d"), equity_prices["value"].astype(float)))

            option_quotes["underlying"] = option_quotes["date"].map(equity_prices_dict)
            opt_details = common.extract_option_ticker(option_quotes, "ticker")
            option_quotes["strike"] = option_quotes["ticker"].map(opt_details.strike)
            option_quotes["expiry"] = option_quotes["ticker"].map(opt_details.expiry)
            option_quotes["pct_otm"] = np.where(self.call_put == "call", option_quotes["strike"]/option_quotes["underlying"]-1, option_quotes["underlying"]/option_quotes["strike"]-1)
            # option_quotes = option_quotes[(option_quotes["pct_otm"] <= 0.5) & (option_quotes["pct_otm"] >= pct_otm)]
            # option_quotes = option_quotes[(option_quotes["pct_otm"] >= pct_otm)]

            idx = 0
            for i, d in enumerate(self.rebal_dates_list[:-4]):  # stop 4 weeks early
                d_str = d.strftime("%Y-%m-%d")
                expiry_target = self.rebal_dates_list[i + 4]    # week 5 Friday

                df_option_candidates = option_quotes[option_quotes["date"] == d_str].copy()
                if not df_option_candidates.empty:
                    df_option_candidates["expiry"] = pd.to_datetime(df_option_candidates["expiry"])
                    df_option_candidates = df_option_candidates[df_option_candidates["expiry"] == expiry_target]

                    if df_option_candidates.empty:
                        print(f"No options for expiry {expiry_target.strftime('%Y-%m-%d')} on {d_str}")
                        continue
                    print(d_str, expiry_target, df_option_candidates[["ticker","strike","pct_otm"]]) #debuging line

                    # pick strike closest to target pct_otm
                    ix = (df_option_candidates["pct_otm"] - pct_otm).abs().idxmin()
                    row = df_option_candidates.loc[ix]

                    self.options_list.append([
                        d_str,
                        row["ticker"],
                        row["underlying"],
                        row["pct_otm"],
                        1
                    ])
                else:
                    print(f"No options for {self.opt_underlying_ticker} on {d_str}")
                print(d)
                idx += 1



    #def save_custom_file(self, save_loc:str, des:str=""):
    #    if self.options_list == []:
    #        print(f"No option tickers to save for: {self.opt_underlying_ticker}")
    #    else:
    #        output = pd.DataFrame(data=self.options_list, columns=["date", "ticker", "underlying_price", "pct_otm", "weight"])
    #        output.to_csv(f"{save_loc}\\custom_options_list\\{self.opt_underlying_ticker}{des}_option_list.csv", index=False)
    
    def save_custom_file(self, save_loc:str, des:str=""):
        if self.options_list == []:
            print(f"No option tickers to save for: {self.opt_underlying_ticker}")
        else:
            output = pd.DataFrame(
                data=self.options_list,
                columns=["date", "ticker", "underlying_price", "pct_otm", "weight"]
            )

            # assign rolling buckets: 1–4, repeating
            output["bucket"] = [i % 4 + 1 for i in range(len(output))]

            # save 4 separate CSVs (one per ladder bucket)
            for b in range(1, 5):
                df_b = output[output["bucket"] == b].copy()
                if not df_b.empty:
                    df_b.to_csv(
                        f"{save_loc}\\custom_options_list\\{self.opt_underlying_ticker}_week{b}{des}_option_list.csv",
                        index=False
                    )

if __name__ == '__main__':
    start_date = dt.datetime(2024, 4, 19)   # first Friday you want
    end_date   = dt.datetime(2024, 9, 20)

    tsx_holidays = common.tsx_holidays()

    # All Fridays in range
    option_rebal_dates = pd.date_range(start=start_date, end=end_date, freq='W-FRI')

    # hds_cls = data_library.etf_master_cls(["GLCC"], dt.datetime.now())
    # equity_basket = hds_cls.collect_full_holdings_db(True)
    # equity_basket = equity_basket[equity_basket["security_type"]!="O(Option)"]
    # ticker_list = np.unique(equity_basket['ticker']).tolist()
    ticker_list = ["ZEB CN"]
    call_put = "call"

    conn = common.db_connection()

    # Create individual bank options
    for ticker in ticker_list:
        print(ticker)
        opt_cls = custom_option(ticker, conn, option_rebal_dates, call_put)
        opt_cls.generate_custom_list(pct_otm=0.05)
        opt_cls.save_custom_file(cur_dir, "_5_weekly")