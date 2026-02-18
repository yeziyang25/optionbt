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
from helper_functions.rebalance_dates_customized import option_dates_customized

# Define output directory relative to project root
OUTPUT_DIR = f"{cur_dir}\\custom_options_list"

holidays = data_library.tsx_holidays()
class custom_option():
    def __init__(self, ticker:str, conn, rebal_dates, call_put):
        self.ticker = ticker
        self.opt_underlying_ticker = ticker
        self.conn = conn
        self.rebal_dates_list = rebal_dates
        self.call_put = call_put

    def generate_custom_list(self, pct_otm:float=0):
        self.missing_dates = [] 
        self.options_list = []
        base_dates = list(self.rebal_dates_list)
        fallback_dates = [d + dt.timedelta(days=7) for d in base_dates[:-1]]
        fallback_dates_2 = [d + dt.timedelta(days=14) for d in base_dates[:-1]]
        fallback_dates_3 = [d + dt.timedelta(days=21) for d in base_dates[:-1]]
        fallback_dates_4 = [d + dt.timedelta(days=28) for d in base_dates[:-1]]
        holiday_minus_1 = [(pd.to_datetime(h) - pd.Timedelta(days=1)).date() for h in holidays.keys()]
        date_pool = sorted(set(base_dates + fallback_dates + fallback_dates_2 + fallback_dates_3 + fallback_dates_4 + holiday_minus_1))
        date_list_str = [x.strftime("%Y-%m-%d") for x in date_pool]
        #date_list_str = [x.strftime("%Y-%m-%d") for x in self.rebal_dates_list]
        call_put_cond = "___/__/__ C%" if self.call_put.lower() == "call" else "___/__/__ P%"
        str_sql_option = (f"""SELECT * FROM market_data WHERE ticker LIKE ('{self.opt_underlying_ticker} %')  and ticker LIKE ('%{call_put_cond}%') and (source = 'tmx' or source = 'bloomberg') and field='px_bid' and [date] IN ('{"','".join(date_list_str)}');""")
        option_quotes = self.conn.query_tbl(str_sql_option)
        if not option_quotes.empty:
            option_quotes["date"] = pd.to_datetime(option_quotes["date"]).dt.strftime("%Y-%m-%d")

            str_sql_equity = (f"""SELECT * FROM market_data WHERE ticker = '{self.ticker}' and field='px_last' and [date] IN ('{"','".join(date_list_str)}');""")
            equity_prices = self.conn.query_tbl(str_sql_equity)
            equity_prices_dict = dict(zip(pd.to_datetime(equity_prices["date"]).dt.strftime("%Y-%m-%d"), equity_prices["value"].astype(float)))

            option_quotes["underlying"] = option_quotes["date"].map(equity_prices_dict)
            opt_details = common.extract_option_ticker(option_quotes, "ticker")
            option_quotes["strike"] = option_quotes["ticker"].map(opt_details.strike)
            option_quotes["expiry"] = option_quotes["ticker"].map(opt_details.expiry)
            option_quotes["pct_otm"] = np.where(self.call_put == "call", option_quotes["strike"]/option_quotes["underlying"]-1, option_quotes["underlying"]/option_quotes["strike"]-1)
            # option_quotes = option_quotes[(option_quotes["pct_otm"] <= 0.5) & (option_quotes["pct_otm"] >= pct_otm)]
            # option_quotes = option_quotes[(option_quotes["pct_otm"] >= pct_otm)]

            idx = 0
            while idx < len(self.rebal_dates_list) - 1:
                d = self.rebal_dates_list[idx]
                target_expiry = self.rebal_dates_list[idx + 1]
                    

                df_option_candidates = option_quotes[
                    (option_quotes["date"] == d.strftime("%Y-%m-%d")) &
                    (pd.to_datetime(option_quotes["expiry"]) == target_expiry.strftime("%Y-%m-%d"))
                ]
              
            
                
                if df_option_candidates.empty:
                    alt_expiry = target_expiry + dt.timedelta(days=7)
                    df_option_candidates = option_quotes[
                        (option_quotes["date"] == d.strftime("%Y-%m-%d")) &
                        (pd.to_datetime(option_quotes["expiry"]) == alt_expiry.strftime("%Y-%m-%d"))
                    ]
                        
                    
                    if not df_option_candidates.empty:
                        print(f"No option for {self.opt_underlying_ticker} {target_expiry.strftime('%m/%d/%y')}, using {alt_expiry.strftime('%m/%d/%y')} instead.")
                        # replace rebal_dates[idx+1] with new expiry if desired:
                        #target_expiry = alt_expiry
                        #self.rebal_dates_list[idx+1] = alt_expiry
                        #self.rebal_dates_list = [d for d in self.rebal_dates_list if d < alt_expiry or d > target_expiry]
                        #self.rebal_dates_list.pop(idx + 1)
                        #self.rebal_dates_list.insert(idx + 1, alt_expiry)
                        shift_days = (alt_expiry - target_expiry).days
    
                        for j in range(idx + 1, len(self.rebal_dates_list)):
                            self.rebal_dates_list[j] = self.rebal_dates_list[j] + dt.timedelta(days=shift_days)
                            if self.rebal_dates_list[j].weekday() == 3 and self.rebal_dates_list[j].strftime("%Y-%m-%d") not in holidays:
                                self.rebal_dates_list[j] += dt.timedelta(days=1)
                            if self.rebal_dates_list[j].weekday() == 4 and self.rebal_dates_list[j].strftime("%Y-%m-%d") in holidays:
                                self.rebal_dates_list[j] -= dt.timedelta(days=1)
                                
                
                if df_option_candidates.empty:
                    print(f"No options for {self.opt_underlying_ticker} on {d.strftime('%Y-%m-%d')} (including 1 week after)")
                    self.missing_dates.append(d.strftime("%Y-%m-%d"))
                else:
                    df_option_candidates = df_option_candidates.sort_values("pct_otm", ascending=False)
                    _above = df_option_candidates[(df_option_candidates["pct_otm"] >= pct_otm)].reset_index(drop=True)
                    if not _above.empty:
                        self.options_list += [[d.strftime("%Y-%m-%d"),
                                               _above.tail(1).get("ticker").values[0],
                                               _above.tail(1).get("underlying").values[0],
                                               _above.tail(1).get("pct_otm").values[0],
                                               1]]
                    else:
                        _below = df_option_candidates[(df_option_candidates["pct_otm"] <= pct_otm)].reset_index(drop=True)
                        if not _below.empty:
                            self.options_list += [[d.strftime("%Y-%m-%d"),
                                                   _below.head(1).get("ticker").values[0],
                                                   _below.head(1).get("underlying").values[0],
                                                   _below.head(1).get("pct_otm").values[0],
                                                   1]]

                print(d)
                idx += 1
            if self.missing_dates:
                pd.Series(self.missing_dates).to_csv(
                    f"{OUTPUT_DIR}\\{self.opt_underlying_ticker}_missing_option_dates.csv",
                    index=False,
                    header=["missing_dates"]
                )
                print(f"Saved missing option dates for {self.opt_underlying_ticker}: {len(self.missing_dates)} missing")

            valid_dates = [d.strftime("%Y-%m-%d") for d in self.rebal_dates_list if d.strftime("%Y-%m-%d") not in self.missing_dates]
            pd.Series(valid_dates).to_csv(
                f"{OUTPUT_DIR}\\{self.opt_underlying_ticker}_valid_option_dates.csv",
                index=False,
                header=["valid_option_dates"]
            )
            print(f"Saved valid option dates for {self.opt_underlying_ticker}: {len(valid_dates)} valid")


    def save_custom_file(self, save_loc:str, des:str=""):
        if self.options_list == []:
            print(f"No option tickers to save for: {self.opt_underlying_ticker}")
        else:
            output = pd.DataFrame(data=self.options_list, columns=["date", "ticker", "underlying_price", "pct_otm", "weight"])
            output.to_csv(f"{save_loc}\\custom_options_list\\{self.opt_underlying_ticker}{des}_option_list.csv", index=False)

if __name__ == '__main__':
    start_date = dt.datetime(2025, 1, 1)
    end_date = dt.datetime(2025, 12, 31)
    option_rebal_dates = option_dates_customized(start_date, common.tsx_holidays(), end_date, 60)
    # hds_cls = data_library.etf_master_cls(["GLCC"], dt.datetime.now())
    # equity_basket = hds_cls.collect_full_holdings_db(True)
    # equity_basket = equity_basket[equity_basket["security_type"]!="O(Option)"]
    # ticker_list = np.unique(equity_basket['ticker']).tolist()
    ticker_list = ["XIU CN"]
    call_put = "call"

    conn = common.db_connection()

    # Create individual bank options
    for ticker in ticker_list:
        print(ticker)
        opt_cls = custom_option(ticker, conn, option_rebal_dates, call_put)
        opt_cls.generate_custom_list(pct_otm=0.00)
        opt_cls.save_custom_file(cur_dir, "_0")
        
        
        
