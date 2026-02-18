import pandas as pd
import numpy as np
import datetime as dt

import os
import helper_functions.rebalance_dates as rebal_dates
cur_dir = os.path.dirname(__file__)
import sys
sys.path.append('Z:\\ApolloGX')
if "\\im_dev\\" in cur_dir:
    import im_dev.std_lib.common as common
    import im_dev.std_lib.data_library as data_library
else:
    import im_prod.std_lib.common as common
    import im_prod.std_lib.data_library as data_library

class security_data():  # load the relevant data for each security
    def __init__(self, data, cur_dir:str, start_date:str, end_date:str, opt_rebal_dates,bs_flag=False):
        self.bs_flag = bs_flag
        self.sec_id = data['sec_id']
        self.sec_name = data['sec_name']
        self.sec_type = data['sec_type']
        self.currency = data['currency']
        self.start_date = start_date
        self.end_date = end_date
        self.opt_rebal_dates = opt_rebal_dates

        self.allocation = None if str(data['allocation']) == 'nan' else float(data['allocation'])

        self.option_w_against = None if str(data['option_w_against']) == 'nan' else str(data['option_w_against'])
        self.option_sell_to_open_price = 'bid' if str(data['option_sell_to_open_price']) == 'nan' else str(
            data['option_sell_to_open_price'])
        self.option_buy_to_close_price = 'intrinsic' if str(data['option_buy_to_close_price']) == 'nan' else str(
            data['option_buy_to_close_price'])

        if 'option' in self.sec_type.lower():
            if str(data['option_selection']) == 'custom':
                self.option_selection = None
                _df_custom = pd.read_csv(f"{cur_dir}\\{data['custom_options_file']}")
                if 'sec_id' in _df_custom.columns.tolist():
                    _df_custom = _df_custom[_df_custom['sec_id'] == self.sec_id].reset_index(drop=True)
                self.option_selection_custom_map = dict(zip(pd.to_datetime(_df_custom['date']).dt.strftime("%Y-%m-%d"), _df_custom['ticker']))
                self.option_custom_alloc_ovrd = dict(zip(pd.to_datetime(_df_custom['date']).dt.strftime("%Y-%m-%d"), _df_custom['weight']))

            option_underlying_override = {"BTCC CN": "BTCC/B CN", "RCI CN": "RCI/B CN"}
            if not option_underlying_override.get(self.sec_name) is None:
                option_underlying_ticker = option_underlying_override.get(self.sec_name)
            else:
                option_underlying_ticker = self.sec_name
            self.underlying_pricing = self.retrieve_equity_pricing(option_underlying_ticker)
            self.option_pricing = self.retrieve_option_pricing(self.sec_name)

        elif self.sec_type.lower() == 'equity':
            self.equity_pricing = self.retrieve_equity_pricing(self.sec_name)
            self.dvd_schedule = self.retrieve_dvd()
    

    def get_raw_data_path(self, filename):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        csv_path = os.path.join(project_root, 'data_download', 'raw_data', filename)
        return csv_path

    def retrieve_equity_pricing(self, _ticker):
        if self.bs_flag:
            cur_dir = os.path.dirname(os.path.dirname(__file__))
            df_sorted = pd.read_csv(f"{cur_dir}\\data_download\\adhoc_pricing\\equity\\{_ticker} equity_pricing.csv",delimiter=',')
        else:
            conn = common.db_connection()
            base_query = (
                f"""SELECT [date] as date, [value] as px_last, source
                FROM [dbo].[market_data]
                WHERE ticker = '{_ticker}' AND field = 'px_last'""")

            if ('equity' in self.sec_type.lower()) or ('option' in self.sec_type.lower()):
                query = base_query + f""" AND [date] >= '{self.start_date}' AND [date] <= '{self.end_date}';"""
            else:
                raise ValueError(f"Security Type - {self.sec_type} is not recognized")
            df_equity = conn.query_tbl(query)
            df_equity['px_last'] = df_equity['px_last'].astype(float)

            # Define custom hierarchy
            custom_hierarchy = {"bloomberg":1, "solactive":2, "mellon":3}

            # Sort column values by hierarchy
            df_equity["ranking"] = df_equity["source"].map(custom_hierarchy)
            df_sorted = df_equity.sort_values(["date", "ranking"]).drop_duplicates(subset="date")


        return self.build_dict(df_sorted, 'date', 'px_last')
        #return self.build_dict(df_sorted, 'date', 'px_last')

    
    def retrieve_option_pricing(self, _ticker):
        if self.bs_flag:
            cur_dir = os.path.dirname(os.path.dirname(__file__))
            options_data = pd.read_csv(f"{cur_dir}\\data_download\\adhoc_pricing\\options\\{_ticker}_backtest_format_options.csv",delimiter=',')
            options_data['date'] = pd.to_datetime(options_data['date']).dt.strftime('%Y-%m-%d')
            options_data['value'] = options_data['value'].astype(float)
            options_data['ticker'] = options_data['ticker']
        else:
            conn = common.db_connection()
            if self.option_selection_custom_map:
                params = (x.replace(" Equity", "") for x in self.option_selection_custom_map.values())
                tickers = "', '".join(params)
                query = f"""
                SELECT [ticker], [date], [field] as side, [value]
                FROM [dbo].[market_data]
                WHERE ticker IN ('{tickers}') AND (field = 'px_ask' OR field = 'px_bid')
                """
                options_data = conn.query_tbl(query)
            else:
                query = f"""
                SELECT [ticker], [date], [field] as side, [value]
                FROM [dbo].[market_data]
                WHERE ticker LIKE '{_ticker + " CN"}___/__/__ C%' AND (field = 'px_ask' OR field = 'px_bid')
                """
                options_data = conn.query_tbl(query)

        return options_data


    def retrieve_dvd(self):
        conn = common.db_connection()
        sql_str = f"SELECT ticker, ex_date, payable_date, dvd_amount from dividends WHERE ticker='{self.sec_name.upper()}';"
        df_dvd = conn.query_tbl(sql_str)
        if not df_dvd.empty:
            df_dvd['ex_date'] = pd.to_datetime(df_dvd['ex_date']).dt.strftime('%Y-%m-%d')
            df_dvd['dvd_amount'] = df_dvd['dvd_amount'].astype(float)
            return self.build_dict(df_dvd, 'ex_date', 'dvd_amount')
        else:
            return {}

    def build_dict(self, df: pd.DataFrame, _key: str, _val: str):
        return dict(zip(pd.to_datetime(df[_key]).dt.strftime('%Y-%m-%d'), df[_val]))


class cash():
    def __init__(self):
        self.bid = 1
        self.ask = 1
        self.eod_price = 1

class fx_fwd():
    def __init__(self, date: dt.datetime, fx_dict: dict, hedge_currency:str, base_currency:str):
        self.date = date
        # _fx_rate = fx_dict.get(hedge_currency).get(date.strftime('%Y-%m-%d'))/fx_dict.get(base_currency).get(date.strftime('%Y-%m-%d'))
        self.bid = 1
        self.ask = 1
        self.eod_price = 1
        # rebalance_dates = rebal_dates.option_dates((self.date - dt.timedelta(days=40)), data_library.tsx_holidays(), self.date)
        # self.start_period = rebalance_dates[-1]

class equity():
    def __init__(self, date: dt.datetime, sec:security_data, prior_sec:dict):
        self.date = date
        if not sec.dvd_schedule.get(date.strftime('%Y-%m-%d')) is None:
            self.dvd_rate = sec.dvd_schedule.get(date.strftime('%Y-%m-%d'))
        else:
            self.dvd_rate = ''
        # self.price_note = ""

        if not sec.equity_pricing.get(date.strftime('%Y-%m-%d')) is None:
            self.eod_price = sec.equity_pricing.get(date.strftime('%Y-%m-%d'))
        else:
            self.eod_price = prior_sec.get('eod_price')
            # self.price_note += "stale"

        self.bid = self.eod_price
        self.ask = self.eod_price


class option():
    def __init__(self, date: dt.datetime, sec:security_data, prior_sec:dict, eod_pricing_method:str='mid'):
        self.date = date
        self.price_note = ""
        self.security_type = sec.sec_type
        self.sec_name = sec.sec_name
        self.option_selection = sec.option_selection
        self.selection_map_dict = sec.option_selection_custom_map
        if not sec.underlying_pricing.get(self.date.strftime('%Y-%m-%d')) is None:
            self.option_underlying_price = sec.underlying_pricing.get(self.date.strftime('%Y-%m-%d'))
        else:
            self.option_underlying_price = prior_sec.get('opt_u_price')

        _opt_chain_today = sec.option_pricing[sec.option_pricing['date'] == self.date.strftime('%Y-%m-%d')].reset_index(drop=True)
        if not _opt_chain_today.empty:
            self.option_chain = _opt_chain_today
        else:
            # self.option_chain = sec.option_pricing[sec.option_pricing['date'] == prior_sec.get('date').strftime('%Y-%m-%d')].reset_index(drop=True)            
            #if not prior_sec is None:
            #    _data = {'ticker': [prior_sec.get("sec_ticker"), prior_sec.get("sec_ticker")], 'date': [self.date.strftime("%Y-%m-%d"), self.date.strftime("%Y-%m-%d")], 'side': ['px_bid', 'px_ask'], 'value': [prior_sec.get("bid"), prior_sec.get("ask")]}
            #    self.option_chain = pd.DataFrame(data=_data)
            #else:
            #    raise ValueError(f"Missing option data for security: {self.sec_name} on {self.date.strftime('%Y-%m-%d')}")
            if not prior_sec is None:
                _data = {'ticker': [prior_sec.get("sec_ticker"), prior_sec.get("sec_ticker")],
                         'date': [self.date.strftime("%Y-%m-%d"), self.date.strftime("%Y-%m-%d")],
                         'side': ['px_bid', 'px_ask'],
                         'value': [prior_sec.get("bid"), prior_sec.get("ask")]}
                self.option_chain = pd.DataFrame(data=_data)
            else:
                import warnings
                warnings.warn(
                    f"Missing option data for {self.sec_name} on {self.date.strftime('%Y-%m-%d')} — skipping this date"
                )
                self.option_chain = pd.DataFrame()   # keep empty but don’t crash
                self.sec_ticker = None
                self.expiry = None
                self.eod_price = None
                self.bid = None
                self.ask = None
                return
                


        if not self.option_chain.empty:
            opt_cls = common.extract_option_ticker(self.option_chain, 'ticker')
            self.option_chain['expiry'] = self.option_chain['ticker'].map(opt_cls.expiry)
            self.option_chain['strike'] = self.option_chain['ticker'].map(opt_cls.strike)
            self.option_chain['underlying_price'] = self.option_underlying_price

        if prior_sec is None:
            # if we want to base the writing on the prior day price
            # self._option_underlying_price = sec.underlying_pricing.get(common.workday(self.date, -1, data_library.nyse_holidays()).strftime('%Y-%m-%d'))
            if not self.selection_map_dict is None:
                if self.selection_map_dict.get(self.date.strftime("%Y-%m-%d")) is None:
                    self.sec_ticker = None
                else:
                    self.sec_ticker = self.selection_map_dict.get(self.date.strftime("%Y-%m-%d"))
            else:
                #Decommission soon
                print(f"Decommissioning soon. We will no longer allow the securities.py file to choose the option.")
                self.sec_ticker = self.select_option(underlying_ref_price=self.option_underlying_price, pct_otm=self.option_selection)
        else:
            self.sec_ticker = prior_sec.get('sec_ticker')

        if self.sec_ticker is None:
            self.expiry = None
            self.eod_price = None
            self.bid = None
            self.ask = None
        else:
            # self.expiry = self.option_chain[self.option_chain['ticker']==self.sec_ticker]['expiry'].max()
            self.expiry = dt.datetime.strptime(self.sec_ticker.split(' ')[2], '%m/%d/%y')
            self.eod_price, self.bid, self.ask = self.find_price(prior_sec, eod_pricing_method=eod_pricing_method)

            ## Get Moneyness of option
            strike = float(self.sec_ticker.split(" ")[3][1:])
            if self.sec_ticker.split(" ")[3][0]=="C":
                self.moneyness = float(strike)/self.option_underlying_price-1
            elif self.sec_ticker.split(" ")[3][0]=="P":
                self.moneyness = self.option_underlying_price/float(strike)-1

    def find_price(self, prior_sec:dict, eod_pricing_method:str='mid'):
        _df = self.option_chain[self.option_chain['ticker'] == self.sec_ticker]
        _df_bid = _df[_df['side'] == str('px_bid')]
        _df_ask = _df[_df['side'] == str('px_ask')]

        if self.security_type.lower() == 'call option':  # fix this
            _df['intrinsic'] = np.where(_df['underlying_price'] > _df['strike'], _df['underlying_price'] - _df['strike'], 0)
        elif self.security_type.lower() == 'put option':
            _df['intrinsic'] = np.where(_df['strike'] > _df['underlying_price'], _df['strike'] - _df['underlying_price'], 0)

        _intrinsic = _df['intrinsic'].max()

        if (not _df_bid.empty) and (not _df_ask.empty):
            bid = float(_df_bid.reset_index()['value'][0]) # Ricardo made edition by adding float
            ask = float(_df_ask.reset_index()['value'][0]) # Ricardo made edition by adding float
            _mid = 0.5 * (bid + ask)
        else:
            # if data is not available
            bid = 0 if _df_bid.empty else _df_bid.reset_index()['value'][0]
            ask = 0 if _df_ask.empty else _df_ask.reset_index()['value'][0]
            _mid = _intrinsic

        if not prior_sec is None:
            undl_price_change = (self.option_underlying_price/prior_sec.get('opt_u_price')-1)+0.01
            opt_price_change = 0 if prior_sec.get('eod_price') == 0 else _mid/prior_sec.get('eod_price')-1

        price_override = False
        if not prior_sec is None:
            # sanity check
            if not ((not _df_bid.empty) and (not _df_ask.empty)) or (abs(opt_price_change/undl_price_change)>1000 and abs(_mid-prior_sec.get('eod_price'))>0.25):
                eod_price = bid
                price_override = True

        if not price_override:
            if eod_pricing_method == 'mid':
                eod_price = _mid
            elif eod_pricing_method == 'intrinsic':
                eod_price = _intrinsic

        return eod_price, bid, ask

    def select_option(self, underlying_ref_price:float, pct_otm:float=1.0):
        # select the option that we will sell on the roll-day. Only look at the options that have a bid price and sell based on the pct_otm
        opt_cls = common.extract_option_ticker(self.option_chain, 'ticker')
        self.option_chain['expiry'] = self.option_chain['ticker'].map(opt_cls.expiry)
        self.option_chain['strike'] = self.option_chain['ticker'].map(opt_cls.strike)
        self.option_chain['option_type'] = self.option_chain['ticker'].map(opt_cls.option_type)

        prices = self.option_chain[(self.option_chain['expiry'] > self.date.date()) & (self.option_chain['side'] == str('px_bid'))]
        # filters to the next months standard options
        prices = prices[(((pd.to_datetime(prices['expiry']).dt.month - self.date.month)==1) & ((pd.to_datetime(prices['expiry']).dt.year - self.date.year)==0)) | (((pd.to_datetime(prices['expiry']).dt.month - self.date.month)==-11) & ((pd.to_datetime(prices['expiry']).dt.year - self.date.year)==1))]
        #filter rebal dates
        rebalance_dates = rebal_dates.option_dates(self.date, data_library.tsx_holidays(), (self.date+dt.timedelta(days=40)))
        prices = prices[prices['expiry'].isin(rebalance_dates)]
        prices = prices[prices['value']>0]

        if self.security_type.lower() == 'call option':
            df_otm = prices[(prices['strike'] >= underlying_ref_price*pct_otm)]
            if not df_otm.empty:
                return df_otm[df_otm['strike'] == df_otm['strike'].min()]['ticker'].values[0]
            else:
                _df = prices[(prices['strike'] < underlying_ref_price*pct_otm)]
                if _df[_df['strike'] == _df['strike'].max()]['ticker'].empty:
                    return None
                else:
                    return _df[_df['strike'] == _df['strike'].max()]['ticker'].values[0]

        elif self.security_type.lower() == 'put option':
            df_otm = prices[(prices['strike'] <= underlying_ref_price/pct_otm)]
            if not df_otm.empty:
                return df_otm[df_otm['strike'] == df_otm['strike'].max()]['ticker'].values[0]
            else:
                _df = prices[(prices['strike'] > underlying_ref_price/pct_otm)]
                # return _df[_df['strike'] == _df['strike'].min()]['ticker'].values[0]
                if _df[_df['strike'] == _df['strike'].min()]['ticker'].empty:
                    return None
                else:
                    return _df[_df['strike'] == _df['strike'].min()]['ticker'].values[0]
        else:
            return str('')

class fx():
    def __init__(self, fx_rates:dict, date:dt.datetime, find_currency:str, base_currency:str):
        self.fx_rates = fx_rates
        self.date = date
        self.find_currency = find_currency
        self.base_currency = base_currency

    def get_fx(self):
        if self.find_currency == self.base_currency:
            return 1
        else:
            if not self.fx_rates.get(self.find_currency) is None and not self.fx_rates.get(self.base_currency) is None:
                if not self.fx_rates.get(self.find_currency).get(self.date.strftime("%Y-%m-%d")) is None and not self.fx_rates.get(self.base_currency).get(self.date.strftime("%Y-%m-%d")) is None:
                    return self.fx_rates.get(self.find_currency).get(self.date.strftime("%Y-%m-%d"))/self.fx_rates.get(self.base_currency).get(self.date.strftime("%Y-%m-%d"))
                else:
                    return None
            else:
                return None