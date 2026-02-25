import pandas as pd
import numpy as np
import datetime as dt
from helper_functions.securities import security_data
from helper_functions.securities import cash, equity, option, fx, fx_fwd
from helper_functions.reinvestment import call_option_contract_calculator_new

import os
cur_dir = os.path.dirname(__file__)
import sys
sys.path.append('Z:\\ApolloGX')
if "\\im_dev\\" in cur_dir:
    import im_dev.std_lib.common as common
    import im_dev.std_lib.data_library as data_library
    import im_dev.std_lib.pricing_model as pricing_model
else:
    import im_prod.std_lib.common as common
    import im_prod.std_lib.data_library as data_library
    import im_prod.std_lib.pricing_model as pricing_model


# -----------------------------------------
# Version 2.0 accepts positions as a vector and can flip between CAD and USD returns
# Version 2.1 separate cash account for premiums and buybacks
# Version 2.2 vary by different option writing %
# Version 2.3 dividends are not reinvested. they go to a cash account. Option for buyback
# Version 2.4 all cash flows are reinvested. cash inflows and outflows are tracked
# Version 2.5 stale pricing for holidays. this is a fix for portfolios holding a combination of CAD and US portfolios
# Version 2.6 read portfolio constitutents from config file
# Version 2.7 accepts a non roll date start date. retrieves rebalance dates from external library
# -----------------------------------------

class security():
    def __init__(self, d:dt.datetime, prior_sec_details:dict, fx_rates:dict, base_currency:str, sec: str, sec_details:security_data, eod_pricing_method:str="mid"):
        self.date = d
        self.sec_id = sec
        self.sec_name = sec_details.sec_name
        self.security_type = sec_details.sec_type
        self.crncy = sec_details.currency
        self.security_allocation = sec_details.allocation

        fx_dict = fx(fx_rates, self.date, self.crncy, base_currency)
        if not fx_dict.get_fx() is None:
            self.fx = fx_dict.get_fx()
        else:
            self.fx = prior_sec_details.get('fx')

        self.cash_inflow = 0  # track inflows (option premiums and dividends)
        self.cash_outflow = 0  # track outflows (for now, just option buybacks)

        if self.security_type == str('cash'):
            _sec_cls = cash()
            self.sec_ticker = sec_details.sec_name
            self.option_u_price = ''
            self.dvd_rate = ''
        elif self.security_type == str('equity'):
            _sec_cls = equity(d, sec_details, prior_sec_details)
            self.sec_ticker = sec_details.sec_name
            self.option_u_price = ''
            self.dvd_rate = _sec_cls.dvd_rate
        elif str('option') in self.security_type:
            _sec_cls = option(d, sec_details, prior_sec_details, eod_pricing_method=eod_pricing_method)
            self.sec_ticker = _sec_cls.sec_ticker
            self.option_u_price = _sec_cls.option_underlying_price
            self.dvd_rate = ''
            self.expiry = _sec_cls.expiry
            self.moneyness = getattr(_sec_cls, 'moneyness', '')

            # we use this if we are writing more than one option for an equity. Typically to alter the pct OTM
            if sec_details.option_custom_alloc_ovrd is None:
                self.alloc_ovrd = 1
            else:
                self.alloc_ovrd = sec_details.option_custom_alloc_ovrd.get(self.date.strftime("%Y-%m-%d"), 1)

        elif str('fx_fwd') in self.security_type:
            _sec_cls = fx_fwd(d, fx_rates, self.crncy, base_currency)
            if prior_sec_details is None:
                self.sec_ticker = f"{sec_details.sec_name}{base_currency}_{self.date.strftime('%Y%m%d')}"
            else:
                self.sec_ticker = prior_sec_details.get('sec_ticker')
            self.option_u_price = ''
            self.dvd_rate = ''
            # self.expiry = _sec_cls.expiry

        self.eod_price = _sec_cls.eod_price
        self.bid = _sec_cls.bid
        self.ask = _sec_cls.ask

        if prior_sec_details is None:
            self.open_qty = 0
            self.close_qty = 0
        else:
            self.open_wt = prior_sec_details.get('close_wt')
            self.open_qty = prior_sec_details.get('close_qty')
            self.close_qty = prior_sec_details.get('close_qty')

    def add_sec(self, all_fx_rates:dict={}):

        if self.security_type == 'fx_fwd':
            str_split = self.sec_ticker.split('_')
            _start_fx = all_fx_rates.get(self.crncy).get(dt.datetime.strptime(str_split[-1], '%Y%m%d').strftime('%Y-%m-%d'))
            close_mv = self.close_qty*(self.fx-_start_fx)
        elif 'option' in self.security_type:
            close_mv = self.close_qty*self.eod_price*self.fx*100
        else:
            close_mv = self.close_qty*self.eod_price*self.fx

        _dict = {'date': self.date, 'sec_id': self.sec_id, 'sec_ticker': self.sec_ticker, 'sec_name': self.sec_name,
                 'security_type': self.security_type, 'currency': self.crncy,
                 'open_qty': self.open_qty, 'close_qty': self.close_qty,
                 'bid': self.bid, 'ask': self.ask, 'eod_price': self.eod_price,
                 'fx': self.fx, 'dvd_rate': self.dvd_rate, 'opt_u_price': self.option_u_price, 'cash_inflow': self.cash_inflow,
                 'cash_outflow': self.cash_outflow, 'close_mv': close_mv, 'moneyness': getattr(self, 'moneyness', '')}
        return _dict

    def close_fx_hedge(self, all_fx_rates:dict):
        str_split = self.sec_ticker.split('_')
        _start_fx = all_fx_rates.get(self.crncy).get(dt.datetime.strptime(str_split[-1], '%Y%m%d').strftime('%Y-%m-%d'))
        return self.open_qty*(self.fx-_start_fx), -self.open_qty

    def close_option(self, buy_price_setting:str):
        _strike = float(self.sec_ticker.split(' ')[3][1:])
        if str('C') == self.sec_ticker.split(' ')[3][0]:
            _intrinsic = max(float(self.option_u_price - _strike), 0)
        elif str('P') == self.sec_ticker.split(' ')[3][0]:
            _intrinsic = max(float(_strike - self.option_u_price), 0)

        if buy_price_setting == str('ask'):
            if (self.ask-self.bid) < 1: #close the price out on the market price
                _buy_price = self.bid #in extreme prices, we have to use the bid
            else:
                _buy_price = self.ask
        elif buy_price_setting == str('intrinsic'):
            _buy_price = _intrinsic # closing the option position on roll-day at the intrinsic price
        elif buy_price_setting.startswith('ask'):
            percentage = float(buy_price_setting.split('_')[1])
            mid_price = 0.5*(self.bid+self.ask)
            _buy_price = (1 - (percentage/100))*mid_price

        trade_qty = -self.open_qty
        return -float(_buy_price*trade_qty)*100*self.fx, trade_qty
        # if (self.ask-self.bid)>1:
        #     return -float(self.bid*trade_qty)*100*self.fx,trade_qty
        # else:
        #     return -float(self.ask*trade_qty) * 100 * self.fx, trade_qty

    def write_option(self, equity_against:dict, trade_price_setting:str, reinvest_premium:bool=False):
        # this step chooses the quantity to sell and the price that the option will be sold at

        # assumption is that the fx of the option is the same as the underlying
        # ratio will not be 1 if the collateral equity is not the same as the option
        _ratio = (equity_against.get('eod_price')*equity_against.get('fx'))/(self.option_u_price*self.fx)

        if trade_price_setting == str('bid'):
            _sell_price = self.bid
        elif trade_price_setting == str('mid'):
            _sell_price = 0.5*(self.bid+self.ask)
        elif trade_price_setting.startswith('mid'):
            # sell at mid - x%
            percentage = float(trade_price_setting.split('_')[1])
            mid_price = 0.5 * (self.bid + self.ask)
            _sell_price = (1 - (percentage / 100)) * mid_price

        # determine the coverage ratio with respect to the equity for the given option
        _coverage_ratio = self.security_allocation*self.alloc_ovrd

        if reinvest_premium:
            trade_qty = round(
                _ratio * call_option_contract_calculator_new((equity_against.get('close_qty') * self.option_u_price), _sell_price, self.option_u_price, _coverage_ratio, False))
        else:
            trade_qty = round(_ratio*equity_against.get('close_qty')*_coverage_ratio/100, 0)
        return -float(100*_sell_price*trade_qty*self.fx), trade_qty

    def buy_option(self, equity_against:dict, trade_price_setting:str, reinvest_premium:bool=False):
        # this step chooses the quantity to buy and the price that the option will be sold at

        # assumption is that the fx of the option is the same as the underlying
        # ratio will not be 1 if the collateral equity is not the same as the option
        _ratio = (equity_against.get('eod_price')*equity_against.get('fx'))/(self.option_u_price*self.fx)

        if trade_price_setting == str('bid'):
            _buy_price = self.bid
        elif trade_price_setting == str('mid'):
            _buy_price = 0.5*(self.bid+self.ask)
        elif trade_price_setting == str('ask'):
            _buy_price = self.ask
        elif trade_price_setting.startswith('mid'):
            # sell at mid - x%
            percentage = float(trade_price_setting.split('_')[1])
            mid_price = 0.5 * (self.bid + self.ask)
            _buy_price = (1 + (percentage / 100)) * mid_price

        # determine the coverage ratio with respect to the equity for the given option
        _coverage_ratio = self.security_allocation*self.alloc_ovrd

        trade_qty = round(_ratio*equity_against.get('close_qty')*_coverage_ratio/100, 0)
        return -float(100*_buy_price*trade_qty*self.fx), trade_qty

    def trade_equity(self, cash_amt:float, cash_currency:str, base_currency:str, fx_rates:dict):
        fx_dict = fx(fx_rates, self.date, cash_currency, base_currency)
        _fx = fx_dict.get_fx()
        cash_base_amount = (cash_amt*_fx)
        trade_qty = cash_base_amount/(self.eod_price*self.fx)
        return -cash_base_amount, trade_qty

    def add_hedge(self, full_portfolio):
        """
        This function calculates the hedge notional amount
        Last Updated May 07 2024
        """
        full_mv_local = 0
        for sec, cls in full_portfolio.items():
            if not 'fx_fwd' in str(cls.get('security_type')):
                _factor = 100 if 'option' in str(cls.get('security_type')) else 1
                full_mv_local += cls.get('eod_price')*cls.get('fx')*cls.get('close_qty')*_factor
        return -full_mv_local/self.fx

def daily_returns(detailed: pd.DataFrame):
    #leverage_targets = [1.25,1.33,1.4,1.5] # this is for w series testing
    summary = pd.DataFrame(columns=['date', 'daily_return'])
    df_agg = detailed.groupby(by=['date'], group_keys=True)[['close_mv']].apply(sum).reset_index()
    summary['date'] = df_agg['date']
    summary['daily_return'] = df_agg['close_mv'].pct_change(1)
    summary['cumulative_return'] = (1 + summary['daily_return']).cumprod() - 1
    summary['cumulative_return'] = summary['cumulative_return'].fillna(0)
    summary['portfolio_mv'] = df_agg['close_mv']

    # for lev in leverage_targets: #this is for 2 series testing
    #     lev_daily_col = f'levered_daily_return_{lev:.2f}'
    #     lev_cum_col = f'levered_cumulative_return_{lev:.2f}'
    #
    #     summary[lev_daily_col] = summary['daily_return'] * lev
    #     summary[lev_cum_col] = (1 + summary[lev_daily_col]).cumprod() - 1
    #     summary[lev_cum_col] = summary[lev_cum_col].fillna(0)

    #drawdown calc

    summary['peak_mv'] = summary['portfolio_mv'].rolling(window=len(summary), min_periods=1).max()
    summary.at[0, 'peak_mv'] = summary.at[0, 'portfolio_mv']
    summary['drawdown_value'] = np.where(summary['portfolio_mv']<summary['peak_mv'],summary['portfolio_mv']-summary['peak_mv'],0)
    summary['drawdown_%'] = np.where(summary['drawdown_value'] != 0, summary['drawdown_value']/summary['peak_mv'],"")
    summary['drawdown_value'] = np.where(summary['drawdown_value'] == 0, '', summary['drawdown_value'])
    summary['drawdown_%'] = np.where(summary['drawdown_%'] == 0, '', summary['drawdown_%'])

    summary['drawdown_%'] = summary['drawdown_%'].replace("",0)
    summary['drawdown_value'] = summary['drawdown_value'].replace("", 0)

    #rolling_1yr_sharpe_ratio_calc # this is not necassary but sometimes useful
    # conn = common.db_connection()
    # min_date = summary['date'].min()
    # df_rf_rate = conn.query_tbl(f"""SELECT [date], CAST([value] as DECIMAL(8,2))/100 as value FROM market_data WHERE ticker = 'CABROVER Index' and date>'{min_date.strftime('%Y-%m-%d')}';""")
    # df_rf_rate['value'] = df_rf_rate['value']/252
    # df_rf_rate.rename(columns={'value': 'rf_rate'}, inplace=True)
    #summary = summary.merge(df_rf_rate,on="date",how="left")
    # summary["rf_rate"] = summary["rf_rate"].ffill()
    # summary["rolling_1yr_sharpe_ratio"] = ((summary["daily_return"] - summary["rf_rate"]).rolling(252).apply(lambda x: np.mean(x) / np.std(x), raw=True)* np.sqrt(252))

    return summary



def run_portfolio_backtest(portfolio: dict, start_date: dt.datetime, end_date: dt.datetime,
                           rebal_dates: list, holidays: dict, reinvest_premium: bool = False):
    all_fx_rates = data_library.fx_rates()
    base_currency = portfolio.get('cash').currency

    col_data = ['date', 'sec_id', 'sec_ticker', 'sec_name', 'security_type', 'currency', 'open_qty',
                'close_qty', 'bid', 'ask', 'eod_price', 'dvd_rate', 'fx', 'opt_u_price',
                'close_mv', 'cash_inflow', 'cash_outflow', 'moneyness']

    d = start_date
    df_values_output = pd.DataFrame(columns=col_data)

    while d <= end_date:
        print(f"{d.strftime('%Y-%m-%d')}")
        rebal_date = True if d.date() in rebal_dates else False

        # prior basket state
        if d == start_date:
            prior_basket = {}
        else:
            prior_basket = cur_basket
        cur_basket = {}

        # ------------------------------
        # Rebalance or start of backtest
        # ------------------------------
        if rebal_date or (d == start_date):
            # initialize cash
            if d == start_date:
                temp_sec = security(d, None, all_fx_rates, base_currency, 'cash', portfolio.get('cash'))
                cur_basket['cash'] = temp_sec.add_sec(all_fx_rates)
                cur_basket['cash']['open_qty'] = portfolio.get('cash').allocation
                cur_basket['cash']['close_qty'] = portfolio.get('cash').allocation
            else:
                temp_sec = security(d, prior_basket.get('cash'), all_fx_rates, base_currency, 'cash', portfolio.get('cash'))
                cur_basket['cash'] = temp_sec.add_sec(all_fx_rates)

            # close existing positions first
            dvds_on_rebal_day = {}
            for sec, prior_basket_dict in prior_basket.items():
                if prior_basket_dict.get('security_type') != 'cash':
                    if 'option' in prior_basket_dict.get('security_type'):
                        # close only if this option expires today
                        temp_sec = security(d, prior_basket_dict, all_fx_rates, base_currency,
                                            sec, portfolio.get(sec), eod_pricing_method="intrinsic")
                        if temp_sec.expiry == d.date():
                            cash_base_amt, trade_qty = temp_sec.close_option(
                                buy_price_setting=portfolio.get(sec).option_buy_to_close_price)
                            temp_sec.close_qty += trade_qty
                            temp_sec.cash_outflow += cash_base_amt
                            cur_basket[sec + '_old'] = temp_sec.add_sec(all_fx_rates)
                            cur_basket['cash']['close_qty'] += cash_base_amt
                        else:
                            # carry forward non-expiring option
                            cur_basket[sec] = prior_basket_dict

                    elif 'equity' in prior_basket_dict.get('security_type'):
                        temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, portfolio.get(sec))
                        if temp_sec.dvd_rate != '':
                            cur_basket['cash']['open_qty'] += temp_sec.dvd_rate * temp_sec.open_qty
                            dvds_on_rebal_day.update({sec: temp_sec.dvd_rate * temp_sec.open_qty})
                        cur_basket['cash']['close_qty'] += temp_sec.eod_price * temp_sec.open_qty * temp_sec.fx

                    elif 'fx_fwd' in prior_basket_dict.get('security_type'):
                        temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, portfolio.get(sec))
                        cash_base_amt, trade_qty = temp_sec.close_fx_hedge(all_fx_rates)
                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_outflow += cash_base_amt
                        cur_basket[sec + '_old'] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt

            # rebalance equities (same logic)
            _start_cash_balance = cur_basket.get('cash').get('close_qty')
            for sec, cls in portfolio.items():
                if (cls.sec_type == 'equity') and (cls.allocation > 0):
                    temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, cls)
                    cash_base_amt, trade_qty = temp_sec.trade_equity(
                        cash_amt=cls.allocation * _start_cash_balance,
                        cash_currency=cur_basket.get('cash').get('currency'),
                        base_currency=base_currency,
                        fx_rates=all_fx_rates)
                    temp_sec.close_qty = trade_qty
                    if sec in dvds_on_rebal_day:
                        temp_sec.cash_inflow += temp_sec.dvd_rate * temp_sec.open_qty
                    cur_basket[sec] = temp_sec.add_sec(all_fx_rates)
                    cur_basket['cash']['close_qty'] += cash_base_amt
            '''
            # Staggered weekly ladder roll logic
            week_idx = get_week_number(start_date.date(), d.date())
            leg_to_roll = (week_idx % 4) + 1   # 1 = option1, 2 = option2, 3 = option3, 4 = option4

            for i in range(1, 5):  # handle option1 → option4
                sec_name = f"option{i}"
                cls = portfolio.get(sec_name)
                if cls is None:
                    continue  # skip if not in portfolio config

                if i == leg_to_roll:
                    # --- close old leg ---
                    if sec_name in prior_basket:
                        temp_sec = security(d, prior_basket.get(sec_name), all_fx_rates, base_currency, sec_name, cls, eod_pricing_method="intrinsic")
                        cash_base_amt, trade_qty = temp_sec.close_option(buy_price_setting=cls.option_buy_to_close_price)
                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_outflow += cash_base_amt
                        cur_basket[sec_name + "_old"] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt

                    # --- open new rolled leg ---
                    new_cls = load_next_week_option(sec_name, week_idx+4, option_files)
                    if new_cls is not None:
                        temp_sec = security(d, None, all_fx_rates, base_currency, sec_name, new_cls, eod_pricing_method="mid")
                        cash_base_amt, trade_qty = temp_sec.write_option(
                            equity_against=cur_basket.get(new_cls.option_w_against),
                            trade_price_setting=new_cls.option_sell_to_open_price,
                            reinvest_premium=reinvest_premium)
                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_inflow += cash_base_amt
                        cur_basket[sec_name] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt
                    else:
                        # no new contract found → carry forward
                        cur_basket[sec_name] = prior_basket.get(sec_name)
                else:
                    # keep other option legs untouched
                    if sec_name in prior_basket:
                        cur_basket[sec_name] = prior_basket.get(sec_name)
            '''


            # roll expiring options only
            for sec, cls in portfolio.items():
                if 'option' in cls.sec_type:
                    temp_sec = security(d, None, all_fx_rates, base_currency, sec, cls, eod_pricing_method="mid")
                    if temp_sec.expiry == d.date():  # 👉 only roll this leg
                        if cls.allocation < 0:
                            cash_base_amt, trade_qty = temp_sec.write_option(
                                equity_against=cur_basket.get(cls.option_w_against),
                                trade_price_setting=cls.option_sell_to_open_price,
                                reinvest_premium=reinvest_premium)
                        else:
                            cash_base_amt, trade_qty = temp_sec.buy_option(
                                equity_against=cur_basket.get(cls.option_w_against),
                                trade_price_setting=cls.option_sell_to_open_price,
                                reinvest_premium=False)

                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_inflow += cash_base_amt
                        cur_basket[sec] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt
                        cur_basket[sec]['moneyness'] = getattr(temp_sec, 'moneyness', '')

            # fx hedges same logic
            for sec, cls in portfolio.items():
                if cls.sec_type == 'fx_fwd':
                    temp_sec = security(d, None, all_fx_rates, base_currency, sec, cls)
                    hedge_qty = temp_sec.add_hedge(cur_basket)
                    temp_sec.close_qty = hedge_qty
                    cur_basket[sec] = temp_sec.add_sec(all_fx_rates)

        # ------------------------------
        # Normal day (no rebalance)
        # ------------------------------
        else:
            for sec, prior_basket_dict in prior_basket.items():
                if ('cash' in sec) or (abs(prior_basket_dict.get('close_qty')) > 0):
                    temp_sec = security(d, prior_basket_dict, all_fx_rates, base_currency, sec, portfolio.get(sec))
                    if ('equity' in sec) and temp_sec.dvd_rate != '':
                        cash_base_amt, trade_qty = temp_sec.trade_equity(
                            cash_amt=temp_sec.dvd_rate * prior_basket_dict.get('open_qty'),
                            cash_currency=temp_sec.crncy,
                            base_currency=base_currency,
                            fx_rates=all_fx_rates)
                        temp_sec.cash_inflow -= cash_base_amt
                        temp_sec.close_qty += trade_qty
                    cur_basket[sec] = temp_sec.add_sec(all_fx_rates)

        # ------------------------------
        # End of day NAV snapshot
        # ------------------------------
        temp_basket = pd.DataFrame.from_dict(cur_basket, orient='index').reset_index()
        temp_basket['sec_id'] = temp_basket['index']
        temp_basket = temp_basket.drop(columns=['index'])

        if df_values_output.empty:
            df_values_output = temp_basket[col_data]
        else:
            df_values_output = pd.concat([df_values_output, temp_basket[col_data]])

        d = common.workday(d, 1, holidays)

    output_detailed = df_values_output.copy(deep=True)
    output_summary = daily_returns(output_detailed)
    return output_summary, output_detailed


'''
def run_portfolio_backtest(portfolio:dict, start_date:dt.datetime, end_date:dt.datetime, rebal_dates:list, holidays:dict, reinvest_premium:bool=False):
    all_fx_rates = data_library.fx_rates()
    base_currency = portfolio.get('cash').currency

    col_data = ['date', 'sec_id', 'sec_ticker', 'sec_name', 'security_type', 'currency', 'open_qty',
                'close_qty', 'bid', 'ask', 'eod_price', 'dvd_rate', 'fx', 'opt_u_price', 'close_mv', 'cash_inflow', 'cash_outflow', 'moneyness']
    d = start_date
    df_values_output = pd.DataFrame(columns=col_data)
    while d <= end_date:
        print(f"{d.strftime('%Y-%m-%d')}")
        rebal_date = True if d.date() in rebal_dates else False

        if (d == start_date):
            prior_basket = {}
        else:
            prior_basket = cur_basket
        cur_basket = {}

        if rebal_date or (d == start_date):
            if d == start_date:
                # seeding invest cash to equities ----------------------------------------------
                temp_sec = security(d, None, all_fx_rates, base_currency, 'cash', portfolio.get('cash'))
                cur_basket['cash'] = temp_sec.add_sec(all_fx_rates)
                cur_basket['cash']['open_qty'] = portfolio.get('cash').allocation
                cur_basket['cash']['close_qty'] = portfolio.get('cash').allocation
            else:
                temp_sec = security(d, prior_basket.get('cash'), all_fx_rates, base_currency, 'cash', portfolio.get('cash'))
                cur_basket['cash'] = temp_sec.add_sec(all_fx_rates)

            # close option positions and add any dividend payments. move equity positions to cash
            dvds_on_rebal_day = {}
            for sec, prior_basket_dict in prior_basket.items():
                
                if prior_basket_dict.get('security_type') != str('cash'):
                    if str('option') in prior_basket_dict.get('security_type'):
                        # close options
                        temp_sec = security(d, prior_basket_dict, all_fx_rates, base_currency, sec, portfolio.get(sec), eod_pricing_method="intrinsic")
                        cash_base_amt, trade_qty = temp_sec.close_option(buy_price_setting=portfolio.get(sec).option_buy_to_close_price)
                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_outflow += cash_base_amt
                        cur_basket[sec + str('_old')] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt
                    elif str('equity') in prior_basket_dict.get('security_type'):
                        # if there is a dividend, add to cash
                        temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, portfolio.get(sec))
                        if temp_sec.dvd_rate != str(''):
                            cur_basket['cash']['open_qty'] += temp_sec.dvd_rate*temp_sec.open_qty
                            dvds_on_rebal_day.update({sec: temp_sec.dvd_rate*temp_sec.open_qty}) #record dvd on the rebalance equity section
                        # move mv of equities to cash. hypothetically, sell all equities and buy them back at the target weights
                        cur_basket['cash']['close_qty'] += temp_sec.eod_price*temp_sec.open_qty*temp_sec.fx
                    elif str('fx_fwd') in prior_basket_dict.get('security_type'):
                        temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, portfolio.get(sec))
                        cash_base_amt, trade_qty = temp_sec.close_fx_hedge(all_fx_rates)
                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_outflow += cash_base_amt
                        cur_basket[sec + str('_old')] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt

            # rebalance equity
            _start_cash_balance = cur_basket.get('cash').get('close_qty')

            for sec, cls in portfolio.items():
                if (cls.sec_type == str('equity')) and (cls.allocation > 0):
                    temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, cls)
                    cash_base_amt, trade_qty = temp_sec.trade_equity(cash_amt=cls.allocation*_start_cash_balance,
                                                          cash_currency=cur_basket.get('cash').get('currency'),
                                                          base_currency=base_currency,
                                                          fx_rates=all_fx_rates)
                    temp_sec.close_qty = trade_qty
                    if not dvds_on_rebal_day.get(sec) is None:
                        temp_sec.cash_inflow += temp_sec.dvd_rate * temp_sec.open_qty
                    cur_basket[sec] = temp_sec.add_sec(all_fx_rates)
                    cur_basket['cash']['close_qty'] += cash_base_amt

            # initiate options position
            for sec, cls in portfolio.items():
                if str('option') in cls.sec_type:
                    temp_sec = security(d, None, all_fx_rates, base_currency, sec, cls, eod_pricing_method="mid")
                    if not temp_sec.sec_ticker is None:
                        if temp_sec.security_allocation < 0:
                            cash_base_amt, trade_qty = temp_sec.write_option(
                                                            equity_against=cur_basket.get(cls.option_w_against),
                                                            trade_price_setting=portfolio.get(sec).option_sell_to_open_price,
                                                            reinvest_premium=reinvest_premium)
                        else:
                            cash_base_amt, trade_qty = temp_sec.buy_option(
                                                            equity_against=cur_basket.get(cls.option_w_against),
                                                            trade_price_setting=portfolio.get(sec).option_sell_to_open_price,
                                                            reinvest_premium=False)
                        temp_sec.close_qty += trade_qty
                        temp_sec.cash_inflow += cash_base_amt
                        cur_basket[sec] = temp_sec.add_sec(all_fx_rates)
                        cur_basket['cash']['close_qty'] += cash_base_amt
                        cur_basket[sec]['moneyness'] = getattr(temp_sec, 'moneyness', '')

            # reinvest option premiums into the portfolio
            if reinvest_premium:
                _reinvestment_amt = cur_basket.get('cash').get('close_qty')
                for sec, cls in portfolio.items():
                    if (cls.sec_type == str('equity')) and (cls.allocation > 0):
                        temp_sec = security(d, prior_basket.get(sec), all_fx_rates, base_currency, sec, cls)
                        cash_base_amt, trade_qty = temp_sec.trade_equity(cash_amt=cls.allocation*_reinvestment_amt,
                                                                          cash_currency=cur_basket.get('cash').get('currency'),
                                                                          base_currency=base_currency,
                                                                          fx_rates=all_fx_rates)
                        cur_basket[sec]['close_qty'] += trade_qty
                        cur_basket[sec]['close_mv'] += -cash_base_amt
                        cur_basket['cash']['close_qty'] += cash_base_amt

            # apply fx hedge
            for sec, cls in portfolio.items():
                if cls.sec_type == str('fx_fwd'):
                    temp_sec = security(d, None, all_fx_rates, base_currency, sec, cls)
                    hedge_qty = temp_sec.add_hedge(cur_basket) #denominated in foreign currency
                    temp_sec.close_qty = hedge_qty
                    cur_basket[sec] = temp_sec.add_sec(all_fx_rates)
        else:
            # normal day. no activity ----------------------------------------------
            for sec, prior_basket_dict in prior_basket.items():
                if ('cash' in sec) or (abs(prior_basket_dict.get('close_qty')) > 0):
                    temp_sec = security(d, prior_basket_dict, all_fx_rates, base_currency, sec, portfolio.get(sec))
                    if ('equity' in sec) and temp_sec.dvd_rate != str(''):
                        cash_base_amt, trade_qty = temp_sec.trade_equity(cash_amt=temp_sec.dvd_rate*prior_basket_dict.get('open_qty'),
                                                                         cash_currency=temp_sec.crncy,
                                                                         base_currency=base_currency,
                                                                         fx_rates=all_fx_rates)
                        temp_sec.cash_inflow -= cash_base_amt
                        temp_sec.close_qty += trade_qty

                    cur_basket[sec] = temp_sec.add_sec(all_fx_rates)

        # end of day NAV calculation --------------------------------------------
        temp_basket = pd.DataFrame.from_dict(cur_basket, orient='index').reset_index()
        temp_basket['sec_id'] = temp_basket['index']
        temp_basket = temp_basket.drop(columns=['index'])
        # temp_basket['close_mv'] = np.where(temp_basket['security_type'].str.contains('fx_fwd'), None,
        #                                    np.where(temp_basket['security_type'].str.contains('option'), 100, 1)*temp_basket['close_qty']*temp_basket['eod_price']*temp_basket['fx'])
        #
        # df_fx_hedge = temp_basket[temp_basket['security_type']=='fx_fwd']
        # for idx, row in df_fx_hedge.iterrows():
        #     str_split = row['sec_ticker'].split('_')
        #     temp_basket.at[idx, 'close_mv'] = float(row['close_qty']*(row['fx']-all_fx_rates.get(row['currency']).get(dt.datetime.strptime(str_split[-1], '%Y%m%d').strftime('%Y-%m-%d'))))

        # append ---------------
        if df_values_output.empty:
            df_values_output = temp_basket[col_data]
        else:
            df_values_output = pd.concat([df_values_output, temp_basket[col_data]])

        d=common.workday(d, 1, holidays)

    output_detailed = df_values_output.copy(deep=True)
    output_summary = daily_returns(output_detailed)
    # output_summary = prepare_summary(output_detailed)
    return output_summary, output_detailed
'''

def cashflow_period_report(_detailed:pd.DataFrame, rebal_dates:list):
    _detailed['date'] = pd.to_datetime(_detailed['date']).dt.date
    # Ricardo made addition below
    _detailed['bid'] = pd.to_numeric(_detailed['bid'], errors='coerce')
    _detailed['ask'] = pd.to_numeric(_detailed['ask'], errors='coerce')
    #Ricardo made addition above
    _detailed['option_spread'] = np.where((_detailed['security_type'].str.contains('option') & (abs(_detailed['cash_inflow']) > 0)), 2*(_detailed['ask']-_detailed['bid'])/(_detailed['ask']+_detailed['bid']), 0)
    cond_equity = (_detailed['security_type'] == 'equity')
    cond_opt = (_detailed['security_type'].str.contains('option'))

    equities_pricing = _detailed[_detailed['security_type']=='equity']
    equities_pricing = common.total_return_calc(equities_pricing, 'eod_price', 'dvd_rate')
    equities_pricing_dict = dict(zip(equities_pricing['date'], equities_pricing['total_return_price']))

    summary = pd.DataFrame(data=rebal_dates, columns=['start_date'])
    summary['end_date'] = summary['start_date'].shift(-1)
    summary['moneyness'] = None
    summary['avg_moneyness'] = None

    for idx, row in summary.iterrows():
        cond1 = (pd.to_datetime(_detailed['date']).dt.date >= row['start_date'])
        cond2 = (pd.to_datetime(_detailed['date']).dt.date < row['end_date'])
        avg_correlation = equity_correlation_matrix(equities_pricing, row['start_date'], row['end_date'])
        summary.loc[idx, 'avg_correlation'] = avg_correlation
        summary.loc[idx, 'option_premium'] = _detailed[cond1 & cond2 & cond_opt]['cash_inflow'].sum()
        summary.loc[idx, 'option_start_mv'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['start_date']) & (_detailed['security_type'].str.contains('option')) & (~_detailed['sec_id'].str.contains('_old'))]['close_mv'].sum()
        summary.loc[idx, 'option_end_mv'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['end_date']) & (_detailed['security_type'].str.contains('option')) & (_detailed['sec_id'].str.contains('_old'))]['cash_outflow'].sum()
        summary.loc[idx, 'exclude_option_premium'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['end_date']) & (_detailed['security_type'].str.contains('option')) & (~_detailed['sec_id'].str.contains('_old'))]['cash_inflow'].sum()
        summary.loc[idx, 'exclude_option_start_mv'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['end_date']) & (_detailed['security_type'].str.contains('option')) & (~_detailed['sec_id'].str.contains('_old'))]['close_mv'].sum()

        summary.loc[idx, 'portfolio_start_mv'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['start_date'])]['close_mv'].sum() - (summary.loc[idx, 'option_start_mv'] + summary.loc[idx, 'option_premium'])
        summary.loc[idx, 'portfolio_end_mv'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['end_date'])]['close_mv'].sum() - (summary.loc[idx, 'exclude_option_start_mv'] + summary.loc[idx, 'exclude_option_premium'])

        summary.loc[idx, 'dvd'] = _detailed[cond1 & cond2 & cond_equity]['cash_inflow'].sum()
        summary.loc[idx, 'option_buyback'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['end_date']) & cond_opt]['cash_outflow'].sum()
        summary.loc[idx, 'avg_option_spread'] = _detailed[cond1 & cond2 & cond_opt & (_detailed['cash_inflow']>0)]['option_spread'].sum()/_detailed[cond1 & cond2 & cond_opt & (_detailed['cash_inflow']>0)]['option_spread'].count()


        if not _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['end_date']) & (_detailed['security_type'].str.contains('option')) & (_detailed['cash_inflow'] > 0)].empty:
            summary.loc[idx, 'moneyness'] = _detailed[(pd.to_datetime(_detailed['date']).dt.date == row['start_date']) & (_detailed['security_type'].str.contains('option')) & (_detailed['cash_inflow'] > 0)]['moneyness'].sum()

    summary = summary[~summary['end_date'].isnull()].reset_index(drop=True)
    summary['option_prem_return'] = summary['option_premium']/summary['portfolio_start_mv']
    summary['option_prem_annual_yield'] = 12*summary['option_prem_return']
    summary['avg_option_prem_annual_yield'] = summary['option_prem_annual_yield'].mean()
    summary['12m_rolling_avg_option_prem_annual_yield'] = summary['option_prem_annual_yield'].rolling(window=12).mean()

    summary['buyback_return'] = summary['option_buyback']/summary['portfolio_start_mv']
    summary['net_premium'] = summary['option_premium']+summary['option_buyback']
    summary['period_return'] = (summary['portfolio_end_mv']-summary['portfolio_start_mv'])/summary['portfolio_start_mv']
    summary['option_port_return'] = (summary['option_end_mv']-summary['option_start_mv'])/summary['portfolio_start_mv']
    summary['option_trading_return'] = (summary['option_start_mv'] + summary['option_premium']) / summary['portfolio_start_mv']
    summary['1_option_trading_return'] = summary['option_trading_return'] + 1
    summary['cum_option_trading_return'] = summary['1_option_trading_return'].cumprod()-1
    summary['basket_return'] = ((summary['end_date'].map(equities_pricing_dict)/summary['start_date'].map(equities_pricing_dict))-1) * (1+summary["option_premium"]/summary["portfolio_start_mv"]) # adding leverage factor
    summary['other_return'] = summary['period_return'] - (summary['basket_return'] + summary['option_prem_return'] + summary['buyback_return'])

    summary['period_less_premium'] = summary['period_return'] - summary['option_prem_return']
    summary['3m_rolling_return'] = summary['portfolio_start_mv'].pct_change(periods=3)
    summary['6m_rolling_return'] = summary['portfolio_start_mv'].pct_change(periods=6)
    summary['avg_moneyness'] = summary['moneyness'].mean()

    return summary[['start_date', 'end_date', 'period_return', 'basket_return', 'option_prem_return', 'buyback_return', 'other_return', 'option_port_return', 'option_trading_return',
                    'cum_option_trading_return', 'option_prem_annual_yield', 'avg_option_prem_annual_yield', 'avg_option_spread', '12m_rolling_avg_option_prem_annual_yield', 'dvd',
                    'option_premium', 'option_buyback', 'net_premium', 'portfolio_start_mv', 'portfolio_end_mv', 'period_less_premium', '3m_rolling_return', '6m_rolling_return', 'avg_correlation', 'moneyness', 'avg_moneyness']]

def equity_correlation_matrix(equities_pricing: pd.DataFrame, start_date=None, end_date=None) -> float:
    """Calculate average correlation for a specific period"""
    if start_date and end_date:
        equities_pricing = equities_pricing[(equities_pricing['date'] >= start_date) & 
                                          (equities_pricing['date'] < end_date)]
    
    equity_prices = equities_pricing.pivot(index='date', columns='sec_id', values='total_return_price')
    equity_returns = equity_prices.pct_change().dropna()
    correlation_matrix = equity_returns.corr()
    
    # Calculate average correlation (excluding self-correlations)
    mask = ~np.eye(correlation_matrix.shape[0], dtype=bool)
    avg_correlation = correlation_matrix.where(mask).mean().mean()
    
    return avg_correlation