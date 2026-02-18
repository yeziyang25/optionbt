import pandas as pd
import numpy as np
import datetime as dt

import os
cur_dir = os.path.dirname(__file__)
import sys
from helper_functions.securities import security_data
from helper_functions.securities import cash, equity, option, fx
sys.path.append('Z:\\ApolloGX')
if "\\im_dev\\" in cur_dir:
    import im_dev.std_lib.common as common
    import im_dev.std_lib.data_library as data_library
else:
    import im_prod.std_lib.common as common
    import im_prod.std_lib.data_library as data_library

class security():
    def __init__(self, d:dt.datetime, sec:str, sec_details:security_data, prior_sec_details:dict, fx_rates:dict, base_currency:str, eod_pricing_method:str="mid"):
        self.date = d
        self.sec_id = sec
        self.sec_name = sec_details.sec_name
        self.security_type = sec_details.sec_type
        self.crncy = sec_details.currency

        fx_dict = fx(fx_rates, self.date, self.crncy, base_currency)
        if not fx_dict.get_fx() is None:
            self.fx = fx_dict.get_fx()
        else:
            self.fx = prior_sec_details.get('fx')

        self.cash_inflow = 0 # track inflows (option premiums and dividends)
        self.cash_outflow = 0 # track outflows (for now, just option buybacks)

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

        self.eod_price = _sec_cls.eod_price
        self.bid = _sec_cls.bid
        self.ask = _sec_cls.ask

        if prior_sec_details is None:
            self.prior_eod_price = None
            self.prior_fx = None
            self.open_wt = 0
            self.close_wt = sec_details.allocation
        else:
            self.prior_fx = prior_sec_details.get('fx')
            self.prior_eod_price = prior_sec_details.get('eod_price')
            self.open_wt = prior_sec_details.get('close_wt')
            if (str('option') in self.security_type) and (self.expiry==self.date.date()):
                self.close_wt = 0
            else:
                self.close_wt = self.open_wt

    def add_sec(self):
        _dict = {'date': self.date, 'sec_id': self.sec_id, 'sec_ticker': self.sec_ticker, 'sec_name': self.sec_name, 'security_type': self.security_type, 'currency': self.crncy,
                 'open_wt': self.open_wt, 'close_wt': self.close_wt, 'prior_fx': self.prior_fx, 'prior_eod_price': self.prior_eod_price,
                 'bid': self.bid, 'ask': self.ask, 'eod_price': self.eod_price,
                 'fx': self.fx, 'dvd_rate': self.dvd_rate, 'opt_u_price': self.option_u_price, 'cash_inflow': self.cash_inflow, 'cash_outflow': self.cash_outflow}
        return _dict

def prepare_summary(_detailed:pd.DataFrame):
    _detailed['date'] = pd.to_datetime(_detailed['date']).dt.date
    _detailed = _detailed[_detailed['sec_id']!='cash'].reset_index(drop=True) # remove cash

    # (1+Ra) prior day to today
    # (1+Rb) execution costs for today
    _detailed['eod_price_dvd'] = _detailed['eod_price'] + np.where(_detailed['dvd_rate']==str(''), 0, _detailed['dvd_rate'])
    _detailed['ra_start'] = np.where(abs(_detailed['open_wt'])>0, _detailed['prior_eod_price'], 0)*_detailed['open_wt']
    _detailed['ra_end'] = np.where(abs(_detailed['open_wt'])>0, _detailed['eod_price_dvd'], 0)*_detailed['open_wt']
    _detailed['rb_start'] = np.where((_detailed['open_wt']==0) & (abs(_detailed['close_wt'])>0),
                                     _detailed['bid'],
                                     np.where((abs(_detailed['open_wt'])>0) & (abs(_detailed['close_wt'])>0), _detailed['eod_price'], 0))*_detailed['close_wt']
    _detailed['rb_end'] = np.where(abs(_detailed['close_wt'])>0, _detailed['eod_price'], 0)*_detailed['close_wt']

    #consolidate to a summary
    summary = pd.DataFrame(columns=['date', 'daily_return', 'ra', 'rb'])
    df_agg = _detailed.groupby(by=['date'], group_keys=True)[['ra_start', 'ra_end', 'rb_start', 'rb_end']].apply(sum).reset_index()
    summary['date'] = df_agg['date']
    summary['ra'] = df_agg['ra_end']/df_agg['ra_start']
    summary['rb'] = df_agg['rb_end']/df_agg['rb_start']
    summary['daily_return'] = summary['ra']*summary['rb']-1
    return summary

def build_backtest(portfolio:dict, start_date:dt.datetime, end_date: dt.datetime, rebal_dates:list, holidays:dict):
    all_fx_rates = data_library.fx_rates()
    base_currency = portfolio.get('cash').currency
    col_data = ['date', 'sec_id', 'sec_ticker', 'sec_name', 'security_type', 'currency', 'open_wt',
                'close_wt', 'prior_fx', 'prior_eod_price', 'bid', 'ask', 'eod_price', 'dvd_rate', 'fx', 'opt_u_price']
    df_values_output = pd.DataFrame(columns=col_data)

    d = start_date
    while d <= end_date:
        rebal_date = True if d.date() in rebal_dates else False

        if (d == start_date):
            prior_basket = {}
        else:
            prior_basket = cur_basket
        cur_basket = {}

        if rebal_date or (d==start_date):
            # seed/roll-date ----------------------------------------------
            for sec, cls in portfolio.items():
                if (abs(cls.allocation) > 0):
                    if str('option') in cls.sec_type:
                        temp_sec = security(d, sec, cls, None, all_fx_rates, base_currency, eod_pricing_method="mid")
                    else:
                        temp_sec = security(d, sec, cls, prior_basket.get(sec), all_fx_rates, base_currency)
                    cur_basket[sec] = temp_sec.add_sec()

            for sec, val in prior_basket.items():
                if str('option') in val.get('security_type'):
                    temp_sec = security(d, sec, portfolio.get(sec), prior_basket.get(sec), all_fx_rates, base_currency, eod_pricing_method="intrinsic")
                    cur_basket[sec + str('_old')] = temp_sec.add_sec()
        else:
            # non roll-date ----------------------------------------------
            for sec, cls in portfolio.items():
                if str('option') in cls.sec_type:
                    temp_sec = security(d, sec, cls, prior_basket.get(sec), all_fx_rates, base_currency, eod_pricing_method="mid")
                else:
                    temp_sec = security(d, sec, cls, prior_basket.get(sec), all_fx_rates, base_currency)
                cur_basket[sec] = temp_sec.add_sec()

        # end of day NAV calculation --------------------------------------------
        temp_basket = pd.DataFrame.from_dict(cur_basket, orient='index').reset_index()
        temp_basket['sec_id'] = temp_basket['index']
        temp_basket = temp_basket.drop(columns=['index'])

        # append ---------------
        if df_values_output.empty:
            df_values_output = temp_basket[col_data]
        else:
            df_values_output = pd.concat([df_values_output, temp_basket[col_data]])

        d=common.workday(d, 1, holidays)

    output_detailed = df_values_output.copy(deep=True)
    output_summary = prepare_summary(output_detailed)
    return output_summary, output_detailed