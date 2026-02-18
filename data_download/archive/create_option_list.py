import pandas as pd
import datetime as dt
import os
import sys
from typing import Tuple, List, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper_functions.rebalance_dates import option_dates
sys.path.append('Z:\\ApolloGX\\im_dev\\std_lib')
import common

def get_option_data(_ticker, _rebal_dates):
    conn = common.db_connection()

    formatted_dates = ", ".join(f"'{date.strftime('%Y-%m-%d')}'" for date in _rebal_dates)
    
    query = f"""
    SELECT [ticker], [date], SUBSTRING(field, 4, 6) as side, CAST([value] AS FLOAT) as value
    FROM [dbo].[market_data]
    WHERE ticker LIKE '{_ticker + " CN"}___/__/__ C%' 
    AND (field = 'px_ask' OR field = 'px_bid') 
    AND date IN ({formatted_dates})
    AND CAST([value] AS FLOAT) > 0
    """
    options_data = pd.read_sql(query, conn.conn)
    return options_data


def create_option_list(
    startdate: str,
    enddate: str,
    underlying: str,
    equity_data_dir: str,
    restrictions: str = 'ATM',
    restriction_val: float = 0 
) -> list[str]:


    if restrictions not in ['ITM', 'ATM', 'OTM']:
        raise ValueError("Invalid restriction type for custom option list creation")

    startdate = dt.datetime.strptime(startdate, '%Y-%m-%d')
    enddate = dt.datetime.strptime(enddate, '%Y-%m-%d')

    rebal_dates = option_dates(startdate, common.tsx_holidays(), enddate)

    equity_data = pd.read_csv(os.path.join(equity_data_dir, f'{underlying}.csv'))
    option_data = get_option_data(underlying, rebal_dates)
    
    option_data['strike_price'] = option_data['ticker'].str.split().str[-1].str.replace('C', '').astype(float)
    option_data['expiration_date'] = pd.to_datetime(option_data['ticker'].str.split().str[-2], format='%m/%d/%y')
    option_data['TTM'] = (option_data['expiration_date'] - pd.to_datetime(option_data['date'])).dt.days
    option_data = option_data[(option_data['TTM'] < 43) & (option_data['TTM'] > 22)]


    option_list = []
    for date in rebal_dates:
        date_str = date.strftime('%Y-%m-%d')
        underlying_price = equity_data.loc[equity_data['Dates'] == date_str, 'PX_LAST'].values[0]

        filtered_options = option_data[option_data['date'] == date_str].copy()  
        
        if restrictions in ['ATM', 'OTM']:
            filtered_options = filtered_options[filtered_options['strike_price'] >= underlying_price]
        elif restrictions == 'ITM':
            filtered_options = filtered_options[filtered_options['strike_price'] <= underlying_price]

        # Take the option with the strike price closest to the target strike price which is determined by the 
        #   underlying price and the restriction type and value

        filtered_options['otm_pct'] = ((filtered_options['strike_price'] - underlying_price) / underlying_price).round(8)
        filtered_options_new = filtered_options[filtered_options['otm_pct'] > restriction_val]
        filtered_options_new = filtered_options_new[filtered_options_new['otm_pct'] < (2*restriction_val)]
        if not filtered_options_new.empty:
            filtered_options = filtered_options_new
        
        filtered_options['otm_pct'] = filtered_options['otm_pct'].abs()
        filtered_options['diff'] = (filtered_options['otm_pct'] - restriction_val).abs()

        best_option = filtered_options.loc[filtered_options['diff'].idxmin()]
        option_list.append({
            'date': date_str,
            'ticker': best_option['ticker'] + " Equity",
            'underlying_price': underlying_price,
            'otm_pct': ((best_option['strike_price'] - underlying_price) / underlying_price).round(8),
            'TTM': best_option['TTM']
        })
    option_list = pd.DataFrame(option_list).to_csv(os.path.join(equity_data_dir, f'{underlying}_option_list_{restrictions}.csv'), index=False)
    return option_list

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    equity_data_dir = os.path.join(project_root, 'data_download', 'raw_data')   
    for ticker in ['BMO', 'BNS', 'CM', 'NA', 'TD', 'RY','ZEB']:
        option_list =  create_option_list('2021-07-01', '2024-05-01', ticker, equity_data_dir, restrictions='OTM', restriction_val=0.02)
        print(f"Option list created for {ticker}")