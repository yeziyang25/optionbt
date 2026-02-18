import pandas as pd
import numpy as np
import datetime as dt

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

def option_dates_customized(start_date: dt.datetime, holidays: dict, end_date: dt.datetime, tenor: int):
    output_dates = []
    d = start_date
               
    while d.date() <= end_date.date():
        # 1. target date = today + tenor
        if d == start_date:
            target_date = d
        else:
            target_date = d + dt.timedelta(days=tenor)

        # 2. find the closest Friday to target_date
        weekday_target = target_date.weekday()  
        days_until_friday = (4 - weekday_target) % 7
        if days_until_friday == 0:
            days_until_friday = 7 
        closest_friday = target_date + dt.timedelta(days=days_until_friday)

        # 3. adjust if holiday
        if closest_friday is not None:
            if holidays.get(closest_friday.strftime('%Y-%m-%d')):
                adjusted = closest_friday - dt.timedelta(days=1)
                if adjusted.weekday() == 3:
                    expiry = adjusted.date()
                else:
                    expiry = closest_friday.date()
            else:
                expiry = closest_friday.date()

            output_dates.append(expiry)
            d = closest_friday
        else:
            break

    return output_dates

def equity_rebalance_dates(start_date: dt.datetime, end_date: dt.datetime, rule: str, option_rebal_dates: list):
    """
    Generate equity rebalance dates based on rule:
    Q = Quarterly (3rd Friday Mar/Jun/Sep/Dec)
    S = Semi-Annual (3rd Friday Mar/Sep)
    A = Annual (3rd Friday Dec)
    O = Same as option roll schedule (i.e. everytime an option is rolled, the portfolio is rebalanced)
    """
    def third_friday(year, month):
        d = dt.date(year, month, 15)  
        while d.weekday() != 4:      
            d += dt.timedelta(days=1)
        return d

    dates = []

    if rule == "O":
        return [d if isinstance(d, dt.date) else d.date() for d in option_rebal_dates]

    for year in range(start_date.year, end_date.year + 1):
        if rule == "Q":
            months = [3, 6, 9, 12]
        elif rule == "S":
            months = [3, 9]
        elif rule == "A":
            months = [12]
        else:
            raise ValueError("Invalid equity rebalance rule. Use Q, S, A, or O.")

        for m in months:
            d = third_friday(year, m)
            if start_date.date() <= d <= end_date.date():
                dates.append(d)

    return dates

"""
start_date = dt.datetime(2022, 1, 20)
end_date = dt.datetime(2025, 9, 30)
option_rebal_dates = option_dates_customized(start_date, common.tsx_holidays(), end_date + dt.timedelta(days=31), 16)
equity_rebal = equity_rebalance_dates(start_date, end_date,'Q', option_rebal_dates)
print(option_rebal_dates)
"""