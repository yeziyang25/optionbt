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

def option_dates(start_date:dt.datetime, holidays:dict, end_date:dt.datetime=dt.datetime.now()):
    output_dates = []
    week_cal = common.week_count(start_date, (end_date.year - start_date.year) + 1)
    d = start_date
    while d.date() <= end_date.date():
        d_str = d.strftime('%Y-%m-%d')
        if (week_cal.get(d_str) == 3):
            if (d.weekday() == 3) and (not holidays.get((d+dt.timedelta(days=1)).strftime('%Y-%m-%d')) is None):
                output_dates.append(d.date())
            elif (week_cal.get(d_str)==3) and (d.weekday()==4) and (holidays.get(d.strftime('%Y-%m-%d')) is None):
                output_dates.append(d.date())
        d = common.workday(d, 1, holidays)
    return output_dates