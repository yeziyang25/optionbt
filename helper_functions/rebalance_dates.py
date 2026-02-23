import pandas as pd
import numpy as np
import datetime as dt

import os
import sys

# ---------------------------------------------------------------------------
# Internal-library imports — optional.  Falls back to the standalone
# implementations in utils.market_utils when the IM library is unavailable.
# ---------------------------------------------------------------------------
_common = None
_data_library = None
try:
    sys.path.append("Z:\\ApolloGX")
    _cur_dir = os.path.dirname(__file__)
    if "\\im_dev\\" in _cur_dir:
        import im_dev.std_lib.common as _common          # type: ignore
        import im_dev.std_lib.data_library as _data_library  # type: ignore
    else:
        import im_prod.std_lib.common as _common         # type: ignore
        import im_prod.std_lib.data_library as _data_library  # type: ignore
except Exception:
    try:
        _proj_root = os.path.dirname(os.path.dirname(__file__))
        if _proj_root not in sys.path:
            sys.path.insert(0, _proj_root)
        import common as _common          # type: ignore
        import data_library as _data_library  # type: ignore
    except Exception:
        pass

# Convenience aliases
common = _common
data_library = _data_library

def option_dates(start_date: dt.datetime, holidays: dict, end_date: dt.datetime = dt.datetime.now()):
    """
    Return a list of monthly third-Friday option expiry/roll dates.

    Uses the internal IM library (common.week_count / common.workday) when
    available, otherwise falls back to the standalone implementations in
    utils.market_utils.
    """
    # Pick up week_count and workday from whichever source is available
    if common is not None:
        _week_count = common.week_count
        _workday = common.workday
    else:
        from utils.market_utils import week_count as _week_count, workday as _workday

    output_dates = []
    week_cal = _week_count(start_date, (end_date.year - start_date.year) + 1)
    d = start_date
    while d.date() <= end_date.date():
        d_str = d.strftime('%Y-%m-%d')
        if week_cal.get(d_str) == 3:
            if (d.weekday() == 3) and (not holidays.get((d + dt.timedelta(days=1)).strftime('%Y-%m-%d')) is None):
                output_dates.append(d.date())
            elif (week_cal.get(d_str) == 3) and (d.weekday() == 4) and (holidays.get(d.strftime('%Y-%m-%d')) is None):
                output_dates.append(d.date())
        d = _workday(d, 1, holidays)
    return output_dates


def weekly_option_dates(start_date: dt.datetime, holidays: dict, end_date: dt.datetime = dt.datetime.now()):
    """
    Return a list of weekly Friday option roll dates (every Friday that is
    not a holiday).
    """
    if common is not None:
        _workday = common.workday
    else:
        from utils.market_utils import workday as _workday

    output_dates = []
    d = start_date
    while d.date() <= end_date.date():
        if d.weekday() == 4 and holidays.get(d.strftime('%Y-%m-%d')) is None:
            output_dates.append(d.date())
        d = _workday(d, 1, holidays)
    return output_dates