import pandas as pd
import numpy as np
import datetime as dt
import pyodbc
import os
import requests
from io import BytesIO
import calendar
import logging
from collections import Counter
from collections.abc import (Hashable, Sequence)
import paramiko

LOG_SAVE_LOC = "Z:\\IPS\\python\\logs\\"
TODAY_STR = dt.datetime.today().strftime("%Y%m%d")
class db_connection():
    def __init__(self, database_conn:str="ips_sandbox"):

        if database_conn == "ips_sandbox":
            server = 'hemi-ips-sql-srv.database.windows.net'
            database = 'hemi-ips-db'
            username = 'ips_login'
            password = 'BxigZoHCdWdvnp*MtX3V!Uia'
        elif database_conn == "globalx":
            server = 'hemi-fund-sql-srv.database.windows.net'
            database = 'hemi-fund-db-prod'
            username = 'ipsdev'
            password = 'qmb*pht0pnp8RGT3wzm'
        else:
            raise ValueError(f"Error connecting to: {database_conn}. Please specify a proper database to connect to.")

        self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        self.cursor = self.conn.cursor()

    def query_tbl(self, sql_query:str) -> pd.DataFrame:
        return pd.read_sql(sql_query, self.conn)

    def insert_data(self, df_insert: pd.DataFrame, tbl_name: str, sql_check_existing: str = ''):
        """
        This function inserts records to a table in the database. It assumes the dataframe has all the necessary fields to append to the table.
        There's an option to check for existing records and remove them to avoid duplications.

        :param df_insert: DataFrame of input data
        :param tbl_name: Name of table that we want to insert data into
        :param sql_check_existing: sql statement of the data that should be overwritten
        :param existing_id_col_name: unique ID

        Last Updated Apr 11 2024
        """

        if (sql_check_existing != '') and (not "WHERE" in sql_check_existing):
            raise ValueError(
                f"Must include a WHERE condition for the existing records that will be updated: {sql_check_existing}")

        # overwrite existing data if there is a sql_check_existing parameter passed through
        primary_key = self.query_tbl(
            f"SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.key_column_usage WHERE TABLE_NAME = '{tbl_name}'").loc[
            0, 'COLUMN_NAME']
        if sql_check_existing != '':
            existing_records = self.query_tbl(sql_query=sql_check_existing)
            if not existing_records.empty:
                # delete the existing data
                if existing_records.shape[0] < 1000:
                    _values_tuple = self.list_to_sql_tuple([existing_records[primary_key].astype(str).tolist()],
                                                           convert_elements=False)
                    delete_statement = f"DELETE FROM {tbl_name} WHERE {primary_key} IN ({','.join('?' * len(_values_tuple[0]))})"
                    self.cursor.executemany(delete_statement, _values_tuple)
                else:
                    delete_statement = f"DELETE FROM {tbl_name} WHERE{sql_check_existing.split('WHERE')[1]}"
                    self.cursor.execute(delete_statement)
                self.conn.commit()

        # append the new data
        self.insert_data_from_dataset(df_insert, tbl_name, primary_key)

    def insert_data_from_dataset(self, input_dataset: pd.DataFrame, tbl_name: str, primary_key: str):
        _tbl_flds = self.query_tbl(
            f"SELECT COLUMN_NAME as flds FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{tbl_name}' ORDER BY ORDINAL_POSITION;")[
            'flds'].tolist()
        _tbl_flds.remove(primary_key)

        export_dataset = pd.DataFrame(columns=_tbl_flds)
        input_dataset_flds_list = input_dataset.columns.tolist()
        for fld in _tbl_flds:
            if fld in input_dataset_flds_list:
                export_dataset[fld] = input_dataset[fld]
            else:
                if fld != primary_key:
                    export_dataset[fld] = None

        _values_tuple = self.list_to_sql_tuple(export_dataset.values, convert_elements=True)
        insert_statement = f"INSERT INTO {tbl_name} {self.list_to_sql_str(export_dataset.columns.tolist(), convert_elements=False, square_brackets=True)} values ({','.join('?' * len(_values_tuple[0]))})"
        self.cursor.executemany(insert_statement, _values_tuple)
        self.conn.commit()

    def update_row(self, input_dataset: pd.DataFrame, tbl_name: str, primary_key_col_name: str) -> None:
        _tbl_flds = self.query_tbl(
            f"SELECT COLUMN_NAME as flds FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{tbl_name}' ORDER BY ORDINAL_POSITION;")[
            'flds'].tolist()

        export_dataset = pd.DataFrame()
        input_dataset_flds_list = input_dataset.columns.tolist()
        for fld in _tbl_flds:
            if fld in input_dataset_flds_list:
                export_dataset[fld] = input_dataset[fld]
            else:
                if fld != primary_key_col_name:
                    export_dataset[fld] = None

        update_col_list = export_dataset.columns.to_list()
        update_col_list.remove(primary_key_col_name)
        for i in export_dataset.index:
            query = "UPDATE " + tbl_name + " SET "

            # find the primary key. use this to identify the record to update
            primary_key_val = export_dataset.loc[i, primary_key_col_name]

            # Adds [Field Name] = [Field Value] to query
            for j in update_col_list:
                if isinstance(export_dataset.loc[i, j], str) or isinstance(export_dataset.loc[i, j], dt.datetime):
                    query += j + " = \'" + str(export_dataset.loc[i, j]) + "\'"
                elif export_dataset.loc[i, j] is None:
                    query += j + " = NULL"
                else:
                    query += j + " = " + str(export_dataset.loc[i, j])

                if j != export_dataset.columns.to_list()[-1]:
                    query += ", "

            query += " WHERE " + primary_key_col_name + " = '" + str(primary_key_val) + "';"

            self.cursor.execute(query)
            self.conn.commit()

    def list_to_sql_str(self, dataset: list, convert_elements: bool, square_brackets: bool = False) -> str:
        """
        converts a list of values to a list in a format acceptable for SQL

        :param dataset: list of values to convert
        :param convert_elements: optional, if True then it function will convert values into an acceptable format to insert the data to the database. i.e. None values need to be replaced as "NULL" for SQL
        :param square_brackets: optional, if True then it adds square brackets around each element which is important for SQL statements dealing with column names
        :return:
        """
        sql_str = "("
        for val in dataset:
            if square_brackets:
                _left_char = "["
                _right_char = "]"
            else:
                _left_char = ""
                _right_char = ""

            if convert_elements:
                sql_str += _left_char + self.convert_elm_sql(val) + _right_char + ','
            else:
                sql_str += _left_char + val + _right_char + ','
        return sql_str[0:len(sql_str) - 1] + ")"

    def list_to_sql_tuple(self, dataset: list, convert_elements: bool) -> list:
        """
        This function takes the dataset (list) that we will append to the table in the database and converts it into a list of typles to utilize the executemany function.
        :param dataset: input data
        :param convert_elements: if this is True then we convert each value to the format that allows us to insert the data to the table. If it is false then we do not add any conversion.
        Last Updated Apr 11 2024
        """
        if convert_elements:
            return list(tuple(self.convert_tuple_sql(row)) for row in dataset)
        else:
            return list(tuple(row) for row in dataset)

    def convert_elm_sql(self, elm: tuple) -> tuple:
        """
        This function takes the dataset that we will append to the table in the database. It converts the data to the format that allows us to insert the data.
        :param elm: input data
        Last Updated Apr 11 2024
        """
        if (elm is None):
            return f"NULL"
        elif (type(elm) is int) or (type(elm) is float):
            if np.isnan(input_val):
                return f"NULL"
            else:
                return elm
        else:
            return f"'{elm}'"

    def convert_tuple_sql(self, row_data: tuple) -> tuple:
        """
        This function takes the dataset that we will append to the table in the database. It converts the data to the format that allows us to insert the data.
        :param row_data: input data
        Last Updated Apr 11 2024
        """
        for i, input_val in enumerate(row_data):
            if (input_val is None):
                row_data[i] = None
            elif (str(input_val) == "None") or (str(input_val) == "nan"):
                row_data[i] = None
            elif (type(input_val) is int) or (type(input_val) is float):
                if np.isnan(input_val):
                    row_data[i] = None
                else:
                    pass
            else:
                pass
        return row_data

def tsx_holidays():
    conn = db_connection()
    query = f"""SELECT * FROM holidays WHERE [holiday_type] = 'tsx trading'"""
    df = conn.query_tbl(query)
    df['delta'] = 1
    return dict(zip(pd.to_datetime(df.date).dt.strftime("%Y-%m-%d"), df.delta))

def tsx_setl_holidays():
    conn = db_connection()
    query = f"""SELECT * FROM holidays WHERE [holiday_type] = 'tsx settlement'"""
    df = conn.query_tbl(query)
    df['delta'] = 1
    return dict(zip(pd.to_datetime(df.date).dt.strftime("%Y-%m-%d"), df.delta))

def nyse_holidays():
    conn = db_connection()
    query = f"""SELECT * FROM holidays WHERE [holiday_type] = 'nyse trading'"""
    df = conn.query_tbl(query)
    df['delta'] = 1
    return dict(zip(pd.to_datetime(df.date).dt.strftime("%Y-%m-%d"), df.delta))

def download_from_url(_url:str):
    bytes_data = requests.get(_url).content
    df = pd.read_csv(BytesIO(bytes_data))
    return df

def get_fx_rates_gx_db(pricing_date:dt.datetime, pricing_source:str) -> dict:
    """
    Function gathers the mellon fx rates from the currencyfxrates table in the Global X database
    :param pricing_date:
    :param pricing_source: pricing source field in table
    :return: dictionary
    """
    conn1 = db_connection("globalx")
    q_FX = f"""SELECT [FX Rate] as rate,[Local Price Currency] as currency, [As of Date] as date FROM currencyfxrates as fx
            WHERE [Data Source]= '{pricing_source}' and fx.[As of Date] = '{pricing_date.strftime('%Y-%m-%d')}';"""
    fx_df = conn1.query_tbl(q_FX)
    fx_df["fx_key"] = fx_df["currency"] + str("_") + pd.to_datetime(fx_df["date"]).dt.strftime("%Y%m%d")
    fx_dict = dict(zip(fx_df["fx_key"], fx_df["rate"]))
    return fx_dict

def update_dataframe(old: pd.DataFrame, new: pd.DataFrame, keys: dict=None) -> pd.DataFrame:
    """
    Drops old values with identical key column entries, returns old dataframe concatenated with new dataframe.
    NOTE: Datatypes supported so far are ONLY strs, ints, floats, and dt.datetime objects.

    :param old: DataFrame to update
    :param new: DataFrame with new Information
    :param keys: Identifying column labels
    :return: updated old DataFrame

    Last Updated Jan 11 2024
    """

    if old.empty:
        return new
    if new.empty:
        return old
    if keys is None:
        if set(old.columns) != set(new.columns):
            raise Exception('keys param cannot be None; DataFrame columns do not match. Please specify key columns.')
        keys = old.columns
    if isinstance(keys, str) or isinstance(keys, float) or isinstance(keys, int) or isinstance(keys, dt.datetime):
        keys = [keys]

    new['temp_unique_key'] = None
    old['temp_unique_key'] = None

    def str_convert(s):
        return s.strftime('%Y%m%d') if s is dt.datetime else str(s)

    new['temp_unique_key'] = new[keys].apply(lambda row: '_'.join([str_convert(x) for x in row.array]), axis=1)
    old['temp_unique_key'] = old[keys].apply(lambda row: '_'.join([str_convert(x) for x in row.array]), axis=1)

    # old_filtered = old[old['temp_unique_key'].apply(lambda k: k not in new['temp_unique_key'].array)]
    old_filtered = old[~old['temp_unique_key'].isin(np.unique(new['temp_unique_key']).tolist())]
    return pd.concat([old_filtered, new], ignore_index=True).drop(columns=['temp_unique_key'])

def round_opt_strike_df(dataset:pd.DataFrame):
    dataset = dataset.reset_index(drop=True)
    output = pd.DataFrame(dataset.astype(float).astype(str).str.split('.', expand=True).to_numpy(), columns=['int', 'dec'])
    return pd.DataFrame(np.where(output['dec'].astype(float)==0, output['int'].astype(str), output['int'].astype(str)+str('.')+output['dec'].astype(int).astype(str)), columns=['col'])['col']

def round_opt_strike_elm(val:float):
    var_split = str(val).split(".")
    if float(var_split[1]) == 0:
        return str(var_split[0])
    else:
        return f"{var_split[0]}.{int(var_split[1])}"

def round_opt_strike(values_to_round) ->str:
    if type(values_to_round) == pd.core.series.Series:
        return round_opt_strike_df(values_to_round)
    elif (type(values_to_round) == float):
        return round_opt_strike_elm(values_to_round)
    else:
        raise Exception("Value type unrecognized")

class extract_option_ticker():
    def __init__(self, initial_dataset:pd.DataFrame, ticker_col_name:str):
        dataset = initial_dataset.copy(deep=True)
        underlying_override = {'SPX US': 'SPX Index', 'SPXW US': 'SPX Index', 'NDX US': 'NDX Index', 'NDXP US': 'NDX Index', 'MXEF US': 'MXEF Index', 'BTCC CN': 'BTCC/B CN'}
        if not dataset.empty:
            dataset['expiry'] = pd.to_datetime(dataset[ticker_col_name].str.split(' ', expand=True)[2], format='%m/%d/%y').dt.date
            dataset['option_type'] = np.where(dataset[ticker_col_name].str.split(' ', expand=True)[3].str[0].astype(str) == 'C',
                                              'call', 'put')
            dataset['strike'] = dataset[ticker_col_name].str.split(' ', expand=True)[3].str[1:].astype(float)
            dataset['underlying_ticker'] = (np.where(dataset[ticker_col_name].str[0].isin(['1', '2', '3', '4', '5']),
                                                     dataset[ticker_col_name].str.split(' ', expand=True)[0].str[1:],
                                                     dataset[ticker_col_name].str.split(' ', expand=True)[0]) +
                                            str(' ') + dataset[ticker_col_name].str.split(' ', expand=True)[1])
            dataset['underlying_ticker'] = dataset['underlying_ticker'].str.replace("TRP1 CN", "TRP CN")
            dataset['underlying_ticker'] = dataset['underlying_ticker'].map(underlying_override).fillna(dataset['underlying_ticker'])
            dataset['currency'] = np.where(dataset[ticker_col_name].str.contains(' CN '), 'CAD', 'USD')

            self.expiry = dict(zip(dataset[ticker_col_name], dataset['expiry']))
            self.option_type = dict(zip(dataset[ticker_col_name], dataset['option_type']))
            self.strike = dict(zip(dataset[ticker_col_name], dataset['strike']))
            self.underlying_ticker = dict(zip(dataset[ticker_col_name], dataset['underlying_ticker']))
            self.currency = dict(zip(dataset[ticker_col_name], dataset['currency']))

class TaskLog():
    def __init__(self, log_message):
        component_list = log_message.split(" | ")
        self.timestamp = component_list[0].strip()
        self.location = component_list[1].strip()
        self.status = component_list[2].strip()
        self.task_name = None
        if len(component_list) > 3:
            self.task_name = component_list[3].strip()

def total_return_calc(input_data:pd.DataFrame, price_col:str, dvd_col:str):
    """
    This function calculates the Total Return of a security

    :param data: DataFrame of prices and dividends
    :param price_col: name of the price column
    :param dvd_col: name of the dividend column
    :return: adds a column called total_return_price which is the price of the security with reinvestment

    Last Updated Feb 01 2024
    """
    data = input_data.copy(deep=True).reset_index(drop=True)
    data[dvd_col] = data[dvd_col].fillna(0)
    data[dvd_col] = np.where(data[dvd_col]==str(""), 0, data[dvd_col])
    data["dvd_reinvestment"] = None
    
    for row in data.itertuples():
        if row.Index == 0:
            if (data.loc[row.Index, dvd_col] > 0):
                raise Exception(
                    "There cannot be a dividend on the first day otherwise we cannot properly calculate the reinvestment.")
            else:
                data.loc[row.Index, "dvd_reinvestment"] = 0
        else:
            if (data.loc[row.Index, dvd_col] > 0):
                daily_return = data.loc[row.Index, price_col] / (data.loc[row.Index - 1, price_col] - data.loc[row.Index, dvd_col])
                data.loc[row.Index, "dvd_reinvestment"] = (data.loc[row.Index - 1, "dvd_reinvestment"] + data.loc[row.Index, dvd_col]) * daily_return
            else:
                daily_return = data.loc[row.Index, price_col] / data.loc[row.Index - 1, price_col]
                data.loc[row.Index, "dvd_reinvestment"] = data.loc[row.Index - 1, "dvd_reinvestment"] * daily_return
    data['total_return_price'] = data['dvd_reinvestment'] + data[price_col]
    return data

def dfToHTML(df: pd.DataFrame, alignRight=False):
    arr = df.T.reset_index().T.to_numpy()
    htmlBody = '''<table>'''
    for line in arr:
        if ('Total' in line[0]) or min(line == arr[0]):
            _bold_s = '''<b>'''
            _bold_e = '''</b>'''
        else:
            _bold_s = ''''''
            _bold_e = ''''''

        htmlLine = '''<tr>'''
        for elm in line:
            if (elm == 0) or (elm == '0'):
                elm = '-'

            if (elm != line[0]) and alignRight and (arr[0].tolist() != line.tolist()):
                align = ''' align="right"'''
            else:
                align = ''

            htmlLine += _bold_s + '''<td''' + align + '''>''' + str(elm) + '''</td>''' + _bold_e
        htmlLine += '''</tr>'''
        htmlBody += htmlLine
    htmlBody += '''</table>'''
    return htmlBody

def workday_df(df:pd.DataFrame, delta:int, holidays:dict=tsx_holidays()):
    _df_date = pd.DataFrame(df.values, columns=['old_date'])
    if delta > 0:
        offset_dict = {'4': 3, '5': 2, 'else': 1}
    elif delta < 0:
        offset_dict = {'0': -3, '6': -2, 'else': -1}
    else:
        offset_dict = {'else': 0}

    _df_date['old_date'] = pd.to_datetime(_df_date['old_date'])
    _df_date['week_day'] = _df_date['old_date'].dt.dayofweek
    _df_date['delta'] = _df_date['week_day'].astype(str).map(offset_dict).fillna(offset_dict.get('else'))
    _df_date['new_date'] = _df_date['old_date'] + pd.to_timedelta(_df_date['delta'],'d')
    return holiday_check_df(_df_date['new_date'], holidays, offset_dict)
    #
    # _df_date['holidays_check'] = _df_date['new_date'].dt.strftime('%Y-%m-%d').map(holidays).fillna(0)
    # _df_date['new_date_week_day'] = _df_date['new_date'].dt.dayofweek
    # _df_date['holidays_delta'] = np.where(_df_date['holidays_check']==1, _df_date['new_date_week_day'].astype(str).map(offset_dict).fillna(offset_dict.get('else')), 0)
    #
    # _df_date['new_date'] = _df_date['new_date'] + pd.to_timedelta(_df_date['holidays_delta'], 'd')
    #
    # return _df_date['new_date']

def holiday_check_df(df:pd.DataFrame, holidays:dict, offset_dict:dict):
    df = pd.DataFrame(df.values, columns=['new_date'])
    df['holiday_check'] = df['new_date'].dt.strftime('%Y-%m-%d').map(holidays).fillna(0)

    df_holidays = df[df['holiday_check']==1]

    if not df_holidays.empty:
        df['new_date_week_day'] = df['new_date'].dt.dayofweek
        df['holidays_delta'] = np.where(df['holiday_check']==1, df['new_date_week_day'].astype(str).map(offset_dict).fillna(offset_dict.get('else')), 0)
        df['new_date'] = df['new_date'] + pd.to_timedelta(df['holidays_delta'], 'd')

        return holiday_check_df(df['new_date'], holidays, offset_dict)
    else:
        return df['new_date']

def holiday_check(d:dt.datetime, holidays:dict, offset_dict:dict):
    #if the date is a holiday then it takes the prior business day. If it is not a holiday then return date.
    if holidays.get(d.strftime('%Y-%m-%d')) is None:
        return d
    else:
        _new_date_week_day = d.weekday()
        _holidays_delta = offset_dict.get('else') if offset_dict.get(str(_new_date_week_day)) is None else offset_dict.get(str(_new_date_week_day))
        return holiday_check(d + dt.timedelta(days=_holidays_delta), holidays, offset_dict)

def workday_dt(_dt:dt.datetime, delta:int, holidays:dict=tsx_holidays()):
    if delta > 0:
        offset_dict = {'4': 3, '5': 2, 'else': 1}
    elif delta < 0:
        offset_dict = {'0': -3, '6': -2, 'else': -1}
    else:
        offset_dict = {'else': 0}

    _weekday = _dt.weekday()
    _delta = offset_dict.get('else') if offset_dict.get(str(_weekday)) is None else offset_dict.get(str(_weekday))
    new_date = _dt + dt.timedelta(days=_delta)
    return holiday_check(new_date, holidays, offset_dict)
    # _holiday_check = False if holidays.get(_new_date.strftime('%Y-%m-%d')) is None else True
    # if _holiday_check:
    #     _new_date_week_day = _new_date.weekday()
    #     _holidays_delta = offset_dict.get('else') if offset_dict.get(str(_new_date_week_day)) is None else offset_dict.get(str(_new_date_week_day))
    #     return _new_date + dt.timedelta(days=_holidays_delta)
    # else:
    #     return _new_date

def workday(_date, delta:int, holidays:dict=tsx_holidays()):
    _remaining = delta
    if type(_date) == pd.core.series.Series:
        while abs(_remaining) > 0:
            _date = workday_df(_date, delta, holidays)
            _remaining-=1*np.sign(delta)
        return _date
    elif (type(_date) == dt.datetime) or (type(_date) == dt.date):
        while abs(_remaining) > 0:
            _date = workday_dt(_date, delta, holidays)
            _remaining-=1*np.sign(delta)
        return _date

def week_count(execute_date:dt.datetime, years:int=2):
    # functions returns a map of a date and what week number it calls on. This will help identify the 3rd friday option rebalance days
    dict = {}
    calendar.setfirstweekday(calendar.SUNDAY)
    for iy in range(years):
        for i in range(12):
            imonth = calendar.monthcalendar(execute_date.year+iy, i+1)
            week_count = 1
            for ii in imonth:
                if ii[5] != 0:
                    _year = execute_date.year
                    _month = i+1
                    for elm in ii:
                        if elm > 0:
                            dict[dt.datetime(execute_date.year+iy, i+1, elm).strftime('%Y-%m-%d')] = week_count
                    week_count += 1
    return dict

def trade_entry_bbg_flds(dataset:pd.DataFrame):
    trans_map = {'Sell': 'S', 'Buy': 'B', 'Sell to Open': 'H', 'Buy to Close': 'C'}
    dataset['bbg_ticker'] = dataset['ticker'] + str(' Equity')
    dataset['bbg_transaction'] = dataset['trade'].map(trans_map)
    dataset['bbg_quantity'] = dataset['quantity']
    dataset['bbg_empty'] = None
    dataset['bbg_fund'] = dataset['fund']
    dataset['bbg_order_type'] = dataset['order_type']
    dataset['bbg_limit'] = dataset['limit']
    return dataset

def trade_entry_add_autonumber(dataset:pd.DataFrame, d:dt.datetime):
    fileLocation = f"Z:\\IPS\\trades\\{d.strftime('%Y%m%d')}_trades.txt"
    df_existing = pd.read_csv(fileLocation, sep='\t')
    if df_existing.empty:
        last_autonumber = 0
    else:
        last_autonumber = df_existing['transaction_id'].astype(int).max()
    dataset['transaction_id'] = range(last_autonumber + 1, last_autonumber + len(dataset) + 1) # add autonumber
    dataset['transaction_id'] = dataset['transaction_id'].astype(str)
    return dataset


def logging_start(script_path: str, script_name: str = None):
    log_file = LOG_SAVE_LOC + TODAY_STR + ".log"
    log_format = "%(asctime)s | %(message)s"
    logging.basicConfig(filename=log_file, level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger("my_logger")
    message = script_path.upper() + " | Started"
    if script_name is not None:
        message += " | " + script_name
    logger.info(message)

    connectDb = db_connection()

    inDf = pd.DataFrame(
        [[script_path, script_name, dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), "STARTED", None, None]],
        columns=["path", "name", "occurenceTime", "occurenceType", "runStatus", "closestPairId"])
    connectDb.insert_data(inDf, "dailyLog")

# NOTE: There is the potential with concurrent runs of the same software
# for the "closestPairId" to be recorded improperly
def logging_end(script_path: str, script_name: str = None):
    logger = logging.getLogger("my_logger")
    message = script_path.upper() + " | Completed"
    if script_name is not None:
        message += " | " + script_name
    logger.info(message)
    logging.shutdown()

    connectDb = db_connection()

    # Insert the completion log
    inDf = pd.DataFrame([[script_path, script_name, dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), "COMPLETED", 0, None]],
                        columns=["path", "name", "occurenceTime", "occurenceType", "runStatus", "closestPairId"])

    connectDb.insert_data(inDf, "dailyLog")

    # Update start/complete log
    startDf = connectDb.query_tbl("SELECT * FROM dailyLog WHERE id = (SELECT max(id) FROM dailyLog WHERE (path = \'" + script_path + "\') AND (occurenceType = 'STARTED'))")
    startId = startDf.loc[0, "id"]

    endDf = connectDb.query_tbl("SELECT * FROM dailyLog WHERE id = (SELECT max(id) FROM dailyLog WHERE (path = \'" + script_path + "\') AND (occurenceType = 'COMPLETED'))")
    endId = endDf.loc[0, "id"]

    # Edit values
    startDf.loc[0, "runStatus"] = 0
    startDf.loc[0, "closestPairId"] = endId
    startDf.loc[0, "occurenceTime"] = startDf.loc[0, "occurenceTime"].strftime("%Y-%m-%d %H:%M:%S.%f")

    endDf.loc[0, "closestPairId"] = startId
    endDf.loc[0, "occurenceTime"] = endDf.loc[0, "occurenceTime"].strftime("%Y-%m-%d %H:%M:%S.%f")

    connectDb.update_row(startDf, "dailyLog", "id")
    connectDb.update_row(endDf, "dailyLog", "id")

def check_task_status(script_loc:list, d:dt.datetime=dt.datetime.now(), status_filter:str=None):
    script_loc_upper = list(map(str.upper, script_loc))
    file_path = f"Z:\\IPS\\python\\logs\\{d.strftime('%Y%m%d')}.log"
    output_lis = []
    with open(file_path, "r") as file:
        line_list = file.readlines()
        for line in line_list:
            if len(line) == 1:
                continue
            tasklog = TaskLog(line)
            if tasklog.location.upper() in script_loc_upper:
                if (status_filter is None) or (tasklog.status == status_filter):
                    output_lis.append([tasklog.location, tasklog.status, tasklog.timestamp])
    return output_lis

def build_universe(dataset_list:list, keys:list):
    output = pd.DataFrame()

    for df in dataset_list:
        temp_df = df[keys]
        if output.empty:
            output = temp_df
        else:
            output = pd.concat([output, temp_df])
    return output.drop_duplicates(keys, keep='first').sort_values(by=keys).reset_index(drop=True)

def groupby_dict(dataset:pd.DataFrame, _by:list, _val:str, group_type=sum):
    if dataset.empty:
        return {}
    else:
        _group = dataset.groupby(by=_by)[[_val]].apply(group_type).reset_index()
        for col in _by:
            if str('id') in _group.columns.tolist():
                _group['id'] += str('_') + _group[col]
            else:
                _group['id'] = _group[col]
        return dict(zip(_group['id'], _group[_val]))

def left_join_dataframe(output: pd.DataFrame, df_join: pd.DataFrame, key_lookup:list, column_map:dict):

    for col in key_lookup:
        if str('id') in output.columns.tolist():
            output['id'] += str('_') + output[col]
        else:
            output['id'] = output[col]

        if str('id') in df_join.columns.tolist():
            df_join['id'] += str('_') + df_join[col]
        else:
            df_join['id'] = df_join[col]

    for key, val in column_map.items():
        if val is None:
            output[key] = None
        else:
            output[key] = output['id'].map(df_join.set_index('id')[val])

    return_col = [j for j in output.columns if not j in ['id']]
    return output[return_col]

def create_tkt_upload(dataset: pd.DataFrame, col_dict:dict={}):
    """
    This function coverts a dataframe to the tkt csv uplaod format.

    :param dataset: DataFrame of input data
    :param col_dict: maps the datasets columns to the tkt field which is represented in the tkt_col

    Last Updated Apr 23 2024
    """
    d = dt.datetime.now()
    tkt_col = ['Account', 'Security', 'Side', 'Quantity', 'Order Type', 'Limit Level', 'TIF', 'Price', 'Broker', 'Commission Amount 1', 'Commission Amount 2', 'As Of Date', 'Settlement Date']
    save_folder = f"Z:\\tkt_upload\\{d.strftime('%Y%m%d')}\\"
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    if not dataset.empty:
        tkt_upload = pd.DataFrame(columns=tkt_col)

        for j in tkt_col:
            if not col_dict.get(j) is None:
                tkt_upload[j] = dataset[col_dict.get(j)]
            else:
                if j in dataset.columns:
                    tkt_upload[j] = dataset[j]

        tkt_upload['Security'] = tkt_upload['Security'] + np.where(tkt_upload['Security'].str.upper().str.contains(' Equity'), '', ' Equity')
        tkt_upload['As Of Date'] = dt.datetime.now().strftime('%m/%d/%Y')
        account_names = "_".join(tkt_upload['Account'].unique())
        
        if len(tkt_upload[~(tkt_upload['Security'].str.split(' ').apply(lambda x: x[1]).isin(['US', 'CN']))]) > 0:
            tkt_upload_na = tkt_upload[(tkt_upload['Security'].str.split(' ').apply(lambda x: x[1]).isin(['US', 'CN']))]
            fname_na = f"{account_names}_NA_{d.strftime('%Y%m%d_%H%M')}.csv"
            tkt_upload_na.to_csv(save_folder + fname_na, header=True, index=False, sep=',', mode='w')
            
            tkt_upload_intl = tkt_upload[(~tkt_upload['Security'].str.split(' ').apply(lambda x: x[1]).isin(['US', 'CN']))]
            fname_intl = f"{account_names}_INTL_{d.strftime('%Y%m%d_%H%M')}.csv"
            tkt_upload_intl.to_csv(save_folder + fname_intl, header=True, index=False, sep=',', mode='w')
        
        else:
            fname = f"{account_names}_{d.strftime('%Y%m%d_%H%M')}.csv"
            tkt_upload.to_csv(save_folder + fname, header=True, index=False, sep=',', mode='w')


class sftp_connection:
    def __init__(self, hostname, username, password, port=22):
        logging.getLogger("paramiko").setLevel(logging.ERROR)
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.client = None
        self.sftp = None

        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.client.connect(self.hostname, self.port, self.username, self.password)
            print(f'Connection to server successful')

            self.sftp = self.client.open_sftp()

        except Exception as err:
            print(f'Failed to connect or login: {err}')

    #generic download - ive been using download function below this to get the files
    def gen_download(self, remote_path, save_path):
        try:
            self.sftp.chdir(remote_path)
            for file_name in self.sftp.listdir():
                save_name = os.path.join(save_path, file_name)
                self.sftp.get(remote_path, save_name)

        except Exception as err:
            print(f"An error occurred: {err}")

    def download(self, remote_path, naming_conventions):
        try:
            self.sftp.chdir(remote_path)

            for file_name in self.sftp.listdir():
                remote_file_path = os.path.join(remote_path, file_name)
                local_folder = None

                for pattern, folder in naming_conventions.items():
                    if pattern in file_name:
                        local_folder = folder
                        break

                if local_folder is None:
                    print(f"Irrelevant file: {file_name} - skip")
                    continue

                local_file_path = os.path.join(local_folder, file_name)
                if not os.path.exists(local_file_path):
                    self.sftp.get(remote_file_path, local_file_path)


        except Exception as err:
            print(f"An error occurred: {err}")

    def close_connection(self):
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()

#imagine deletion here

class Mellon_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname='hrznblb01.blob.core.windows.net', username='hrznblb01.fundops', password='P5dwNng9/bNPcqZbIRCpg2ghxHjVGdzW', port=22)

class MSCI_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="sftp.msci.com", username="xeqankzx", password="ZOpaUdOr0rZEhMI+8WsU", port=22)

class Solactive_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="clients.solactive.com", username="Kh1aYOv6Rt5D", password="aaY7w5V2BIIL", port=10022)
        
class Mirae_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="hrznblb01.blob.core.windows.net", username="hrznblb01.mirae", password="KxeOH15g5+JQVtvUOPiS5WwCCchI7HvI", port=22)
        
class TMX_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="sftp.tmxwebstore.com", username="AAlbrecht@globalx.ca", password="Bxcly387", port=22)
        
class SandP_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="edx.standardandpoors.com", username="Glob7175", password="G4zvTEI,k", port=22)
        
class VettaFi_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="indexdata.vettafi.com", username="pm@globalx.ca", password="GXVettaFi25!", port=22)
    
class ICE_FTP(sftp_connection):
    def __init__(self):
        super().__init__(hostname="sftp.pna.icedataservices.com", username="GLOBALXFTPH2", password="oWuv7958", port=22)

if __name__ == "__main__":
    print("Hello World")