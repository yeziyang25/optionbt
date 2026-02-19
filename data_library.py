import pandas as pd
import numpy as np
import datetime as dt
import itertools
import warnings

import os
cur_dir = os.path.dirname(__file__)
import sys
sys.path.append('Z:\\ApolloGX')
if "\\im_dev\\" in cur_dir:
    import im_dev.std_lib.common as common
    import im_dev.std_lib.bloomberg_session as bbg_session
    import im_dev.std_lib.bloomberg_emsx_sub as bbg_emsx_sub
else:
    import im_prod.std_lib.common as common
    import im_prod.std_lib.bloomberg_session as bbg_session
    import im_prod.std_lib.bloomberg_emsx_sub as bbg_emsx_sub

def portfolio_return_ffill_prices(df_input:pd.DataFrame, date_col:str, id_col:str) -> pd.DataFrame:
    """
    function takes the union of securities and dates and forward/back fills any missing data

    :param df_input: dataframe of prices
    :param date_col: name of the column representing the date
    :param id_col: name of the column representing the id
    :return: dataframe with missing prices
    """

    dates_list = sorted(df_input[date_col].unique())
    id_list = sorted(df_input[id_col].unique())
    output = pd.DataFrame(list(itertools.product(id_list, dates_list)), columns=[id_col, date_col])
    output['lookup'] = output[id_col] + str('_') + pd.to_datetime(output[date_col]).dt.strftime('%Y%m%d')
    df_input['lookup'] = df_input[id_col] + str('_') + pd.to_datetime(df_input[date_col]).dt.strftime('%Y%m%d')
    output['value'] = output['lookup'].map(dict(zip(df_input['lookup'], df_input['value']))).ffill()
    output['value'] = output['value'].bfill()
    return output

def calculate_portfolio_return(dates:list, ticker_weights: dict) -> dict:
    """
    Calculates the portfolios total return.

    :param dates: list of dates. The earliest date will be the date returns are anchored to.
    :param ticker_weights: dictionary with the ticker and its initial weights
    :return: dictionary of dates and the portfolio return

    Last Updated April 16 2024
    """
    x = common.db_connection()  # build connection
    query_str = (f"SELECT * FROM market_data "
                 f"WHERE (date >= '{min(dates).strftime('%Y-%m-%d')}') and "
                 f"(date <= '{max(dates).strftime('%Y-%m-%d')}') and "
                 f"(ticker IN {x.list_to_sql_str(list(ticker_weights.keys()), convert_elements=True)});")
    df_prices = x.query_tbl(query_str)

    df_start_prices = df_prices[
        pd.to_datetime(df_prices['date']).dt.strftime("%Y-%m-%d") == min(dates).strftime("%Y-%m-%d")]
    df_start_prices['start_wt'] = df_start_prices['ticker'].map(ticker_weights)

    df_all_prices = portfolio_return_ffill_prices(df_prices, 'date', 'ticker') #fwd fill prices

    output = {}
    for d in dates:
        df_calcs = df_start_prices.copy(deep=True)
        df_day = df_all_prices[pd.to_datetime(df_all_prices['date']).dt.strftime("%Y-%m-%d") == d.strftime("%Y-%m-%d")]
        end_prices = dict(zip(df_day['ticker'], df_day['value']))
        df_calcs['end_p'] = df_calcs['ticker'].map(end_prices)
        df_calcs['eod_wt'] = (df_calcs['end_p'].astype(float) / df_calcs['value'].astype(float)) * df_calcs['start_wt']
        if df_calcs[df_calcs['end_p'].isnull()].shape[0] > 1:
            output.update({d: None})
        else:
            output.update({d: df_calcs["eod_wt"].sum()/df_calcs["start_wt"].sum()})
    return output

class email_order():
    def __init__(self, _data):
        self.order_id = _data['order_id']
        self.ticker = _data['ticker']
        self.pnu = _data['pnu']
        self.order_type = _data['order_type']
        self.td = _data['td']
        self.ap = _data['ap']
        self.order_status = _data['order_status']
        self.received_time = _data['received_time']

    def build_data_elm(self):
        data_elm = {'ticker': self.ticker, 'pnu': self.pnu, 'order_type': self.order_type, 'td': self.td, 'ap': self.ap,
                    'pending': None, 'approved': None, 'cancelled': None, 'last_update': self.received_time}
        return data_elm

def intraday_trades_old(d:dt.datetime):
    ## old funtion, will deccommisison soon
    raw_trades = pd.read_csv(f"Z:\\IPS\\trades\\{d.strftime('%Y%m%d')}_trades.txt", delimiter='\t')
    raw_trades = raw_trades[raw_trades['status'] != 'cancelled']
    raw_trades['net_quantity'] = np.where(raw_trades['trade'].isin(['Sell', 'Sell to Open']), -1*raw_trades['quantity'].astype(float), raw_trades['quantity'].astype(float))

    if "\\im_dev\\" in cur_dir:
        _path = f"Z:\\ApolloGX\\im_dev"
        _path = f"Z:\\ApolloGX\\im_prod"

    override_trades = pd.read_csv(f"{_path}\\tactigon\\config\\adhoc_trades.csv", delimiter=',')
    override_trades = override_trades[pd.to_datetime(override_trades['date']).dt.strftime('%Y-%m-%d')==d.strftime('%Y-%m-%d')]
    override_trades['ticker'] = override_trades['ticker'].str.replace(' Equity', '')
    df_raw_trades = raw_trades[['fund', 'ticker', 'net_quantity', 'est_trade_value']]
    df_override_trades = override_trades[['fund', 'ticker', 'net_quantity', 'est_trade_value']]

    df_trades = pd.concat([df_raw_trades, df_override_trades])
    grouped_trades = df_trades.groupby(by=['fund', 'ticker'], group_keys=True)[['net_quantity', 'est_trade_value']].apply(sum).reset_index()
    grouped_trades['fund_ticker'] = grouped_trades['fund'] + str('_') + grouped_trades['ticker']
    return grouped_trades

class intraday_trades():
    def __init__(self, d:dt.datetime=dt.datetime.now()):
        self.d = d
        self.db_conn = common.db_connection("ips_sandbox")
        self.output_col = ["fund", "security", "ticker", "currency", "net_quantity", "est_trade_value_cad"]

    def build_output_dataframe(self, input_dataset:pd.DataFrame) -> pd.DataFrame:
        output = pd.DataFrame(columns=self.output_col)
        for _col in self.output_col:
            if _col in input_dataset.columns.tolist():
                output[_col] = input_dataset[_col]
        return output

    def override_trades(self):
        if "\\im_dev\\" in cur_dir:
            _path = f"Z:\\ApolloGX\\im_dev"
        else:
            _path = f"Z:\\ApolloGX\\im_prod"

        override_trades = pd.read_csv(f"{_path}\\tactigon\\config\\adhoc_trades.csv", delimiter=',')
        override_trades = override_trades[pd.to_datetime(override_trades['date']).dt.strftime('%Y-%m-%d') == self.d.strftime('%Y-%m-%d')]

        if override_trades.empty:
            return pd.DataFrame(columns=self.output_col)
        else:
            override_trades["ticker"] = override_trades["ticker"].str.replace(" Equity", "")
            return self.build_output_dataframe(override_trades)


    def intraday_trades_tactigon(self) -> pd.DataFrame:
        """

        :return:
        """

        _map_side = {"Buy": 1, "Sell": -1, "Buy to Close": 1, "Sell to Open": -1}

        # North American trades are loaded on the same day
        _strSQLNA = (f"SELECT fund, security, ticker, currency, quantity, trade, est_trade_value_cad FROM tactigon_trades WHERE "
                     f"status='sent' and "
                     f"creation_time >= '{self.d.strftime('%Y-%m-%d')} 00:00:00.0' and "
                     f"creation_time < '{common.workday(self.d, 1).strftime('%Y-%m-%d')} 00:00:00.0';")
        raw_na_trades = self.db_conn.query_tbl(_strSQLNA)

        # # International trades are loaded one day before
        # _strSQLINTL = "SELECT fund, ticker, quantity, side, est_trade_val_cad, currency FROM tactigon_trades WHERE status='sent' and not currency IN ('CAD', 'USD');"
        # raw_intl_trades = self.db_conn.query_tbl(_strSQL)

        # raw_trades = pd.concat([raw_na_trades, raw_intl_trades])
        raw_trades = raw_na_trades.copy(deep=True)
        raw_trades["net_quantity"] = raw_trades["quantity"]*raw_trades["trade"].map(_map_side)

        df_trades = pd.concat([self.build_output_dataframe(raw_trades), self.override_trades()])
        grouped_trades = df_trades.groupby(by=["fund", "security", "ticker", "currency"], group_keys=True)[["net_quantity", "est_trade_value_cad"]].apply(sum).reset_index()
        grouped_trades["fund_ticker"] = grouped_trades["fund"] + str("_") + grouped_trades["ticker"]
        return grouped_trades


    def load_emsx_trades_db(self, dataset:pd.DataFrame):
        d = dt.datetime.now()
        dataset["order_date"] = pd.to_datetime(dataset["order_date"], format="%Y%m%d")
        dataset["load_time"] = d

        format_map = {"etf": str, "order_date": dt.datetime, "full_ticker": str, "ticker": str, "currency":str, "side":str,
                      "quantity_start":float, "quantity_filled":float, "quantity_not_filled":float,
                      "net_quantity":float, "quantity_working":float, "emsx_avg_price": float,
                      "mid_price": float, "fx_rate": float, "est_trade_value_cad": float, "asset_class": str,
                      "status": str, "fund": str, "load_time": dt.datetime}

        df_load = pd.DataFrame(columns=format_map.keys())
        for col, _format in format_map.items():
            if _format != dt.datetime:
                df_load[col] = dataset[col].astype(_format)
            else:
                df_load[col] = dataset[col]
        if not df_load.empty:
            # It should wipe out the table before each load. By design, the insert_data query requires a WHERE statement to determine which records to update, that's why we hard code Jan 1, 2024.
            self.db_conn.insert_data(df_load, "emsx_trades_allocation_temp", sql_check_existing=f"SELECT * FROM emsx_trades_allocation_temp WHERE load_time > '2024-01-01 00:00:00.0';")


    def parse_emsx_trades(self) -> pd.DataFrame:
        """
        Pulls intraday trades that are in EMSX. Assumption is that these are the trades for today
        :return: pd.DataFrame of net trades
        """

        df_emsx_trades = bbg_emsx_sub.emsx_orders()

        if df_emsx_trades.empty:
            # No intraday trades from EMSX
            return pd.DataFrame()

        else:
            df_emsx_trades["fund"] = df_emsx_trades["EMSX_PORT_NAME"].str.split(".", expand=True)[0]

            output_col_map = {"etf": "EMSX_PORT_NAME", "order_date": "EMSX_ORDER_AS_OF_DATE", "full_ticker": "EMSX_TICKER",
                              "ticker": None, "currency": None, "side": "EMSX_SIDE", "quantity_start": "EMSX_START_AMOUNT", "quantity_filled": "EMSX_FILLED",
                              "quantity_not_filled": None, "net_quantity": None, "quantity_working": "EMSX_WORKING", "emsx_avg_price": "EMSX_AVG_PRICE",
                              "mid_price": None, "est_trade_value_cad": None, "asset_class": "EMSX_ASSET_CLASS", "status": "EMSX_STATUS", "fund": "fund", "load_time": None}

            df_output = pd.DataFrame(columns=output_col_map.keys())
            for output_col, col_map in output_col_map.items():
                if col_map is None:
                    df_output[output_col] = None
                elif col_map in df_emsx_trades.columns.tolist():
                    df_output[output_col] = df_emsx_trades[col_map]
                else:
                    df_output[output_col] = "Not Found"
            df_output["ticker"] = df_output["full_ticker"].str.replace(" Equity", "").str.replace(" Index", "")

            # Bloomberg Pulls. Add mid-price and currency
            bbg_pull = bbg_session.BDP_Session()
            bbg_data = bbg_pull.bdp_request(ticker=np.unique(df_output["full_ticker"].tolist()), flds=["MID", "CRNCY"])
            data_crncy_dict = bbg_pull.unpact_dictionary(bbg_data, fld="CRNCY")
            data_mid_dict = bbg_pull.unpact_dictionary(bbg_data, fld="MID")
            df_output["currency"] = df_output["full_ticker"].map(data_crncy_dict)
            df_output["mid_price"] = df_output["full_ticker"].map(data_mid_dict)

            # fx conversion
            fx_dict = {"CAD": 1}
            bbg_pull = bbg_session.BDP_Session()
            curncy_list = np.unique(df_output[df_output["currency"]!="CAD"]["currency"].str.upper().tolist()) + "CAD CURNCY"
            bbg_data = bbg_pull.bdp_request(ticker=curncy_list, flds=["LAST PRICE"])
            for key, val in bbg_data.items():
                fx_dict.update({key.replace("CAD CURNCY", ""): val.get("LAST PRICE")})

            #map currencies. pull from database
            conn = common.db_connection()
            df_output["currency"] = df_output["currency"].str.upper()
            df_currency_factor_adjustment = conn.query_tbl("SELECT currency, price_adjustment, fx_adjustment FROM currency_factor_adjustment;")
            fx_adjust = dict(zip(df_currency_factor_adjustment["currency"], df_currency_factor_adjustment["fx_adjustment"]))
            price_adjust = dict(zip(df_currency_factor_adjustment["currency"], df_currency_factor_adjustment["price_adjustment"]))
            df_output["mid_price"] = df_output["mid_price"]*df_output["currency"].map(price_adjust).fillna(1)
            df_output["emsx_avg_price"] = df_output["emsx_avg_price"] * df_output["currency"].map(price_adjust).fillna(1)
            df_output["fx_rate"] = df_output["currency"].map(fx_dict)*df_output["currency"].map(fx_adjust).fillna(1)

            #recalc estim trade value by approximating working orders
            df_output["quantity_not_filled"] = df_output["quantity_start"] - df_output["quantity_filled"]
            df_output["quantity_not_filled"] = np.where(df_output["side"] == "BUY", 1, -1) * abs(df_output["quantity_not_filled"])
            df_output["quantity_filled"] = np.where(df_output["side"] == "BUY", 1, -1) * abs(df_output["quantity_filled"])
            df_output["net_quantity"] = df_output["quantity_filled"] + df_output["quantity_not_filled"]
            df_output["est_trade_value_cad"] = -((df_output["emsx_avg_price"]*df_output["quantity_filled"] + np.where(abs(df_output["quantity_not_filled"])>0, df_output["mid_price"], 0)*df_output["quantity_not_filled"])
                                              *np.where(df_output["asset_class"]=="Option", 100, 1))*df_output["fx_rate"]
        return df_output

    def intraday_trades_emsx(self):
        # find last load
        strSQL = "SELECT MAX(load_time) FROM emsx_trades_allocation_temp;"
        last_load = self.db_conn.query_tbl(strSQL).iloc[0, 0]

        if (last_load is None) or ((dt.datetime.now()-last_load).total_seconds() / 60 > 5):
            # if the data is more than 5 min stale then refresh
            emsx_parsed_trades = self.parse_emsx_trades()
            if not emsx_parsed_trades.empty:
                self.load_emsx_trades_db(emsx_parsed_trades)

        df_detailed = self.db_conn.query_tbl("SELECT * FROM emsx_trades_allocation_temp WHERE not status IN ('CANCEL', 'SUSPENDED');")
        df_emsx = self.build_output_dataframe(df_detailed)

        df_trades = pd.concat([df_emsx, self.override_trades()])
        grouped_trades = df_trades.groupby(by=["fund", "ticker"], group_keys=True)[["net_quantity", "est_trade_value_cad"]].apply(sum).reset_index()
        grouped_trades["fund_ticker"] = grouped_trades["fund"] + str('_') + grouped_trades["ticker"]
        return grouped_trades


class etf_master_cls():
    def __init__(self,_etf_list:list, inav_date:dt.datetime):#, _drill_down:bool=True, capture_live_underlying_etf_trades:bool=False):
        self.conn = common.db_connection("globalx")
        self.conn_ips = common.db_connection()
        self._etf_list = _etf_list

        self.inav_date = inav_date

        # Initiates the baseline query
        self.default_query = """
            SELECT 
                ib.Date,
                ib.[Primary Basket ID],
                map.[ETF Trading Ticker] AS fund_ticker,
                ib.CUSIP,
                ib.ISIN,
                ib.SEDOL,
                ib.[Alternative Ticker],
                sm.ticker,
                ib.[Market Price Local],
                ib.[Local Price Currency],
                ib.[FX Rate],
                ib.[Cash In Lieu Indicator],
                ib.[Security Description],
                ib.[Fund Accounting Asset Group Code],
                ib.[Basket Quantity],
                ib.[Benchmark Quantity],
                ib.[Fund Quantity],
                ib.[Payable Cost Base],
                ib.[Payable Currency Code],
                ib.[Payable Market Value],
                ib.[Payable Units],
                ib.[Receivable Cost Base],
                ib.[Receivable Currency Code],
                ib.[Receivable Market Value],
                ib.[Receivable Units],
                ib.[Contract Rate For Forwards And Futures],
                ib.[Forward Rate],
                ib.[Market Value Or Unrealized Base],
                ib.[Market Value Or Unrealized From Prior Day Base],
                ib.[Market Value Or Unrealized From Prior Day Local],
                ib.[Market Value Or Unrealized Local],
                ib.[Local Unrealized Gain Loss]
            FROM inav_baskets_w_gxid AS ib 
            LEFT JOIN security_master AS sm ON sm.gx_id = ib.gx_id 
            LEFT JOIN inav_primary_basket_id_map AS map ON map.[Primary Basket ID] = ib.[Primary Basket ID]
            WHERE ib.Date = """ + f"""'{inav_date.strftime('%Y-%m-%d')}' 
            AND map.[ETF Trading Ticker] IN ({", ".join(f"'{ticker}'" for ticker in self._etf_list)}) AND (ib.isin NOT LIKE '%DUM' or ib.isin is NULL)"""

        self.exclude_curr_str = " AND not ib.[Fund Accounting Asset Group Code] IN ('CU(Currency Security)', 'CC(Currency Contract)')"


        # Pull fund data which will be utilized for the drill down
        fund_data_query = """
            SELECT *, ROUND([Projected Net Assets Per Creation Unit]/[Projected NAV],0) AS pnu from inavbasketsheader
            WHERE [Basket Trade Date] = '""" + inav_date.strftime('%Y-%m-%d') + "'"
        self.funds_data = self.conn.query_tbl(fund_data_query)
        self.id_ticker_map = dict(zip(self.funds_data['Primary Basket ID'], self.funds_data['ETF Trading Ticker']))
        self.shares_dict = dict(zip(self.funds_data['ETF Trading Ticker'], self.funds_data['ETF Shares Outstanding']))
        self.pnu_dict = dict(zip(self.funds_data['ETF Trading Ticker'], self.funds_data['pnu']))
        self.nav_dict = dict(zip(self.funds_data['ETF Trading Ticker'], self.funds_data['Projected NAV']))

        self.output_col_map = {"fund": "fund_ticker", "date": "Date", "ticker": "ticker", "isin": "ISIN", "name": "Security Description", "price": "Market Price Local",
                               "currency": "Local Price Currency", "fx": "FX Rate", "security_type": "Fund Accounting Asset Group Code",
                               "underlying_ticker": "", "cil": "Cash In Lieu Indicator", "shares": "Fund Quantity", "pnu": "Basket Quantity"}

    def collect_forwards_usd(self,usdcad:float=None,mtm_summary_bool:bool=True):

        curr_contract_filter = """AND ib.[Fund Accounting Asset Group Code] = 'CC(Currency Contract)'"""

        df = self.conn_ips.query_tbl(self.default_query +" "+curr_contract_filter)

        if usdcad is None:
            bdp = bbg_session.BDP_Session()
            dict = bdp.bdp_request(['USDCAD CURNCY'],['PX_LAST'])
            usdcad = dict.get('USDCAD CURNCY')['PX_LAST']

        df = df[(df['Payable Currency Code'].isin(['CAD', 'USD'])) & (df['Receivable Currency Code'].isin(['CAD', 'USD']))]
        df['USD_hedge'] = df.apply(lambda x: x['Payable Units'] if x['Payable Currency Code']=='USD' else x['Receivable Units'], axis=1)
        df['MTM C$ PnL'] = df.apply(lambda x: x['Receivable Units']-(x['Payable Units']*usdcad*-1) if x['Receivable Currency Code']=='CAD' else (x['Receivable Units']*usdcad)+x['Payable Units'],axis=1)
        mtm_summary = df[['fund_ticker','MTM C$ PnL']].groupby('fund_ticker').sum().reset_index()

        if mtm_summary_bool:
            return mtm_summary
        else:
            return df

    def collect_forwards(self,mtm_summary_bool:bool=True):
        curr_contract_filter = """AND ib.[Fund Accounting Asset Group Code] = 'CC(Currency Contract)'"""
        df = self.conn_ips.query_tbl(self.default_query + " "+ curr_contract_filter)
        if df.empty:
            columns = ['fund_ticker', 'MTM C$ PnL', 'hedged_currency', 'bbg_currency_code', 'live_rate',
                       'current_hedge']
            empty_df = pd.DataFrame(columns=columns)
            if mtm_summary_bool:
                return empty_df[['fund_ticker', 'MTM C$ PnL', 'hedged_currency']].drop_duplicates().reset_index(drop=True)
            else:
                return empty_df

        df['hedged_currency'] = df.apply(lambda x: x['Payable Currency Code'] if x['Receivable Currency Code'] =='CAD' else x['Receivable Currency Code'],axis=1)
        df['bbg_currency_code'] = df['hedged_currency'] + 'CAD CURNCY'

        bbg_curr_ticker_list = list(df['bbg_currency_code'].unique())
        bdp = bbg_session.BDP_Session()
        dict = bdp.bdp_request(bbg_curr_ticker_list, ['PX_LAST'])

        df['live_rate'] = df['bbg_currency_code'].map(bdp.unpact_dictionary(dict,'PX_LAST'))
        df['current_hedge'] = df.apply(lambda x: x['Payable Units'] if x['Receivable Currency Code'] == 'CAD' else x['Receivable Units'], axis=1)
        df['MTM C$ PnL'] = df.apply(lambda x: x['Receivable Units'] - (x['Payable Units'] * x['live_rate'] * -1) if x['Receivable Currency Code'] == 'CAD' else (x['Receivable Units'] * (x['live_rate'])) +x['Payable Units'],axis=1)
        mtm_summary = df.groupby('fund_ticker').agg({'MTM C$ PnL': 'sum','hedged_currency': lambda x: ', '.join(set(x))}).reset_index()

        if mtm_summary_bool:
            return mtm_summary
        else:
            return df

    def collect_equities(self):
        sql_str = f"""EXECUTE [GetEquities] @InavDate = '{self.inav_date.strftime("%Y-%m-%d")}', @FundList = '{",".join(self._etf_list)}';"""
        return self.conn_ips.query_tbl(sql_str)

    def collect_options(self):
        options_filter = """AND ib.[Fund Accounting Asset Group Code] = 'O(Option)'"""
        df_db_raw = self.conn_ips.query_tbl(self.default_query + " " + options_filter)
        df = self.output_rename_col(df_db_raw)

        opt_cls = common.extract_option_ticker(df, "ticker")
        df["expiry"] = pd.to_datetime(df["ticker"].map(opt_cls.expiry), format="%Y-%m-%d")
        df["strike"] = df["ticker"].map(opt_cls.strike)
        df["underlying_ticker"] = df["ticker"].map(opt_cls.underlying_ticker)
        df["option_type"] = df["ticker"].map(opt_cls.option_type)
        return df

    def collect_full_holdings_db(self, _drill_down: bool = True, capture_live_underlying_etf_trades: bool = False):
        # Query all holdings for the initial etf list passed into this class self._etf_list
        inav_basket = self.conn_ips.query_tbl(self.default_query + self.exclude_curr_str)

        if _drill_down:
            # drill down line by line
            _data = []
            for etf_ticker in self._etf_list:
                print(etf_ticker)
                if not inav_basket[inav_basket['fund_ticker'] == etf_ticker].empty:
                    x = etf_db(inav_basket, self.inav_date, etf_ticker, self.id_ticker_map, 1.0, 1.0, self.shares_dict,
                                 self.pnu_dict, _drill_down,capture_live_underlying_etf_trades=capture_live_underlying_etf_trades)
                    agg = x.basket.groupby(
                        ["fund", "date", "ticker", "isin", "name", "price", "currency", "fx", "security_type", "underlying_ticker","cil"], dropna=False)[["shares", "pnu"]].sum().reset_index().values.tolist()
                    _data += agg

            if _data == []:
                return pd.DataFrame(columns=self.output_col_map.keys())
            else:
                return pd.DataFrame(data=_data, columns=self.output_col_map.keys()).sort_values(["fund", "security_type", "ticker"])
        else:
            # Transform columns and add options fields
            df_output = self.output_rename_col(input_dataset=inav_basket)

            df_options = df_output[df_output["security_type"]=="O(Option)"]
            if not df_options.empty:
                opt_cls = common.extract_option_ticker(df_options, "ticker")
                df_options["underlying_ticker"] = df_options["ticker"].map(opt_cls.underlying_ticker)
                remaining_data = df_output[df_output["security_type"]!="O(Option)"]
                return pd.concat([remaining_data, df_options]).sort_values(["fund", "security_type", "ticker"])
            else:
                return df_output.sort_values(["fund", "security_type", "ticker"])

    def output_rename_col(self, input_dataset):
        output = pd.DataFrame()
        for k, v in self.output_col_map.items():
            if v in input_dataset.columns.tolist():
                output[k] = input_dataset[v]
            else:
                output[k] = None
        return output

class etf_db(etf_master_cls):
    def __init__(self, inav_basket, _date, _etf, id_mapping_dict,ownership_percentage, pnu_percentage, _shares_dict, _pnu_dict, _drill_down=True, _top_level='', capture_live_underlying_etf_trades:bool=False):
        # this class pulls the positions of the ETF.
        # If drill down is set to True then the function will perform a look through for any ETFs held
        # top level represents the initial fund that we are trying to gather the holdings for. This is important if we are looking through funds
        # if capture_live_trades to True then it will pull the intraday trades
        super().__init__([_etf],_date)

        if _etf in np.unique(inav_basket["fund_ticker"]).tolist():
            self.inav_basket = inav_basket[inav_basket['fund_ticker'] == _etf]
        else:
            self.inav_basket = self.conn_ips.query_tbl(self.default_query + self.exclude_curr_str + """ AND ib.[Fund Accounting Asset Group Code] != 'CC(Currency Contract)'""")
            self.inav_basket = self.inav_basket[self.inav_basket['fund_ticker']==(_etf.replace('/','.'))]

        self.inav_basket['Alternative Ticker'] = self.inav_basket['Alternative Ticker'].fillna(self.inav_basket['ticker'])
        if (capture_live_underlying_etf_trades) and (_top_level!=''):
            _trades = self.capture_intraday_trades(_date, _etf)
            new_tickers = list(set(_trades.keys()) - set(self.inav_basket['ticker']))
            new_currency = [{'CN':'CAD', 'US':'USD'}[x.split(' ')[-1]] for x in new_tickers]
            new_rows = pd.DataFrame({'ticker': new_tickers, 'Local Price Currency' : new_currency})
            self.inav_basket = pd.concat([self.inav_basket, new_rows], ignore_index=True)
            self.inav_basket['Fund Quantity'].fillna(0, inplace = True)
            

            self.inav_basket['trades'] = self.inav_basket['ticker'].map(_trades).fillna(0)
            self.inav_basket['Fund Quantity'] = self.inav_basket['Fund Quantity'] + self.inav_basket['trades']

        self.data = []
        output_col = ['fund', 'date', 'ticker', 'isin', 'name', 'price', 'currency', 'fx', 'shares', 'pnu', 'security_type', 'underlying_ticker', 'cil']
        if len(self.inav_basket) == 0:
            self.basket = pd.DataFrame(columns=output_col)
        else:
            self.shares_dict = _shares_dict
            self.pnu_dict = _pnu_dict
            self.ownership_percentage = ownership_percentage
            self.pnu_percentage = pnu_percentage
            for row in self.inav_basket.iterrows():
                if row[1]['Fund Accounting Asset Group Code'] in ['TI(Treasury Bill)']:
                    _ticker = row[1]['CUSIP']
                elif row[1]['Fund Accounting Asset Group Code'] in ['O(Option)']:
                    _ticker = row[1]['ticker']
                elif row[1]['ticker'] == None:
                    _ticker = row[1]['Security Description']
                else:
                    _ticker = row[1]['ticker'].split(' ')[0].replace('/', '.')

                if (_ticker in list(self.shares_dict.keys())) and ('CN' == row[1]['Alternative Ticker'].split(' ')[1]) and (not _ticker in ['HXT']) and (_drill_down):
                    temp_security = etf_db(inav_basket,_date, _ticker, id_mapping_dict,float(row[1]['Fund Quantity'])/self.shares_dict.get(_ticker), float(row[1]['Basket Quantity'])/self.pnu_dict.get(_ticker), self.shares_dict, self.pnu_dict, _drill_down, _etf, capture_live_underlying_etf_trades=capture_live_underlying_etf_trades)
                else:
                    temp_security = single_security_db(row[1], self.ownership_percentage, self.pnu_percentage, _top_level)

                self.data += temp_security.add_security()

            self.basket = pd.DataFrame(data=self.data, columns=output_col)
            self.basket['date'].fillna(method = 'ffill', inplace = True)
            self.basket['cil'].fillna('N', inplace = True)


    def add_security(self):
        return self.basket.values.tolist()
    
    def capture_intraday_trades(self, _date:dt.datetime, _fund:str):
        trade_cls = intraday_trades()
        df = trade_cls.intraday_trades_tactigon()
        df_fund_only = df[df["fund"] == _fund]
        return dict(zip(df_fund_only['ticker'], df_fund_only['net_quantity']))



class single_security_db():
    def __init__(self, _data, _ownership_percentage, _pnu_percentage, _top_level):
        self.data = _data
        self.ownership_percentage = _ownership_percentage
        self.pnu_percentage = _pnu_percentage
        self.top_level = _top_level

    def underlying_ticker(self):
        if self.data['Fund Accounting Asset Group Code'] == str('O(Option)'):
            _var = self.data['ticker'].split(' ')
            if _var[0][0] in ['1', '2', '4', '5']:
                return _var[0][1:] + str(' ') + _var[1]
            else:
                return _var[0] + str(' ') + _var[1]

        else:
            return str('')

    def fund_ticker(self):
        return self.top_level if self.top_level != str('') else self.data['fund_ticker']

    def add_security(self):

        if self.data['Fund Accounting Asset Group Code'] in ['TI(Treasury Bill)']:
            _ticker = self.data['CUSIP']
        elif self.data['ticker'] == None:
            _ticker = self.data['Security Description']
        else:
            _ticker = self.data['ticker']

        _data = [[self.fund_ticker(), self.data.Date, _ticker, self.data['ISIN'], self.data['Security Description'],
                  self.data['Market Price Local'],
                  self.data['Local Price Currency'],
                  self.data['FX Rate'],
                  self.data['Fund Quantity'] * self.ownership_percentage,
                  self.data['Basket Quantity'] * self.pnu_percentage,
                  self.data['Fund Accounting Asset Group Code'],
                  self.underlying_ticker(),
                  self.data['Cash In Lieu Indicator']]]
        return _data


def collect_full_holdings(_etf_list:list, inav_date:dt.datetime, _drill_down:bool=True, capture_live_underlying_etf_trades:bool=False):
    if (inav_date-dt.datetime(2024, 6, 3)).days >= 0:
        mstr_cls = etf_master_cls(_etf_list, inav_date)
        df = mstr_cls.collect_full_holdings_db(_drill_down, capture_live_underlying_etf_trades)
    else:
        df = collect_full_holdings_old(_etf_list, inav_date, _drill_down, capture_live_underlying_etf_trades)

    if not df.empty:
        return df[abs(df['shares'])>0].reset_index(drop=True)
    else:
        return df


class etf():
    def __init__(self, _folder, _date, _etf, ownership_percentage, pnu_percentage, _shares_dict, _pnu_dict, _drill_down=True, _top_level='', capture_live_underlying_etf_trades:bool=False):
        # this class pulls the positions of the ETF.
        # If drill down is set to True then the function will perform a look through for any ETFs held
        # top level represents the initial fund that we are trying to gather the holdings for. This is important if we are looking through funds
        # if capture_live_trades to True then it will pull the intraday trades

        self.inav_basket = pd.read_csv(_folder + _etf.replace('/', '.') + str('_') + _date.strftime('%Y%m%d') + str('.txt'), delimiter='\t')
        self.inav_basket = self.inav_basket[~self.inav_basket['Ticker'].isnull()]
        if (capture_live_underlying_etf_trades) and (_top_level!=''):
            _trades = self.capture_intraday_trades(_date, _etf)
            self.inav_basket['trades'] = self.inav_basket['Ticker'].map(_trades).fillna(0)
            self.inav_basket['Full Position (Fund Shares)'] = self.inav_basket['Full Position (Fund Shares)'] + self.inav_basket['trades']
        
        if len(self.inav_basket) == 0:
            self.basket = pd.DataFrame(columns=['fund', 'ticker', 'isin', 'name', 'price', 'currency', 'fx', 'shares', 'pnu', 'security_type', 'underlying_ticker', 'cil'])
        else:
            self.basket = pd.DataFrame(columns=['ticker', 'shares'])
            self.shares_dict = _shares_dict
            self.pnu_dict = _pnu_dict
            self.ownership_percentage = ownership_percentage
            self.pnu_percentage = pnu_percentage
            for row in self.inav_basket.iterrows():
                if row[1]['Security Type (Asset Class)'] in ['TI(Treasury Bill)']:
                    _ticker = row[1]['CUSIP']
                else:
                    _ticker = row[1]['Ticker'].split(' ')[0].replace('/', '.')
                if (_ticker in list(self.shares_dict.keys())) and ('CN' == row[1]['Ticker'].split(' ')[1]) and (not _ticker in ['HXT']) and (_drill_down):
                    temp_security = etf(_folder, _date, _ticker, float(row[1]['Full Position (Fund Shares)'])/self.shares_dict.get(_ticker), float(row[1]['PNU Position [Projected Basket Shares]'])/self.pnu_dict.get(_ticker), self.shares_dict, self.pnu_dict, _drill_down, _etf, capture_live_underlying_etf_trades=capture_live_underlying_etf_trades)
                else:
                    temp_security = single_security(row[1], self.ownership_percentage, self.pnu_percentage, _top_level)
    
                if self.basket.empty:
                    self.basket = temp_security.add_security()
                else:
                    self.basket = pd.concat([self.basket, temp_security.add_security()])

    def add_security(self):
        return self.basket

    def capture_intraday_trades(self, _date:dt.datetime, _fund:str):
        trade_cls = intraday_trades()
        df = trade_cls.intraday_trades_tactigon()
        df_fund_only = df[df["fund"] == _fund]
        return dict(zip(df_fund_only['ticker'], df_fund_only['net_quantity']))

class single_security():
    def __init__(self, _data, _ownership_percentage, _pnu_percentage, _top_level):
        self.data = _data
        self.ownership_percentage = _ownership_percentage
        self.pnu_percentage = _pnu_percentage
        self.top_level = _top_level

    def underlying_ticker(self):
        if self.data['Security Type (Asset Class)'] == str('O(Option)'):
            _var = self.data['Ticker'].split(' ')
            if _var[0][0] in ['1', '2', '4', '5']:
                return _var[0][1:] + str(' ') + _var[1]
            else:
                return _var[0] + str(' ') + _var[1]

        else:
            return str('')

    def fund_ticker(self):
        return self.top_level if self.top_level != str('') else self.data['ETF Ticker (Fund Ticker)']

    def add_security(self):

        if self.data['Security Type (Asset Class)'] in ['TI(Treasury Bill)']:
            _ticker = self.data['CUSIP']
        else:
            _ticker = self.data['Ticker']

        _data = [[self.fund_ticker(), _ticker, self.data['ISIN'], self.data['Holding Name'], self.data['Projected Price (local ccy)'],
                  self.data['Local Price Currency'], self.data['FX  Rate'], self.data['Full Position (Fund Shares)']*self.ownership_percentage,
                  self.data['PNU Position [Projected Basket Shares]'] * self.pnu_percentage,
                  self.data['Security Type (Asset Class)'], self.underlying_ticker(), self.data['Code']]]
        return pd.DataFrame(_data, columns=['fund', 'ticker', 'isin', 'name', 'price', 'currency', 'fx', 'shares', 'pnu', 'security_type', 'underlying_ticker', 'cil'])


# def condense_email_orders_wip(executeDate:dt.datetime, max_date:dt.datetime=None):
#     conn = common.db_connection("globalx")
#     sql_query = """
#     SELECT
#         [Order ID Number],
#         [ETF Trading Ticker],
#         [Total Transaction Units],
#         [Basket Settlement],
#         [Order Trade Date],
#         [Authorized Participant Name],
#         [Order Capture Status],
#         [Last Update Date Time EST],
#         [Order Input Time EST]
#     FROM
#         tradeconfirmations
#     WHERE
#         [Order Trade Date] = '""" + executeDate.strftime('%Y-%m-%d') + "'"""
#     return "wtf"

def condense_email_orders(executeDate:dt.datetime, max_date:dt.datetime=None):
    fileLocation = 'Z:\\IPS\\storage\\orders\\' + executeDate.strftime('%Y%m%d') + '_orders.txt'
    if not os.path.isfile(fileLocation):
        df_orders = pd.DataFrame()
    else:
        df_orders = pd.read_csv(fileLocation, sep='\t')

    if not max_date is None:
        df_orders = df_orders[pd.to_datetime(df_orders['received_time']) <= max_date]

    order_status_map = {'pending': 'pending', 'sponsor approval pending': 'pending', 'approved': 'approved',
                        'cancelled': 'cancelled', 'cancel pending': 'cancelled', 'rejected': 'cancelled'}
    output_dict = {}
    for idx, row in df_orders.iterrows():
        order_line = email_order(row)
        if output_dict.get(order_line.order_id) is None:
            elm = order_line.build_data_elm()
            output_dict[order_line.order_id] = elm
            order_status_class = order_status_map.get(order_line.order_status)
            output_dict[order_line.order_id][order_status_class] = 'x'
        else:
            output_dict[order_line.order_id]['last_update'] = order_line.received_time
            order_status_class = order_status_map.get(order_line.order_status)
            if output_dict.get(order_line.order_id).get(order_status_class) is None:
                output_dict[order_line.order_id][order_status_class] = 'x'
    return pd.DataFrame.from_dict(output_dict, orient='index').rename_axis('order_id').reset_index()

def option_positions(fund_list:list, d:dt.datetime):
    df = collect_full_holdings(fund_list, d, _drill_down=False)
    df = df[df['security_type'] == str("O(Option)")].reset_index(drop=True)
    opt_cls = common.extract_option_ticker(df, 'ticker')
    df['expiry'] = pd.to_datetime(df['ticker'].map(opt_cls.expiry), format='%Y-%m-%d')
    df['strike'] = df['ticker'].map(opt_cls.strike)
    df['underlying_ticker'] = df['ticker'].map(opt_cls.underlying_ticker)
    df['option_type'] = df['ticker'].map(opt_cls.option_type)
    return df

def collect_full_holdings_old(_etf_list:list, inav_date:dt.datetime, _drill_down:bool=True, capture_live_underlying_etf_trades:bool=False):
    # these files are generated from the script: Z:\IPS\python\inav_parser\create_full_holdings.py
    folder = 'Z:\\IPS\\storage\\inav_parsed\\' + inav_date.strftime('%Y%m%d') + str('\\')
    funds_data = pd.read_csv(folder + str('fund_') + inav_date.strftime('%Y%m%d') + str('.txt'), delimiter='\t')
    shares_dict = dict(zip(funds_data['Fund Ticker'], funds_data['FUND SHARES OUT']))
    pnu_dict = dict(zip(funds_data['Fund Ticker'], funds_data['CU']))

    output = pd.DataFrame()
    for etf_ticker in _etf_list:
        if os.path.isfile(folder + etf_ticker + str('_') + inav_date.strftime('%Y%m%d') + str('.txt')):
            x = etf(folder, inav_date, etf_ticker, 1.0, 1.0, shares_dict, pnu_dict, _drill_down, capture_live_underlying_etf_trades=capture_live_underlying_etf_trades)
            agg = x.basket.groupby(['fund', 'ticker', 'isin', 'name', 'price', 'currency', 'fx', 'security_type', 'underlying_ticker', 'cil'], dropna=False)[['shares', 'pnu']].sum().reset_index().sort_values(['security_type', 'ticker'])
            if output.empty:
                output = agg
            else:
                output = pd.concat([output, agg])
    return output[abs(output['shares'])>0].reset_index(drop=True)

def collect_index_data(index_code:str, value_type:str, start_date:dt.datetime, end_date: dt.datetime=dt.datetime.today(), opening_start_date:bool=False) -> pd.DataFrame:
    db_conn = common.db_connection()
    if opening_start_date:
        strSQL1 = f"SELECT date, value as index_value FROM index_value WHERE index_code='{index_code}' and date = '{start_date.strftime('%Y-%m-%d')}' and value_type='opening';"
    else:
        strSQL1 = f"SELECT date, value as index_value FROM index_value WHERE index_code='{index_code}' and date = '{start_date.strftime('%Y-%m-%d')}' and value_type='closing';"

    strSQL2 = f"SELECT date, value as index_value FROM index_value WHERE index_code='{index_code}' and (date > '{start_date.strftime('%Y-%m-%d')}' and date <= '{end_date.strftime('%Y-%m-%d')}') and value_type='{value_type}' ORDER BY date;"
    df1 = db_conn.query_tbl(strSQL1)
    df2 = db_conn.query_tbl(strSQL2)
    return pd.concat([df1, df2])

def collect_index_data_old(index_code:str, value_type:str, start_date:dt.datetime, end_date: dt.datetime=dt.datetime.today(), opening_start_date:bool=False) -> pd.DataFrame:

    # -----------------------------------------
    # calculates the performance of an ETF vs its benchmark
    # index_code: the index name
    # value_type: the value type of the index i.e. CLOSING, OPENING
    # start_date: the start date of this period
    # end_date: the start date of this period
    # opening_start_date: indicates if the start date value should be the OPENING file.
    #------------------------------------------

    source_folder = "Z:\\IPS\\storage\\index\\Solactive\\index_basket_parsed\\"
    start_date = common.workday(start_date, delta=0)
    end_date = common.workday(end_date, delta=0)

    if start_date > end_date:
        start_date, end_date = end_date, start_date
    iter_date = start_date
    ret = {
        'date': [],
        'index_value': []
    }

    while iter_date <= end_date:
        print(iter_date)
        iter_folder = f"{iter_date.strftime('%Y%m%d')}\\"
        index_value_file = f"index_value_{iter_date.strftime('%Y%m%d')}.txt"
        if os.path.isfile(source_folder + iter_folder + index_value_file):
            df = pd.read_csv(source_folder + iter_folder + index_value_file, sep='\t', index_col='index_code')
            if opening_start_date and ((iter_date-start_date).days==0):
                df = df[df['value_type'] == "OPENING"]
            else:
                df = df[df['value_type'] == value_type]
            ret['index_value'] += df.loc[index_code, 'index_value'],
            ret['date'] += iter_date,
        iter_date = common.workday(iter_date, 1)

    return pd.DataFrame(ret).set_index('date').reset_index()


def collect_fund_data_old(start_date, end_date:dt.datetime=dt.datetime.now(), ticker_list:list=None, col_list:list=None):
    _data = pd.DataFrame()
    _inav_folder = 'Z:\\IPS\\storage\\inav_parsed\\'
    d = start_date
    while d.date() <= end_date.date():
        folder = _inav_folder + d.strftime('%Y%m%d') + str('\\')
        filename = str('fund_') + d.strftime('%Y%m%d') + str('.txt')
        if os.path.isfile(folder + filename):
            _df = pd.read_csv(folder + filename, delimiter='\t')
            _df['inav_date'] = d.strftime('%Y-%m-%d')
            if not ticker_list is None:
                _df = _df[_df['Fund Ticker'].isin(ticker_list)]

            if _data.empty:
                _data = _df
            else:
                _data = pd.concat([_data, _df])
        d += dt.timedelta(days=1)

    _data = _data.reset_index(drop=True)
    _data['nav_date'] = common.workday(pd.to_datetime(_data['inav_date']), -1, tsx_holidays()).dt.strftime('%Y-%m-%d')
    if col_list is None:
        return _data
    else:
        return _data[col_list]


def collect_fund_data(start_date,end_date:dt.datetime=dt.datetime.now(),ticker_list:list=None,col_list:list=None):
    conn = common.db_connection("globalx")

    #temporary adjustment because of legacy processes
    col_mapping = {'Basket Trade Date':'inav_date',
                   'Basket Evaluation Date':'nav_date',
                   'ETF Trading Ticker':'Fund Ticker',
                   'Estimated Cash Per Creation Unit':'Estimated Cash Per CU',
                   'ETF Shares Outstanding':'FUND SHARES OUT',
                   'Official NAV':'NAV/SHARE',
                   'pnu':'CU'}

    sql_query = """
        SELECT h.*, 
               ROUND(h.[Projected Net Assets Per Creation Unit] / h.[Projected NAV], 0) AS pnu,
               g.[Fund Benchmark Market Value]
        FROM (
            SELECT [Primary Basket ID], 
                   SUM([Benchmark Market Value Base]) AS [Fund Benchmark Market Value], 
                   [Date]
            FROM inavbaskets 
            WHERE [Date] BETWEEN '""" + start_date.strftime('%Y-%m-%d') + "' AND '" + end_date.strftime('%Y-%m-%d') + """'
                  AND not [Fund Accounting Asset Group Code] IN ('CU(Currency Security)', 'CC(Currency Contract)')
            GROUP BY [Primary Basket ID], [Date]
        ) AS g
        JOIN inavbasketsheader AS h
        ON g.[Primary Basket ID] = h.[Primary Basket ID] 
        AND g.[Date] = h.[Basket Trade Date]
        WHERE h.[Basket Trade Date] BETWEEN '""" + start_date.strftime('%Y-%m-%d') + "' AND '" + end_date.strftime(
        '%Y-%m-%d') + """';
    """
    corp_class_sql_query = """
        SELECT *, ROUND([Projected Net Assets Per Creation Unit] / [Projected NAV], 0) AS pnu 
        FROM [inavbasketsheader] 
        WHERE ([Basket Name] LIKE 'BetaPro%' OR [Basket Name] LIKE '%Corporate Class%' OR [ETF Trading Ticker] IN ('CASH','UCSH.U','HBD'))
        AND [Basket Trade Date] BETWEEN '""" + start_date.strftime('%Y-%m-%d') + "' AND '" + end_date.strftime('%Y-%m-%d') + """'
        AND [ETF Trading Ticker] NOT IN ('HULC', 'HXQ','HXQ.U','HULC.U');
    """
    corp_df = conn.query_tbl(corp_class_sql_query)

    _df = conn.query_tbl(sql_query)
    _df = pd.concat([_df, corp_df])
    _df = _df.reset_index(drop=True)

    _df = _df.rename(columns=col_mapping)
    _df['Estimated Cash Per Fund'] = _df['Total Net Assets'] - _df['Fund Benchmark Market Value']
    _df = _df[['Fund Ticker','NAV/SHARE','FUND SHARES OUT','CU','Projected NAV','Estimated Cash Per Fund','Estimated Cash Per CU','inav_date','nav_date']]
    if not ticker_list is None:
        _df = _df[_df['Fund Ticker'].isin(ticker_list)]

    if col_list is None:
        return _df
    else:
        return _df[col_list]

def collect_underlying_prices_old(_etf_list:list, _dates_list:list):
    underlying_prices = pd.DataFrame()
    _inav_folder = 'Z:\\IPS\\storage\\inav_parsed\\'
    for d in _dates_list:
        inav_d = common.workday(d, 1)
        for e in _etf_list:
            inav_d_reformat = inav_d.strftime('%Y%m%d')
            filename = e + str('_') + inav_d_reformat + str('.txt')
            if os.path.isfile(_inav_folder + inav_d_reformat + str('\\') + filename):
                _df = pd.read_csv(_inav_folder + inav_d_reformat + str('\\') + filename, delimiter='\t')
                _df = _df[~_df['Security Type (Asset Class)'].isin(['O(Option)', 'CU(Currency Security)', 'CC(Currency Contract)'])]
                _df['inav_date'] = inav_d_reformat
                _df['pricing_date'] = d.strftime('%Y%m%d')
                if underlying_prices.empty:
                    underlying_prices = _df
                else:
                    underlying_prices = pd.concat([underlying_prices, _df])

    underlying_prices = underlying_prices.reset_index(drop=True)
    underlying_prices['unique_id'] = underlying_prices['Ticker'] + str('_') + underlying_prices['pricing_date']
    df_consolidated = underlying_prices.groupby(['unique_id'])['Projected Price (local ccy)'].min().reset_index()
    prices_dict = dict(zip(df_consolidated.unique_id, df_consolidated['Projected Price (local ccy)']))
    return prices_dict


def collect_underlying_prices(_etf_list:list, _dates_list:list):
    conn = common.db_connection()
    underlying_prices = pd.DataFrame()
    ib_dates = [(date + dt.timedelta(days=1)).strftime('%Y-%m-%d') for date in _dates_list]
    md_dates = [date.strftime('%Y-%m-%d') for date in _dates_list]
    ib_dates_str = "', '".join(ib_dates)
    md_dates_str = "', '".join(md_dates)
    sql_query = f"""
        SELECT 
            ib.Date as inav_date,
            md.date as pricing_date, 
            ibmap.[ETF Trading Ticker], 
            sm.ticker as Ticker, 
            ib.gx_id, 
            md.field, 
            md.value
        FROM 
            inav_baskets_w_gxid AS ib
        JOIN 
            inav_primary_basket_id_map AS ibmap
            ON ib.[Primary Basket ID] = ibmap.[Primary Basket ID]
        JOIN 
            market_data AS md
            ON md.gx_id = ib.gx_id
        JOIN 
            security_master AS sm
            ON sm.gx_id = ib.gx_id
        WHERE 
            ib.Date IN ('{ib_dates_str}') 
            AND md.date IN ('{md_dates_str}') 
            AND ibmap.[ETF Trading Ticker] IN ('{"','".join(_etf_list)}') 
            AND md.field = 'px_last'
            AND ib.[Fund Accounting Asset Group Code] NOT IN 
                ('O(Option)', 'CU(Currency Security)', 'CC(Currency Contract)');
        """
    df = conn.query_tbl(sql_query)
    df['pricing_date'] = pd.to_datetime(df['pricing_date']).dt.strftime('%Y%m%d')
    df['inav_date'] = pd.to_datetime(df['inav_date']).dt.strftime('%Y%m%d')
    df = df.drop_duplicates().reset_index(drop=True)
    if underlying_prices.empty:
        underlying_prices = df
    else:
        underlying_prices = pd.concat([underlying_prices, df])


    underlying_prices = underlying_prices.reset_index(drop=True)
    underlying_prices['unique_id'] = underlying_prices['Ticker'] + str('_') + underlying_prices['pricing_date']
    df_consolidated = underlying_prices.groupby(['unique_id'])['value'].min().reset_index()
    prices_dict = dict(zip(df_consolidated.unique_id, df_consolidated['value'].astype(float)))
    return prices_dict

def collect_ad_hoc_pricing(ticker_list:list, dates_list:list):
    conn = common.db_connection()
    dates_list_str = [d.strftime('%Y-%m-%d') for d in dates_list]
    str_SQL = (f"""SELECT ticker, [date], CAST([value] AS DECIMAL(12,2)) as px_last, [source] FROM market_data 
                WHERE field = 'px_last' and source IN ('yahoo', 'bloomberg') and ticker IN ('{"','".join(ticker_list)}') and [date] IN ('{"','".join(dates_list_str)}');""")
    data_db = conn.query_tbl(str_SQL)
    data_db["key"] = data_db["ticker"] + str("_") + pd.to_datetime(data_db["date"]).dt.strftime('%Y%m%d')

    df_sorted = data_db.sort_values(by=["key", "source"], ascending=[True, False])
    # Drop duplicates, keeping the first occurrence (highest priority)
    df_result = df_sorted.drop_duplicates(subset="key", keep="first")
    return dict(zip(df_result["key"], df_result["px_last"]))


def collect_ad_hoc_pricing_old(_include_list:list, _dates_list:list):
    _folder = 'Z:\\IPS\\storage\\ad_hoc_pricing\\'
    _dates_list = [d.strftime('%Y-%m-%d') for d in _dates_list]
    pricing_dict = {}
    for sec in _include_list:
        if os.path.isfile(_folder + sec + str('.csv')):
            _df = pd.read_csv(_folder + sec + str('.csv'))
            _df = _df[pd.to_datetime(_df['date']).isin(_dates_list)].reset_index(drop=True)
            _df['unique_id'] = str('*') + sec + str('_') + pd.to_datetime(_df['date']).dt.strftime('%Y%m%d')
            if pricing_dict == {}:
                pricing_dict = dict(zip(_df.unique_id, _df['price']))
            else:
                pricing_dict = {**pricing_dict, **dict(zip(_df.unique_id, _df['price']))}
    return pricing_dict

def collect_fx_pricing(_dates_list:list):
    _folder = 'Z:\\IPS\\storage\\ad_hoc_pricing\\'
    _df = pd.read_csv(_folder + str('USD.csv'))
    _dates_list = [d.strftime('%Y-%m-%d') for d in _dates_list]
    _df = _df[pd.to_datetime(_df['date']).isin(_dates_list)].reset_index(drop=True)
    _df['unique_id'] = str('USD') + str('_') + pd.to_datetime(_df['date']).dt.strftime('%Y%m%d')
    fx_dict = dict(zip(_df.unique_id, _df['price']))

    cad = pd.DataFrame(zip(_dates_list), columns=['date'])
    cad['unique_id'] = str('CAD_') + pd.to_datetime(cad['date']).dt.strftime('%Y%m%d')
    cad['price'] = 1
    fx_dict = {**fx_dict, **dict(zip(cad.unique_id, cad.price))}
    return fx_dict

def collect_trade_fills_old(_end_month:str=None, _end_year:str=None):
    _folder = 'Z:\\IPS\\storage\\trade_fills\\'
    df_all = pd.DataFrame()
    for file in reversed(os.listdir(_folder)):
        if str('options.txt') in file:
            _date = dt.datetime.strptime(file.split('_')[0], '%Y%m%d')
            df_temp = pd.read_csv(_folder + file, delimiter='\t')
            df_temp['td'] = _date.date()
            if df_all.empty:
                df_all = df_temp
            else:
                df_all = pd.concat([df_all, df_temp])
    df_all['month'] = pd.to_datetime(df_all['maturity']).dt.strftime('%m').astype(str)
    df_all['year'] = pd.to_datetime(df_all['maturity']).dt.strftime('%Y').astype(str)
    df_all['fund'] = df_all['fund'].str.split('.', expand=True)[0]

    if (not _end_month is None) and (not _end_year is None):
        df_all = df_all[(df_all['month']==_end_month) & (df_all['year']==_end_year)]

    _db = common.db_connection("globalx")
    df_fx_rates = _db.query_tbl("SELECT * FROM currencyfxrates WHERE [Data Source] = 'CIBC Mellon' and [Local Price Currency] = 'USD';")
    fx_rates_dict = dict(zip(pd.to_datetime(df_fx_rates["As of Date"]).dt.strftime("%Y-%m-%d"), df_fx_rates["FX Rate"]))
    df_all["net_premiums_in_cad"] = df_all["net"]*np.where(df_all["currency"]=="USD", pd.to_datetime(df_all["td"]).dt.strftime("%Y-%m-%d").map(fx_rates_dict), 1)
    return df_all

def collect_trade_fills(_end_month:str=None,_end_year:str=None):
    conn = common.db_connection("globalx")
    conn_ips = common.db_connection()
    if (not _end_month is None) and (not _end_year is None):
        df = conn_ips.query_tbl(f"SELECT * FROM trade_fills WHERE MONTH(maturity) = '{_end_month}' and YEAR(maturity) = '{_end_year}'")
    else:
        df = conn_ips.query_tbl(f"SELECT * FROM trade_fills WHERE security = 'Option'")

    df_fx_rates = conn.query_tbl("SELECT * FROM currencyfxrates WHERE [Data Source] = 'CIBC Mellon' and [Local Price Currency] = 'USD';")
    fx_rates_dict = dict(zip(pd.to_datetime(df_fx_rates["As of Date"]).dt.strftime("%Y-%m-%d"), df_fx_rates["FX Rate"]))
    df["net_premiums_in_cad"] = df["net"]*np.where(df["currency"]=="USD", pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d").map(fx_rates_dict), 1)
    return df


def opening_cash(d:dt.datetime, fund_list:list=None):
    # Source is the Asset and Accrual report
    df = pd.read_csv(f"Z:\\IPS\\storage\\cash_positions\\opening_cash_positions_{d.strftime('%Y%m%d')}.txt", delimiter='\t')
    if fund_list is None:
        return df
    else:
        return df[df['etf'].isin(fund_list)].reset_index(drop=True)

def cash_order_proceeds(d:dt.datetime):
    df_orders = condense_email_orders(d)
    if df_orders.empty:
        return pd.DataFrame()
    else:
        df_orders = df_orders[df_orders['cancelled'].isnull()]
        df_cash_orders = df_orders[df_orders['order_type'] == str('Cash')]
        if df_cash_orders.empty:
            return pd.DataFrame(columns=['Fund Ticker', 'tnav_cu', 'currency'])
        else:
            df_cash_orders['fund'] = df_cash_orders['ticker'].str.split('.', expand=True)[0]
            df_consolidate_cash_orders = df_cash_orders.groupby(by=['fund'], group_keys=True)[['pnu']].sum().reset_index()
            if df_consolidate_cash_orders.empty:
                return pd.DataFrame()
            else:
                orders_dict = dict(zip(df_consolidate_cash_orders['fund'], df_consolidate_cash_orders['pnu']))
                fund_data = collect_fund_data(start_date=d, end_date=d)
                fund_data['tnav_cu'] = fund_data['NAV/SHARE'] * fund_data['CU']*fund_data['Fund Ticker'].map(orders_dict).fillna(0)
                fund_data[['fund', 'class']] = fund_data['Fund Ticker'].str.split('.', expand=True)
                fund_data['currency'] = np.where(fund_data['class'].str.contains('U'), 'USD', 'CAD')
                fund_data = fund_data[abs(fund_data['tnav_cu']) > 0]
                return fund_data[['Fund Ticker', 'tnav_cu', 'currency']].reset_index(drop=True)

def in_kind_transactions(d:dt.datetime, fund:str):
    t_1 = common.workday(d, -1, tsx_holidays()) 
    df_orders = pd.concat([condense_email_orders(d), condense_email_orders(t_1)], ignore_index=True, sort=False)
    if df_orders.empty:
        return {}, 0
    else:
        fund_class = fund.split('.')[0]
        df_orders['fund_ticker'] = df_orders['ticker'].str.split('.', expand=True)[0]
        df_orders = df_orders[df_orders['cancelled'].isnull()]
        df_orders = df_orders[df_orders['order_type']==str('In-Kind')]
        df_orders = df_orders[df_orders['td'] == d.strftime("%m/%d/%Y")]
        df_orders = df_orders[df_orders['fund_ticker']==fund_class]
        df_orders['relevant_ticker'] = fund
        df_consolidate_ik = df_orders.groupby(by=['relevant_ticker'], group_keys=True)[['pnu']].sum().reset_index()

        if df_consolidate_ik.empty:
            return {}, 0
        else:
            orders_dict = dict(zip(df_consolidate_ik['relevant_ticker'], df_consolidate_ik['pnu']))
            fund_data = collect_fund_data(start_date=d, end_date=d)
            fund_data = fund_data[fund_data['Fund Ticker'].isin(list(orders_dict.keys()))].reset_index(drop=True)
            fund_data['ik_cash'] = fund_data['Fund Ticker'].map(orders_dict).fillna(0)*fund_data['Estimated Cash Per CU']
            # fund_data['ik_cash'] = fund_data['Fund Ticker'].map(orders_dict).fillna(0) * fund_data['Estimated Cash Per Fund']
            fund_data_group = fund_data.groupby(by=['Fund Ticker'], group_keys=True)[['ik_cash']].sum().reset_index()

            baskets = collect_full_holdings(list(orders_dict.keys()), d, _drill_down=False)
            baskets = baskets[baskets['cil'].str.upper()=="N"]
            baskets['ik_orders'] = baskets['fund'].map(orders_dict)
            baskets['total_delivery'] = baskets['ik_orders']*baskets['pnu']
            baskets_group = baskets.groupby(by=['ticker'], group_keys=True)[['total_delivery']].sum().reset_index()
            return dict(zip(baskets_group['ticker'], baskets_group['total_delivery'])), fund_data_group.loc[0, 'ik_cash']

def inav_fund_details_old(d:dt.datetime, fund_list:list=None):
    df = pd.read_csv(f"Z:\\IPS\\storage\\inav_parsed\\{d.strftime('%Y%m%d')}\\fund_{d.strftime('%Y%m%d')}.txt", delimiter='\t')
    df['Projected TNAV'] = df['Projected NAV'] * df['FUND SHARES OUT']
    if fund_list is None:
        return df
    else:
        return df[df['Fund Ticker'].isin(fund_list)].reset_index(drop=True)

def inav_fund_details(d:dt.datetime, fund_list:list=None):
    conn = common.db_connection("globalx")
    df = conn.query_tbl("SELECT [ETF Trading Ticker],[Total Net Assets] FROM inavbasketsheader WHERE [Basket Trade Date] = '" + d.strftime('%Y-%m-%d') + "'")
    df = df.rename(columns={'ETF Trading Ticker':'Fund Ticker','Total Net Assets':'Projected TNAV'})
    if fund_list is None:
        return df
    else:
        return df[df['Fund Ticker'].isin(fund_list)].reset_index(drop=True)

def flows_summary_page(_today:dt.datetime, etf_list:list):
    _first_day_year = common.workday(dt.datetime(_today.year-1,12,31), 1)
    _first_day_month = common.workday(dt.datetime(_today.year, _today.month,1) + dt.timedelta(days=-1), 1)
    _first_day_week = dt.datetime.now() - dt.timedelta(days=_today.weekday())
    _data = collect_fund_data(_first_day_year, _today)

    _data['inav_date'] = pd.to_datetime(_data['inav_date'])
    _data.sort_values(['Fund Ticker', 'inav_date'], inplace=True)
    _data['shares_change'] = _data.groupby(['Fund Ticker'])['FUND SHARES OUT'].transform(lambda x: x.diff())
    _data['flows'] = _data['shares_change']*_data['NAV/SHARE']
    _data['aum'] = _data['FUND SHARES OUT']*_data['NAV/SHARE']

    output = pd.DataFrame()
    for etf in etf_list:
        _etf_flow = _data[_data['Fund Ticker']==etf]
        _daily = _etf_flow[_etf_flow['inav_date'].dt.date == _etf_flow['inav_date'].max().date()]['flows'].sum()
        _aum = _etf_flow[_etf_flow['inav_date'].dt.date == _etf_flow['inav_date'].max().date()]['aum'].sum()
        _wtd = _etf_flow[_etf_flow['inav_date'].dt.date >= _first_day_week.date()]['flows'].sum()
        _mtd = _etf_flow[_etf_flow['inav_date'].dt.date >= _first_day_month.date()]['flows'].sum()
        _ytd = _etf_flow[_etf_flow['inav_date'].dt.date >= _first_day_year.date()]['flows'].sum()
        _etf_df = pd.DataFrame([[etf, _aum, _daily, _wtd, _mtd, _ytd]], columns=['etf', 'aum', 'yesterday', 'wtd', 'mtd', 'ytd'])
        if output.empty:
            output = _etf_df
        else:
            output = pd.concat([output, _etf_df])

    df_total = pd.DataFrame(data=[['Total', output['aum'].sum(), output['yesterday'].sum(), output['wtd'].sum(), output['mtd'].sum(), output['ytd'].sum()]], columns=output.columns)

    df_output = pd.concat([df_total, output])
    for col in df_output.columns:
        if col != str('etf'):
            df_output[col] = (df_output[col].round(-3)).map('{:,.0f}'.format)

    return df_output

def tsx_holidays():
    conn = common.db_connection()
    query = f"""SELECT * FROM holidays WHERE [holiday_type] = 'tsx trading'"""
    df = conn.query_tbl(query)
    df['delta'] = 1
    return dict(zip(pd.to_datetime(df.date).dt.strftime("%Y-%m-%d"), df.delta))

def tsx_setl_holidays():
    conn = common.db_connection()
    query = f"""SELECT * FROM holidays WHERE [holiday_type] = 'tsx settlement'"""
    df = conn.query_tbl(query)
    df['delta'] = 1
    return dict(zip(pd.to_datetime(df.date).dt.strftime("%Y-%m-%d"), df.delta))

def nyse_holidays():
    conn = common.db_connection()
    query = f"""SELECT * FROM holidays WHERE [holiday_type] = 'nyse trading'"""
    df = conn.query_tbl(query)
    df['delta'] = 1
    return dict(zip(pd.to_datetime(df.date).dt.strftime("%Y-%m-%d"), df.delta))

def yahoo_usd_fx_rates():
    df = pd.read_csv('Z:\\IPS\\storage\\ad_hoc_pricing\\USD.csv')
    return dict(zip(pd.to_datetime(df.date).dt.strftime('%Y-%m-%d'), df.price.astype(float)))

def fx_rates():
    fx_dict = {}
    fx_dict['USD'] = yahoo_usd_fx_rates()
    fx_dict['CAD'] = {i:1 for i in fx_dict['USD'].keys()}
    return fx_dict


def _load_current_trr_1d(load_ticker_list:list, start_dt:dt.datetime, end_dt:dt.datetime):
    """
    Function fetches the one-day total return from the Bloomberg API and loads to the market_data table.
    WARNING can be resource intensive, can download lots of Bloomberg data

    :param load_ticker_list:
    :param start_dt:
    :param end_dt:
    """
    d = start_dt
    while (end_dt - d).days >= 0:
        next_d = d + dt.timedelta(days=1)

        # Fetch Bloomberg Data
        bbg = bbg_session.BDP_Session()
        trr_data = bbg.bdp_request(load_ticker_list,
                                   flds=["CUST_TRR_RETURN_HOLDING_PER", "CURRENCY"],
                                   ovrds={"CUST_TRR_START_DT": d.strftime("%Y%m%d"),
                                          "CUST_TRR_END_DT": next_d.strftime("%Y%m%d")})
        trr_dict = bbg.unpact_dictionary(trr_data, "CUST_TRR_RETURN_HOLDING_PER", remove_equity_str=True)
        curr_dict = bbg.unpact_dictionary(trr_data, "CURRENCY", remove_equity_str=True)

        # Format data for import
        output = pd.DataFrame(columns=["gx_id", "date", "ticker", "field", "value", "currency", "source"])
        output["ticker"] = trr_dict.keys()
        output["gx_id"] = None
        output["date"] = next_d.date()
        output["field"] = "current_trr_1d"
        output["value"] = output["ticker"].map(trr_dict)
        output["currency"] = output["ticker"].map(curr_dict)
        output["source"] = "bloomberg"

        # clean data
        output = output[~output["value"].isnull()].reset_index(drop=True)
        output["currency"] = output["currency"].lower()

        # Load data to database
        if not output.empty:
            overwrite_records_sql = f"SELECT * FROM market_data WHERE field = 'current_trr_1d' and [date] = '{next_d.strftime('%Y-%m-%d')}' and ticker IN {conn.list_to_sql_str(trr_dict.keys(), convert_elements=True)};"
            conn.insert_data(output, "market_data", sql_check_existing=overwrite_records_sql)
            print(f"{output.shape[0]} rows of data for: {next_d.strftime('%Y-%m-%d')}")
        else:
            print(f"No data for: {next_d.strftime('%Y-%m-%d')}")
        d = next_d

def current_trr_1d(ticker_list:list, start_dt:dt.datetime, end_dt:dt.datetime, download_data:bool=False):
    """
    function pulls the one-day total_return. if the data is missing and download_data is selected True then the function will download the data from bloomebrg
    :param ticker_list: list of ticker universe
    :param start_dt: start date as datetime
    :param end_dt: end date as datetime
    :param download_data: if True, then function will download any missing data from Bloomberg's python API and load to the market_data table. Missing data is determined
    :return: dataframe with current_trr_1d data
    """

    ticker_list = [x.replace(" Equity", "") for x in ticker_list]

    # establish connection with database
    conn = common.db_connection()
    sqlStatement = f"SELECT * FROM market_data WHERE field = 'current_trr_1d' and [date] >= '{start_dt.strftime('%Y-%m-%d')}' and [date] <= '{end_dt.strftime('%Y-%m-%d')}' and ticker IN {conn.list_to_sql_str(ticker_list, convert_elements=True)};"
    df_data = conn.query_tbl(sqlStatement)

    # if there's empty data, download from Bloomberg and load to database
    if download_data:
        _database_tickers = df_data["ticker"].unique()
        missing_tickers = list(set(ticker_list) - set(_database_tickers))
        if missing_tickers != []:
            _load_current_trr_1d(missing_tickers, start_dt, end_dt)
            df_data = conn.query_tbl(sqlStatement)

    return df_data

def distributions_tbl(fund_list, dist_max_ex_date:dt.datetime, dist_min_ex_date:dt.datetime=None):
    """

    :param fund_list: list of Global X ETFs
    :param dist_max_ex_date: the max date in scope
    :param dist_min_ex_date: the min date in scope, optional parameter
    :return:
    """
    conn = common.db_connection()

    # temporary mapping
    map_ticker_lis = {}
    for x in fund_list:
        map_ticker_lis.update({x.replace(".", "/") + " CN": x})

    strSQL = (f"""SELECT ticker as fund, ex_date, currency, dvd_amount as rate, payable_date """
              f"""FROM dividends WHERE ticker IN ('{"','".join(map_ticker_lis.keys())}')""")

    if not dist_max_ex_date is None:
        strSQL += f" AND ex_date <= '{dist_max_ex_date.strftime('%Y-%m-%d')}'"

    if not dist_min_ex_date is None:
        strSQL += f" AND ex_date >= '{dist_min_ex_date.strftime('%Y-%m-%d')}'"

    # query data
    df = conn.query_tbl(strSQL)
    df['ex_date'] = pd.to_datetime(df['ex_date'])

    # map back
    df["fund"] = df["fund"].map(map_ticker_lis)
    return df

def recent_distributions(d:dt.datetime, fund_list:list, dist_max_ex_date:dt.datetime=None):
    dist = distributions_tbl(fund_list, dist_max_ex_date)
    most_recent = dist[dist.groupby('fund')['ex_date'].transform('max') == dist['ex_date']].reset_index(drop=True)

    _min_most_recent = most_recent['ex_date'].min()
    _max_most_recent = most_recent['ex_date'].max()
    all_dates = np.unique([_min_most_recent, _max_most_recent, d])
    fund_data = collect_fund_data(min(all_dates), max(all_dates))
    fund_data = fund_data[pd.to_datetime(fund_data['inav_date'])<=_max_most_recent].reset_index(drop=True)
    fund_data['date_str'] = pd.to_datetime(fund_data['inav_date']).dt.strftime('%Y%m%d')

    #for the case where we need to forecast the distributions
    max_fund_data = fund_data[fund_data.groupby('Fund Ticker')['inav_date'].transform('max') == fund_data['inav_date']]
    forecast_pnu_dict = dict(zip(max_fund_data['Fund Ticker'], max_fund_data['CU']))
    forecast_shares_dict = dict(zip(max_fund_data['Fund Ticker'], max_fund_data['FUND SHARES OUT']))

    most_recent['fund_date'] = most_recent['fund'] + str('_') + pd.to_datetime(most_recent['ex_date']).dt.strftime('%Y%m%d')
    most_recent['payable_date'] = pd.to_datetime(most_recent['payable_date'])
    most_recent['shares'] = most_recent['fund_date'].map(common.groupby_dict(fund_data, _by=['Fund Ticker', 'date_str'], _val='FUND SHARES OUT'))

    most_recent['forecast_pnu'] = most_recent['fund'].map(forecast_pnu_dict)
    most_recent['forecast_shares'] = most_recent['fund'].map(forecast_shares_dict)
    most_recent['orders'] = None
    most_recent['total_shares'] = np.where(most_recent['shares'].isnull(), most_recent['forecast_shares'], most_recent['shares'])

    most_recent['shares'] = np.where(most_recent['fund'].isin(['USCC.U', 'SPAY.U', 'MPAY.U', 'LPAY.U']), 0, most_recent['shares'])
    most_recent['total_shares'] = np.where(most_recent['fund'].isin(['USCC.U', 'SPAY.U', 'MPAY.U', 'LPAY.U']), 0, most_recent['total_shares'])
    return most_recent


def get_div_data(equity_list=list, min_date:dt.datetime=dt.datetime.now(), max_date:dt.datetime=None):
    if equity_list == []:
        return pd.DataFrame(columns=['ticker', 'ex_date', 'payable_date', 'dvd_amount', 'currency'])
    else:
        conn = common.db_connection()
        if max_date is None:
            str_sql = f"SELECT ticker, ex_date, payable_date, dvd_amount, currency FROM dividends WHERE ex_date >= '{min_date.strftime('%Y-%m-%d')}' and ticker IN {conn.list_to_sql_str(equity_list, convert_elements=True)};"
        else:
            str_sql = f"SELECT ticker, ex_date, payable_date, dvd_amount, currency FROM dividends WHERE ex_date >= '{min_date.strftime('%Y-%m-%d')}' and ex_date <= '{max_date.strftime('%Y-%m-%d')}' and ticker IN {conn.list_to_sql_str(equity_list, convert_elements=True)};"
        return conn.query_tbl(str_sql)


def performance_analysis(_idx: str, _idx_value_type: str, _etf: str, _start_date: dt.datetime, _end_date: dt.datetime, opening_start_date:bool=False):
    """
    calculates the performance of an ETF vs its benchmark
    :param _idx: the index name
    :param _idx_value_type: the value type of the index i.e. CLOSING, OPENING
    :param _etf: the ETF ticker
    :param _start_date: the start date of this period
    :param _end_date: the start date of this period
    :param opening_start_date: indicates if the start date value should be the OPENING file.
    :return:
    """

    idx_ret = collect_index_data(_idx, _idx_value_type, _start_date, _end_date, opening_start_date=opening_start_date)
    idx_ret['date'] = pd.to_datetime(idx_ret['date']).dt.strftime('%Y-%m-%d')

    if "DAILY_RETURN" in _idx_value_type:
        idx_ret['daily_return'] = idx_ret['index_value']
    else:
        idx_ret['daily_return'] = (idx_ret['index_value'] / idx_ret['index_value'].shift(periods=1)) - 1
    idx_ret.loc[0, 'daily_return'] = 0
    idx_ret['cumulative_return'] = (1 + idx_ret['daily_return']).cumprod()-1

    navs = collect_fund_data(common.workday(_start_date, 1), common.workday(_end_date, 1), [_etf])

    df_dvd = pd.read_csv(f"Z:\\IPS\\storage\\distributions\\distributions_table.txt", delimiter='\t')
    df_dvd = df_dvd[df_dvd['fund'] == _etf]
    df_dvd['ex_date'] = pd.to_datetime(df_dvd['ex_date']).dt.strftime("%Y-%m-%d")
    navs['dvd'] = navs['nav_date'].map(dict(zip(df_dvd['ex_date'], df_dvd['rate'])))
    total_return = common.total_return_calc(navs, "NAV/SHARE", "dvd")
    total_return['daily_return'] = (total_return['total_return_price']/total_return['total_return_price'].shift(periods=1))-1
    total_return['etf_cum_return'] = (total_return['total_return_price']/total_return['total_return_price'].shift(periods=1))
    
    output_col = ['Date', 'ETF', 'Index', 'Index_Value_Type', 'NAV/Share', 'Total_Return_Price', 'Index_Value', 'ETF Return',
                 'Index Return', 'ETF Cum. Return', 'Index Cum. Return', 'Daily Return Diff.', 'Cum. Return Diff.']
    output = pd.DataFrame(columns=output_col)
    output['Date'] = idx_ret['date']
    output['ETF'] = _etf
    output['Index'] = _idx
    output['Index_Value_Type'] = _idx_value_type
    output['NAV/Share'] = output['Date'].map(dict(zip(total_return['nav_date'], total_return['NAV/SHARE'])))
    output['Total_Return_Price'] = output['Date'].map(dict(zip(total_return['nav_date'], total_return['total_return_price'])))
    output['Index_Value'] = output['Date'].map(dict(zip(idx_ret['date'], idx_ret['index_value'])))
    output['ETF Return'] = output['Date'].map(dict(zip(total_return['nav_date'], total_return['daily_return'])))
    output.loc[0, 'ETF Return'] = 0
    output['ETF Cum. Return'] = (1+output['ETF Return']).cumprod(skipna=True)-1
    output['Index Return'] = output['Date'].map(dict(zip(idx_ret['date'], idx_ret['daily_return'])))
    output['Index Cum. Return'] = output['Date'].map(dict(zip(idx_ret['date'], idx_ret['cumulative_return'])))
    output['Daily Return Diff.'] = output['ETF Return'] - output['Index Return']
    output['Cum. Return Diff.'] = output['ETF Cum. Return'] - output['Index Cum. Return']
    return output


'''
Reads excel trade blotter file to match with tmx scrape data
checks every 30 seconds until the last price matches 
adds columns for the bid ask when the interval VWAP was update and the bid ask 30 seconds before it was updated
'''


def tblt_matching(file_path, tmx_folder, buy_or_sell=None, security_filter=None, broker_filter=None):
    df = pd.read_excel(file_path)
    df = df[['Status', 'Brkr Code', 'Side', 'Security', 'Exch Code', 'Qty', 'IntervalVWAP', 'Create Time (As of)',
             '1stFill', 'Last Fill Datetime', 'Dt Last Updt']]
    df.rename(columns={'Status': 'status', 'Brkr Code': 'brkr_code', 'Side': 'side', 'Security': 'security',
                       'Exch Code': 'exch_code', 'Qty': 'qty', 'IntervalVWAP': 'interval_vwap',
                       'Create Time (As of)': 'create_time', '1stFill': 'fst_fill',
                       'Last Fill Datetime': 'last_fill_datetime', 'Dt Last Updt': 'dt_last_update'}, inplace=True)
    if buy_or_sell == 'Sell':
        mask = df['side'] == 'Sell to Open'
    elif buy_or_sell == 'Buy':
        mask = df['side'] == 'Buy to Close'
    else:
        mask = (df['side'] == 'Sell to Open') | (df['side'] == 'Buy to Close')

    df = df[mask]
    df = df.replace({'side': {'Sell to Open': 'SELL',
                              'Buy to Close': 'BUY'}})

    df['bid'] = ''
    df['ask'] = ''
    df['matches'] = ''
    df['time_recorded'] = ''
    df['bid_before'] = ''
    df['ask_before'] = ''
    df['last_price_before'] = ''
    df['time_before'] = ''
    df['creation_bid'] = ''
    df['creation_ask'] = ''

    mask = df['exch_code'] == 'CN'
    df = df[mask]

    df['security'] = df['security'].apply(lambda s: s.split(" ", 1)[0] + " CN " + s.split(" ", 1)[1])

    opt_cls = common.extract_option_ticker(df, 'security')
    df['security_parsed'] = df['security'].map(opt_cls.underlying_ticker).str.split(' ', expand=True)[0]
    if security_filter is not None:
        mask = df['security_parsed'] == security_filter
        df = df[mask]
    # print(df['security_parsed'])
    if broker_filter is not None:
        mask = df['brkr_code'] == broker_filter
        df = df[mask]

    for row in df.itertuples():
        security = str(row.security_parsed)
        expiry = pd.to_datetime(str(row.security).split(' ')[2])
        strike = str(row.security).split(' ')[3].split('C')[1]
        date = pd.to_datetime(str(row.dt_last_update))

        tmx_path = tmx_folder + '\\' + date.strftime('%Y%m%d') + '\\' + security + '_tmx_quotes.txt'

        try:
            df_tmx = pd.read_csv(tmx_path, sep='\t')
            mask = (df_tmx['expiry'] == expiry.strftime('%Y-%m-%d'))
            df_tmx = df_tmx[mask]
            df_tmx = df_tmx[df_tmx['strike'].astype(float) == float(strike)]
            df_tmx.reset_index(inplace=True)
            timedelta = -dt.timedelta(seconds=0)

            sample = pd.to_datetime(df_tmx['timestamp'][0])
            mask = abs(pd.to_datetime(df_tmx['timestamp']) - pd.to_datetime(pd.to_datetime(row.create_time)).replace(
                year=sample.year, month=sample.month, day=sample.day)) <= dt.timedelta(seconds=30)
            # mask  = pd.to_datetime(df_tmx['timestamp']).dt.strftime('%H:%M:%S') == pd.to_datetime(pd.to_datetime(row.last_fill_datetime) -  timedelta ).strftime('%H:%M')
            df_tmx_parsed = df_tmx[mask]  #
            df_tmx_parsed.reset_index(inplace=True)
            df.at[row[0], 'creation_bid'] = df_tmx_parsed['bid'][0]
            df.at[row[0], 'creation_ask'] = df_tmx_parsed['ask'][0]

            for i in range(5):
                sample = pd.to_datetime(df_tmx['timestamp'][0])
                mask = pd.to_datetime(df_tmx['timestamp']) >= pd.to_datetime(
                    pd.to_datetime(row.last_fill_datetime) + timedelta).replace(year=sample.year, month=sample.month,
                                                                                day=sample.day)
                mask = abs(pd.to_datetime(df_tmx['timestamp']) - pd.to_datetime(
                    pd.to_datetime(row.last_fill_datetime) - timedelta).replace(year=sample.year, month=sample.month,
                                                                                day=sample.day)) <= dt.timedelta(
                    seconds=30)
                # mask  = pd.to_datetime(df_tmx['timestamp']).dt.strftime('%H:%M:%S') == pd.to_datetime(pd.to_datetime(row.last_fill_datetime) -  timedelta ).strftime('%H:%M')
                df_tmx_parsed = df_tmx[mask]  #
                df_tmx_parsed.reset_index(inplace=True)

                if df_tmx_parsed['last_price'][0] == row.interval_vwap:
                    df.at[row[0], 'matches'] = 'TRUE'
                    break
                timedelta = timedelta + dt.timedelta(seconds=30)

            if df.at[row[0], 'matches'] != 'TRUE':
                timedelta = dt.timedelta(seconds=0)
                for i in range(5):
                    sample = pd.to_datetime(df_tmx['timestamp'][0])
                    mask = pd.to_datetime(df_tmx['timestamp']) <= pd.to_datetime(
                        pd.to_datetime(row.last_fill_datetime) + timedelta).replace(year=sample.year,
                                                                                    month=sample.month, day=sample.day)
                    mask = pd.to_datetime(pd.to_datetime(row.last_fill_datetime) + timedelta).replace(year=sample.year,
                                                                                                      month=sample.month,
                                                                                                      day=sample.day) - pd.to_datetime(
                        df_tmx['timestamp']) <= dt.timedelta(seconds=30)
                    # mask  = pd.to_datetime(df_tmx['timestamp']).dt.strftime('%H:%M:%S') == pd.to_datetime(pd.to_datetime(row.last_fill_datetime) -  timedelta ).strftime('%H:%M')
                    df_tmx_parsed = df_tmx[mask]  #
                    df_tmx_parsed.reset_index(inplace=True)

                    if df_tmx_parsed['last_price'][0] == row.interval_vwap:
                        df.at[row[0], 'matches'] = 'after'
                        break
                    timedelta = timedelta + dt.timedelta(seconds=30)

            time_recorded = pd.to_datetime(df_tmx_parsed['timestamp'][0])

            df.at[row[0], 'time_recorded'] = pd.to_datetime(df_tmx_parsed['timestamp'][0]).strftime('%H:%M:%S')

            df.at[row[0], 'bid'] = df_tmx_parsed['bid'][0]
            df.at[row[0], 'ask'] = df_tmx_parsed['ask'][0]
            # df.at[row[0], 'mid'] = (df_tmx_parsed['ask'][0] + df_tmx_parsed['bid'][0])/2

            mask = abs(
                pd.to_datetime(df_tmx['timestamp']) - pd.to_datetime(time_recorded - dt.timedelta(seconds=30)).replace(
                    year=sample.year, month=sample.month, day=sample.day)) < dt.timedelta(seconds=30)
            # mask  = pd.to_datetime(df_tmx['timestamp']).dt.strftime('%H:%M:%S') == pd.to_datetime(pd.to_datetime(row.last_fill_datetime) -  timedelta ).strftime('%H:%M')
            df_tmx_parsed = df_tmx[mask]

            df_tmx_parsed.reset_index(inplace=True)
            df.at[row[0], 'bid_before'] = df_tmx_parsed['bid'][0]
            df.at[row[0], 'ask_before'] = df_tmx_parsed['ask'][0]
            df.at[row[0], 'last_price_before'] = (df_tmx_parsed['ask'][0] + df_tmx_parsed['bid'][0]) / 2
            df.at[row[0], 'time_before'] = pd.to_datetime(df_tmx_parsed['timestamp'][0]).strftime('%H:%M:%S')
            print('.....')
        except FileNotFoundError:
            print('file not found: ' + tmx_path)
        except KeyError:
            print('key error: ' + tmx_path)
            print('expiry: ' + str(expiry) + ' strike: ' + str(strike) + ' time: ' + pd.to_datetime(
                row.last_fill_datetime).strftime('%H:%M'))

    return df


def tca_calculation(df):
    warnings.filterwarnings("ignore")

    added_value_list = []
    added_value_percent_list = []
    added_value_total_list = []

    market_cost_list = []
    market_cost_percent_list = []
    market_cost_total_list = []

    our_cost_list = []
    our_cost_percent_list = []
    our_cost_total_list = []

    creation_cost_list = []

    ctc = []
    prem_total = []
    prem_perct = []
    gr_comms = []

    df = df[['security_parsed', 'side', 'security', 'brkr_code', 'qty', 'interval_vwap', 'bid_before', 'ask_before',
             'last_price_before', 'creation_bid', 'creation_ask']]
    df.rename(columns={'security_parsed': 'security',
                       'side': 'side',
                       'security': 'option',
                       'brkr_code': 'broker',
                       'qty': 'qty',
                       'interval_vwap': 'avg_px',
                       'bid_before': 'bid',
                       'ask_before': 'ask',
                       'last_price_before': 'last_trade_px',
                       'creation_bid': 'creation_bid',
                       'creation_ask': 'creation_ask'}
              , inplace=True)
    df.replace("", np.nan, inplace=True)
    df.dropna(inplace=True, ignore_index=True)
    # calculation for necessary analysis

    df['bid'] = pd.to_numeric(df['bid'])
    df['ask'] = pd.to_numeric(df['ask'])
    df['mid'] = (df['bid'] + df['ask']) / 2
    for row in df.itertuples():
        side = str(row.side)
        mid = (row.bid + row.ask) / 2
        if side == 'SELL':
            added_value = row.avg_px - row.bid
            our_cost = mid - row.avg_px
            market_cost = mid - row.bid
            creation_market_cost = ((row.creation_bid + row.creation_ask) / 2) - row.creation_bid
        elif side == 'BUY':
            added_value = row.ask - row.avg_px
            our_cost = row.avg_px - mid
            market_cost = row.ask - mid
            creation_market_cost = row.creation_ask - ((row.creation_bid + row.creation_ask) / 2)

        market_cost_list.append(market_cost)
        market_cost_percent_list.append(market_cost / mid)
        market_cost_total_list.append(100 * row.qty * market_cost)
        creation_cost_list.append(creation_market_cost)

        added_value_list.append(added_value)
        added_value_percent_list.append(added_value / mid)
        added_value_total_list.append(100 * row.qty * added_value)

        our_cost_list.append(our_cost)
        our_cost_percent_list.append(our_cost / mid)
        our_cost_total_list.append(100 * row.qty * our_cost)

        ctc.append(100 * added_value)
        prem_total.append(100 * row.qty * row.avg_px)
        prem_perct.append(added_value / row.avg_px)

        if row.avg_px < 1:
            gr_comms.append(row.qty * 1)
        else:
            gr_comms.append(row.qty * 2)

    df['Added Value Total'] = added_value_total_list
    df['$//ctc'] = ctc
    df['Total Prem'] = prem_total
    df['Percent Prem'] = prem_perct
    df['GrComms'] = gr_comms

    df['added_value'] = added_value_list
    df['added_value_perct'] = added_value_percent_list
    df['added_value_tot'] = added_value_total_list

    df['market_cost'] = market_cost_list
    df['market_cost_perct'] = market_cost_percent_list
    df['market_cost_tot'] = market_cost_total_list

    df['our_cost'] = our_cost_list
    df['our_cost_perct'] = our_cost_percent_list
    df['our_cost_tot'] = our_cost_total_list

    df = df.round(4)

    return df


def tblt_match_all_files(path, file_names: list, save_folder: list, start_time=None, end_time=dt.datetime.now(),
                         buy_or_sell: str = None, security_filter: str = None, broker_filter: str = None):
    start_time = pd.to_datetime(start_time)
    warnings.filterwarnings("ignore")

    for name in file_names:
        date = pd.to_datetime(name.split('_')[0])
        if start_time is None and date <= end_time or date >= start_time and date <= end_time:
            df = tblt_matching(path + name + '.xlsx', save_folder, buy_or_sell, security_filter, broker_filter)
            df.to_csv(save_folder + name + '_matched.csv')

            df = tca_calculation(df)
            df.to_csv(save_folder + name + '_analysis.csv', index=False)


def _cal_weights(db):

    db['pricing (from source)'] = db['pricing (from primary)'].fillna(db['pricing (from secondary)'])

    db['temp_idx'] = db['index_shares']*db['fx_rate']*pd.to_numeric(db['pricing (from source)'])
    db['temp_fund'] = db['shares']*db['fx_rate']*pd.to_numeric(db['pricing (from source)'])
    sum_idx = db['temp_idx'].sum()
    sum_fund = db['temp_fund'].sum()
    db['weights_idx'] = db['temp_idx']/sum_idx
    db['weights_fund'] = db['temp_fund']/sum_fund
    db['Variance'] = db['weights_fund'] - db['weights_idx']
    db['Variance_BPS'] = abs(db['Variance'])*10000
    db_ret = db[['ticker', 'gx_id','currency','weights_idx','weights_fund','fx_rate','Variance', 'Variance_BPS','pricing (from source)']]
    return db_ret #weights


def calculate_index_variance(etf_ticker, pricing_source:str, config_loc:str, backup_source:str="mellon", opening_date=dt.datetime.now()):
    """
    This function compares the index weighting versus the fund's basket weighting (does not consider cash as an asset).

    :param etf_ticker: ETF o
    :param pricing_source: primary source for market price data
    :param backup_source: back up sourcing for market price data
    :param opening_date: opening date
    :param config_loc: this maps the ETF to a respective index
    :return: 
    """
    
    pricing_date = common.workday(opening_date, -1)
    pricing_date_str = pricing_date.strftime("%Y-%m-%d")
    
    temp = pd.read_csv(config_loc)
    config = dict(zip(temp['fund'],temp['index_code']))
    if etf_ticker in ["BKCC", "ENCC"]:
        etf_holdings = collect_full_holdings([etf_ticker], opening_date, _drill_down=True)
    else:
        etf_holdings = collect_full_holdings([etf_ticker], opening_date, _drill_down=False)

    fund_shares = etf_holdings[['shares', 'ticker']]

    #Get FX Data from server
    conn1 = common.db_connection("globalx")
    q_FX = f"""SELECT [FX Rate],[Local Price Currency] as currency FROM currencyfxrates as fx
            WHERE [Data Source]= 'CIBC Mellon' and fx.[As of Date] = '{pricing_date_str}';"""
    fx_df = conn1.query_tbl(q_FX)
    fx_df.set_index('currency', inplace=True)

    #Get Securities/Index data from server
    conn = common.db_connection()
    q_gx_id = f"""SELECT idx_b.index_code, idx_b.date, sm.gx_id, shares as index_shares, idx_b.basket_type, sm.ticker, 1 as fx_rate,sm.currency, md_p.value as 'pricing (from primary)',md_b.value as 'pricing (from secondary)'
            FROM index_basket as idx_b
            LEFT JOIN security_master as sm ON idx_b.gx_id = sm.gx_id
            LEFT JOIN market_data as md_p ON idx_b.gx_id = md_p.gx_id AND md_p.date = '{pricing_date_str}' and md_p.source = '{pricing_source}' and md_p.[field]='px_last'
            LEFT JOIN market_data as md_b ON idx_b.gx_id = md_b.gx_id AND md_b.date = '{pricing_date_str}' and md_b.source = '{backup_source}'
            WHERE idx_b.index_code='""" + str(config.get(etf_ticker)) + """'and idx_b.[date]='""" + str(opening_date.strftime('%Y-%m-%d')) + """' and idx_b.basket_type='opening';"""

    db = conn.query_tbl(q_gx_id)
    db_ret = pd.merge(db, fund_shares, on='ticker')
    fx_dict = dict(zip(fx_df.index, fx_df['FX Rate']))
    fx_dict["CAD"] = 1
    db_ret["fx_rate"] = db_ret["currency"].map(fx_dict)

    return _cal_weights(db_ret) 

def _bmo_opt_ticker_derivation(sec_data, week_count_dict:dict) -> str:
    holding_name_map = {"BANK OF MONTREAL": "BMO CN", "TORONTO-DOMINION BANK/THE": "TD CN", "NATIONAL BANK OF CANADA": "NA CN",
                        "CANADIAN IMPERIAL BANK OF COMM": "CM CN", "BANK OF NOVA SCOTIA/THE": "BNS CN", "ROYAL BANK OF CANADA": "RY CN",
                        "BMO EQUAL WEIGHT BANKS INDEX E ": "ZEB CN"}

    for key, val in holding_name_map.items():
        if str(key) in str(sec_data["underlying_name"]):
            _underlying_ticker = val
            _maturity = sec_data["maturity_date"]
            if _maturity is None:
                # Equity
                return val
            else:
                # Option
                if "CALL" in sec_data["underlying_name"]:
                    _opt_type = "C"
                else:
                    _opt_type = "P"
                _strike = common.round_opt_strike(float(sec_data["strike_price"]))
                _week_adj = week_count_dict.get(_maturity.strftime('%Y-%m-%d'))
                if _week_adj != 3:
                    return f"{_week_adj}{val} {_maturity.strftime('%m/%d/%y')} {_opt_type}{_strike}"
                else:
                    return f"{val} {_maturity.strftime('%m/%d/%y')} {_opt_type}{_strike}"
    else:
        return None

def dvd_yield_calc(d:dt.datetime, gx_id:str=None, ticker:str=None, ovrd_annual_frequency:int=None) -> float:
    """
    Last Update: 2024-11-20
    This function approximates the annualized dividend yield. It attempts to annualize the dividend by:
    1. Taking the most recent dividend
    2. Estimating the frequency of the dividend per year, which can be overrided by the ovrd_annual_frequency parameter

    :param d: yield calc as of this date
    :param gx_id: identifier of the security only need one of gx_id and ticker
    :param ticker: identifier of the security only need one of gx_id and ticker
    :param ovrd_annual_frequency: override for the frequency of the dividend per year
    :return: returns the annualized dividend yield
    """
    conn = common.db_connection("ips_sandbox")

    if not gx_id is None:
        _id = "gx_id"
        _val = gx_id
    elif not ticker is None:
        _id = "ticker"
        _val = ticker
    else:
        raise Exception("No gx_id or ticker specified")

    # pull market price, any source is fine. we just need an estimate
    str_sql_price = (f"SELECT {_id} as id, [value] as px_last, [source] "
                     f"FROM market_data "
                     f"WHERE {_id}='{_val}' and [date] = '{d.strftime('%Y-%m-%d')}' and [field]='px_last' and source IN ('mellon', 'bloomberg', 'yahoo');")
    df_price = conn.query_tbl(str_sql_price)

    # pull dividends
    _lookback_period = dt.datetime(d.year - 1, d.month, 1)
    str_sql = (f"SELECT ticker as id, ex_date, payable_date, dvd_amount, currency "
               f"FROM dividends "
               f"WHERE ticker='{ticker}' and ex_date <= '{d.strftime('%Y-%m-%d')}' and ex_date >= '{_lookback_period.strftime('%Y-%m-%d')}' "
               f"ORDER BY ex_date desc;")
    df_dvds = conn.query_tbl(str_sql)
    if df_dvds.empty:
        print(f"No Dividend Records for {_id}: {_val}")
        return None
    else:
        last_dvd_dt = df_dvds.loc[0, "ex_date"]
        last_dvd = df_dvds.loc[0, "dvd_amount"]

        if not ovrd_annual_frequency is None:
            dvd_per_year = ovrd_annual_frequency
        else:
            if df_dvds.shape[0] >= 1:
                #try to avoid double counting the dividend if the monthly dividend date is not exactly on the same day each year
                dvd_this_month = df_dvds[(pd.to_datetime(df_dvds["ex_date"]).dt.year == d.year) & (pd.to_datetime(df_dvds["ex_date"]).dt.month == d.month)]
                if not dvd_this_month.empty:
                    _next_month = 1 if d.month == 12 else d.month + 1
                    df_dvds = df_dvds[pd.to_datetime(df_dvds["ex_date"]) >= dt.datetime(last_dvd_dt.year-1, _next_month, 1)]
                else:
                    df_dvds = df_dvds[pd.to_datetime(df_dvds["ex_date"]) >= dt.datetime(last_dvd_dt.year - 1, d.month, 1)]

                dvd_per_year = len(np.unique(df_dvds["ex_date"]).tolist())
                if not dvd_per_year in [1, 2, 4, 12]:
                    print("Weird dividend period. Please check")
            else:
                raise Exception(f"Not able to estimate the dvd frequency for gx_id: {str(gx_id)} ticker: {str(ticker)}")

        return dvd_per_year*float(last_dvd)/float(df_price.loc[0, "px_last"])

def pull_cc_competitor_holdings(ticker:str, holdings_date:dt.datetime) -> pd.DataFrame:
    """
    This function queries the basket for a competitor etf which is scraped daily. This stored proc is tested for BMO holdings at the moment.
    :param ticker: ETF ticker
    :param holdings_date: holdings date
    :return:
    """

    conn = common.db_connection("ips_sandbox")

    #query raw data from the database
    sql_str = (f"SELECT etf_ticker, basket_date, underlying_name, ticker, isin, unit_count, weight as stated_weight, '' as calculated_weight, market_value, currency, strike_price, maturity_date "
               f"FROM cc_scraped_data WHERE etf_ticker = '{ticker}' and basket_date = '{holdings_date.strftime('%Y-%m-%d')}';")
    df = conn.query_tbl(sql_query=sql_str)

    weeks_count = common.week_count(holdings_date)

    # manipulate data to derive ticker
    df["ticker"] = df.apply(_bmo_opt_ticker_derivation, axis=1, args=[weeks_count])

    #calc weights
    df["calculated_weight"] = df["market_value"]/df["market_value"].sum()
    return df

def hedge_check(d:dt.datetime) -> pd.DataFrame:
    relevant_funds = "PAYS,PAYM,PAYL,HBAL,HCON,HGRW"
    conn = common.db_connection()

    # Hedges
    sql_str = f"""EXECUTE [GetFXForwards] @InavDate = '{d.strftime("%Y-%m-%d")}', @FundList = '{",".join(relevant_funds.split(","))}';"""
    df_hedges = conn.query_tbl(sql_str)
    df_hedges["Payable Units"] = np.where(df_hedges["Payable Currency Code"] != "CAD", df_hedges["Payable Units"], 0)
    df_hedges["Receivable Units"] = np.where(df_hedges["Receivable Currency Code"] != "CAD", df_hedges["Receivable Units"], 0)
    df_hedges["Currency Code"] = np.where(df_hedges["Receivable Currency Code"] != "CAD", df_hedges["Receivable Currency Code"], df_hedges["Payable Currency Code"])
    total_fwd_exposure = df_hedges.groupby(by=["ETF Trading Ticker", "Currency Code"],
                                           group_keys=True)[["Payable Units", "Receivable Units"]].apply(sum).reset_index()
    total_fwd_exposure["net_notional"] = total_fwd_exposure["Payable Units"] + total_fwd_exposure["Receivable Units"]

    # Equities
    sql_str = f"""EXECUTE [GetEquities] @InavDate = '{d.strftime("%Y-%m-%d")}', @FundList = '{",".join(relevant_funds.split(","))}';"""
    df_positions = conn.query_tbl(sql_str)[["fund", "date", "ticker", "price", "currency", "shares", "pnu"]]
    df_positions["opening_mv"] = df_positions["price"]*df_positions["shares"]
    df_positions["pnu_mv"] = df_positions["price"] * df_positions["pnu"]
    df_positions["total"] = None
    df_hedged_securities = df_positions[(df_positions["currency"]=="USD") | (df_positions["ticker"]=="HTB CN")]

    #fx rates from GX database
    db_conn_gx = common.db_connection("globalx")
    q_FX = f"""SELECT [Local Price Currency] as currency, [FX Rate] as rate FROM currencyfxrates as fx WHERE [Data Source]= 'CIBC Mellon' and fx.[As of Date] = '{common.workday(d, -1).strftime('%Y-%m-%d')}';"""
    fx_dict = db_conn_gx.query_tbl(q_FX)
    fx_dict = dict(zip(fx_dict["currency"], fx_dict["rate"]))

    # convert to hedged currency
    df_hedged_securities["hedged_currency"] = np.where(df_hedged_securities["ticker"]=="HTB CN", "USD", df_hedged_securities["currency"])
    df_hedged_securities["fx_adjustment"] = df_hedged_securities["currency"].map(fx_dict) / df_hedged_securities["hedged_currency"].map(fx_dict)
    df_hedged_securities["total_mv_to_hedge"] = df_hedged_securities["opening_mv"].astype(float) * df_hedged_securities["fx_adjustment"].astype(float)
    df_hedged_securities["pnu_mv_to_hedge"] = df_hedged_securities["pnu_mv"].astype(float) * df_hedged_securities["fx_adjustment"].astype(float)

    agg_sec_mv = df_hedged_securities.groupby(by=["fund", "hedged_currency"],
                                           group_keys=True)[["total_mv_to_hedge", "pnu_mv_to_hedge"]].apply(sum).reset_index()

    total_fwd_exposure["lookup"] = total_fwd_exposure["ETF Trading Ticker"] + str("_") + total_fwd_exposure["Currency Code"]
    total_fwd_exposure["opening_mv"] = total_fwd_exposure["lookup"].map(common.groupby_dict(agg_sec_mv, _by=["fund", "hedged_currency"], _val="total_mv_to_hedge"))
    total_fwd_exposure["pnu_mv"] = total_fwd_exposure["lookup"].map(common.groupby_dict(agg_sec_mv, _by=["fund", "hedged_currency"], _val="pnu_mv_to_hedge"))
    total_fwd_exposure["opening_hr"] = total_fwd_exposure["net_notional"].astype(float)/total_fwd_exposure["opening_mv"].astype(float)

    df_order = condense_email_orders(d)
    df_order = df_order[df_order["cancelled"].isnull()]
    agg_orders = df_order.groupby(by=["ticker"], group_keys=True)[["pnu"]].apply(sum).reset_index()
    agg_orders_dict = dict(zip(agg_orders["ticker"], agg_orders["pnu"]))

    total_fwd_exposure["orders"] = total_fwd_exposure["ETF Trading Ticker"].map(agg_orders_dict).fillna(0)
    total_fwd_exposure["hedge_adjustment"] = total_fwd_exposure["orders"]*total_fwd_exposure["opening_hr"]*total_fwd_exposure["pnu_mv"]
    total_fwd_exposure["hedge_adjustment"] = total_fwd_exposure["hedge_adjustment"].round(-3)
    return total_fwd_exposure

if __name__ == '__main__':
    x = hedge_check(dt.datetime.now())
    print('wow')