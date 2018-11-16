import json
import re
import pandas as pd
import pandas_market_calendars as mcal
import datetime
from pathlib import Path
from ibapi.wrapper import EWrapper
import ibapi.decoder
import ibapi.wrapper
from ibapi.common import *
from ibapi.comm import *
#from ibapi.message import IN, OUT
from ibapi.client import EClient
from ibapi.connection import Connection
from ibapi.utils import *
from ibapi.contract import *


class Stock:
    ticker = ""
    secType = ""
    currency = ""
    exchange = ""
    source = ""
    prices = pd.DataFrame( columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    startDate = ""
    endDate = ""
    def __init__(self, ticker, currency, exchange, secType, source):
        self.ticker = ticker
        self.currency = currency
        self.exchange = exchange
        self.secType = secType
        self.source = source
        self.load()

    # return dictionary of class data
    def get_metadata(self):
        d = {
                'ticker' : self.ticker,
                'secType': self.secType,
                'currency': self.currency,
                'exchange': self.exchange,
                'startDate': self.startDate,
                'endDate': self.endDate,
            }
        prices = self.prices
        prices.loc[:, 'Date'] = prices.index
        prices = prices[ ["Date","Open","High","Low","Close","Volume"] ]
        prices = json.loads(prices.to_json(orient='split', date_format='iso'))
        d['index'] = prices['index']
        d['columns'] = prices['columns']
        d['data'] = prices['data']
        return d
    
    def load(self):
        my_file = Path('db/' + self.ticker + ".json")
        if my_file.is_file():
            json_data=open('db/' + self.ticker+ ".json").read()
            data = json.loads(json_data)
            self.prices = pd.read_json( data['prices'])
            self.startDate = self.prices.index.min()
            if self.prices.shape[0] > 0:
                self.endDate= self.prices.index.max()
            else:
                self.endDate = datetime.datetime(2002, 12, 31, 00, 00)
            self.currency = data['currency']
            self.exchange = data['exchange']
            self.secType = data['secType']
        else:
            self.startDate = datetime.datetime(2002, 12, 31, 00, 00)
            self.endDate = datetime.datetime(2002, 12, 31, 00, 00)

    def last_business_day(self):
        yesterday = datetime.datetime.now()-datetime.timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')
        last_5_days = yesterday - datetime.timedelta(days=5)
        last_5_days_str = last_5_days.strftime('%Y-%m-%d')
        nyse = mcal.get_calendar('NYSE')
        per = ( nyse.schedule(start_date=last_5_days_str, end_date=yesterday_str).index)
        return(per[-1])


    def refreshData(self,ib_client_id, tws_ip, tws_port): #, app, period):
        if self.source == "TWS":
            df = self.source_tws(ib_client_id, tws_ip, tws_port)
        elif self.source == "QUANDL":
            df = self.source_quandl()

        if not df.empty:
            if self.prices.empty:
                self.prices = df
            else:
                joined_series = pd.concat([self.prices, df], sort=True)
                # Need to compare the joined series to identify if full refresh is needed
                # due to stock split?
                self.prices = joined_series[~joined_series.index.duplicated(keep='last')]
            data = {}
            data["ticker"] = self.ticker
            data["exchange"] = self.exchange
            data["currency"] = self.currency
            data["secType"] = self.secType
            data["source"] = self.source
            data["prices"] = self.prices.to_json()
            data_json = json.dumps(data, indent=4)
            with open('db/' + self.ticker + ".json", 'w') as f:
                    f.write(data_json)


    def source_tws(self, ib_client_id, tws_ip, tws_port):
        yesterday = self.last_business_day()
        days_needed = (yesterday- self.endDate).days + 1
        if days_needed > 365:
            period = str( int(days_needed/365) + 1 ) + " Y"
        else:
            period = str(days_needed) + " D"
        data = pd.DataFrame()
        if days_needed > 1:
            print("Refreshing " + period );
            ib = IB_get_data()
            ib.connect(tws_ip, tws_port, ib_client_id)
            contract = Contract()
            contract.symbol = self.ticker
            contract.secType = self.secType
            contract.currency = self.currency
            contract.exchange = self.exchange
            #app.reqHistoricalData(5001, contract, "20180728 16:00:00", period,
            #                             "1 day", "TRADES", 1, 1, False, []) 
            if self.secType == "STK":
                #ib.reqHistoricalData(5001, contract, "", period,
                #                        "1 day", "ADJUSTED_LAST", 1, 1, False, []) 
                ib.reqHistoricalData(5001, contract, "", period,
                                        "1 day", "TRADES", 1, 1, False, []) 
            ib.run()
            ib.disconnect()
            print('disconnected')
            print(ib.df)
            data = ib.df
        return data



    def source_quandl(self):
        import requests
        url = "https://www.quandl.com/api/v3/datasets/" + self.exchange + "/" + self.ticker + ".json?start_date=" + str(self.endDate)
        response = requests.get( url )
        data = json.loads(response.text)
        if 'dataset' in data:
            data_tbl = pd.DataFrame( data['dataset']['data'] )
            columns = data['dataset']['column_names']
            data_tbl.columns = columns
            #
            # find column names if they exist
            settle = [m.group(0) for l in columns for m in [re.compile(".*(Settle).*",re.IGNORECASE).search(l)] if m]
            open_  = [m.group(0) for l in columns for m in [re.compile(".*(Open).*",re.IGNORECASE).search(l)] if m]
            high   = [m.group(0) for l in columns for m in [re.compile(".*(High).*",re.IGNORECASE).search(l)] if m]
            low    = [m.group(0) for l in columns for m in [re.compile(".*(Low).*",re.IGNORECASE).search(l)] if m]
            close  = [m.group(0) for l in columns for m in [re.compile(".*(Close).*",re.IGNORECASE).search(l)] if m]
            volume = [m.group(0) for l in columns for m in [re.compile(".*(Volume).*",re.IGNORECASE).search(l)] if m]
            close_with_ticker_name = [m.group(0) for l in columns for m in [re.compile(".*("+self.ticker+").*",re.IGNORECASE).search(l)] if m]
            #
            # if they exist replace with standard column names
            if settle:
                data_tbl.rename(columns={settle[0]: 'Close'}, inplace=True)
            if open_:
                data_tbl.rename(columns={open_[0]: 'Open'}, inplace=True)
            if high:
                data_tbl.rename(columns={high[0]: 'High'}, inplace=True)
            if low:
                data_tbl.rename(columns={low[0]: 'Low'}, inplace=True)
            if close:
                data_tbl.rename(columns={close[0]: 'Close'}, inplace=True)
            if volume:
                data_tbl.rename(columns={volume[0]: 'Volume'}, inplace=True)
            if not close and close_with_ticker_name:
                data_tbl.rename(columns={close_with_ticker_name[0]: 'Close'}, inplace=True)
            #
            # handle special cases
            if not open_:
                data_tbl.loc[:,"Open"] = data_tbl['Close']
            if not high:
                data_tbl.loc[:,"High"] = data_tbl['Close']
            if not low:
                data_tbl.loc[:,"Low"] = data_tbl['Close']
            if not volume:
                data_tbl.loc[:,"Volume"] = 0
                
            data_tbl = data_tbl[ ['Date','Open','High','Low','Close','Volume']]
            data_tbl['Date'] = pd.to_datetime(data_tbl['Date'])
            data_tbl.index = data_tbl['Date']
            # delete today's date if ib returned live data
            if  data_tbl.index[-1].date() == datetime.datetime.now().date():
                print('deleting today\'s data')
                data_tbl = data_tblf[:-1]
            return data_tbl



class IB_get_data(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.df = pd.DataFrame( columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        self.s = pd.Series()

    @iswrapper
    def nextValidId(self, orderId:int):
        super().nextValidId(orderId)
        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
 
    def nextOrderId(self):
        id = self.nextValidOrderId
        self.nextValidOrderId += 1
        return id

    @iswrapper
    def error(self, *args):
        super().error(*args)
        #print(current_fn_name(), vars())
        x = vars()
        if x["args"][1] not in [2104, 2106] :
            self.disconnect()

    @iswrapper
    def winError(self, *args):
        super().error(*args)
        print(current_fn_name(), vars())

    @iswrapper
    def historicalData(self, reqId: int, bar: BarData):
        self.s = ([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])
        self.df.loc[len(self.df)] = self.s

    @iswrapper
    def historicalDataEnd(self, reqId:int, start:str, end:str):
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df.index = self.df['Date']
        #self.df = self.df.drop('Date',1)
        # delete today's date if ib returned live data
        if  self.df.index[-1].date() == datetime.datetime.now().date():
            print('deleting today\'s data')
            self.df = self.df[:-1]
        self.disconnect()



