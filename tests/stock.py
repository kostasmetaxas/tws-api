import json
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
    secType = "STK"
    currency = ""
    exchange = ""
    prices = []
    startDate = ""
    endDate = ""
    def __init__(self, ticker, currency, exchange):
        self.ticker = ticker
        self.currency = currency
        self.exchange = exchange
        self.loadPrices()

    def loadPrices(self):
        my_file = Path(self.ticker + ".json")
        if my_file.is_file():
            json_data=open(self.ticker+ ".json").read()
            data = json.loads(json_data)
            self.prices = pd.read_json( data['prices'])
            self.startDate = self.prices.index.min()
            self.endDate= self.prices.index.max()
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


    def refreshData(self): #, app, period):
        ib = IB_get_data()
        ib.connect("127.0.0.1", 4001, 0)
        contract = Contract()
        contract.symbol = self.ticker
        contract.secType = "STK"   
        contract.currency = self.currency
        contract.exchange = self.exchange
        yesterday = self.last_business_day()
        days_needed = (yesterday- self.endDate).days 
        if days_needed > 365:
            period = str( int(days_needed/365) + 1 ) + " Y"
        else:
            period = str(days_needed) + " D"
        print("Refreshing " + period );
        if days_needed >0:
            #app.reqHistoricalData(5001, contract, "20180728 16:00:00", period,
            #                             "1 day", "TRADES", 1, 1, False, []) 
            ib.reqHistoricalData(5001, contract, "", period,
                                        "1 day", "ADJUSTED_LAST", 1, 1, False, []) 
            ib.run()
            #print(ib.df.head())
            if ib.df.shape[0] > 1:
                if self.prices == []:
                    self.prices = ib.df
                else:
                    joined_series = pd.concat([self.prices, ib.df])
                    self.prices = joined_series[~joined_series.index.duplicated(keep='last')]
                data = {}
                data["ticker"] = self.ticker
                data["exchange"] = self.exchange
                data["prices"] = self.prices.to_json()
                data_json = json.dumps(data, indent=4)
                with open(self.ticker + ".json", 'w') as f:
                        f.write(data_json)
        else:
            ib.disconnect()



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
        print(current_fn_name(), vars())

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
        self.df = self.df.drop('Date',1)
        self.disconnect()



