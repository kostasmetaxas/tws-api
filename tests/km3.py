"""
Copyright (C) 2018 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""

import sys
import socket
import struct
import array
import datetime
import inspect
import time
import datetime, pytz
import argparse
import json

import os.path

from ibapi.wrapper import EWrapper
import ibapi.decoder
import ibapi.wrapper
from ibapi.common import *
#from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.comm import *
from ibapi.message import IN, OUT
from ibapi.client import EClient
from ibapi.connection import Connection
#from ibapi.reader import EReader
from ibapi.utils import *
#from ibapi.execution import ExecutionFilter
#from ibapi.scanner import ScannerSubscription
#from ibapi.order_condition import *
from ibapi.contract import *
#from ibapi.order import *
#from ibapi.order_state import *
#from ibapi.tag_value import *

import pandas as pd
from stock import Stock

df = pd.DataFrame( columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
s = pd.Series()

class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextValidOrderId = None
        self.permId2ord = {}

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
        s = ([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])
        df.loc[len(df)] = s
        """ returns the requested historical data bars

        reqId - the request's identifier
        date  - the bar's date and time (either as a yyyymmss hh:mm:ssformatted
             string or as system time according to the request)
        open  - the bar's open point
        high  - the bar's high point
        low   - the bar's low point
        close - the bar's closing point
        volume - the bar's traded volume if available
        count - the number of trades during the bar's timespan (only available
            for TRADES).
        WAP -   the bar's Weighted Average Price
        hasGaps  -indicates if the data has gaps or not. """

    @iswrapper
    def historicalDataEnd(self, reqId:int, start:str, end:str):
        df['Date'] = pd.to_datetime(df['Date'])
        df.index = df['Date']
        self.disconnect()


def getStockData(app, ticker, ccy, exchange, period):
    contract = Contract()
    contract.symbol = ticker
    contract.secType = "STK"   
    contract.currency = ccy
    contract.exchange = exchange
    #app.reqHistoricalData(5001, contract, "20180728 16:00:00", period,
    #                             "1 day", "TRADES", 1, 1, False, []) 
    app.reqHistoricalData(5001, contract, "", period,
                                 "1 day", "ADJUSTED_LAST", 1, 1, False, []) 



def main():
    cmdLineParser = argparse.ArgumentParser("api tests")
    #cmdLineParser.add_option("-c", action="store_true", dest="use_cache", default = False, help = "use the cache")
    #cmdLineParser.add_option("-f", action="store", type="string", dest="file", default="", help="the input file")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int, 
        dest="port", default = 4005, help="The TCP port to use")
    cmdLineParser.add_argument("-t", "--ticker", action="store", type=str, 
        dest="ticker", default = 'SPY', help="ticker to download data for")
    args = cmdLineParser.parse_args()
    print("Using args", args)

    import logging
    logging.debug("Using args %s", args)
    #print(args)
    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.ERROR)

    #app = TestApp()
    #app.connect("127.0.0.1", args.port, 0)

    #app.reqCurrentTime()
    #getStockData(app,args.ticker, ccy= "USD", exchange= "SMART", period= "20 D")
    #app.run()
    #data = {}
    #data["ticker"] = args.ticker
    #data["exchange"] = "SMART"
    #data["prices"] = df.drop('Date',1).to_json()
    #data_json = json.dumps(data, indent=4)
    #with open(args.ticker + ".json", 'w') as f:
    #        f.write(data_json)
    stock = Stock(args.ticker,"USD","SMART")
    stock.refreshData()
    #stock.loadPrices()
    #print(stock.prices)
    #x = stock.getData()
    #stock.loadData()
    #print(stock.prices)
    #print(stock.endDate)
    #print(stock.startDate)


if __name__ == "__main__":
    main()


