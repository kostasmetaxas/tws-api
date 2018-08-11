
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

from stock import Stock

import flask

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
        return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"

app.run()


def main():
    cmdLineParser = argparse.ArgumentParser("api tests")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int, 
        dest="port", default = 4005, help="The TCP port to use")
    cmdLineParser.add_argument("-t", "--ticker", action="store", type=str, 
        dest="ticker", default = 'SPY', help="ticker to download data for")
    args = cmdLineParser.parse_args()
    print("Using args", args)

    import logging
    #logging.debug("Using args %s", args)
    #logging.debug("now is %s", datetime.datetime.now())
    #logging.getLogger().setLevel(logging.ERROR)
    stock = Stock(args.ticker,"USD","SMART")
    stock.refreshData()
    #stock.loadPrices()
    #print(stock.prices)
    #x = stock.getData()
    #stock.loadData()
    #print(stock.prices)
    print(stock.prices.tail(5))
    #print(stock.startDate)


#if __name__ == "__main__":
#    main()


