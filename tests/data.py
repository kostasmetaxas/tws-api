
import sys
import socket
import struct
import array
import datetime
import inspect
import time
import datetime, pytz
import argparse

import os.path

from stock import Stock

import flask
from flask import request, jsonify, abort
import json
from pathlib import Path

app = flask.Flask(__name__)
app.config["DEBUG"] = True

tickers = []
def initialise_db():
    tickers = [
        {
            'ticker':'SPY',
            'ccy' :'USD',
            'exchange':'SMART'
        },
        {
            'ticker':'TLT',
            'ccy':'USD',
            'exchange':'SMART'
        }
    ]
    print('saving data')
    store_tickers(tickers)
    print('done')

def store_tickers(tickers):
    data= json.dumps(tickers, indent=4)
    with open("tickers.json", 'w') as f:
        f.write(data)

def load_tickers():
    global tickers
    my_file = Path("tickers.json")
    if my_file.is_file():
        json_data=open(my_file).read()
        tickers =  json.loads(json_data)

##### Functions related to the api ######
@app.route('/tickers', methods=['GET'])
def get_tasks():
    return jsonify(tickers)

@app.route('/', methods=['GET'])
def home():
        return "KM3 Data Management API</h1><p>Welcome</p>"

@app.route('/tickers', methods=['POST'])
def create_ticker():
    if not request.json:
        abort(400)
    ticker = {
        'ticker': request.json['ticker'],
        'ccy': request.json['ccy'],
        'exchange': request.json['exchange']
    }
    tickers.append(ticker)
    store_tickers()
    return jsonify({'ticker': ticker}), 201

@app.route('/refresh', methods=['GET'])
def refreshData():
    for t in tickers:
        stock = Stock(t['ticker'],t['ccy'],t['exchange'])
        stock.refreshData()
    return '<h1>Attemted to refresh ' + str(len(t)) + ' tickers </h1>'

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

########## MAIN ######
load_tickers()
app.run(debug=False,host='0.0.0.0')


