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
import pandas as pd

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
@app.route('/', methods=['GET'])
def home():
        return "KM3 Data Management API</h1><p>Welcome</p>"

@app.route('/tickers', methods=['GET'])
def get_tickers():
    return jsonify(tickers)


@app.route('/ticker', methods=['POST'])
def create_ticker():
    if not request.json:
        abort(400)
    ticker = {
        'ticker': request.json['ticker'],
        'ccy': request.json['ccy'],
        'exchange': request.json['exchange']
    }
    exists = any(t['ticker'] == ticker['ticker'] for t in tickers)
    if not exists:
        tickers.append(ticker)
        store_tickers(tickers)
    else:
        abort(409)
    return jsonify({'ticker': ticker}), 201

@app.route('/ticker/<ticker>', methods=['DELETE'])
def delete_ticker(ticker):
    #exists = any(t['ticker'] == ticker for t in tickers)
    t = list( filter(lambda t: t['ticker'] == ticker, tickers) )
    if t:
        tickers.remove(t[0])
        store_tickers(tickers)
    else:
        abort(404)
    return jsonify({'ticker': ticker}), 201

@app.route('/ticker/<ticker>/info',methods=['GET'])
def get_metadata(ticker):
    t = list( filter(lambda t: t['ticker'] == ticker, tickers) )
    if t:
        stock = Stock(t[0]['ticker'], t[0]['ccy'],t[0]['exchange'], t[0]['secType'])
        data= stock.get_metadata()
    else:
        abort(404)
    return jsonify(data), 200
    

@app.route('/ticker/<ticker>/prices',methods=['GET'])
def get_prices(ticker):
    t = list( filter(lambda t: t['ticker'] == ticker, tickers) )
    if t:
        stock = Stock(t[0]['ticker'], t[0]['ccy'],t[0]['exchange'], t[0]['secType'])
        data= stock.prices.to_json( orient='index') 
    else:
        abort(404)
    return jsonify({'prices': data}), 200


@app.route('/refresh', methods=['GET'])
def refreshData():
    for i in range(0, len(tickers)-1):
        t= tickers[i]
        print(t)
        stock = Stock(t['ticker'],t['ccy'],t['exchange'], t['secType'])
        stock.refreshData(ib_client_id=i)
    return '<h1>Attemted to refresh ' + str(len(tickers)) + ' tickers </h1>'

def main():
    cmdLineParser = argparse.ArgumentParser("api tests")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int, 
        dest="port", default = 4005, help="The TCP port to use")
    cmdLineParser.add_argument("-t", "--ticker", action="store", type=str, 
        dest="ticker", default = 'SPY', help="ticker to download data for")
    args = cmdLineParser.parse_args()
    if len(sys.argv) == 1:
        load_tickers()
        app.run(debug=False,host='0.0.0.0')
    else:
        import logging
        #logging.debug("Using args %s", args)
        #logging.debug("now is %s", datetime.datetime.now())
        #logging.getLogger().setLevel(logging.ERROR)
        stock = Stock(args.ticker,"USD","SMART", 'STK')
        stock.refreshData()
        #stock.loadPrices()
        #print(stock.prices)
        #x = stock.getData()
        #stock.loadData()
        #print(stock.prices)
        print(stock.prices.tail(5))
        #print(stock.startDate)


if __name__ == "__main__":
    main()



