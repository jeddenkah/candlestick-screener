import os
import csv
import talib
import yfinance as yf
import pandas
from flask import Flask, escape, request, render_template, jsonify
from patterns import candlestick_patterns
import datetime
import process
import json
from threading import Thread

app = Flask(__name__)


@app.route('/snapshot')
def snapshot():
    # for end (today)
    today = str(datetime.date.today())

    # for start (first day of last month)
    today_start = datetime.date.today()
    first = today_start.replace(day=1)
    lastMonth = (first - datetime.timedelta(days=1)).replace(day=1)
    stocks = process.get_symbol_details()
    symbols = []

    with open('datasets/symbols_ISSI-JK.csv') as f:
        for line in f:
            if "," not in line:
                continue
            symbol = line.split(",")[0]
            # data = yf.download(symbol, start=lastMonth, end=today)
            # data.to_csv('datasets/daily/{}.csv'.format(symbol))
            symbols.append(symbol)
            
    print(symbols)
    # split array into 4
    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
    symbols = list(split(symbols, os.cpu_count()))
    print('get all tickers')
    # update price
    def get_ticker(num):
        for symbol in symbols[num]:
            print(symbol)
            try:
                company = yf.Ticker(symbol)
                stocks[symbol] = {'company': company.info['longName'], 
                                'price' : company.info['regularMarketPrice'], 
                                'market_cap':company.info['marketCap'],
                                'volume':company.info['volume']}
            except Exception as e:
                stocks[symbol] = {'company': None, 'price' : None, 'market_cap':None,'volume':None}
                print('Error:', e)


    threads = []
    for i in range(os.cpu_count()):
        threads.append(Thread(target=get_ticker, args=(i,)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    with open("datasets/{}".format('symbols_details.txt'), 'w') as outfile:
        json.dump(stocks, outfile)

    return {
        "code": "success"
    }


@app.route('/')
def index():
    pattern = request.args.get('pattern', False)
    stocks = {}

    with open('datasets/symbols_ISSI-JK.csv') as f:
        for row in csv.reader(f):
            stocks[row[0]] = {'company': '-'}

    if pattern:
        for filename in os.listdir('datasets/daily'):
            df = pandas.read_csv('datasets/daily/{}'.format(filename))
            pattern_function = getattr(talib, pattern)
            split = filename.split('.')
            symbol = split[0]+'.'+split[1]

            try:
                results = pattern_function(
                    df['Open'], df['High'], df['Low'], df['Close'])
                last = results.tail(1).values[0]

                if last > 0:
                    stocks[symbol][pattern] = 'bullish'
                elif last < 0:
                    stocks[symbol][pattern] = 'bearish'
                else:
                    stocks[symbol][pattern] = None

                if last != 0:
                    company = yf.Ticker(symbol)
                    company_name = company.info['longName']
                    stocks[symbol]['company'] = company_name
            except Exception as e:
                print('failed on filename: ', filename)

    return render_template('index.html', candlestick_patterns=candlestick_patterns, stocks=stocks, pattern=pattern)


@app.route('/all')
def screen_all():
    file_today = "{}.txt".format(str(datetime.date.today()))

    if os.path.exists("datasets/temp/screen_all/{}".format(file_today)):
        stocks = process.get_temp()
    else:
        process.screen_all()
        stocks = process.get_temp()

    return render_template('index_all.html', stocks=stocks)


@app.route('/create_symbol_details')
def create_symbol_details():
    process.create_symbol_details()

    return {
        "code": "success"
    }