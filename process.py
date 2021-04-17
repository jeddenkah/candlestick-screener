import os
from patterns import candlestick_patterns
import pandas
import talib
import csv
import yfinance as yf
import datetime
import json
from threading import Thread

file_today = "{}.txt".format(str(datetime.date.today()))

def get_temp():
    with open("datasets/temp/screen_all/{}".format(file_today)) as json_file:
        data = json.load(json_file)
        return data

def create_symbol_details():
    stocks = {}
    with open('datasets/symbols_ISSI-JK.csv') as f:
        for row in csv.reader(f):
            print(row[0])
            try:
                company = yf.Ticker(row[0])
                stocks[row[0]] = {'company': company.info['longName'], 
                                'price' : company.info['regularMarketPrice'], 
                                'market_cap':company.info['marketCap'],
                                'volume':company.info['volume']}
            except Exception as e:
                stocks[row[0]] = {'company': None, 'price' : None, 'market_cap':None,'volume':None}
                print('Error:', e)
            

    with open("datasets/{}".format('symbols_details.txt'), 'w') as outfile:
        json.dump(stocks, outfile)

def get_symbol_details():
    with open("datasets/{}".format('symbols_details.txt')) as json_file:
        data = json.load(json_file)
        return data

def screen_all():
    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
    stocks = {}
    count_candlestick_patterns = len(candlestick_patterns)
    split_candlestick_patterns = list(split(candlestick_patterns, os.cpu_count()))
    symbol_detail = get_symbol_details()

    def cari(num):
        for index, pattern in enumerate(split_candlestick_patterns[num]):
            stocks[pattern]={}
            stocks[pattern]['is_exist'] = False
            print(pattern, ' (',index+1,'/',len(split_candlestick_patterns[num]),')')

            for filename in os.listdir('datasets/daily'):
                df = pandas.read_csv('datasets/daily/{}'.format(filename))
                pattern_function = getattr(talib, pattern)
                split = filename.split('.')
                symbol = split[0]+'.'+split[1]
                try:
                    results = pattern_function(df['Open'], df['High'], df['Low'], df['Close'])
                    last = results.tail(1).values[0]
                    if last > 0:
                        stocks[pattern][symbol] = {'indicator' : 'bullish'}
                    elif last < 0:
                        stocks[pattern][symbol]= {'indicator' : 'bearish'}
                    else:
                        pass

                    if last != 0:
                        print(symbol)
                        stocks[pattern]['is_exist'] = True
                        stocks[pattern][symbol]['company'] = symbol_detail[symbol]['company']
                        stocks[pattern][symbol]['price'] = symbol_detail[symbol]['price']
                        stocks[pattern][symbol]['market_cap'] = symbol_detail[symbol]['market_cap'] if not None else 0
                        stocks[pattern][symbol]['volume'] = symbol_detail[symbol]['volume'] if not None else 0

                except Exception as e:
                    print('failed on filename: ', filename, e)
    threads = []
    for i in range(os.cpu_count()):
        threads.append(Thread(target=cari, args=(i,)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    with open("datasets/temp/screen_all/{}".format(file_today), 'w') as outfile:
        json.dump(stocks, outfile)

