import requests
import csv
import json

markets = ['USDT_ETC', 
           'USDT_ETH',
           'USDT_BTC',
           'USDT_XMR',
           'USDT_XRP',
           'USDT_ZEC',
           'USDT_LTC',
           'USDT_REP',
           'USDT_NXT',
           'USDT_STR',
           'USDT_DASH']

def fetchMarket(symbol, period):
    url = 'https://poloniex.com/public?command=returnChartData&currencyPair='+ symbol +'&start=0&end=9999999999&period='+ period
    filepath = '/tmp/' + symbol + '.csv'
    
    r = requests.get(url)
    data = r.content
    market_data = json.loads(data)

    market_data_file = open(filepath, 'w')
    csvwriter = csv.writer(market_data_file)

    count = 0
    for candle in market_data:
          #only write header once
          if count == 0:
                 header = candle.keys()
                 csvwriter.writerow(header)
                 count += 1
          csvwriter.writerow(candle.values())

    market_data_file.close()

for market in markets:
    fetchMarket(market, '86400')
