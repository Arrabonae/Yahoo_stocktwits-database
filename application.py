import requests
import json
import pandas as pd
import numpy as np
import time
from flask import Flask, request, jsonify, render_template, redirect
import os

#Create the Flask API
application = Flask(__name__)
#root = '/Users/graystone/Documents/Github/Andromeda/test/production/data.csv'
root = os.path.join(os.getcwd(), 'data')
@application.route('/')

def StockTwits(stocks):
    for stock in stocks:
        r = requests.get('https://api.stocktwits.com/api/2/streams/symbol/ric/{}.json' .format(stock))
        rj =r.json()

        #if not rj['response']['status']==200:
        #    print ('API is overloaded and gave back to following error code: {}. Now the request has stoped for 1 hour' .format(rj['response']['status']))
        #    time.sleep(3600)
        #    r = requests.get('https://api.stocktwits.com/api/2/streams/symbol/ric/{}.json' .format(stock))
        #    rj =r.json()
        #else:
        list = []

        for i in range(0,len(rj['messages'])):
            message_ID = rj['messages'][i]['id']
            message = rj['messages'][i]['body']
            if rj['messages'][i]['entities']['sentiment']==None: sentiment = 'None' 
            else: sentiment = rj['messages'][i]['entities']['sentiment'].get('basic')
            time = rj['messages'][i]['created_at']
            stock_code = rj['symbol']['symbol']
            list.append([message_ID, message, sentiment, time, stock_code])

        df = pd.DataFrame(list, columns=['Message_ID','Message','Sentiment', 'Time', 'Stock'])
        df.set_index('Time', inplace=True)
        df.to_csv(root, mode='a+',header=False) #,header=False
    return  


stocks = ['AAPL', 'MSFT', 'V', 'INTC', 'MA', 'NVDA', 'CSCO', 'ADBE', 'PYPL', 'CRM']

while True:
    run = 0
    StockTwits(stocks)
    run += 1
    df = pd.read_csv(root).drop_duplicates()
    df.set_index('Time', inplace=True)
    df.to_csv(root)
    count = len(df.index)
    print('This code ran {} times and the dataset currently has {} lines of Tweets.' .format(run,count))
    time.sleep(180)

if __name__ == "__main__":
    application.run(host='0.0.0.0', debug=False)