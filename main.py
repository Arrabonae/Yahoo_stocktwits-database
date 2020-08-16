import requests
import json
import pandas as pd
import numpy as np
import time
from datetime import date, timedelta
import os
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()
import sqlite3

def Create():
    c = sqlite3.connect('sentiment_yahoo.db')
    c = c.cursor()
    c.execute(""" CREATE TABLE IF NOT EXISTS twits( 
                message_id integer,
                message text,
                sentiment text,
                time text,
                stock_code text, UNIQUE(message_id)
                )""")
    c.execute(""" CREATE TABLE IF NOT EXISTS yahoo_data( 
            Datetime text,
            Open integer,
            High integer,
            Low integer,
            Close integer,
            Adj_Close integer, 
            Volume integer,
            Stock text,
            ID text, UNIQUE(ID)
            )""")
    return


def StockTwits(stocks):
    for stock in stocks:
        r = requests.get('https://api.stocktwits.com/api/2/streams/symbol/ric/{}.json' .format(stock))
        rj =r.json()
        print (r)

        for i in range(0,len(rj['messages'])):
            message_ID = rj['messages'][i]['id']
            message = rj['messages'][i]['body']
            if rj['messages'][i]['entities']['sentiment']==None: sentiment = 'None' 
            else: sentiment = rj['messages'][i]['entities']['sentiment'].get('basic')
            time = rj['messages'][i]['created_at']
            stock_code = rj['symbol']['symbol']
            connect = sqlite3.connect('sentiment_yahoo.db')
            c = connect.cursor()
            c.execute("INSERT OR IGNORE INTO twits VALUES (:message_id, :message, :sentiment, :time, :stock_code)",
                { 
                'message_id' : message_ID,
                'message'  : message,
                'sentiment': sentiment,
                'time': time,
                'stock_code': stock_code,})
            connect.commit()
            connect.close()

    return  

def yahoo_data(stocks):
    
    for tick in stocks:
        target = yf.Ticker(tick)
        d = date.today() - timedelta(days=4)
        d2 = date.today()
        OCHL = pdr.get_data_yahoo(tick, start=d.strftime('%Y-%m-%d'), end=d2.strftime('%Y-%m-%d'), interval="1m")
        OCHL['Stock'] = tick
        OCHL.reset_index(inplace=True)
        OCHL['ID'] = OCHL['Stock'] +" "+ OCHL['Datetime'].astype(str)
        OCHL.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        OCHL.set_index('ID', inplace=True)
        connect = sqlite3.connect('sentiment_yahoo.db')
        OCHL.to_sql('TempTable',if_exists='replace', con=connect)
        cur = connect.cursor()
        cur.execute("INSERT OR IGNORE INTO yahoo_data SELECT * FROM TempTable")
        connect.commit()
        connect.close()


stocks = ['AAPL', 'MSFT', 'V', 'INTC', 'MA', 'NVDA', 'CSCO', 'ADBE', 'PYPL', 'CRM']
run = 0


while True:
    Create()    
    StockTwits(stocks)
    yahoo_data(stocks)
    run += 1

    c = sqlite3.connect('sentiment_yahoo.db')

    twits = pd.read_sql_query("SELECT * from twits", c)
    yahoo = pd.read_sql_query("SELECT * from yahoo_data", c)
    c.close()
    count = len(twits.index)
    count2 = len(yahoo.index)
    print('This code ran {} times and the dataset currently has {} lines of Tweets, {} lines of OCHL data' .format(run,count, count2) )
    time.sleep(240)
