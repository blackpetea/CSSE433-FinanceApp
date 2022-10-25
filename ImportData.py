import time
#producer code start---------------------------------------------------------------------------------

import json
from kafka import KafkaProducer
import time
mongoProducer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# producer.send('testtopic', {'aaa':'bbb'})

time.sleep(10)

#producer code end-----------------------------------------------------------------------------------

from pymongo import MongoClient
from alpaca.trading import TradingClient, GetAssetsRequest
import json

import urllib
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime


datetime.strptime('2014-12-04', '%Y-%m-%d')
APIUSER = 'PK4KNHN1288FU75AUBDI'
APIPWD = '2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj'

tradingClient = TradingClient(APIUSER, APIPWD)
allAsset = tradingClient.get_all_assets()
print("got assets")
# get all stock symbols
symbols = [json.loads(x.json())['symbol'] for x in allAsset]



client = MongoClient('mongodb://137.112.104.220:27017')#433-23 2号机
# client = pymongo.MongoClient('localhost', 27017)

db = client['company']
company = db.Company

def DownloadAllCompData():
    for x in allAsset:
        print(x)
        xjson = json.loads(str(x.json()))
        # producer.send('testtopic', {'aaa':'bbb'})
        finalData = {}
        finalData['Operation'] = 'CREATE'
        finalData['Group'] = 'COMPANY'
        finalData['JSONDATA'] = xjson

        mongoProducer.send('testtopic',finalData)

        # company.insert_one(xjson)

def createIndex():
    company.drop_indexes()
    company.create_index([('name', 'text')], default_language='english') # just for name field
    # company.create_index([("$**", 'text')], default_language='english') # for all text field




DownloadAllCompData()
# createIndex()





# import time
#
# from pymongo import MongoClient
# from alpaca.trading import TradingClient, GetAssetsRequest
# import json
#
# import urllib
# from alpaca.data.historical import StockHistoricalDataClient
# from alpaca.data.requests import StockBarsRequest
# from alpaca.data.timeframe import TimeFrame
# from datetime import datetime
#
#
# datetime.strptime('2014-12-04', '%Y-%m-%d')
# APIUSER = 'PK4KNHN1288FU75AUBDI'
# APIPWD = '2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj'
#
# tradingClient = TradingClient(APIUSER, APIPWD)
# allAsset = tradingClient.get_all_assets()
# print("got assets")
# # get all stock symbols
# symbols = [json.loads(x.json())['symbol'] for x in allAsset]
#
#
#
# client = MongoClient('mongodb://137.112.104.220:27017')#433-23 2号机
# # client = pymongo.MongoClient('localhost', 27017)
#
# db = client['company']
# company = db.Company
#
# def DownloadAllCompData():
#     for x in allAsset:
#         print(x)
#         xjson = json.loads(str(x.json()))
#         company.insert_one(xjson)
#
# def createIndex():
#     company.drop_indexes()
#     company.create_index([('name', 'text')], default_language='english') # just for name field
#     # company.create_index([("$**", 'text')], default_language='english') # for all text field
#
#
#
#
# DownloadAllCompData()
# createIndex()