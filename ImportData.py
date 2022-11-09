import time
#producer code start---------------------------------------------------------------------------------
from Config import *
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import time
mongoProducer = KafkaProducer(bootstrap_servers = ['433-25.csse.rose-hulman.edu:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks = 1)
admin_client = KafkaAdminClient(bootstrap_servers = ['433-25.csse.rose-hulman.edu:9092'])
admin_client.delete_topics(topics=['testtopic2'])
mongoProducer.send('testtopic2', {'Operation': 'Test-Operation - initialize topic'})

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
# n_limit = 10000

allAsset = tradingClient.get_all_assets()
# allAsset = allAsset[0:n_limit]

allAsset = allAsset[0:2000]
print("got assets:" + str(len(allAsset)))
# get all stock symbols
symbols = [json.loads(x.json())['symbol'] for x in allAsset]

# client = MongoClient('mongodb://137.112.104.220:27017')#433-23 2号机
# client = pymongo.MongoClient('localhost', 27017)
# client = MongoClient(host = ["localhost:27017"], serverSelectionTimeoutMS = 500)
# client['company1']['company2'].drop()
# db = client['company']
# company = db.Company

def DownloadAllCompData():
    count = 0
    for x in allAsset:
        # print(x)
        xjson = json.loads(str(x.json()))

        finalData = {}
        finalData['Operation'] = 'CREATE'
        finalData['Group'] = 'COMPANY'
        finalData['JSONDATA'] = xjson

        mongoProducer.send('testtopic2',finalData)
        count = count +1
        time.sleep(0.001)
    print('dataImportFinished')
    print("producer count" + str(count))





#initiate this file
DownloadAllCompData()
