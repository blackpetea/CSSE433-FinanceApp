import json
from kafka import KafkaProducer
import time
import datetime
from kafka.admin import KafkaAdminClient
from influx_client import InfluxClient


admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

admin_client.delete_topics(topics= ['stock_data']) # run every 15 mins
producer.send('stock_data', {'query_input':'[to-create topic] bad_test_start'})

import json
from datetime import datetime
import time

# You can generate a Token from the "Tokens Tab" in the UI
bucket = "bucket-test"
org = "organization-test"
token = "fCD_yfLk_iDUPe52t3Zvel8c8d8PFEKPY7uyvcofi4mPPiBb9yp5sba0j0nP73pVgrE6Y6tLTyLW701OPqCnow=="


start_time=datetime.strptime("2021-8-19", '%Y-%m-%d')
stop_time=datetime.strptime("2022-10-19", '%Y-%m-%d')

test_stocks = ['TSLA', 'AAPL', 'SPY', 'JPM', 'META']

IC = InfluxClient(token, org, bucket)

for stock in test_stocks:
    print(stock)
    query_input_list = IC.get_load_query_stock_data_API(stock=stock, start_time=start_time, stop_time=stop_time)
    for rows in query_input_list:
        producer.send('stock_data', {'query_input': rows})
        time.sleep(0.1) #send data to que every 0.1 second