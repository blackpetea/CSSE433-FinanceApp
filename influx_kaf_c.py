import json
from kafka import KafkaProducer
import time
import pymongo
from bson import ObjectId
from pymongo import errors
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from pymongo import MongoClient
import json
from influx_client import InfluxClient

consumer = KafkaConsumer(
    enable_auto_commit = True,
    bootstrap_servers=['433-25.csse.rose-hulman.edu:9092'],
    # auto_offset_reset='earliest', # read from the first index
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id = 'influx_group',
    client_id = 'influx_consumer'
)

topic = TopicPartition('stock_data', 0) #0 is the partition
consumer.assign([topic])

bucket = "stock"
org = "rhit"
token = "raewlvUeYdNj58oRvGZWV-Fro9YUQ1RIH3WV2zcJf3UQ5Jk1npRbiiOI48Q9sPQJQVc0mnxB5RMf1LaFwGOygg=="

IC = InfluxClient(token, org, bucket)
current_offset = -1
k = 0
failed = True

while True:
    if failed:
        IC = InfluxClient(token, org, bucket)
        consumer.seek(TopicPartition("stock_data", 0), 0)
        time.sleep(0.1)
        failed = True
    try:
        if IC._client.ping():#尝试去，ping mongo
            msg = next(consumer)  # 如果 mongo 有反应，我就去consumer里拿下一个message， 最最重要的 一行
            # print(msg.value.get('query_input'))
            current_offset = msg.offset
            print(msg.offset)
            IC.write_data(msg.value.get('query_input'))
            failed = False
        else:
            failed = True
            # print(current_offset)
            print("Influx server is down")
            time.sleep(0.1)

    except:
        current_offset = current_offset - 1
        failed = True
        print("Break at offset (-1 means consumer does not consume anything):", current_offset)
        print("Connection error")
        time.sleep(0.1)




