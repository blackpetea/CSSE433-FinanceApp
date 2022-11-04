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
from neo4j import GraphDatabase

driver = GraphDatabase.driver(encrypted=False, uri="bolt://localhost:7687",
                              auth=("neo4j", "didisucks"))

session = driver.session()


consumer = KafkaConsumer(
    enable_auto_commit = True,
    bootstrap_servers=['localhost:9092'],
    # auto_offset_reset='latest', # read from the first index
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id = 'neo4j_group',
    client_id = 'ne04j_consumer'
)

topic = TopicPartition('news_data', 0) #0 is the partition
consumer.assign([topic])

session.run("MATCH (n) detach delete n")

failed = False

while True:
    if failed:
        consumer.seek(TopicPartition("news_data", 0), 0)
        time.sleep(0.1)
        failed = False
    try:
        if session.run("CALL db.ping()").data()[0]['success']:
            msg = next(consumer)  # 如果 mongo 有反应，我就去consumer里拿下一个message， 最最重要的 一行
            current_offset = msg.offset
            if msg.offset %100 ==0:
                print(msg.offset)
            try:
                # print(msg.value.get('query_input'))
                session.run(msg.value.get('query_input'))
            except Exception as e:
                print(e.code, e.message)
            failed = False
        else:
            failed = True
            # print(current_offset)
            print("Neo4j server is down")
            time.sleep(1)

    except:
        current_offset = current_offset - 1
        failed = True
        print("Break at offset (-1 means consumer does not consume anything):", current_offset)
        print("Connection error")
        time.sleep(1)
