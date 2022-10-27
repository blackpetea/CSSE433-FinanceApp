#waht is auto commit.
# what is the earliest , latest commit stuff
# does the topic recognize this consumer as the same consumer?



import pymongo
from bson import ObjectId
from pymongo import errors
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from pymongo import MongoClient
import json
consumer = KafkaConsumer(
    # 'testtopic', # somehow assigning topic here not working for partition stuff
    # auto_offset_reset='earliest', # 暂时不知道有什么用
    enable_auto_commit = True,# 暂时不知道有什么用
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    # group_id = 'the_only_group', # i think you guys will have different groups for influxdb
    client_id = 'company_Create_Consumer' # name for this single consumer client
)

topic = TopicPartition('testtopic2', 0) #0 is the partition, bad naming,
consumer.assign([topic])

client = MongoClient(host = ["localhost:27017"], serverSelectionTimeoutMS = 500)

counter = 0
# mongoid = '635965a46689bbab7a886ee3'
# print(str(client['company1'].company2.find_one({"_id": ObjectId(mongoid)})))
# print(str(client['company1'].company2.find_one({"_id" : ObjectId('63595c5f843e3de6556b8190')})))


failList = []

while True:
    #current-offset and log-end-offset

    #if mongo insert failed, go back one offset(or save the offset temporarily)
    try:

        if(client.company1.command('ping')['ok'] == 1.0):#尝试去，ping mongo
            msg = next(consumer)  # 如果 mongo 有反应，我就去consumer里拿下一个message， 最最重要的 一行
            # message = message.value # you can do this actually
            if (msg[6]['Operation'] == 'CREATE'):  # 拿到了message 就可以insert到mongo里去了
                counter = counter + 1
                print("insert success: " + str(counter))
                print("current topic position:" +str(consumer.position(topic)))


                mongoid = client['company1'].company2.insert_one(msg[6]['JSONDATA'])

                try:
                    if(client['company1'].company2.find_one({"_id": ObjectId(mongoid.inserted_id)}) == None):
                        failList.append(msg[6]['JSONDATA']["id"])
                    # print(str(client['company1'].company2.find_one({"_id" : ObjectId(mongoid)})))
                except:
                    failList.append(msg[6]['JSONDATA']["id"])
                print("failedList: " + str(failList))


    # except pymongo.errors.ServerSelectionTimeoutError:# when can't insert due to shut down mongo
    #     print ("pymongo's mistake")
    #

    except:
        print("server down, connection error")

# print(type(message)) # message type 是一个tuple，要用数字index去access
# message index:
# 0: topic
# 2: offset
# 6: value (our json file)
#
# print("row 0: " + str(message[0]))
# print("row 6: " + str(message[6]))
# print(type(message[6])) # dictionary
# print(type(message[6]['JSONDATA']))# dictionary
# print("row json: " + str(message[6]['JSONDATA']))