from kafka import KafkaConsumer
from pymongo import MongoClient
import json
consumer = KafkaConsumer(
    'testtopic',
    # auto_offset_reset='earliest', # 暂时不知道有什么用
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

client = MongoClient(host = ["localhost:27017"], serverSelectionTimeoutMS = 500)


while True:

    try:#尝试去，ping mongo

        client.MongoCompany.command('ping')
        print("connection success")


    except:
        print("server down, connection error")
        break; # 如果mongo 没反应，我就break

    msg = next(consumer)#如果 mongo 有反应，我就去consumer里拿下一个message

    if(msg[6]['Operation'] == 'CREATE'): # 拿到了message 就可以insert到mongo里去了

        client['company1'].company2.insert_one(msg[6]['JSONDATA'])


# print(type(message)) # message type 是一个tuple，要用数字index去access

# 0: topic
# 2: offset
# 6: value (our json file)
#
# print("row 0: " + str(message[0]))
# print("row 6: " + str(message[6]))
# print(type(message[6])) # dictionary
# print(type(message[6]['JSONDATA']))# dictionary
# print("row json: " + str(message[6]['JSONDATA']))


