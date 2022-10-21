
from pymongo import MongoClient


def search(inputDic):
    if 'name' not in inputDic:
        result = company.find(inputDic)
        print(inputDic)
        print(str(company.count_documents(inputDic)))

    else:
        removedName = inputDic.pop('name', None)
        result = company.find({'$and' : [{ '$text': { '$search': removedName}},inputDic]})

    return result


def createIndex():
    company.drop_indexes()
    company.create_index([('name', 'text')], default_language='english') # just for name field
    # company.create_index([("$**", 'text')], default_language='english') # for all text field
    # print(company.index_information())

def DeleteAllCompData():
    company.delete_one({})


client = MongoClient('mongodb://137.112.104.220:27017')#433-23 2号机

db = client['company']
company = db.Company


