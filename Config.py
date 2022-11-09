from pymongo import MongoClient


client = MongoClient(host = ['mongodb://137.112.104.222:27017'], serverSelectionTimeoutMS = 500)
company = client['companydb']['companycol']


def createIndex():
    company.drop_indexes()
    company.create_index([('name', 'text')], default_language='english')  # just for name field
    # company.create_index([("$**", 'text')], default_language='english') # for all text field

def DeleteAllData():
    company.deleteMany()
    print("Company Collection is deleted")

def search(inputDic):
    if 'name' not in inputDic:
        result = company.find(inputDic)
        print(inputDic)
        print(str(company.count_documents(inputDic)))

    else:
        removedName = inputDic.pop('name', None)
        result = company.find({'$and' : [{ '$text': { '$search': removedName}},inputDic]})

    return result