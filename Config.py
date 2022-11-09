from pymongo import MongoClient
from influx_client import InfluxClient


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

def log_trade(acc, order_info, symbol):
    bucket = "trade_log"
    org = "rhit"
    token = "raewlvUeYdNj58oRvGZWV-Fro9YUQ1RIH3WV2zcJf3UQ5Jk1npRbiiOI48Q9sPQJQVc0mnxB5RMf1LaFwGOygg=="
    IC = InfluxClient(token, org, bucket)

    data = {'measurement': f'ACC_{acc.get("account_number")},', 'tags': {}, 'fields': {}}
    data['tags']['type'] = "order"
    data['tags']['equity_type'] = "us_equity"
    data['fields']['asset_id'] = str(order_info.get("asset_id"))
    data['fields']['symbol'] = str(symbol)
    data['fields']['client_order_id'] = str(order_info.get("client_order_id"))
    data['fields']['qty'] = order_info.get("qty")
    data['fields']['filled_qty'] = order_info.get("filled_qty")
    data['fields']['order_type'] = str(order_info.get("order_type"))
    data['fields']['side'] = str(order_info.get("side"))
    data['fields']['time_in_force'] = str(order_info.get("time_in_force"))
    IC.write_data(data)


# def OrderRequest(symbol=stock, qty=1, side="buy", type="market", time_in_force="day")