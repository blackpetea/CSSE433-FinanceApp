from flask import Flask, render_template, request, url_for, redirect
from AllinOne import *
from influx_loading_search_stock import *

app = Flask(__name__)


@app.route("/")
def main():

    return render_template('index.html')


@app.route("/search_company")
def search_company():
    return render_template('search_company.html')


@app.route('/return_company', methods = ['GET', 'POST'])
def CompanySearchResult():
    form = request.form

    # headings = ('name', 'symbol','exchange','asset_class','status','tradable','marginable','shortable','easy_to_borrow','fractionable')
    headings = ('Name', 'Symbol','Exchange','Asset Class','Status','Tradable','Marginable','Shortable','Easy_to_borrow','Fractionable')

    inputDic = {}

    if(form["compName"] != ''): inputDic['name'] = form["compName"]
    if(form["compSymbol"] != ''): inputDic['symbol'] = form["compSymbol"]
    if(form["compExchange"] != ''): inputDic['exchange'] = form["compExchange"]

    createIndex()# need to create index before doing text search, but we already have one.

    return render_template('UglyCompanies2.html', headings = headings, data = search(inputDic))


@app.route("/search_news")
def search_the_news():
    return render_template('search_news.html')


@app.route("/get_stock")
def get_stock():
    return render_template('get_stock.html')

@app.route('/return_stock', methods = ['GET', 'POST'])
def StockSearchResult():

    form = request.form

    # headings = ('_time','_measurement','close','high','low','open','trade_count', 'volume', 'vwap')
    headings = ('Time', 'Measurement', 'Close', 'High', 'Low', 'Open', 'Trade Count', 'Volume', 'Vwap')

    inputDic = {
        'stock': form["stock"],
        'startTime': form["startTime"],
        'endTime': form["endTime"]
    }

    first_result = get_stock_price_json(inputDic)#unprocessed json from api.
    second_result = processJson(first_result)#processed, formatted json

    print(json.dumps(json.loads(first_result), indent=2)) # prettyprint for json
    # print(second_result)

    return render_template('StockTable.html', headings = headings, data = second_result)


@app.route("/make_order")
def search_news():
    return render_template('make_order.html')



if __name__ == "__main__":
    app.run()






