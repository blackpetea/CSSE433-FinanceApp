import alpaca_trade_api as tradeapi
import requests, json
import pandas as pd
from neo4j import GraphDatabase
import time



API_NAME = 'PK4KNHN1288FU75AUBDI'
API_KEY = '2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj'

headers = { "APCA-API-KEY-ID": "PK4KNHN1288FU75AUBDI",
    "APCA-API-SECRET-KEY": "2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj"}

# data_url = "https://paper-api.alpaca.markets"
data_url = "https://data.alpaca.markets"


driver = GraphDatabase.driver(encrypted=False, uri="bolt://localhost:7687",
                              auth=("neo4j", "didisucks"))
session = driver.session()


def create_company_node(symbol):
    try:
        session.run("MERGE (c:Company {symbol: $symbol}) \
                    ON CREATE \
                      SET c.symbol = $symbol", symbol = symbol)
        time.sleep(0.01)
    except Exception as e:
        print(e.code)
        print(e.message)

def create_date_node(date):
    try:
        session.run("MERGE (d:Date {date: $date}) \
                    ON CREATE \
                      SET d.date = $date", date = date)
        time.sleep(0.01)
    except Exception as e:
        print(e.code)
        print(e.message)

def create_news_node(newsID, author, created_at, source, summary, symbols, url):
    try:
        session.run("CREATE (n: News {author: $author, \
                                     id:$newsID, \
                                     created_at: $created_at, \
                                     source:$source, \
                                     summary:$summary, \
                                     symbols:$symbols, \
                                     url:$url})", newsID =newsID, author=author
                    , created_at=created_at, source=source,
                    summary=summary, symbols=symbols, url=url)
        time.sleep(0.01)
        print("Successful insert a news")
    except Exception as e:
        print(e.code)
        print(e.message)

def create_relationship_IS_REPORTED_BY(symbol, newsID):
    try:
        session.run("MATCH \
                  (c:Company {symbol: $symbol}), \
                  (n:News {id: $newsID}) \
                MERGE (n) <- [reported_by:IS_REPORTED_BY] - (c)", symbol=symbol,newsID=newsID)
        time.sleep(0.01)
    except Exception as e:
        print(e.code)
        print(e.message)

def create_relationship_HAS(date, newsID):
    try:
        session.run("MATCH \
                  (d:Date {date: $date}), \
                  (n:News {id: $newsID}) \
                MERGE (n) <- [reported_by:HAS] - (d)",
                    date=date, newsID=newsID)
    except Exception as e:
        print(e.code)
        print(e.message)

session.run("MATCH (n) detach delete n")

symbols = ['NIO', 'TESLA', 'SPY', 'META', 'AAPL']

for symbol in symbols:
    start_time ="2022-05-01T19:00:00Z"
    end_time = "2022-05-11T19:00:00Z"
    r = requests.get(f'{data_url}/v1beta1/news?symbols={symbol}&start={start_time}&end={end_time}&limit=50', headers = headers)
    print(r)
    data = r.json()['news']

    for news in data:
        author = news['author']
        created_at = news['created_at']
        date = created_at[0:10]
        newsID = news['id']
        source = news['source']
        summary = news['summary']
        symbols = news['symbols']
        url = news['url']

        create_date_node(date)
        create_news_node(newsID=newsID, author=author, created_at=created_at,
                         source=source, summary=summary, symbols=symbols, url=url)
        create_relationship_HAS(date=date, newsID=newsID)
        for symbol in symbols:
            create_company_node(symbol=symbol)
            create_relationship_IS_REPORTED_BY(symbol=symbol,newsID=newsID)
            time.sleep(0.01)

    time.sleep(1)



# print(f'{data_url}/v1beta1/news?symbols={symbols}')