from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import requests, json
from neo4j import GraphDatabase
import time

driver = GraphDatabase.driver(encrypted=False, uri="bolt://localhost:7687",
                              auth=("neo4j", "darthvader521"))

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

def create_company_node_query_str(symbol):

    query_str = "MERGE (c:Company {symbol: '%(symbol)s'}) \
                ON CREATE \
                  SET c.symbol = '%(symbol)s'" %{"symbol":symbol}
    time.sleep(0.01)

    return query_str



def create_date_node(date):
    try:
        session.run("MERGE (d:Date {date: $date}) \
                    ON CREATE \
                      SET d.date = $date", date = date)
        time.sleep(0.01)
    except Exception as e:
        print(e.code)
        print(e.message)

def create_date_node_query_str(date):
    query_str = "MERGE (d:Date {date: '%(date)s'}) \
                    ON CREATE \
                      SET d.date = '%(date)s'"%{"date":date}
    time.sleep(0.01)
    return query_str

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


def create_news_node_query_str(newsID, author, created_at, source, summary, symbols, url):
    query_str = "CREATE (n: News {author: '%(author)s', \
                                         id:'%(newsID)s', \
                                         created_at: '%(created_at)s', \
                                         source: '%(source)s', \
                                         summary: '%(summary)s', \
                                         symbols: %(symbols)s, \
                                         url: '%(url)s'})"%{"author":author, "newsID": newsID,
                                                             "created_at" : created_at, "source": source,
                                                             "summary": summary, "symbols": symbols,
                                                             "url": url}
    time.sleep(0.01)
    # print(query_str)
    return query_str

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

def create_relationship_IS_REPORTED_BY_query_str(symbol, newsID):
    query_str = "MATCH \
                  (c:Company {symbol: '%(symbol)s'}), \
                  (n:News {id: '%(newsID)s'}) \
                MERGE (n) <- [reported_by:IS_REPORTED_BY] - (c)"\
                %{"symbol":symbol, "newsID": newsID}

    time.sleep(0.01)
    return query_str



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


def create_relationship_HAS_query_str(date, newsID):
    query_str = "MATCH \
                  (d:Date {date: '%(date)s'}), \
                  (n:News {id: '%(newsID)s'}) \
                MERGE (n) <- [reported_by:HAS] - (d)"%{"date":date, "newsID": newsID}
    time.sleep(0.01)
    return query_str


session.run("MATCH (n) detach delete n")

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

admin_client.delete_topics(topics= ['news_data']) # run every 15 mins
producer.send('news_data', {'query_input':'[to-create topic] bad_test_start'})

API_NAME = 'PK4KNHN1288FU75AUBDI'
API_KEY = '2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj'

headers = { "APCA-API-KEY-ID": "PK4KNHN1288FU75AUBDI",
    "APCA-API-SECRET-KEY": "2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj"}

data_url = "https://data.alpaca.markets"


symbols = ['TSLA', 'AAPL', 'SPY', 'JPM', 'META']
start_time = "2022-05-01T19:00:00Z"
end_time = "2022-05-11T19:00:00Z"

for symbol in symbols:
    start_time ="2022-05-01T19:00:00Z"
    end_time = "2022-05-11T19:00:00Z"
    r = requests.get(f'{data_url}/v1beta1/news?symbols={symbol}&start={start_time}&end={end_time}&limit=50', headers = headers)
    # print(r)
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

        # producer.send('news_data', {'query_input': create_company_node_query_str(date)})
        # producer.send('news_data', {'query_input': create_news_node_query_str(newsID=newsID, author=author, created_at=created_at,
        #                  source=source, summary=summary, symbols=symbols, url=url)})

        # session.run(create_company_node_query_str(date))
        # session.run(create_news_node_query_str(newsID=newsID, author=author, created_at=created_at,
        #                  source=source, summary=summary, symbols=symbols, url=url))
        # session.run(create_relationship_HAS_query_str(date=date, newsID=newsID))

        producer.send('news_data', {'query_input': create_company_node_query_str(symbol=symbol)})
        producer.send('news_data', {'query_input': create_news_node_query_str(newsID=newsID, author=author, created_at=created_at,
                         source=source, summary=summary, symbols=symbols, url=url)})
        producer.send('news_data',
                      {'query_input': create_date_node_query_str(date=date)})
        producer.send('news_data', {'query_input': create_relationship_HAS_query_str(date=date, newsID=newsID)})


        for symbol in symbols:
            producer.send('news_data', {'query_input': create_company_node_query_str(symbol=symbol)})
            producer.send('news_data', {'query_input': create_relationship_IS_REPORTED_BY_query_str(symbol=symbol, newsID=newsID)})
            # session.run(create_company_node_query_str(symbol=symbol))
            # session.run(create_relationship_IS_REPORTED_BY_query_str(symbol=symbol, newsID=newsID))
            time.sleep(0.01)

        print("Inserted a news")
    time.sleep(1)