from neo4j import GraphDatabase
import time

# driver = GraphDatabase.driver(encrypted=False, uri="bolt://localhost:7687",
#                               auth=("neo4j", "didisucks"))

driver = GraphDatabase.driver(uri = "neo4j://localhost:7687", auth=("neo4j", "darthvader521"))
session = driver.session()

def get_stock_news(symbol, date = None):

    if date is None:
        result = session.run("MATCH (c: Company {symbol: $symbol}) -[:IS_REPORTED_BY] -> (n: News) \
                        return n", symbol = symbol)
        val = result.data()
        print_list = []
        if len(val) != 0:
            print(f"[Searched News]")
            for record in val:
                # print("Summary: ", record['n']['summary'], ", Source: ", record['n']['source'],
                #       ", Author:", record['n']['author'], ", reported at time :", record['n']['created_at'])
                print_list.append("Summary: " + record['n']['summary'] + ", Source: " + record['n']['source']+
                      ", Author:" + record['n']['author'] + ", reported at time :"+ record['n']['created_at'])
        return print_list
    else:
        result = session.run("MATCH (c: Company {symbol: $symbol}) -[:IS_REPORTED_BY] -> (n: News) <- [:HAS] - (d: Date {date: $date})\
                               return n", symbol=symbol, date = date)
        val = result.data()
        print_list = []
        if len(val) != 0:
            print(f"[Searched News]")
            for record in val:
                # print("Summary: ", record['n']['summary'], ", Source: ", record['n']['source'],
                #       ", Author:", record['n']['author'], ", reported at time :", record['n']['created_at'])
                print_list.append("Summary: " + record['n']['summary'] + ", Source: " + record['n']['source'] +
                                  ", Author:" + record['n']['author'] + ", reported at time :" + record['n'][
                                      'created_at'])
        return print_list


# get_stock_news(symbol='NIO')

def get_stock_news_date(date):
    result = session.run("MATCH (d: Date {date: $date}) -[:HAS] -> (n: News) \
                    return n", date = date)
    val = result.data()
    print_list = []
    if len(val) != 0:
        print(f"[Searched News]")
        for record in val:
            # print("Summary: ", record['n']['summary'], ", Source: ", record['n']['source'],
            #       ", Author:", record['n']['author'], ", reported at time :", record['n']['created_at'])
            print_list.append("Summary: " + record['n']['summary'] + ", Source: " + record['n']['source']+
                  ", Author:" + record['n']['author'] + ", reported at time :"+ record['n']['created_at'])
    return print_list

# get_stock_news(symbol='NIO')
# get_stock_news_date(date='2022-05-11')
# get_stock_news(symbol='NIO', date= '2022-05-11')