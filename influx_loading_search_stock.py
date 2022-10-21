import json
from datetime import datetime
import time

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
bucket = "bucket-test"
org = "rhit"
token = "ygSb-vEX8UOTnQL0Yak_D7MDTsxNnWimsVDFGypu8hZXvWGWEGo1VG5FXbeiLileZ-2DRmj78FUw64tfjZP_DA=="


# print(data.to_csv())

class InfluxClient:
    def __init__(self, token, org, bucket):
        self._org = org
        self._bucket = bucket
        self._client = InfluxDBClient(url="http://localhost:8086", token=token)

    def write_data(self, data, write_option=SYNCHRONOUS):
        write_api = self._client.write_api(write_option)
        write_api.write(self._bucket, self._org, data, write_precision='s')

    # datetime.strptime("2021-07-05", '%Y-%m-%d'
    def load_stock_data_API(self, stock, start_time, stop_time, time_frame=TimeFrame.Day):
        our_client = StockHistoricalDataClient('PK4KNHN1288FU75AUBDI',
                                               '2pfsnioo6cjRVs4PqVlPHKd2WRl47yndffIVqQpj')
        # multi symbol request - single symbol is similar
        request_data = StockBarsRequest(symbol_or_symbols=[stock],
                                        start=start_time,
                                        end=stop_time,
                                        timeframe=time_frame)
        data_bar = our_client.get_stock_bars(request_data)
        # print(len(data_bar.data.get(stock)))

        batch_number = 50
        for i in range(0, len(data_bar.data.get(stock)), batch_number):
            rows = []
            if i + batch_number > len(data_bar.data.get(stock)):
                batch_number = len(data_bar.data.get(stock)) - i

            for k in range(batch_number):
                row = data_bar.data.get(stock)[i + k].dict()
                date, symbol, open, high, low, close, volume, trade_count, vwap = row["timestamp"], row['symbol'], row[
                    "open"], row["high"], row["low"], row["close"], row["volume"], row["trade_count"], row["vwap"]
                line_protocol_string = ''
                line_protocol_string += f'stock_{symbol},'
                line_protocol_string += f'stock={symbol} '
                line_protocol_string += f'open={open},high={high},low={low},close={close},' \
                                        f'volume={volume},trade_count={trade_count},vwap={vwap} '
                line_protocol_string += str(int(date.timestamp()))
                rows.append(line_protocol_string)
            self.write_data(rows)
            print("Update batch")
            time.sleep(0.1)

    def get_stock_data(self, stock, start_time, stop_time):
        start_time_int = int(start_time.timestamp())
        stop_time_int = int(stop_time.timestamp())
        query_client = self._client.query_api()
        # query_input = f'from(bucket: "{self._bucket}")\
        #                 |> range(start: {start_time_int})\
        #                 |> filter(fn: (r) => r._field == "high")\
        #                 |> filter(fn: (r) => r.stock == "{stock}")'
        query_input =  f'from(bucket:"{self._bucket}")\
                                                   |> range(start: {start_time_int}, stop: {stop_time_int}) \
                                                   |> filter(fn: (r) => r["_measurement"] == "stock_{stock}")\
                                                   |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")\
                                                   |> keep(columns: ["_measurement","_time","open", "high", "low", "close","volume", "trade_count","vwap"])'
        data_frame = query_client.query_data_frame(org=self._org,query=query_input)
        # print(data_frame.to_string())
        print("success")
        return data_frame




start_time=datetime.strptime("2021-10-19", '%Y-%m-%d')
stop_time=datetime.strptime("2022-10-19", '%Y-%m-%d')

test_stocks = ['NIO', 'TSLA', 'AAPL', 'SPY', 'JPM', 'META']

def get_stock_price_json(inputDic):
    bucket = "bucket-test"
    org = "rhit"
    token = "ygSb-vEX8UOTnQL0Yak_D7MDTsxNnWimsVDFGypu8hZXvWGWEGo1VG5FXbeiLileZ-2DRmj78FUw64tfjZP_DA=="

    IC = InfluxClient(token, org, bucket)
    start_time = datetime.strptime(inputDic['startTime'], '%Y-%m-%d')
    stop_time = datetime.strptime(inputDic['endTime'], '%Y-%m-%d')


    result_dataframe = IC.get_stock_data(stock=inputDic['stock'], start_time=start_time, stop_time=stop_time)
    _times = result_dataframe['_time'].apply(lambda x: x.strftime("%m/%d/%Y, %H:%M:%S"))
    # print(_times)
    result_dataframe['_time'] = _times
    result_dataframe = result_dataframe.transpose()
    # result_dataframe = IC.get_stock_data(stock=inputDic['stock'], start_time=start_time, stop_time=stop_time).transpose()
    return result_dataframe.to_json()

def processJson(inputJson):

    result = json.loads(inputJson)

    bigList = []

    for count, item in enumerate(result, start=0):
        smallDic = {}
        # smallDic['result'] = result[str(count)]["result"]
        # smallDic['table'] = result[str(count)]["table"]
        smallDic['_time'] = result[str(count)]["_time"]
        smallDic['_measurement'] = result[str(count)]["_measurement"]
        smallDic['close'] = result[str(count)]["close"]
        smallDic['high'] = result[str(count)]["high"]
        smallDic['low'] = result[str(count)]["low"]
        smallDic['open'] = result[str(count)]["open"]
        smallDic['trade_count'] = result[str(count)]["trade_count"]
        smallDic['volume'] = result[str(count)]["volume"]
        smallDic['vwap'] = result[str(count)]["vwap"]
        bigList.append(smallDic)
    return bigList
