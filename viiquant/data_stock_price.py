import requests as req
import pandas as pd

from datetime import datetime as dt
from datetime import timezone as tz
from zoneinfo import ZoneInfo

from typing import Union
from typing import List
from typing import Optional
from typing import Any


class DataStockPrice:
    
    def __init__(self, vnd_root_uri: str=None, entrade_root_uri: str=None, vps_root_uri:str=None, ticker_type:str='stock'):
        self.VNDIRECT_ROOT_URI = "https://finfo-api.vndirect.com.vn/v4/stock_prices/"
        self.ENTRADE_ROOT_URI = "https://services.entrade.com.vn/chart-api/v2/ohlcs/"
        self.VPS_ROOT_URI = "https://bgapidatafeed.vps.com.vn/getliststockdata/"
        
        if vnd_root_uri != None:
            self.VNDIRECT_ROOT_URI = vnd_root_uri

        if entrade_root_uri != None:
            self.ENTRADE_ROOT_URI = entrade_root_uri

        if vps_root_uri != None:
            self.VPS_ROOT_URI = vps_root_uri
    
        # self.columnName = ['open', 'high', 'low', 'close', 'volume']
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1788.0', 
            'DNT': '1'
        }

        self._ticker_type = ticker_type

    def get_lastest_price_rows(self, ticker: str, start_date: str, end_date: str, curr_last_timestamp: int, bar_size: int=1, bar_type: str='D') -> List[dict]:
        lst_prices = self.get_historical_price(ticker, start_date, end_date, bar_size, bar_type)
        if len(lst_prices) == 0:
            return []
        
        # obj_filtered = filter(lambda x: dt.strptime(x['datetime'], '%Y-%m-%d %H:%M:%S') > dt.strptime(curr_last_time, '%Y-%m-%d %H:%M:%S'), lst_prices)
        obj_filtered = filter(lambda x: x['ts'] > curr_last_timestamp, lst_prices)
        # print("get_lastest_price_rows", list(obj_filtered))

        return list(obj_filtered)
        

    def get_historical_price(self, ticker: str, start_date: str, end_date: str, bar_size: int=1, bar_type: str='D') -> List[Optional[dict]]:

        start_date = f"{start_date} 00:00:00" # '%Y-%m-%d %H:%M:%S'
        end_date = f"{end_date} 23:59:59" # '%Y-%m-%d %H:%M:%S'

        if bar_type.upper() == 'D':
            return self.get_historical_price_by_vnd(ticker, start_date, end_date)
        
        if bar_type in ['m', 'H']:
            return self.get_historical_price_by_entrade(ticker, start_date, end_date, bar_size, bar_type)
        
        return []
    

    def get_historical_price_by_vnd(self, ticker: str, start_date: str, end_date: str) -> List[Optional[dict]]:
        _start = dt.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        _end = dt.strptime(end_date, '%Y-%m-%d %H:%M:%S')

        query = 'code:' + ticker + '~date:gte:' + _start.strftime('%Y-%m-%d') + '~date:lte:' + _end.strftime('%Y-%m-%d')
        delta = _end - _start
        _params = {
            "sort": "date",
            "size": delta.days + 1,
            "page": 1,
            "q": query
        }

        keys_mapping = {
            'ts': 'ts',
            'datetime': 'datetime',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'nmVolume'
        }

        try:
            response = req.get(self.VNDIRECT_ROOT_URI, params=_params, headers=self.headers)
            if response.status_code == 200:
                json_data = response.json()['data']
                json_data = sorted(json_data, key=lambda x: x['date'])

                lst_prices: List[dict] = []
                for i in range(len(json_data)):
                    item = {
                        'ticker': ticker
                    }

                    for k in keys_mapping:
                        data_key = keys_mapping[k]
                        if k == 'ts':
                            item[k] = int(dt.strptime(json_data[i]['date'] + ' ' + json_data[i]['time'], '%Y-%m-%d %H:%M:%S').timestamp())
                        elif k == 'datetime':
                            item[k] = json_data[i]['date'] + ' ' + json_data[i]['time']
                        else:
                            item[k] = json_data[i][data_key]
                    
                    lst_prices.append(item)

                return lst_prices

        except Exception as err:
            print('ERROR: ', err)
        
        return []
    
    

    def get_historical_price_by_entrade(self, ticker: str, start_date: str, end_date: str, bar_size: int=1, bar_type: str='D') -> List[Optional[dict]]:
        _start = dt.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        _end = dt.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        resolution = ''

        if bar_type == 'H':
            resolution = '1H'
        elif bar_type == 'D':
            resolution = '1D'
        else:
            resolution = bar_size
            
        url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{self._ticker_type}"
        _params = {
            'from': int(_start.timestamp()),
            'to': int(_end.timestamp()),
            'symbol': ticker,
            'resolution': resolution
        }

        # url2 = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{self._ticker_type}?from={_params['from']}&to={_params['to']}&symbol={_params['symbol']}&resolution={_params['resolution']}"
        # print(url2)

        keys_mapping = {
            'ts': 't',
            'datetime': 't',
            'open': 'o',
            'high': 'h',
            'low': 'l',
            'close': 'c',
            'volume': 'v'
        }

        try:
            response = req.get(url, params=_params, headers=self.headers)
            if response.status_code == 200:
                json_data = response.json()
                cols = list(json_data.keys())
                cols.remove('nextTime')

                lst_prices: List[dict] = []
                for i in range(len(json_data['t'])):
                    item = {
                        'ticker': ticker
                    }

                    for k in keys_mapping:
                        data_key = keys_mapping[k]
                        if k == 'datetime':
                            item[k] = self.__timestamp_to_datetime(json_data[data_key][i])
                        else:
                            item[k] = json_data[data_key][i]
                    
                    lst_prices.append(item)
                
                return lst_prices
            
        except Exception as err:
            print('ERROR: ', err)
            
        return []
    
    def get_market_quotes(self, tickers:List[str]):
        url = f"{self.VPS_ROOT_URI}{','.join(tickers)}"

        rename_cols = {
            'sym': 'Mã CP',
            'c': 'Giá Trần',
            'f': 'Giá Sàn',
            'r': 'Giá tham chiếu',
            'lot': 'Tổng Khối Lượng',
            'highPrice': 'Giá cao',
            'lowPrice': 'Giá thấp',
            'avePrice': 'Giá TB',
            'lastPrice': 'Giá khớp lệnh',
            'lastVolume': 'KL Khớp lệnh',
            'ot': '+/- (Khớp lệnh)',
            'changePc': '% (Khớp lệnh)',
            'fBVol': 'ĐTNN Mua',
            'fSVolume': 'ĐTNN Bán',
            'fRoom':'ĐTNN Room'
        }

        data = dict.fromkeys(tickers)
        try:
            response = req.get(url, headers=self.headers)
            if response.status_code == 200:
                json_data = response.json()
                for item in json_data:
                    data[item['sym']] = item
                return data

        except Exception as err:
            print('ERROR: ', err)
        
        return data


    # Callback function for convert utc time
    # def __convert_utc_time(self, x):
    #     d = pd.to_datetime(x, unit='s', origin='unix', utc=True).tz_convert('Asia/Ho_Chi_Minh')
    #     return pd.to_datetime(d.strftime('%Y-%m-%d %H:%M:%S'))
    
    
    def __timestamp_to_datetime(self, timestamp:int):
        return dt.fromtimestamp(timestamp, tz=ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')