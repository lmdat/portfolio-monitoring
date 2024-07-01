import numpy as np
import pandas as pd

from pandas.core.groupby import DataFrameGroupBy
from pandas.core.window import RollingGroupby
from pandas.core.window import Window

from datetime import datetime as dt

from typing import List
from typing import Dict
from typing import Union


class StockPriceFrame:

    def __init__(self, origin_data: Dict[str, List[dict]]):
        self._data = origin_data        
        self._price_frame: pd.DataFrame = None
        self._ticker_groupby: DataFrameGroupBy = None
        self._ticker_groupby_rolling: RollingGroupby = None
        self.create_data_frame()
        self.get_ticker_groupby()
    
    def create_data_frame(self) -> pd.DataFrame:
        """
        Tạo dataframe từ historical data lấy từ API
        """
        lst_prices = []
        for k in self._data:           
            # Merge các dữ liệu giá của các ticker lại với nhau thành một list duy nhất
            lst_prices += self._data[k]

        # Tạo dataframe
        cols = ['ticker', 'ts', 'datetime', 'open', 'high', 'low', 'close', 'volume']
        if len(lst_prices) == 0:
            df = pd.DataFrame(columns=cols)
        else:
            df = pd.DataFrame(lst_prices, columns=cols)

        df['datetime'] = pd.to_datetime(df['datetime'])

        # Tạo multi index từ ticker và timestamp
        df = df.set_index(keys=['ticker', 'ts'])

        df.sort_index(inplace=True)

        self._price_frame = df

        return self._price_frame
       
    
    def add_new_row_price(self, new_rows: Dict[str, List[dict]]):
        """
        Cập nhật thêm dữ liệu mới chạy về vào dataframe hiện tại
        """
        # Tên các cột dữ liệu trong dataframe
        column_names = [
            'datetime',
            'open',
            'high',
            'low',
            'close',
            'volume'
        ]

        # if self._price_frame.shape[0] > 0:
        #     column_names = self._price_frame.columns

        for k in new_rows:
            lst_prices = new_rows[k]
            for item in lst_prices:
                # Tạo multi index = (ticker, timestamp)
                _idx = (k, item['ts'])
                
                # Tạo dict chứa các giá trị của các cột trên một dòng
                _values = {}
                for col in column_names:
                    _values[col] = item[col]

                # Thêm _values này vào dataframe để tạo dòng dữ liệu mới
                # self._price_frame.loc[_idx] = pd.Series(data=_values, index=column_names)
                self._price_frame.loc[_idx, column_names] = _values

        # Sort lại dataframe
        self._price_frame.sort_index(inplace=True)


    def get_last_row(self, ticker: str=None) -> dict:
        # filtered = self._price_frame.filter(like=ticker, axis=0)
        # if filtered.shape[0] > 0:
        #     last = filtered.tail(1)
        #     last = last.to_dict(orient='records')
        #     if len(last) > 0:
        #         return last[0]
        
        # return {}
        return self.get_previous_row_at(ticker, 1)
    
    def get_previous_row_at(self, ticker: str=None, n: int=1) -> dict:
        filtered = None
        if not ticker:
            filtered = self._price_frame
        else:
            filtered = self._price_frame.filter(like=ticker, axis=0)

        if filtered.shape[0] >= n:
            row = filtered.iloc[-n].to_dict()
            row['ts'] = row['datetime'].tz_localize('Asia/Ho_Chi_Minh').timestamp()
            return row
        
        return {}
    
    
    def get_ticker_groupby(self) -> DataFrameGroupBy:
        self._ticker_groupby = self._price_frame.groupby(by='ticker', as_index=False, sort=True)
        return self._ticker_groupby
    
    @property
    def ticker_groupby_prop(self) -> DataFrameGroupBy:
        return self.get_ticker_groupby()

    # def get_ticker_groupby_rolling(self, n: int) -> RollingGroupby:

    #     if not self._ticker_groupby:
    #         self.get_ticker_groupby()

    #     self._ticker_groupby_rolling = self._ticker_groupby.rolling(window=n)

    #     return self._ticker_groupby_rolling
   