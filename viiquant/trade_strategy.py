import numpy as np
import pandas as pd

from typing import Dict
from typing import List

from colorama import Fore, Back, Style

from viiquant.stock_price_frame import StockPriceFrame
from viiquant.stock_indicator import StockIndicator

class Strategy():

    def __init__(self, spf:StockPriceFrame = None):

        self._spf:StockPriceFrame = None
        self._indicator:StockIndicator = None
        if spf:
            self._spf = spf
            self._indicator = StockIndicator(spf)

        self._used_indicators: Dict[str, dict] = {}

        self._conditions: Dict[str, str] = {}
        self._mapping_state: List[str] = []
    
    def set_indicator(self, indicator:StockIndicator):
        self._indicator = indicator
        self._spf = indicator._spf

    def set_used_indicators(self, used_indicators: Dict[str, dict]=None):
        self._used_indicators = used_indicators
        self.create_used_indicators()

    def create_used_indicators(self):

        for method in self._used_indicators:
            params = self._used_indicators[method]
            indicator_func = getattr(self._indicator, method.upper())

            indicator_func(**params)

    def refresh_indicators(self):
        self._indicator.update()

    def get_available_indicators(self):
        return self._indicator.get_available_indicators()

    def set_signals(self, conditions:Dict[str, str], mapping_state:List[str]):
        self._conditions = conditions
        self._mapping_state = mapping_state

    def check_signals(self):
        
        # last_rows = self._indicator._ticker_groupby.tail(1)
        last_rows = self._spf.get_ticker_groupby().tail(1)
        print('='*100)
        print('Check Signals:')
        print(Fore.LIGHTYELLOW_EX + str(self._conditions))
        print(Style.RESET_ALL)
        print('-'*25)
        print(Fore.LIGHTCYAN_EX, end='')
        print(last_rows)
        print(Style.RESET_ALL)

        signals = {}    
        for key in last_rows.index:
            ticker = key[0]
            # print(ticker, last_rows.loc[key]['open'])
            
            for col in self._mapping_state:
                exec(f"{col}={last_rows.loc[key][col]}")

            signals[ticker] = {
                'buy': eval(self._conditions['buy']),
                'sell': eval(self._conditions['sell']),
                'at_time': last_rows.loc[key]['datetime'].tz_localize('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d %H:%M:%S'),
                'close_price': last_rows.loc[key]['close']
            }
        
        # print("Buy/Sell Conditions:")
        # print(self._conditions)
        # print(signals)

        return signals

       