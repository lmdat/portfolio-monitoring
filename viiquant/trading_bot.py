from viiquant.stock_price_frame import StockPriceFrame
from viiquant.data_stock_price import DataStockPrice
from viiquant.stock_indicator import StockIndicator
from viiquant.stock_portfolio import Portfolio
from viiquant.trade_strategy import Strategy

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from typing import List, Dict, Union, Tuple
import os, sys, time
import copy

import xlwings as xlw

from colorama import Fore, Back, Style
from colorama import init as init_terminal_color


class TradingBot:

    def __init__(self, start_date:datetime, end_date:datetime, bar_size:int=15, bar_type:str='m', show_tail_rows:int = 5, write_log:bool=False):
        self._end_date:datetime = end_date
        self._start_date:datetime = start_date
        self._bar_size = bar_size # minute: 1, 5, 10, 15, 30 | hourly: 1 | Daily: 1
        self._bar_type = bar_type # m: minute, H: hourly, D: daily
        self._ticker_type = 'stock' # stock: Stock, index: Index (VNINDEX, VN30, HNX, HNX30, UPCOM, VNXALLSHARE, VN30F1M, VN30F2M, VN30F1Q, VN30F2Q)

        self._dsp:DataStockPrice = DataStockPrice(ticker_type=self._ticker_type)
        self._spf:StockPriceFrame = None
        self._portfolio: Portfolio = Portfolio(self._dsp)
        self._indicator: StockIndicator = StockIndicator()
        self._strategy: Strategy = Strategy()

        self._used_indicators:Dict[str, dict] = {}
        self._space_tab = " "*4
        self._show_tail_rows = show_tail_rows
        self._waiting_extra_time = 10 # second

        self._new_data_come:dict = {}

        self._write_log = write_log
        
        init_terminal_color()
        print(Style.RESET_ALL, end='')

    def create_portfolio(self, assets_list:List[dict]) -> Portfolio:
        self._portfolio.add_assets(assets_list)
        return self._portfolio
    
    def create_price_frame(self):
        data = {}
        for ticker in self._portfolio.get_asset_labels():
            data[ticker] = self._dsp.get_historical_price(
                                ticker,
                                self._start_date.strftime('%Y-%m-%d'),
                                self._end_date.strftime('%Y-%m-%d'),
                                bar_size=self._bar_size,
                                bar_type=self._bar_type)
        
        self._spf = StockPriceFrame(data)
        # print(self._spf._ticker_groupby.tail(self._show_tail_rows))
        self._indicator.set_price_frame(self._spf)
        self.create_strategy()

    def create_strategy(self):
        self._strategy.set_indicator(self._indicator)
        

    def get_available_indicators(self) -> dict:
        return self._strategy.get_available_indicators()

    def set_used_indicators(self, used_indicators: Union[List[str], Dict[str, dict]]):
        
        _available_indicators = self._strategy.get_available_indicators()
        if isinstance(used_indicators, list):
            for ind_name in used_indicators:
                self._used_indicators[ind_name] = copy.deepcopy(_available_indicators[ind_name])

        else:
            self._used_indicators = used_indicators

        return self._used_indicators

    def set_signal_conditions(self, conditions:Dict[str, str], mapping_state:List[str]):
        self._strategy.set_signals(conditions, mapping_state)

    def get_lastest_row(self):
        new_data = {}
        for ticker in self._portfolio.get_asset_labels():
            last_row = self._spf.get_last_row(ticker)
            new_data[ticker] = self._dsp.get_lastest_price_rows(
                                ticker,
                                self._end_date.strftime('%Y-%m-%d'),
                                self._end_date.strftime('%Y-%m-%d'),
                                last_row['ts'],
                                bar_size=self._bar_size,
                                bar_type=self._bar_type)
            
            self._new_data_come[ticker] = True if len(new_data[ticker]) > 0 else False
        
        print("Fetch lastest price:")
        print(Fore.LIGHTCYAN_EX + str(new_data))
        print(Style.RESET_ALL, end='')
        self._spf.add_new_row_price(new_data)
        
        # print(self._spf.get_ticker_groupby().tail())

    def clear_terminal(self):
        if sys.platform in ['linux', 'darwin', 'cygwin']:
            os.system('clear')
        else:
            os.system('cls')

    def is_market_opening(self) -> Tuple[bool, int]:
        open_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=9, minute=0, second=0)
        close_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=15, minute=0, second=0)

        current_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
        
        if current_time.weekday() in [5, 6]:
            print(f"Today is {current_time.strftime('%A')}.")
            sys.exit()

        wait = 0
        if open_time <= current_time <= close_time:
            return True, wait
        
        if current_time < open_time:
            wait = (open_time - current_time).total_seconds()
        else:
            tmp = open_time + timedelta(days=1)
            wait = (tmp - current_time).total_seconds()

        return False, int(wait)

    def is_market_lunch_break(self):
        start_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=11, minute=30, second=0)
        end_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=13, minute=0, second=0)
        current_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))

        if start_time <= current_time <= end_time:
            return True
        return False
    
    def waiting_until_market_open(self):
        is_open, wait_seconds = self.is_market_opening()
        if not is_open:
            print("The Vietnam Stock Market is not open now!. Open time: Monday -> Friday at [09:00 - 15:00].")
            print(f"{self._space_tab}-Now: {datetime.now(tz=ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')}")
            if wait_seconds < 60:
                print(f"{self._space_tab}-Please wait for {wait_seconds} secs.")
            elif wait_seconds < 3600:
                print(f"{self._space_tab}-Please wait for {round(wait_seconds/60)} mins.")
            else:
                print(f"{self._space_tab}-Please wait for {round(wait_seconds/3600, 1)} hrs.")
            
            self.portfolio_info()
            
            print("*** Press Ctrl + C to exit. ***")
            time.sleep(wait_seconds)

    def waiting_for_next_rows(self):
        # if not self.is_market_opening():
        #     print(Fore.LIGHTYELLOW_EX + "The Vietnam Stock Market is close now!")
        #     sys.exit()

        last_row = self._spf.get_last_row()
        last_time = datetime.fromtimestamp(last_row['ts'], tz=ZoneInfo("Asia/Ho_Chi_Minh"))
        delta = 0
        if self._bar_type.upper() == 'D':
            delta = timedelta(days=1)
        else:
            delta = timedelta(minutes=self._bar_size)

        next_time = last_time + delta
        
        current_time = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
        
        _lunch_break = ""
        if self.is_market_lunch_break():
            # if current_time >= datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=12, minute=0, second=0):
            _lunch_break = ' (lunch break)'
            end_time_break = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).replace(hour=13, minute=0, second=0)
            next_time = end_time_break

        waiting_time = next_time - current_time
        waiting_seconds = waiting_time.total_seconds()
        if waiting_seconds < 0:
            waiting_seconds = 0
        
        waiting_seconds += self._waiting_extra_time
        
        print("="*100)
        print("Waiting for next price...")
        print(f"{self._space_tab}-Now: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{self._space_tab}-Next: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if waiting_seconds < 60:
            print(f"{self._space_tab}-Waiting for{_lunch_break}: {waiting_seconds} secs.")
        else:
            print(f"{self._space_tab}-Waiting for{_lunch_break}: {round(waiting_seconds/60)} mins.")
        
        print("*** Press Ctrl + C to exit. ***")

        time.sleep(waiting_seconds + self._waiting_extra_time)

    def create_wb(self):
        wb = xlw.Book()
        wb.sheets[0][f"A1"].value = 'Ticker'
        wb.sheets[0][f"B1"].value = 'Time'
        wb.sheets[0][f"C1"].value = 'Signal'
        wb.sheets[0][f"D1"].value = 'Close Price'
        return wb
    
    def write_log(self, wb:xlw.Book, signal:dict, row:int=2):
        wb.sheets[0][f"A{row}"].value = signal['ticker']
        wb.sheets[0][f"B{row}"].value = signal['at_time']
        wb.sheets[0][f"C{row}"].value = signal['signal']
        wb.sheets[0][f"D{row}"].value = signal['close_price']

        return (row)
    
    def portfolio_metrics(self):
        print(pd.DataFrame(self._portfolio.metrics()).rename(columns={'portfolio': 'Portfolio'}))
        

    def portfolio_summary(self):
        print(pd.DataFrame(self._portfolio.summary()['projected_market_values']).rename(columns={'portfolio': 'Portfolio'}))
    
    def portfolio_info(self):
        print('='*100)
        print('Portfolio Info:')
        print('\nMetrics (By Daily Historical Price):')
        self.portfolio_metrics()
        print('\nSummary:')
        self.portfolio_summary()



    def run(self):
        
        if self._write_log:
            wb = self.create_wb()
            excel_row = 2
        
        self._strategy.set_used_indicators(self._used_indicators)

        while (True):
            try:
                self.waiting_until_market_open()                                

                self.clear_terminal()
                
                self.get_lastest_row()

                self._strategy.refresh_indicators()
                
                print('='*100)
                print(self._spf.get_ticker_groupby().tail(self._show_tail_rows))

                signals = self._strategy.check_signals()

                print('='*100)
                print(f'Signals:')
                for ticker in signals:
                    is_owned = '(Owned: Y)'
                    if not self._portfolio.is_owned(ticker):
                        is_owned = '(Owned: N)'
                        
                    print(f"{self._space_tab}-{ticker}{is_owned} ({signals[ticker]['at_time']})", end=" ")

                    buy = 'Yes' if signals[ticker]['buy'] == True else 'No'
                    sell = 'Yes' if signals[ticker]['sell'] == True else 'No'
                    
                    _signal_str = ""
                    if signals[ticker]['buy'] == True:
                        print(Fore.LIGHTGREEN_EX + f"[Buy: {buy},", end=" ")
                        _signal_str = 'Buy'
                    else:
                        print(Fore.LIGHTYELLOW_EX + f"[Buy: {buy},", end=" ")

                    if signals[ticker]['sell'] == True:
                        print(Fore.LIGHTRED_EX + f"Sell: {sell}]")
                        _signal_str = "Sell"
                    else:
                        print(Fore.LIGHTYELLOW_EX + f"Sell: {sell}]")

                    print(Style.RESET_ALL, end='')

                    if self._write_log == True and self._new_data_come[ticker] == True:
                        _signal = {
                            'ticker': ticker,
                            'at_time': signals[ticker]['at_time'],
                            'signal': _signal_str,
                            'close_price': signals[ticker]['close_price'] if _signal_str != "" else ''
                        }
                        self.write_log(wb, _signal, excel_row)
                        excel_row += 1

                print("="*100)
                print('Portfolio Summary:')
                self.portfolio_summary()
                
                self.waiting_for_next_rows()

            except KeyboardInterrupt:
                print("Exit. Bye!!!")
                sys.exit()
