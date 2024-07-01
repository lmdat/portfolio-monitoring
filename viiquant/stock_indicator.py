import numpy as np
import pandas as pd
import inspect

from typing import List
from typing import Any

# from pandas.core.groupby import DataFrameGroupBy

from viiquant.stock_price_frame import StockPriceFrame


class StockIndicator:

    def __init__(self, spf: StockPriceFrame=None):
        self._spf: StockPriceFrame = None
        self._price_frame: pd.DataFrame = None
        self._ticker_groupby = None
        self._curr_indicators: dict = {}
        self._indicator_signals: dict = {}
        self._indicator_compared_signals: dict = {}

        if spf:
            self.set_price_frame(spf)

    def set_price_frame(self, spf: StockPriceFrame):
        self._spf: StockPriceFrame = spf
        self._price_frame: pd.DataFrame = spf._price_frame
        self._ticker_groupby = spf.ticker_groupby_prop

    def MACD(self, fast_period:int = 12, slow_period:int = 26, macd_signal_period:int = 9,
             macd_col:str = 'macd', signal_col:str = 'macd_signal', indicator_key:str = None) -> pd.DataFrame:
        """
        The Moving Average Convergence Divergence (MACD) is one of the most popular technical indicators used to generate signals among stock traders.
        This indicator serves as a momentum indicator that can help signal shifts in market momentum and help signal potential breakouts
        """
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = f"{macd_col}_{fast_period}_{slow_period}"

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.MACD
        }
        
        groupby = self._spf.get_ticker_groupby()
        self._price_frame['ma_fast'] = groupby['close'].transform(lambda x: x.ewm(span=fast_period, min_periods=fast_period).mean())
        self._price_frame['ma_slow'] = groupby['close'].transform(lambda x: x.ewm(span=slow_period, min_periods=slow_period).mean())
        self._price_frame[macd_col] = self._price_frame['ma_fast'] - self._price_frame['ma_slow']

        groupby = self._spf.get_ticker_groupby()
        self._price_frame[signal_col] = groupby[macd_col].transform(lambda x: x.ewm(span=macd_signal_period, min_periods=macd_signal_period).mean())
        
        self._price_frame.drop(columns=['ma_fast', 'ma_slow'], axis=1, inplace=True)

        return self._price_frame
    
    def SMA(self, period:int = 20, sma_col:str = 'sma_20', indicator_key:str = None) -> pd.DataFrame:
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = sma_col

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.SMA
        }
        
        self._price_frame[sma_col] = self._spf.get_ticker_groupby()['close'].transform(lambda x: x.rolling(window=period).mean())

        return self._price_frame
    
    def EMA(self, period:int = 20, alpha:float = 0, ema_col:str = 'ema_20', indicator_key:str = None) -> pd.DataFrame:
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = ema_col

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.EMA
        }

        if 0 < alpha <= 1:
            self._price_frame[ema_col] = self._spf.get_ticker_groupby()['close'].transform(lambda x: x.ewm(span=period, alpha=alpha).mean())
        else:
            self._price_frame[ema_col] = self._spf.get_ticker_groupby()['close'].transform(lambda x: x.ewm(span=period).mean())
            
        return self._price_frame
    
    
    def RSI(self, period:int = 14, ewm:bool = True, rsi_col:str = 'rsi_14', indicator_key:str = None) -> pd.DataFrame:
        """
        The Relative Strength Index (RSI) is a momentum indicator that describes the current price relative to average high and low prices over a previous trading period.
        This indicator estimates overbought or oversold status and helps spot trend reversals, price pullbacks, and the emergence of bullish or bearish markets
        """
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = rsi_col

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.RSI
        }

        self._price_frame['price_changed'] = self._spf.get_ticker_groupby()['close'].transform(lambda x: x.diff())

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['gain'] = groupby['price_changed'].transform(lambda x: np.where(x >= 0, x, 0))
        self._price_frame['loss'] = groupby['price_changed'].transform(lambda x: np.where(x < 0, abs(x), 0))

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['avg_gain'] = groupby['gain'].transform(lambda x: x.ewm(span=period, min_periods=period).mean() if ewm == True else x.rolling(window=period).mean())
        self._price_frame['avg_loss'] = groupby['loss'].transform(lambda x: x.ewm(span=period, min_periods=period).mean() if ewm == True else x.rolling(window=period).mean())

        self._price_frame['relative_strength'] = self._price_frame['avg_gain'] / self._price_frame['avg_loss']
        self._price_frame[rsi_col] = 100 - (100 / (1 + self._price_frame['relative_strength']))

        self._price_frame.drop(columns=['price_changed', 'gain', 'loss', 'avg_gain', 'avg_loss', 'relative_strength'], axis=1, inplace=True)
        return self._price_frame
    
    def STOCH_RSI(self, period:int = 14, ewm:bool = True, stochrsi_col:str = 'stochrsi_14', indicator_key:str = None) -> pd.DataFrame:
        """
        The Relative Strength Index (RSI) is a momentum indicator that describes the current price relative to average high and low prices over a previous trading period.
        This indicator estimates overbought or oversold status and helps spot trend reversals, price pullbacks, and the emergence of bullish or bearish markets
        """
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = stochrsi_col

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.STOCH_RSI
        }

        self._price_frame['price_changed'] = self._spf.get_ticker_groupby()['close'].transform(lambda x: x.diff())

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['gain'] = groupby['price_changed'].transform(lambda x: np.where(x >= 0, x, 0))
        self._price_frame['loss'] = groupby['price_changed'].transform(lambda x: np.where(x < 0, abs(x), 0))

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['avg_gain'] = groupby['gain'].transform(lambda x: x.ewm(span=period, min_periods=period).mean() if ewm == True else x.rolling(window=period).mean())
        self._price_frame['avg_loss'] = groupby['loss'].transform(lambda x: x.ewm(span=period, min_periods=period).mean() if ewm == True else x.rolling(window=period).mean())

        self._price_frame['relative_strength'] = self._price_frame['avg_gain'] / self._price_frame['avg_loss']
        self._price_frame['tmp_rsi'] = 100 - (100 / (1 + self._price_frame['relative_strength']))

        groupby = self._spf.get_ticker_groupby()
        self._price_frame[stochrsi_col] = 100 * (groupby['tmp_rsi'].transform(lambda x: x) - groupby['tmp_rsi'].transform(lambda x: x.rolling(period).min())) / (groupby['tmp_rsi'].transform(lambda x: x.rolling(period).max()) - groupby['tmp_rsi'].transform(lambda x: x.rolling(period).min()))

        self._price_frame.drop(columns=['price_changed', 'gain', 'loss', 'avg_gain', 'avg_loss', 'relative_strength', 'tmp_rsi'], axis=1, inplace=True)
        return self._price_frame
    
    def ATR(self, period:int = 14, ewm:bool = True, atr_col:str = 'atr_14', indicator_key:str = None) -> pd.DataFrame:
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = atr_col

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.ATR
        }
        groupby = self._spf.get_ticker_groupby()
        self._price_frame['prev_close'] = groupby['close'].transform(lambda x: x.shift(1))
        self._price_frame['HmL'] = abs(self._price_frame['high'] - self._price_frame['low'])
        self._price_frame['HmPrvC'] = abs(self._price_frame['high'] - self._price_frame['prev_close'])
        self._price_frame['LmPrvC'] = abs(self._price_frame['low'] - self._price_frame['prev_close'])
        self._price_frame['true_range'] = self._price_frame[['HmL', 'HmPrvC', 'LmPrvC']].max(axis=1, skipna=False)

        groupby = self._spf.get_ticker_groupby()
        self._price_frame[atr_col] = groupby['true_range'].transform(lambda x: x.ewm(span=period, min_periods=period).mean() if ewm == True else x.rolling(window=period).mean())
        
        self._price_frame.drop(columns=['prev_close', 'HmL', 'HmPrvC', 'LmPrvC', 'true_range'], axis=1, inplace=True)
        return self._price_frame

    def BOLLINGER_BANDS(self, period:int = 20, sigma_width:int = 2, bb_upper_col:str = 'bb_upper', bb_lower_col:str = 'bb_lower', bb_width_col:str = 'bbw', indicator_key:str = 'bbands') -> pd.DataFrame:
        local_data = locals()
        del local_data['self']

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.BOLLINGER_BANDS
        }

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['sma'] = groupby['close'].transform(lambda x: x.rolling(window=period).mean())
        self._price_frame['sigma'] = groupby['close'].transform(lambda x: x.rolling(window=period).std())

        self._price_frame[bb_upper_col] = self._price_frame['sma'] + (sigma_width * self._price_frame['sigma'])
        self._price_frame[bb_lower_col] = self._price_frame['sma'] - (sigma_width * self._price_frame['sigma'])
        if bb_width_col:
            self._price_frame[bb_width_col] = (self._price_frame[bb_upper_col] - self._price_frame[bb_lower_col])/self._price_frame["sma"] * 100

        self._price_frame.drop(columns=['sma', 'sigma'], axis=1, inplace=True)
        return self._price_frame
    
    def COMMODITY_CHANNEL_INDEX(self, period:int = 20, use_mad:bool = True, cci_col:str = 'cci_20', indicator_key:str = None) -> pd.DataFrame:
        local_data = locals()
        del local_data['self']

        if not indicator_key:
            indicator_key = cci_col

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.COMMODITY_CHANNEL_INDEX
        }


        self._price_frame['tp'] = (self._price_frame['high'] + self._price_frame['low'] + self._price_frame['close']) / 3

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['tp_sma'] = groupby['tp'].transform(lambda x: x.rolling(window=period).mean())
        self._price_frame['tp_mad'] = groupby['tp'].transform(lambda x: x.rolling(window=period).apply(lambda z: pd.Series(z).mad()))
        self._price_frame['tp_std'] = groupby['tp'].transform(lambda x: x.rolling(window=period).std())

        if use_mad == True:
            self._price_frame[cci_col] = (self._price_frame['tp'] - self._price_frame['tp_sma']) / (self._price_frame['tp_mad'] * 0.015)
        else:
            self._price_frame[cci_col] = (self._price_frame['tp'] - self._price_frame['tp_sma']) / (self._price_frame['tp_std'] * 0.015)

        self._price_frame.drop(columns=['tp', 'tp_sma', 'tp_mad', 'tp_std'], axis=1, inplace=True)
        return self._price_frame
    
    def STOCH(self, k_period:int = 14, d_period:int = 3, stoch_k_col:str = 'stoch_14', stoch_d_col:str = 'stoch_3', indicator_key:str = 'stoch') -> pd.DataFrame:
        """
        The Stochastic Oscillator is a momentum indicator used to signal trend reversals in the stock market.
        It describes the current price relative to the high and low prices over a trailing number of previous trading periods
        """

        local_data = locals()
        del local_data['self']

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.STOCH
        }
        
        groupby = self._spf.get_ticker_groupby()
        self._price_frame['k_high'] = groupby['high'].transform(lambda x: x.rolling(window=k_period).max())
        self._price_frame['k_low'] = groupby['low'].transform(lambda x: x.rolling(window=k_period).min())

        self._price_frame[stoch_k_col] = 100 * (self._price_frame['close'] - self._price_frame['k_low']) / (self._price_frame['k_high'] - self._price_frame['k_low'])
        
        groupby = self._spf.get_ticker_groupby()
        self._price_frame[stoch_d_col] = groupby[stoch_k_col].transform(lambda x: x.rolling(window=d_period).mean())

        self._price_frame.drop(columns=['k_high', 'k_low'], axis=1, inplace=True)
        return self._price_frame
    

    def CHAIKIN(self, fast_period:int = 3, slow_period:int = 10, chaikin_col:str = 'chaikin', indicator_key:str = 'chaikin') -> pd.DataFrame:
        """
        The Chaikin Oscillator measures the momentum of the Accumulation Distribution Line using the MACD formula
        """

        local_data = locals()
        del local_data['self']

        self._curr_indicators[indicator_key] = {
            'params': local_data,
            'function': self.CHAIKIN
        }

        self._price_frame['money_flow_mult'] = ((2 * self._price_frame['close']) - self._price_frame['low'] - self._price_frame['high']) / (self._price_frame['high'] - self._price_frame['low'])
        self._price_frame['money_flow_vol'] = self._price_frame['money_flow_mult'] * self._price_frame['volume']

        groupby = self._spf.get_ticker_groupby()
        self._price_frame['money_flow_vol_fast'] = groupby['money_flow_vol'].transform(lambda x: x.ewm(span=fast_period, min_periods=fast_period).mean())
        self._price_frame['money_flow_vol_slow'] = groupby['money_flow_vol'].transform(lambda x: x.ewm(span=slow_period, min_periods=slow_period).mean())
        self._price_frame[chaikin_col] = self._price_frame['money_flow_vol_fast'] - self._price_frame['money_flow_vol_slow']

        self._price_frame.drop(columns=['money_flow_mult', 'money_flow_vol', 'money_flow_vol_fast', 'money_flow_vol_slow'], axis=1, inplace=True)
        return self._price_frame

    def get_available_indicators(self):
        methods = inspect.getmembers(self, lambda attr: inspect.ismethod(attr))   
        # methods_filtered = [m for m in methods if not(m[0].startswith("__") and m[0].endswith("__")) and m[0].isupper()]
        available_indicator = {}
        for method in methods:
            if not(method[0].startswith("__") and method[0].endswith("__")) and method[0].isupper():
                _method_name = method[0].lower() 
                available_indicator[_method_name] = {}
                sig = inspect.signature(method[1])
                for arg in sig.parameters:
                    if sig.parameters[arg].annotation is int or (sig.parameters[arg].annotation is str and sig.parameters[arg].name != 'indicator_key'):
                        available_indicator[_method_name][sig.parameters[arg].name] = sig.parameters[arg].default
                        # print(method[0], sig.parameters[arg].name, sig.parameters[arg].default)

        return available_indicator
    

    def update(self):
        for k in self._curr_indicators:
            params = self._curr_indicators[k]['params']
            indicator_function = self._curr_indicators[k]['function']

            indicator_function(**params)
    
    def set_signal(self, indicator:str, buy_threshold:float, sell_threshold:float, buy_condition:Any, sell_condition:Any,
                   buy_max:float = None, sell_max:float = None, buy_max_condition:Any = None, sell_max_condition:Any = None):
        
        self._indicator_signals[indicator] = {
            'buy': buy_threshold,
            'sell': sell_threshold,
            'buy_condition': buy_condition,
            'sell_condition': sell_condition,
            'buy_max': buy_max,
            'sell_max': sell_max,
            'buy_max_condition': buy_max_condition,
            'sell_max_condition': sell_max_condition
        }
        
    def set_compared_signals(self, ticker:str, indicator_a:str, indicator_b:str, buy_condition:Any, sell_condition:Any):

        
        key = f"{indicator_a}|{indicator_b}"
        self._indicator_compared_signals[key] = {
            'indicator_a': indicator_a,
            'indicator_b': indicator_b,
            'buy_condition': buy_condition,
            'sell_condition': sell_condition
        }
    
    def check_indicator_in_dataframe(self, col_names:List[str]) -> bool:

        set_cols = set(col_names)
        if set_cols.issubset(self._price_frame.columns):
            return True
        
        missing_cols = set_cols.difference(self._price_frame.columns)
        print(f"Missing indicator columns: {missing_cols}")
        return False
        

    def check_signals(self):

        signals = {}
        last_rows = self._ticker_groupby.tail(1)

        if self.check_indicator_in_dataframe(col_names=list(self._indicator_signals.keys())):

            for indicator in self._indicator_signals.keys():
                col = last_rows[indicator]

                buy_condition_ope = self._indicator_signals[indicator]['buy_condition']
                sell_condition_ope = self._indicator_signals[indicator]['sell_condition']

                buy_result:pd.Series = buy_condition_ope(col, self._indicator_signals[indicator]['buy'])
                sell_result:pd.Series = sell_condition_ope(col, self._indicator_signals[indicator]['sell'])

                buy_result = buy_result.where(lambda x: x == True).dropna()
                sell_result = sell_result.where(lambda x: x == True).dropna()

                signals['buy'] = buy_result
                signals['sell'] = sell_result

        return signals
    