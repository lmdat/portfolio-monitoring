from viiquant.data_stock_price import DataStockPrice
from viiquant.stock_price_frame import StockPriceFrame

from typing import List
from typing import Any
from typing import Union

from datetime import datetime, timedelta
import math
import numpy as np
from pandas import DataFrame

class Portfolio:

    def __init__(self, dsp:DataStockPrice):
        self._assets = {}

        self._dsp = dsp
    
    def add_asset(self, ticker:str, asset_type:str = 'equity', purchased_date:str = None, qty:int = 0, purchased_price:float = 0.0, is_owned:bool = Any) -> dict:
        """
        Add a asset to Portfolio
        """
        
        asset = {
            'ticker': ticker,
            'asset_type': asset_type, 
            'purchased_date': purchased_date,
            'qty': qty, 
            'purchased_price': purchased_price,
            'is_owned': bool(is_owned)
        }

        self._assets[ticker] = asset

        return asset

    def add_assets(self, assets_list:List[dict]) -> dict:
        """
        Add multiple assets to Portfolio
        """
        for asset in assets_list:
            self.add_asset(
                ticker=asset['ticker'],
                asset_type=asset['asset_type'],
                purchased_date=asset['purchased_date'],
                qty=asset['qty'],
                purchased_price=asset['purchased_price'],
                is_owned=asset['is_owned']
            )

        return self._assets
    
    def remove_asset(self, ticker:str) -> Union[dict, bool]:

        if ticker in self._assets:
            asset = self._assets[ticker]
            del self._assets[ticker]
            return asset
        
        return False
    
    def is_exist(self, ticker:str) -> bool:
        if ticker in self._assets:
            return True
        return False
    
    def is_owned(self, ticker:str) -> bool:
        return self._assets[ticker]['is_owned']

    def get_asset_labels(self) ->list:
        return list(self._assets.keys())
    

    def get_owner_asset_labels(self) ->list:
        l = []
        for k in self._assets:
            if self._assets[k]['is_owned'] == True:
                l.append(k)
        return l

    
    def is_ticker_profitable(self, ticker:str, current_price:float) -> bool:
        if self._assets[ticker]['purchased_price'] <= current_price:
            return True
        return False
        
    
    def projected_ticker_market_values(self, current_quotes):

        projected_mv = {}

        total_market_value = 0
        total_invested_value = 0
        total_return = 0
        
        for ticker in current_quotes:
            projected_mv[ticker] = {
                'purchased_price': self._assets[ticker]['purchased_price'],
                'current_price': current_quotes[ticker]['lastPrice'],
                'qty': self._assets[ticker]['qty'],
                'weighted': np.nan,
                'market_value': self._assets[ticker]['qty'] * current_quotes[ticker]['lastPrice'],
                'invested_value': self._assets[ticker]['qty'] * self._assets[ticker]['purchased_price'],
                'return': round((current_quotes[ticker]['lastPrice'] - self._assets[ticker]['purchased_price']) * self._assets[ticker]['qty']),
                # 'return_pct': round(math.log(current_quotess[ticker]['lastPrice'] / self._assets[ticker]['purchased_price']), 2)
                'return_pct': round((current_quotes[ticker]['lastPrice'] / self._assets[ticker]['purchased_price']) - 1, 2),
                'profitable': self.is_ticker_profitable(ticker, current_quotes[ticker]['lastPrice'])
            }

            total_market_value += projected_mv[ticker]['market_value']
            total_invested_value += projected_mv[ticker]['invested_value']
            total_return += projected_mv[ticker]['return']
        
        projected_mv['portfolio'] = {
            'weighted': 1,
            'market_value': total_market_value,
            'invested_value': total_invested_value,
            'return': total_return,
            'return_pct': round((total_market_value / total_invested_value) - 1, 2),
            'profitable': True if total_return > 0 else False
        }
        
        return projected_mv
    
    def fetch_historical_price_daily(self) -> StockPriceFrame:
        tickers = self.get_owner_asset_labels()

        end_date = datetime.today()
        start_date = end_date - timedelta(days=365)

        data = {}
        for ticker in tickers:
            data[ticker] = self._dsp.get_historical_price(
                                        ticker=ticker,
                                        start_date=start_date.strftime('%Y-%m-%d'),
                                        end_date=end_date.strftime('%Y-%m-%d')
                                    )
            
        return StockPriceFrame(data)
    

    def weights(self, projected_market_values:dict = None):
        
        if not projected_market_values:
            tickers = self.get_owner_asset_labels()
            current_quotes = self._dsp.get_market_quotes(tickers=tickers)
            projected_market_values = self.projected_ticker_market_values(current_quotes)

        weights = {}
        for ticker in projected_market_values:
            if ticker == 'portfolio':
                continue                
            else:
                weights[ticker] = round(projected_market_values[ticker]['market_value'] / projected_market_values['portfolio']['market_value'], 2)

        return weights
    
    def variance(self, weights:dict, cov:DataFrame):
        keys = list(weights.keys())        
        keys.sort()

        sorted_weights_arr = np.array([weights[ticker] for ticker in keys])

        return np.dot(np.dot(sorted_weights_arr, cov), sorted_weights_arr.T)
    
    def mean(self, weights:dict, return_mean:dict):
        keys = list(weights.keys())       
        keys.sort()

        sorted_weights_arr = np.array([weights[ticker] for ticker in keys])
        return_mean_arr = np.array([return_mean[ticker] for ticker in keys])

        return np.dot(return_mean_arr, sorted_weights_arr.T)
        
    
    def summary(self):

        portfolio_summary = {}
        tickers = self.get_owner_asset_labels()

        quotes = self._dsp.get_market_quotes(tickers=tickers)
        projected_market_values = self.projected_ticker_market_values(quotes)

        weights = self.weights(projected_market_values)

        for ticker in weights:             
            projected_market_values[ticker]['weighted'] = weights[ticker]

        portfolio_summary['projected_market_values'] = projected_market_values
        portfolio_summary['weights'] = weights
        
        return portfolio_summary
    
    def metrics(self):
        spf_daily = self.fetch_historical_price_daily()

        weights = self.weights()

        spf_daily._price_frame['return_pct'] = spf_daily.get_ticker_groupby()['close'].transform(lambda x: x.pct_change())
        # spf_daily._price_frame['previous_close'] = spf_daily.get_ticker_groupby()['close'].transform(lambda x: x.shift(1))
        # spf_daily._price_frame['return_log'] = np.log(spf_daily._price_frame['close']) - np.log(spf_daily._price_frame['previous_close'])
        spf_daily._price_frame['return_avg'] = spf_daily.get_ticker_groupby()['return_pct'].transform(lambda x: x.mean())
        spf_daily._price_frame['return_std'] = spf_daily.get_ticker_groupby()['return_pct'].transform(lambda x: x.std())


        # Calculate metrics
        unstack_df = spf_daily._price_frame.unstack(level=0)
        return_avg = unstack_df['return_pct'].mean().to_dict()
        return_std = unstack_df['return_pct'].std().to_dict()
        return_cov = unstack_df['return_pct'].cov()

        port_variance = self.variance(weights, return_cov)
        port_mean = self.mean(weights, return_avg)
        
        metrics = {}

        for ticker in weights:
            metrics[ticker] = {
                'weight': weights[ticker],
                'mean': return_avg[ticker],
                'std': return_std[ticker]
            }
           
        metrics['portfolio'] = {
            'weight': 1,
            'mean': port_mean,
            'std': port_variance**0.5
        }  

        return metrics



        

        
