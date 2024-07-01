from viiquant.trading_bot import TradingBot

from datetime import datetime
from datetime import timedelta

#=====================================================================================
# Tạo danh sách tài sản để khởi tạo danh mục
assets = [
    {
        'ticker': 'STB',
        'asset_type': 'equity', 
        'purchased_date': '2023-01-03',
        'qty': 400, 
        'purchased_price': 23.5,
        'is_owned': True
    },
    {
        'ticker': 'FPT',
        'asset_type': 'equity', 
        'purchased_date': '2023-01-03',
        'qty': 300, 
        'purchased_price': 80,
        'is_owned': True
    },
    {
        'ticker': 'NLG',
        'asset_type': 'equity', 
        'purchased_date': '',
        'qty': 0, 
        'purchased_price': 0,
        'is_owned': False
    }
]

# Khai báo các indicators sẽ sử dụng. Ở đây dùng cặp chỉ báo là: MACD và RSI
indicators = ['macd', 'rsi']

#====================================================================================

#Tạo function main
def main():

    # Apply start_date, end_date để lấy dữ liệu lịch sử làm cơ sở để tính các chỉ số cho lần đầu tiên
    end_date = datetime.today()
    start_date = end_date - timedelta(days=15)
    
    # Khởi tạo bot
    bot = TradingBot(
            start_date=start_date,
            end_date=end_date,
            bar_size=1,
            bar_type='m', # m = minute; H = hourly;
            show_tail_rows=3,
            write_log=True)
    
    bot.create_portfolio(assets)
    
    # Khởi tạo, load dữ liệu giá
    bot.create_price_frame()

    # Apply chỉ báo ở trên
    used_indicators = bot.set_used_indicators(indicators)

    # Thiết lập điều kiện theo cặp chỉ báo để tính ra tính hiệu Buy/Sell

    # BOLLINGER BANDS & RSI
    # indicator_conditions = {
    #     'buy': f"(close < {used_indicators['bollinger_bands']['bb_lower_col']}) and ({used_indicators['rsi']['rsi_col']} < 30)",
    #     'sell': f"(close > {used_indicators['bollinger_bands']['bb_upper_col']}) and ({used_indicators['rsi']['rsi_col']} > 70)"
    # }

    # MACD & RSI
    indicator_conditions = {
        'buy': f"({used_indicators['macd']['macd_col']} > {used_indicators['macd']['signal_col']}) and ({used_indicators['rsi']['rsi_col']} < 30)",
        'sell': f"({used_indicators['macd']['macd_col']} < {used_indicators['macd']['signal_col']}) and ({used_indicators['rsi']['rsi_col']} > 70)"
    }

    # Tạo mapping các cột dữ liệu của các chỉ báo, dùng để kiểm tra tính hiệu
    
    # BOLLINGER BANDS & RSI
    # mapping_state = [
    #     'close',
    #     used_indicators['bollinger_bands']['bb_upper_col'],
    #     used_indicators['bollinger_bands']['bb_lower_col'],
    #     used_indicators['rsi']['rsi_col']
    # ]
    
    # MACD & RSI
    mapping_state = [
        used_indicators['macd']['macd_col'],
        used_indicators['macd']['signal_col'],
        used_indicators['rsi']['rsi_col']
    ]

    # Apply các chỉ báo và điều kiện ở trên vào chương trình
    bot.set_signal_conditions(indicator_conditions, mapping_state)

    # Cho bot chạy
    bot.run()


if __name__ == "__main__":
    main()