import akshare as ak
import pandas as pd
import datetime

def get_A_stock_info():
    stocks = ak.stock_info_a_code_name()
    return stocks

def get_A_stock_data(stock_code, start_date, end_date):
    stock_data = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
    return stock_data

def get_A_volum_ma5(stock_code):
    # Fetch historical stock price data
    stock_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily")

    # Ensure that the data is sorted by date in ascending order
    stock_data = stock_data.sort_values(by='日期')

    # Calculate the 5-day Moving Average for the trading volume
    stock_data['MA5_Volume'] = stock_data['成交量'].rolling(window=5).mean()
    # Display the resulting DataFrame
    # print(stock_data[['日期', '成交量', 'MA5_Volume']])
    return stock_data


def __main__():
    stocks = get_A_stock_info()
    # 设置日期范围
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')  # 获取最近30天的数据
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    for stock_code,stock_name in zip(stocks["code"],stocks["name"]):
        stock_data = get_A_stock_data(stock_code, start_date, end_date)
        stock_data = stock_data.sort_values(by='日期')
        # Calculate the 5-day Moving Average for the trading volume
        stock_data['MA5_Volume'] = stock_data['成交量'].rolling(window=5).mean()
        stock_data['MA5_Volume_Flag'] = stock_data['成交量'] > (stock_data['MA5_Volume'] * 2)
        last_trade = stock_data.iloc[-1]
        if last_trade["MA5_Volume_Flag"]:
            print(stock_code,stock_name,last_trade['日期'], last_trade['成交量'], last_trade['MA5_Volume'],last_trade['MA5_Volume_Flag'])

def test():
   stock_data = get_A_stock_data("605366","20240101","20240116")
   stock_data = stock_data.sort_values(by='日期')
   stock_data['MA5_Volume'] = stock_data['成交量'].rolling(window=5).mean()
   print(stock_data[['日期', '成交量', 'MA5_Volume']])

__main__()