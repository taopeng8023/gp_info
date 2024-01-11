import yfinance as yf
import pandas as pd
import datetime

def get_stock_data(symbol, start_date, end_date):
    stock_data = yf.download(symbol, start=start_date, end=end_date, progress=False)
    return stock_data

def calculate_average_volume(stock_data, window=5):
    stock_data['Avg_Volume'] = stock_data['Volume'].rolling(window=window).mean()
    return stock_data

def check_volume_condition(stock_data, threshold=1.5):
    stock_data['Volume_Check'] = stock_data['Volume'] > (stock_data['Avg_Volume'] * threshold)
    return stock_data

def check_continuous_increase(stock_data, days=5):
    stock_data['Price_Increase'] = stock_data['Close'] > stock_data['Close'].shift(1)
    stock_data['Continuous_Increase'] = stock_data['Price_Increase'].rolling(window=days).sum() == days
    return stock_data

def filter_stocks(stock_data):
    return stock_data[stock_data['Continuous_Increase']]

def main():
    # 设置股票符号和日期范围
    symbol_list = ['AAPL']  # 替换成你想要的股票符号列表
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # 获取最近30天的数据

    for symbol in symbol_list:
        # 获取股票数据
        stock_data = get_stock_data(symbol, start_date, end_date)

        # 计算历史五日平均成交量
        stock_data = calculate_average_volume(stock_data, window=5)

        # 检查当日成交量是否超过平均成交量的1.5倍
        stock_data = check_volume_condition(stock_data, threshold=1.5)

        # 检查连续上涨的股票
        stock_data = check_continuous_increase(stock_data, days=3)

        # 过滤符合条件的股票
        filtered_stocks = filter_stocks(stock_data)

        # 输出股票代码信息
        print(f"\n符合条件的股票代码 ({symbol}):")
        print(filtered_stocks.reset_index()[['Date', 'Close', 'Volume', 'Avg_Volume', 'Continuous_Increase']])

if __name__ == "__main__":
    main()
