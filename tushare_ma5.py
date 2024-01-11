import tushare as ts
import pandas as pd
import datetime

# 设置tushare pro的token，需要在tushare官网申请
ts.set_token('a7358a0255666758b3ef492a6c1de79f2447de968d4a1785b73716a4')

# 初始化pro接口
pro = ts.pro_api()

def get_A_stock_data(code, start_date, end_date):
    stock_data = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
    return stock_data

def calculate_average_volume(stock_data, window=5):
    stock_data['Avg_Volume'] = stock_data['vol'].rolling(window=window).mean()
    return stock_data

def check_volume_condition(stock_data, threshold=1.5):
    stock_data['Volume_Check'] = stock_data['vol'] > (stock_data['Avg_Volume'] * threshold)
    return stock_data

def check_continuous_increase(stock_data, days=5):
    stock_data['Price_Increase'] = stock_data['close'] > stock_data['close'].shift(1)
    stock_data['Continuous_Increase'] = stock_data['Price_Increase'].rolling(window=days).sum() == days
    return stock_data

def filter_stocks(stock_data):
    return stock_data[stock_data['Continuous_Increase']]

def main():
    # 设置股票代码和日期范围
    stock_code = '600000.SH'  # 例如，中国平安的股票代码
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')  # 获取最近30天的数据

    # 获取A股股票数据
    stock_data = get_A_stock_data(stock_code, start_date, end_date)

    # 计算历史五日平均成交量
    stock_data = calculate_average_volume(stock_data, window=5)

    # 检查当日成交量是否超过平均成交量的1.5倍
    stock_data = check_volume_condition(stock_data, threshold=1.5)

    # 检查连续上涨的股票
    stock_data = check_continuous_increase(stock_data, days=5)

    # 过滤符合条件的股票
    filtered_stocks = filter_stocks(stock_data)

    # 输出股票代码信息
    print(f"符合条件的A股股票代码 ({stock_code}):")
    print(filtered_stocks.reset_index()[['trade_date', 'close', 'vol', 'Avg_Volume', 'Continuous_Increase']])

if __name__ == "__main__":
    main()
