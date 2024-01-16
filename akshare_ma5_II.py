import akshare as ak
import pandas as pd

# 获取股票数据
def get_stock_data(stock_code, start_date, end_date):
    stock_data = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
    return stock_data

# 计算成交量的5日均线
def calculate_ma5(data):
    data['avg_volume'] = data['成交量'].rolling(window=5).mean()
    return data

# 筛选出当日成交量是前5个交易日平均成交量1.5倍以上的股票数据，并取出后五个交易日的股票交易信息
def filter_high_volume_stocks(data):
    high_volume_stocks = data[data['成交量'] > 1.5 * data['avg_volume']]
    print(high_volume_stocks.tail(3))
    return high_volume_stocks.tail(3)

# 统计后连续五日股价每天都上涨的股票
def count_continuous_price_increase(stock_code,data):
    count = 0
    for i in range(len(data) - 2):
        price_increase = data['收盘'].pct_change().iloc[i:i+3] > 0
        if price_increase.all():
            count += 1
            print(f"\n符合条件的股票: {stock_code} 日期: {data['日期'].iloc[i]}")
            print("符合条件的股票数据：")
            print(data.iloc[i:i+3][['日期', '开盘', '收盘', '成交量']])
            print("=" * 50)
    return count

# 获取所有A股的股票代码
def get_all_a_stocks():
    stock_info = ak.stock_info_a_code_name()
    return stock_info['code'].tolist()

# 初始化统计变量
count_high_volume = 0
count_continuous_price_count = 0

# 遍历所有A股
for stock_code in get_all_a_stocks():
    print("开始处理股票：",stock_code)
    try:
        # 获取股票数据
        stock_data = get_stock_data(stock_code, start_date="20230401", end_date="20240112")

        # 计算成交量的5日均线
        stock_data = calculate_ma5(stock_data)

        # 筛选出当日成交量是5日平均成交量1.5倍以上的股票数据
        high_volume_stocks = filter_high_volume_stocks(stock_data)

        # 统计后连续五日股价每天都上涨的股票
        if not high_volume_stocks.empty:
            _price_increase_count = count_continuous_price_increase(stock_code,high_volume_stocks)
            count_continuous_price_count += _price_increase_count
            count_high_volume += 1

            # 输出同时满足条件1和2的信息
            if _price_increase_count > 0:
                print(f"\n同时满足条件1和2的股票代码: {stock_code}")
                print("符合条件1的股票数据：")
                print(high_volume_stocks[['日期', '开盘', '收盘', '成交量']])
                print("=" * 50)

    except Exception as e:
        print(f"Error processing stock {stock_code}: {str(e)}")

# 输出统计结果
print(f"\n成交量是五日平均成交量1.5倍以上的次数：{count_high_volume}")
print(f"同时满足条件1和2的次数：{count_continuous_price_increase}")
