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
        stock_data['MA5_Volume_Flag'] = stock_data['成交量'] > (stock_data['MA5_Volume'] * 1.5)
        calc_kdj(stock_data)
        last_trade = stock_data.iloc[-1]
        if last_trade["MA5_Volume_Flag"] and last_trade['kdj'] ==1 and last_trade['kdjcross'] == 1 and last_trade['k'] < 50 and last_trade['d'] < 50 and last_trade['j'] < 50:
            print(stock_code,stock_name,last_trade['日期'], last_trade['成交量'], last_trade['MA5_Volume'],last_trade['MA5_Volume_Flag'])

def test():
   stock_data = get_A_stock_data("603099","20231201","20231229")
   stock_data = stock_data.sort_values(by='日期')
   stock_data['MA5_Volume'] = stock_data['成交量'].rolling(window=5).mean()
   stock_data = stock_data.sort_values(by='日期')
   # Calculate the 5-day Moving Average for the trading volume
   stock_data['MA5_Volume'] = stock_data['成交量'].rolling(window=5).mean()
   stock_data['MA5_Volume_Flag'] = stock_data['成交量'] > (stock_data['MA5_Volume'] * 1.5)
   # print(stock_data[['日期', '成交量', 'MA5_Volume','MA5_Volume_Flag']])
   calc_kdj(stock_data)
   print(stock_data[['日期', '成交量', 'MA5_Volume','MA5_Volume_Flag','k','d','j']])

# 在k线基础上计算KDF，并将结果存储在df上面(k,d,j)
def calc_kdj(df):
    low_list = df['最低'].rolling(9, min_periods=9).min()
    low_list.fillna(value=df['最低'].expanding().min(), inplace=True)
    high_list = df['最高'].rolling(9, min_periods=9).max()
    high_list.fillna(value=df['最高'].expanding().max(), inplace=True)
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['k'] = pd.DataFrame(rsv).ewm(com=2).mean()
    df['d'] = df['k'].ewm(com=2).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']
    df['kdj'] = 0
    series = df['k']>df['d']
    df.loc[series[series == True].index, 'kdj'] = 1
    df.loc[series[(series == True) & (series.shift() == False)].index, 'kdjcross'] = 1
    df.loc[series[(series == False) & (series.shift() == True)].index, 'kdjcross'] = -1
    return df


# test()
__main__()
# 一、kdj公式：
#
# RSV=(CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100;
#
# K:SMA(RSV,M1,1);
#
# D:SMA(K,M2,1);
#
# 首先rsv 计算方式 就是 （收盘-给定周期最低价）/(给定周期最高价 - 给定周期最低价) * 100
#
# K值计算 就是给定周期m1 加权移动平均值 目前权重1
#
# 转换一下就是：
#
# K = (RSV*1 + (M1 - 1) * K[-1]) / M1 (K[-1]上一根K线K值)
#
# 同理
#
# D = (K*1 + (M2 - 1) * D[-1]) / M2 (D[-1]上一根K线K值)