import akshare as ak
import pandas as pd
import datetime


def get_A_stock_data(stock_code, start_date, end_date):
    stock_data = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
    return stock_data


def calculate_average_volume(stock_data, window=5):
    stock_data['Avg_Volume'] = stock_data['成交量'].rolling(window=window).mean()
    return stock_data


def check_volume_condition(stock_data, threshold=1.5):
    stock_data['Volume_Check'] = stock_data['成交量'] > (stock_data['Avg_Volume'] * threshold)
    return stock_data


def check_continuous_increase(stock_data, days=5):
    stock_data['Price_Increase'] = stock_data['收盘'] > stock_data['收盘'].shift(1)
    stock_data['Continuous_Increase'] = stock_data['Price_Increase'].rolling(window=days).sum() >= days
    return stock_data


def filter_stocks(stock_data):
    return stock_data[stock_data['Continuous_Increase']]
def filter_stocks_volume_count(stock_data,volumeIncrease):
    volume_data = stock_data
    volume_data = volume_data[volume_data['Volume_Check']]
    return volumeIncrease + len(volume_data)


def filter_stocks_continuous_count(stock_data,continuousIncrease):
    continuous_data = stock_data
    continuous_data = continuous_data[continuous_data['Continuous_Increase']]
    return continuousIncrease + len(continuous_data)


def main():
    # 获取上海和深圳交易所A股股票代码
    all_stock_info_sh = ak.stock_info_sh_name_code(symbol="主板A股")
    all_stock_info_sz = ak.stock_info_sz_name_code(symbol= "A股列表")
    # 只选择需要的列
    all_stock_info_sh = all_stock_info_sh[['证券代码']]
    all_stock_info_sz = all_stock_info_sz[['A股代码']]

    all_stock_codes = all_stock_info_sh['证券代码'].tolist() + all_stock_info_sz['A股代码'].tolist()

    # 设置日期范围
    end_date = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')  # 获取最近30天的数据
    volumeIncrease = 0
    continuousIncrease = 0
    gpNum = 0
    for stock_code in all_stock_codes:
        try:
            # 获取股票数据
            stock_data = get_A_stock_data(stock_code, start_date, end_date)
            stock_data['股票代码'] = stock_code

            # 如果数据长度不足，跳过当前股票
            if len(stock_data) < 5:
                continue

            # 计算历史五日平均成交量
            stock_data = calculate_average_volume(stock_data, window=5)

            # 检查当日成交量是否超过平均成交量的1.5倍
            stock_data = check_volume_condition(stock_data, threshold=1.5)

            # 检查连续上涨的股票
            stock_data = check_continuous_increase(stock_data, days=5)
            volumeIncrease = filter_stocks_volume_count(stock_data, volumeIncrease)
            continuousIncrease = filter_stocks_continuous_count(stock_data, continuousIncrease)
            print("成交量1.5倍出现次数:", volumeIncrease , " 股价连续五天增长出现次数:",continuousIncrease)
            gpNum = gpNum + 1
            print("当前处理股票代码：",stock_code," 当前处理股票数量：",gpNum)
            # 过滤符合条件的股票
            # filtered_stocks = filter_stocks(stock_data)

            # 输出符合条件的股票代码信息
            # if not filtered_stocks.empty:
            #     # print(f"\n符合条件的 A 股股票代码 ({stock_code}):")
            #     print(filtered_stocks[['日期','股票代码', '收盘', '成交量', 'Avg_Volume', 'Continuous_Increase']])

        except Exception as e:
            print(f"获取 {stock_code} 数据时发生错误: {e}")
    print("成交量1.5倍出现总次数：", volumeIncrease, " 股价连续五天增长出现总次数：", continuousIncrease)


if __name__ == "__main__":
    main()
