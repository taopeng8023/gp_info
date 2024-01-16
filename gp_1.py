import akshare as ak
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# 1. 获取所有股票名称
def get_all_stock_names():
    stock_info = ak.stock_info_a_code_name()
    return stock_info


# 2. 通过爬虫技术采集股票相关的新闻信息
def scrape_stock_news(stock_symbol):
    url = f'https://finance.yahoo.com/quote/{stock_symbol}/news?p={stock_symbol}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    headlines = [headline.get_text(strip=True) for headline in soup.select('.js-stream-content li h3')]
    return headlines


# 3. 获取股票对应的财务数据信息
def get_financial_data(stock_symbol):
    financial_data = ak.stock_financial_report_sina(stock_symbol,symbol="资产负债表")
    return financial_data


# 4. 获取股价走势信息
def get_stock_price(stock_symbol):
    stock_price = ak.stock_zh_a_minute(symbol=stock_symbol, period="30d")
    return stock_price


# 5. 通过股价信息计算KDJ指标
def calculate_kdj(data):
    low_list = pd.Series.rolling(data['low'], window=9, min_periods=1).min()
    high_list = pd.Series.rolling(data['high'], window=9, min_periods=1).max()
    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    data['K'] = pd.Series.ewm(rsv, span=3, min_periods=1).mean()
    data['D'] = pd.Series.ewm(data['K'], span=3, min_periods=1).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    return data


# 6. 通过股价信息计算Boll指标
def calculate_boll(data):
    data['20_MA'] = data['close'].rolling(window=20).mean()
    data['upper_band'] = data['20_MA'] + 2 * data['close'].rolling(window=20).std()
    data['lower_band'] = data['20_MA'] - 2 * data['close'].rolling(window=20).std()
    return data


# 7. 通过综合分析得到适合投资的股票
def analyze_stock(stock_symbol):
    try:
        # 2. 采集股票相关新闻
        news = scrape_stock_news(stock_symbol)

        # 3. 获取股票对应的财务数据信息
        financial_data = get_financial_data(stock_symbol)

        # 4. 获取股价走势信息
        stock_price = get_stock_price(stock_symbol)

        # 5. 计算KDJ指标
        kdj_data = calculate_kdj(stock_price)

        # 6. 计算Boll指标
        boll_data = calculate_boll(stock_price)

        # 7. 进行适合投资的分析（这里简化为Boll上轨向上、K线上穿D线为买入信号）
        if kdj_data['K'].iloc[-1] > kdj_data['D'].iloc[-1] and boll_data['close'].iloc[-1] > \
                boll_data['upper_band'].iloc[-1]:
            print(f"\n股票代码: {stock_symbol}")
            print("适合投资，买入信号。")

    except Exception as e:
        print(f"Error processing stock {stock_symbol}: {str(e)}")


# 1. 获取所有股票名称
stocks = get_all_stock_names()

# 遍历所有股票
for stock_symbol, stock_name in zip(stocks['code'], stocks['name']):
    # 逐个分析股票
    analyze_stock(stock_symbol)



# python 写一个程序，要求如下
#
# 1，获取所有股票名称
# 2，通过爬虫技术采集股票相关的新闻信息
# 3，获取股票对应的财务数据信息
# 4，获取股价走势信息
# 5，通过股价信息计算KDJ指标
# 6，通过股价信息计算boll指标
# 7，通过综合分析新闻信息，财务数据信息，KDJ指标信息，boll指标信息得到适合投资的股票