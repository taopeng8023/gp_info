import akshare as ak
import pandas as pd

# Replace 'stock_symbol' with the desired stock symbol (e.g., 'sh600519' for Kweichow Moutai)
stock_symbol = '605366'

# Fetch historical stock price data
stock_data = ak.stock_zh_a_hist(symbol=stock_symbol, period="daily")

# Ensure that the data is sorted by 日期 in ascending order
stock_data = stock_data.sort_values(by='日期')

# Define the parameters for KDJ
n = 9  # Length of the KDJ period

# Calculate RSV (未成熟随机值)
stock_data['L9'] = stock_data['最低'].rolling(window=n).min()
stock_data['H9'] = stock_data['最高'].rolling(window=n).max()
stock_data['RSV'] = (stock_data['收盘'] - stock_data['L9']) / (stock_data['H9'] - stock_data['L9']) * 100

# Calculate K, D, and J values
stock_data['K'] = stock_data['RSV'].ewm(span=3, adjust=False).mean()
stock_data['D'] = stock_data['K'].ewm(span=3, adjust=False).mean()
stock_data['J'] = 3 * stock_data['D'] - 2 * stock_data['K']

# Display the resulting DataFrame
print(stock_data[['日期', '收盘', '最低', '最高', 'RSV', 'K', 'D', 'J']])
