import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.signal import argrelextrema

# ========== 参数 ==========
stock_code = "600089"   # 示例：浦发银行
start_date = "20240101" # 起始时间
end_date   = "20250801" # 截止时间

# ========== 获取数据 ==========
df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date="20240101", adjust="qfq")
df.columns = ["日期","开盘","收盘","最高","最低","成交量","成交额","振幅","涨跌幅","涨跌额","换手率"]
df["日期"] = pd.to_datetime(df["日期"])
df.set_index("日期", inplace=True)

# ========== 计算成交密集区 ==========
price_bins = np.linspace(df["收盘"].min(), df["收盘"].max(), 30)  # 30 个价格区间
vol_profile, edges = np.histogram(df["收盘"], bins=price_bins, weights=df["成交量"])
price_levels = (edges[:-1] + edges[1:]) / 2

# ========== 支撑/压力位（局部极值） ==========
window = 10
df["收盘_max"] = df["收盘"].rolling(window=window).max()
df["收盘_min"] = df["收盘"].rolling(window=window).min()
local_max = argrelextrema(df["收盘"].values, np.greater, order=window)[0]
local_min = argrelextrema(df["收盘"].values, np.less, order=window)[0]

# ========== 可视化 ==========
fig, ax = plt.subplots(figsize=(12, 7))

# 股价走势
ax.plot(df.index, df["收盘"], label="收盘价", color="blue")

# 标注支撑/压力位
ax.scatter(df.index[local_max], df["收盘"].iloc[local_max], marker="^", color="red", label="压力位")
ax.scatter(df.index[local_min], df["收盘"].iloc[local_min], marker="v", color="green", label="支撑位")

# 成交密集区（右侧直方图）
ax2 = ax.twinx()
ax2.barh(price_levels, vol_profile, height=(price_bins[1]-price_bins[0])*0.8, alpha=0.3, color="gray", label="成交密集区")

ax.set_title(f"{stock_code} 支撑/压力位 + 成交密集区")
ax.set_ylabel("股价 (元)")
ax.legend()
plt.show()