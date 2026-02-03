import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

# 时间区间
end_date = datetime.today()
start_date = end_date - timedelta(days=120)
start_str = start_date.strftime('%Y%m%d')
end_str = end_date.strftime('%Y%m%d')

# 获取沪深 A 股代码（不包含北交所）
try:
    df_sh = ak.stock_info_sh_name_code()
    df_sz = ak.stock_info_sz_name_code()
    stock_list = pd.concat([df_sh, df_sz], ignore_index=True)
except Exception as e:
    print("获取股票列表失败：", e)
    stock_list = pd.DataFrame()

results = []

for _, row in stock_list.iterrows():
    code = row['证券代码']
    name = row['证券简称']
    ts_code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"

    try:
        df = ak.stock_zh_a_hist(ts_code, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
        if df is None or df.empty or len(df) < 30:
            continue
        df = df.sort_values(by='日期')
        df.reset_index(drop=True, inplace=True)

        # N: 当前价格接近20日内新高
        high_20 = df['最高'][-20:].max()
        if df['收盘'].iloc[-1] < 0.98 * high_20:
            continue

        # S: 当前成交量 > 近10日均量的1.5倍
        vol = df['成交量']
        if vol.iloc[-1] < vol[-10:].mean() * 1.5:
            continue

        # M: 均线多头排列
        close = df['收盘']
        ma5 = close.rolling(5).mean().iloc[-1]
        ma10 = close.rolling(10).mean().iloc[-1]
        if ma5 <= ma10:
            continue

        # MACD 金叉（DIF 上穿 DEA）
        def calc_macd(close, fast=12, slow=26, signal=9):
            ema_fast = close.ewm(span=fast).mean()
            ema_slow = close.ewm(span=slow).mean()
            dif = ema_fast - ema_slow
            dea = dif.ewm(span=signal).mean()
            macd = (dif - dea) * 2
            return dif, dea, macd

        dif, dea, _ = calc_macd(close)
        if not (dif.iloc[-2] < dea.iloc[-2] and dif.iloc[-1] > dea.iloc[-1]):
            continue

        # C + A: 利润增长（需真实财务数据，可接 Tushare token）
        # 这里用模拟判断：过滤掉新股（上市日期 > 2023）
        if code.startswith('8') or code.startswith('4'):
            continue
        try:
            year = int(code[:4])
            if year > 2023:
                continue
        except:
            pass

        results.append({
            'ts_code': ts_code,
            'name': name,
            'close': round(close.iloc[-1], 2),
            'volume_ratio': round(vol.iloc[-1] / vol[-10:].mean(), 2),
            'macd_golden_cross': 'Yes'
        })

    except Exception as e:
        print(f"{ts_code} 处理失败: {e}")
        time.sleep(1)
        continue

# 输出结果
df_result = pd.DataFrame(results)
df_result = df_result.sort_values(by='volume_ratio', ascending=False)
print("\n🎯 筛选结果（前20只）:\n")
print(df_result.head(20))