import tushare as ts
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from datetime import datetime, timedelta

# 设置 Tushare token
ts.set_token("a7358a0255666758b3ef492a6c1de79f2447de968d4a1785b73716a4")
pro = ts.pro_api()

# 时间区间
end_date = datetime.today()
start_date = end_date - timedelta(days=120)
start_str = start_date.strftime('%Y%m%d')
end_str = end_date.strftime('%Y%m%d')

# 获取 A 股基本信息
stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry,list_date')

results = []

for _, row in stocks.iterrows():
    ts_code = row['ts_code']
    name = row['name']

    try:
        # 获取 K 线数据
        df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_str, end_date=end_str)
        if df is None or df.empty or len(df) < 30:
            continue
        df = df.sort_values('trade_date')

        # 技术指标计算
        close = df['close']
        high = df['high']
        volume = df['vol']

        # 判断 N（新高）
        if close.iloc[-1] < high[-20:].max() * 0.98:
            continue

        # 判断 S（量能）
        if volume.iloc[-1] < volume[-10:].mean() * 1.5:
            continue

        # 判断 M（趋势）
        ma5 = close.rolling(5).mean().iloc[-1]
        ma10 = close.rolling(10).mean().iloc[-1]
        if ma5 <= ma10:
            continue

        # 判断 C + A（财务）
        indicators = pro.fina_indicator(ts_code=ts_code, start_date='20230101', end_date=end_str)
        if indicators.empty:
            continue
        roe_ttm = indicators.iloc[0]['roe']
        if roe_ttm is None or roe_ttm < 10:
            continue

        annual = pro.fina_indicator(ts_code=ts_code, period='20231231')
        if annual.empty or annual.iloc[0]['roe'] < 10:
            continue

        results.append({
            'ts_code': ts_code,
            'name': name,
            'close': close.iloc[-1],
            'vol_ratio': round(volume.iloc[-1] / volume[-10:].mean(), 2),
            'roe_ttm': round(roe_ttm, 2)
        })

    except Exception as e:
        continue

# 输出筛选结果
df_result = pd.DataFrame(results)
df_result = df_result.sort_values(by='vol_ratio', ascending=False)
print(df_result.head(20))