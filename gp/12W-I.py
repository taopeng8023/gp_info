import akshare as ak
import pandas as pd
import datetime
import numpy as np
from tqdm import tqdm


def get_candidate_stocks():
    stock_df = ak.stock_info_a_code_name()
    # 剔除ST、退市股
    return stock_df[~stock_df['name'].str.contains('ST|退')]


def get_recent_ohlcv(stock_code: str, days: int = 60):
    stock_code = stock_code.strip()
    if stock_code.startswith("6"):
        code = f"sh{stock_code}"
    else:
        code = f"sz{stock_code}"
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20240101", adjust="qfq")
        df = df.tail(days)
        df = df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'turnover'
        })
        df['date'] = pd.to_datetime(df['date'])
        return df.reset_index(drop=True)
    except:
        return pd.DataFrame()


def calculate_macd(df):
    close = df['close']
    short_ema = close.ewm(span=12, adjust=False).mean()
    long_ema = close.ewm(span=26, adjust=False).mean()
    macd = short_ema - long_ema
    signal = macd.ewm(span=9, adjust=False).mean()
    df['macd'] = macd
    df['signal'] = signal
    df['macd_diff'] = macd - signal
    return df


def is_macd_golden_cross(df):
    if len(df) < 2:
        return False
    return df['macd_diff'].iloc[-2] < 0 and df['macd_diff'].iloc[-1] > 0


def is_volume_breakout(df):
    if len(df) < 6:
        return False
    recent_vol = df['volume'].iloc[-1]
    avg_vol = df['volume'].iloc[-6:-1].mean()
    return recent_vol > 1.5 * avg_vol


def calc_turnover_rate(df):
    if len(df) < 1:
        return 0
    last = df.iloc[-1]
    try:
        return float(last['volume']) / 100 / get_float_shares(last['code'])  # 百万股为单位
    except:
        return 0


def get_float_shares(stock_code: str) -> float:
    try:
        df = ak.stock_fund_share_structure(stock=stock_code)
        latest = df[df['报告日期'] == df['报告日期'].max()]
        return float(latest['流通A股(万股)'].values[0])
    except:
        return 1e8  # fallback


def calc_volatility(df, period: int = 5) -> float:
    if len(df) < period:
        return 0
    returns = df['close'].pct_change().dropna()
    return returns[-period:].std()


def stock_selector():
    result = []
    stocks = get_candidate_stocks()

    for _, row in tqdm(stocks.iterrows(), total=len(stocks)):
        code = row['code']
        name = row['name']
        df = get_recent_ohlcv(code, 60)
        if df.empty or len(df) < 30:
            continue

        df = calculate_macd(df)
        if not is_macd_golden_cross(df):
            continue
        if not is_volume_breakout(df):
            continue

        # 替换代码，用于换手率和波动率控制
        df['code'] = code
        turnover_rate = calc_turnover_rate(df)
        if turnover_rate < 0.01 or turnover_rate > 0.2:
            continue

        volatility = calc_volatility(df)
        if volatility < 0.01 or volatility > 0.06:
            continue

        result.append({
            'code': code,
            'name': name,
            'turnover_rate': round(turnover_rate, 4),
            'volatility': round(volatility, 4),
            'close': df['close'].iloc[-1],
            'macd_diff': round(df['macd_diff'].iloc[-1], 4),
        })

    return pd.DataFrame(result)


if __name__ == '__main__':
    selected = stock_selector()
    if not selected.empty:
        print("✅ 本轮选出的优质股票：")
        print(selected.sort_values(by='macd_diff', ascending=False).reset_index(drop=True))
        selected.to_csv(f"selected_stocks_{datetime.date.today()}.csv", index=False)
    else:
        print("❌ 本轮无符合条件股票")