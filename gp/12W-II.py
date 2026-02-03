import akshare as ak
import pandas as pd
import datetime as dt
from tqdm import tqdm

# 配置参数
START_DAYS_AGO = 60
VOLUME_BREAK_MULTIPLE = 2
TURNOVER_RATE_MIN = 1.5
TURNOVER_RATE_MAX = 20
VOLATILITY_THRESHOLD = 0.03
TOP_N_CONCEPTS = 10

def get_all_stock_codes():
    stock_df = ak.stock_info_a_code_name()
    return stock_df["code"].tolist()

def get_stock_kline(code, days=60):
    try:
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=(dt.datetime.today() - dt.timedelta(days=days)).strftime('%Y%m%d'), adjust="qfq")
        df = df[['日期', '开盘', '收盘', '最高', '最低', '成交量', '换手率']]
        df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover']
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date')
        return df
    except Exception as e:
        print(f"[WARN] 获取 {code} 历史数据失败：{e}")
        return None

def calculate_macd(df):
    close = df['close']
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9).mean()
    macd = 2 * (dif - dea)
    df['macd'] = macd
    df['dif'] = dif
    df['dea'] = dea
    return df

def is_macd_gold_cross(df):
    if len(df) < 2:
        return False
    return df.iloc[-2]['dif'] < df.iloc[-2]['dea'] and df.iloc[-1]['dif'] > df.iloc[-1]['dea']

def is_volume_breakout(df, multiple=VOLUME_BREAK_MULTIPLE):
    if len(df) < 20:
        return False
    recent_vol = df.iloc[-1]['volume']
    avg_vol = df['volume'][-20:-1].mean()
    return recent_vol > multiple * avg_vol

def is_turnover_reasonable(df):
    recent = df.iloc[-1]['turnover']
    return TURNOVER_RATE_MIN <= recent <= TURNOVER_RATE_MAX

def is_volatility_stable(df):
    recent = df.tail(10)
    daily_range = (recent['high'] - recent['low']) / recent['close']
    avg_range = daily_range.mean()
    return avg_range < VOLATILITY_THRESHOLD

def get_concept_stock_map(top_n=TOP_N_CONCEPTS):
    try:
        concept_df = ak.stock_board_concept_name_em()
        if concept_df.empty:
            print("[INFO] 无概念板块数据，跳过热度筛选。")
            return pd.DataFrame(columns=["代码", "名称"])

        concept_df = concept_df.sort_values(by='涨跌幅', ascending=False).head(top_n)
        concept_codes = concept_df['板块编码'].tolist()

        result = pd.DataFrame()
        for code in concept_codes:
            try:
                temp_df = ak.stock_board_concept_cons_em(symbol=code)
                result = pd.concat([result, temp_df[['代码', '名称']]])
            except Exception as e:
                print(f"[WARN] 板块 {code} 抓取失败：{e}")
        return result.drop_duplicates()
    except Exception as e:
        print(f"[ERROR] 获取概念板块异常：{e}")
        return pd.DataFrame(columns=["代码", "名称"])

def get_candidate_stocks():
    all_stocks = get_all_stock_codes()
    concept_stocks_df = get_concept_stock_map()
    concept_codes = set(concept_stocks_df['代码'].tolist())

    selected = []
    for code in tqdm(all_stocks, desc="选股中"):
        df = get_stock_kline(code)
        if df is None or len(df) < 30:
            continue

        df = calculate_macd(df)

        if (
            is_macd_gold_cross(df)
            and is_volume_breakout(df)
            and is_turnover_reasonable(df)
            and is_volatility_stable(df)
        ):
            if code in concept_codes:
                selected.append(code)

    return selected

if __name__ == "__main__":
    stocks = get_candidate_stocks()
    if stocks:
        print("\n✅ 筛选出的优质股票：")
        for code in stocks:
            print(code)
    else:
        print("\n❌ 当前无符合条件的股票")