import akshare as ak
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm


def get_stock_data(stock_code):
    """获取股票最新行情"""
    try:
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
        if df is None or df.empty:
            return None
    except Exception as e:
        print(f"⚠️ 获取 {stock_code} 数据失败，错误: {e}")
        return None

    df.rename(columns={"日期": "date", "收盘": "close", "开盘": "open", "最高": "high", "最低": "low",
                       "成交量": "volume", "换手率": "turnover", "涨跌幅": "change_pct"}, inplace=True)

    df["date"] = pd.to_datetime(df["date"])

    # 计算量比（成交量/近5日均量）
    df["volume_ratio"] = df["volume"] / df["volume"].rolling(5).mean()

    # 计算 K 线形态：是否存在长上影线（高点比收盘价高 3% 以上）
    df["upper_shadow"] = (df["high"] - df["close"]) / df["close"] > 0.03

    # 获取最新数据
    latest = df.iloc[-1]

    # 获取市值
    stock_info = ak.stock_individual_info_em(symbol=stock_code)
    if stock_info is None or stock_info.empty:
        return None
    market_cap = stock_info.loc[stock_info["item"] == "总市值", "value"].values
    if len(market_cap) == 0:
        return None
    market_cap_value = market_cap[0]  # 取出第一个值
    if isinstance(market_cap_value, str):  # 只有当它是字符串时才替换
        market_cap_value = market_cap_value.replace("亿", "")
    market_cap = float(market_cap_value)  # 转换为浮点数

    return {
        "code": stock_code,
        "close": latest["close"],
        "change_pct": latest["change_pct"],
        "volume_ratio": latest["volume_ratio"],
        "turnover": latest["turnover"],
        "upper_shadow": latest["upper_shadow"],
        "market_cap": market_cap
    }


def filter_stock(stock_data):
    """筛选符合条件的股票"""
    if stock_data is None:
        return None

    # 1. 涨幅 3%-5%
    if not (3 <= stock_data["change_pct"] <= 5):
        return None

    # 2. 量比大于 1
    if stock_data["volume_ratio"] < 1:
        return None

    # 3. 换手率在 5%-10% 之间
    if not (5 <= stock_data["turnover"] <= 10):
        return None

    # 4. 剔除长上影线股票
    if stock_data["upper_shadow"]:
        return None

    # 5. 市值筛选：50-200 亿
    if not (50 <= stock_data["market_cap"] <= 200):
        return None

    return stock_data


if __name__ == '__main__':  # 👈 **必须加上这个**
    # 1️⃣ 获取 A 股列表，并剔除 ST、退市、新三板、科创板股票
    stock_list_df = ak.stock_info_a_code_name()
    stock_list_df = stock_list_df[
        ~stock_list_df["name"].str.contains("ST|退市") &
        ~stock_list_df["code"].astype(str).str.startswith(("688", "8", "4"))
        ]
    stock_list = stock_list_df["code"].tolist()

    # 2️⃣ 并行处理
    with Pool(processes=10) as pool:
        results = list(tqdm(pool.imap(get_stock_data, stock_list), total=len(stock_list), desc="获取股票数据"))

    # 3️⃣ 筛选符合条件的股票
    filtered_stocks = [filter_stock(stock) for stock in results if filter_stock(stock)]

    # 4️⃣ 输出结果
    print("\n📈 **符合筛选条件的股票**:")
    for stock in filtered_stocks:
        print(
            f"✅ {stock['code']} | 涨幅: {stock['change_pct']:.2f}% | 换手率: {stock['turnover']:.2f}% | 市值: {stock['market_cap']} 亿")