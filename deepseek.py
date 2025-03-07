import akshare as ak
import pandas as pd
import time
from tqdm import tqdm
from multiprocessing import Pool, freeze_support


def get_stock_data(stock_code):
    """获取股票历史数据，增加重试机制"""
    df = None
    retries = 3
    for i in range(retries):
        try:
            df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
            if df is not None and not df.empty:
                return df
        except Exception as e:
            print(f"⚠️ 获取 {stock_code} 数据失败（{i + 1}/{retries}），错误: {e}")
            time.sleep(1)  # 避免 API 频繁请求
    return None


def calculate_signals(stock_code):
    """计算买卖信号"""
    df = get_stock_data(stock_code)
    if df is None or df.empty:
        return None

    try:
        df.rename(columns={"日期": "date", "收盘": "close", "开盘": "open", "最高": "high", "最低": "low",
                           "成交量": "volume"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"])

        # 计算均线
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()

        # 计算 MACD
        df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["dif"] = df["ema12"] - df["ema26"]
        df["dea"] = df["dif"].ewm(span=9, adjust=False).mean()
        df["macd"] = (df["dif"] - df["dea"]) * 2

        # 计算 KDJ
        low_list = df["low"].rolling(9).min()
        high_list = df["high"].rolling(9).max()
        df["rsv"] = (df["close"] - low_list) / (high_list - low_list) * 100
        df["kdj_k"] = df["rsv"].ewm(alpha=1 / 3).mean()
        df["kdj_d"] = df["kdj_k"].ewm(alpha=1 / 3).mean()
        df["kdj_j"] = 3 * df["kdj_k"] - 2 * df["kdj_d"]

        # 计算 RSI
        df["rsi"] = 100 - (100 / (
                    1 + df["close"].pct_change().rolling(14).mean() / df["close"].pct_change().rolling(14).std()))

        # 计算布林带
        df["boll_mid"] = df["close"].rolling(20).mean()
        df["boll_std"] = df["close"].rolling(20).std()
        df["boll_upper"] = df["boll_mid"] + 2 * df["boll_std"]
        df["boll_lower"] = df["boll_mid"] - 2 * df["boll_std"]

        # 计算成交量突破
        df["volume_avg"] = df["volume"].rolling(window=20).mean()

        # 计算信号
        df["ma5_shift"] = df["ma5"].shift(1)
        df["ma20_shift"] = df["ma20"].shift(1)
        df["macd_shift"] = df["macd"].shift(1)
        df["volume_shift"] = df["volume"].shift(1)

        def generate_signal(row):
            signal = 0
            if row["ma5"] > row["ma20"] and row["ma5_shift"] <= row["ma20_shift"]:
                signal += 1  # 均线金叉
            if row["ma5"] < row["ma20"] and row["ma5_shift"] >= row["ma20_shift"]:
                signal -= 1  # 均线死叉
            if row["macd"] > 0 and row["macd_shift"] <= 0:
                signal += 1  # MACD 金叉
            if row["macd"] < 0 and row["macd_shift"] >= 0:
                signal -= 1  # MACD 死叉
            if row["kdj_j"] < 30 and row["kdj_k"] < row["kdj_d"] and row["rsi"] < 30:
                signal += 1  # KDJ 超卖 + RSI 低
            if row["kdj_j"] > 85 and row["kdj_k"] > row["kdj_d"] and row["rsi"] > 70:
                signal -= 1  # KDJ 超买 + RSI 高
            if row["volume"] > row["volume_avg"] * 1.5 and row["volume"] > row["volume_shift"]:
                signal += 1  # 成交量突破
            if row["close"] < row["boll_lower"]:
                signal += 1  # 布林带下轨突破
            if row["close"] > row["boll_upper"]:
                signal -= 1  # 布林带上轨突破
            return signal

        df["final_signal"] = df.apply(generate_signal, axis=1)

        # 获取最新信号
        last_signal = df.iloc[-1]["final_signal"]
        return (stock_code, last_signal, df.iloc[-1]["date"].date(), df.iloc[-1]["close"])
    except Exception as e:
        print(f"⚠️ 处理 {stock_code} 时出错: {e}")
    return None


if __name__ == '__main__':
    freeze_support()  # 适用于打包 exe

    # 获取所有 A 股股票列表，并过滤 ST 和退市股
    stock_list_df = ak.stock_info_a_code_name()
    stock_list_df = stock_list_df[
        ~stock_list_df["name"].str.contains("ST|退市") &
        ~stock_list_df["code"].astype(str).str.startswith(("688", "8", "4"))  # 去掉科创板和新三板
]
    stock_list = stock_list_df["code"].tolist()

    # 多进程处理
    with Pool(processes=8) as pool:
        results = list(tqdm(pool.imap(calculate_signals, stock_list), total=len(stock_list), desc="分析 A 股"))

    # 处理结果
    buy_signals_list = []
    sell_signals_list = []

    for res in results:
        if res:
            stock_code, last_signal, date, close_price = res
            stock_name = stock_list_df.loc[stock_list_df["code"] == stock_code, "name"].values[0]

            if last_signal >= 3:
                buy_signals_list.append((stock_code, stock_name, date, close_price))
            elif last_signal <= -3:
                sell_signals_list.append((stock_code, stock_name, date, close_price))

    # 输出买卖信号
    print("\n📈 **买入信号股票**:")
    for code, name, date, price in buy_signals_list:
        print(f"✅ {date} | {code} {name} | 收盘价: {price:.2f}")

    print("\n📉 **卖出信号股票**:")
    for code, name, date, price in sell_signals_list:
        print(f"❌ {date} | {code} {name} | 收盘价: {price:.2f}")