import akshare as ak
import pandas as pd
from tqdm import tqdm

# 获取所有 A 股股票列表
stock_list_df = ak.stock_info_a_code_name()
stock_list = stock_list_df["code"].tolist()

# 过滤 ST、退市股
stock_list = [code for code in stock_list if not code.startswith(("ST", "*"))]

# 存储符合交易信号的股票
buy_signals_list = []
sell_signals_list = []

# 遍历所有股票
for stock_code in tqdm(stock_list, desc="分析 A 股股票"):
    try:
        # 获取日 K 线数据（更换为 stock_zh_a_hist）
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")

        # 检查数据是否为空
        if df is None or df.empty:
            print(f"⚠️ {stock_code} 数据为空，跳过")
            continue

        # 重命名列，确保兼容性
        df.rename(columns={"日期": "date", "收盘": "close", "开盘": "open", "最高": "high", "最低": "low", "成交量": "volume"}, inplace=True)

        # 转换 date 格式
        df["date"] = pd.to_datetime(df["date"])

        # 计算均线
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()

        # 计算 MACD（12,26,9 标准参数）
        df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["dif"] = df["ema12"] - df["ema26"]
        df["dea"] = df["dif"].ewm(span=9, adjust=False).mean()
        df["macd"] = (df["dif"] - df["dea"]) * 2

        # 计算 KDJ（9,3,3 标准参数）
        low_list = df["low"].rolling(9).min()
        high_list = df["high"].rolling(9).max()
        df["rsv"] = (df["close"] - low_list) / (high_list - low_list) * 100
        df["kdj_k"] = df["rsv"].ewm(alpha=1 / 3).mean()
        df["kdj_d"] = df["kdj_k"].ewm(alpha=1 / 3).mean()
        df["kdj_j"] = 3 * df["kdj_k"] - 2 * df["kdj_d"]

        # 计算买卖信号
        df["signal"] = 0
        df.loc[(df["ma5"] > df["ma20"]) & (df["ma5"].shift(1) <= df["ma20"].shift(1)), "signal"] = 1  # 均线金叉买入
        df.loc[(df["ma5"] < df["ma20"]) & (df["ma5"].shift(1) >= df["ma20"].shift(1)), "signal"] = -1  # 均线死叉卖出

        # 筛选 MACD 金叉/死叉
        df["macd_signal"] = 0
        df.loc[(df["macd"] > 0) & (df["macd"].shift(1) <= 0), "macd_signal"] = 1  # MACD 金叉
        df.loc[(df["macd"] < 0) & (df["macd"].shift(1) >= 0), "macd_signal"] = -1  # MACD 死叉

        # 筛选 KDJ 指标
        df["kdj_signal"] = 0
        df.loc[(df["kdj_j"] < 30) & (df["kdj_k"] < df["kdj_d"]), "kdj_signal"] = 1  # KDJ 超卖区买入
        df.loc[(df["kdj_j"] > 85) & (df["kdj_k"] > df["kdj_d"]), "kdj_signal"] = -1  # KDJ 超买区卖出

        # 筛选成交量突破
        df["volume_signal"] = 0
        df["volume_avg"] = df["volume"].rolling(window=20).mean()
        df.loc[(df["volume"] > df["volume_avg"] * 1.5) & (df["volume"] > df["volume"].shift(1)), "volume_signal"] = 1  # 成交量突破

        # 汇总所有信号
        df["final_signal"] = df["signal"] + df["macd_signal"] + df["kdj_signal"] + df["volume_signal"]

        # 获取最新信号
        last_signal = df.iloc[-1]["final_signal"]
        stock_name = stock_list_df[stock_list_df["code"] == stock_code]["name"].values[0]

        if last_signal == 3:  # 满足多个条件的买入信号
            buy_signals_list.append((stock_code, stock_name, df.iloc[-1]["date"].date(), df.iloc[-1]["close"]))
        elif last_signal == -3:  # 满足多个条件的卖出信号
            sell_signals_list.append((stock_code, stock_name, df.iloc[-1]["date"].date(), df.iloc[-1]["close"]))
    except Exception as e:
        print(f"⚠️ 处理 {stock_code} 时出错: {e}")

# 输出买卖信号
print("\n📈 **买入信号股票**:")
for code, name, date, price in buy_signals_list:
    print(f"✅ {date} | {code} {name} | 收盘价: {price:.2f}")

print("\n📉 **卖出信号股票**:")
for code, name, date, price in sell_signals_list:
    print(f"❌ {date} | {code} {name} | 收盘价: {price:.2f}")