import akshare as ak
import pandas as pd
import numpy as np
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
from sklearn.ensemble import VotingClassifier
import xgboost as xgb
import matplotlib.pyplot as plt

# 映射中文列名到英文
COLUMNS_MAP = {
    "日期": "Date",
    "开盘": "Open",
    "收盘": "Close",
    "最高": "High",
    "最低": "Low",
    "成交量": "Volume",
    "成交额": "Amount",
    "振幅": "Amplitude",
    "涨跌幅": "Change Percent",
    "涨跌额": "Change Amount",
    "换手率": "Turnover Rate"
}


# 获取A股数据
def fetch_stock_data_a(ticker, start_date, end_date):
    """
    获取A股股票历史数据，并映射列名。
    """
    try:
        stock_data = ak.stock_zh_a_hist(
            symbol=ticker,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust="qfq"
        )
        stock_data.rename(columns=COLUMNS_MAP, inplace=True)
        stock_data['Date'] = pd.to_datetime(stock_data['Date'])
        stock_data.set_index('Date', inplace=True)
        return stock_data[['Open', 'Close', 'High', 'Low', 'Volume']].dropna()
    except Exception as e:
        print(f"获取A股数据失败：{e}")
        return pd.DataFrame()


# 计算技术指标
def calculate_technical_indicators(data):
    """
    计算技术指标。
    """
    # 移动平均线
    data['MA20'] = data['Close'].rolling(window=20).mean()
    data['MA50'] = data['Close'].rolling(window=50).mean()

    # 相对强弱指数 (RSI)
    delta = data['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = 100 - (100 / (1 + rs))

    # 布林带
    data['BB_Mid'] = data['Close'].rolling(window=20).mean()
    data['BB_Upper'] = data['BB_Mid'] + 2 * data['Close'].rolling(window=20).std()
    data['BB_Lower'] = data['BB_Mid'] - 2 * data['Close'].rolling(window=20).std()

    # MACD指标
    ema_12 = data['Close'].ewm(span=12, adjust=False).mean()
    ema_26 = data['Close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = ema_12 - ema_26
    data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()

    return data


# 准备特征和标签
def prepare_features(data, target_period=1):
    """
    准备机器学习模型的特征和标签，使用target_period调整目标期。
    """
    data['Target'] = (data['Close'].shift(-target_period) > data['Close']).astype(int)  # 修改目标期（1、3、5天）

    features = data[['MA20', 'MA50', 'RSI', 'BB_Mid', 'BB_Upper', 'BB_Lower',
                     'MACD', 'Signal_Line']].fillna(0)
    labels = data['Target']
    return features, labels


# 使用XGBoost进行训练
def train_xgboost_model(features, labels):
    """
    使用XGBoost模型进行训练和预测，并调优超参数。
    """
    # 参数调优
    param_grid = {
        'max_depth': [3, 5, 7],
        'learning_rate': [0.01, 0.05, 0.1],
        'n_estimators': [100, 200],
        'subsample': [0.8, 0.9, 1.0],
        'colsample_bytree': [0.8, 0.9, 1.0]
    }
    grid_search = GridSearchCV(xgb.XGBClassifier(eval_metric='logloss', random_state=42),
                               param_grid, cv=3, n_jobs=-1, verbose=2)
    grid_search.fit(features, labels)
    print("最佳参数：", grid_search.best_params_)

    # 使用最佳参数训练模型
    model = grid_search.best_estimator_
    predictions = model.predict(features)

    # 模型评估
    accuracy = accuracy_score(labels, predictions)
    precision = precision_score(labels, predictions)
    recall = recall_score(labels, predictions)
    f1 = f1_score(labels, predictions)

    print(f"模型评估：准确率: {accuracy:.4f}, 精确度: {precision:.4f}, 召回率: {recall:.4f}, F1分数: {f1:.4f}")
    print("分类报告：")
    print(classification_report(labels, predictions))

    return model, predictions


# 回测并输出买卖点
def backtest(data, predictions, initial_balance=100000):
    """
    执行回测并计算收益，同时记录买卖点。
    """
    balance = initial_balance
    position = 0
    buy_signals = []
    sell_signals = []

    for i in range(len(predictions)):
        close_price = data['Close'].iloc[i]
        if predictions[i] == 1 and position == 0:  # 买入信号
            position = balance / close_price
            balance = 0
            buy_signals.append(data.index[i])
        elif predictions[i] == 0 and position > 0:  # 卖出信号
            balance = position * close_price
            position = 0
            sell_signals.append(data.index[i])

    # 最终结算
    if position > 0:
        balance += position * data['Close'].iloc[-1]

    total_return = (balance - initial_balance) / initial_balance * 100
    print(f"回测结果：初始资金: {initial_balance:.2f} 元, 最终资金: {balance:.2f} 元, 总收益率: {total_return:.2f}%")

    # 绘制图表
    plt.figure(figsize=(12, 6))
    plt.plot(data['Close'], label="Stock Price", color='blue', alpha=0.7)
    plt.scatter(buy_signals, data.loc[buy_signals]['Close'], marker='^', color='green', label='Buy Signal', alpha=1)
    plt.scatter(sell_signals, data.loc[sell_signals]['Close'], marker='v', color='red', label='Sell Signal', alpha=1)
    plt.title(f"Stock Price with Buy/Sell Signals ({total_return:.2f}% Total Return)")
    plt.xlabel('Date')
    plt.ylabel('Price (CNY)')
    plt.legend(loc='best')
    plt.grid(True)
    plt.show()

    return total_return


# 调整分类阈值
def adjust_threshold(predictions, probabilities, threshold=0.6):
    """
    调整分类阈值
    """
    return (probabilities[:, 1] > threshold).astype(int)


if __name__ == "__main__":
    ticker = input("请输入A股股票代码（例如 600519）：")
    start_date = input("请输入开始日期（格式：YYYY-MM-DD）：")
    end_date = input("请输入结束日期（格式：YYYY-MM-DD）：")

    # 获取数据
    stock_data = fetch_stock_data_a(ticker, start_date, end_date)

    if not stock_data.empty:
        stock_data = calculate_technical_indicators(stock_data)

        # 准备特征和标签
        features, labels = prepare_features(stock_data, target_period=5)  # 使用5天目标期

        # 训练模型并评估
        model, predictions = train_xgboost_model(features, labels)

        # 获取预测概率并调整阈值
        probabilities = model.predict_proba(features)
        adjusted_predictions = adjust_threshold(predictions, probabilities, threshold=0.7)  # 调整阈值为0.7

        # 回测并输出收益
        backtest(stock_data, adjusted_predictions)
