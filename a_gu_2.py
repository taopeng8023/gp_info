import akshare as ak
import pandas as pd
import numpy as np
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
from imblearn.over_sampling import SMOTE
import xgboost as xgb
import lightgbm as lgb
import matplotlib.pyplot as plt


# 获取A股数据
def fetch_stock_data_a(ticker, start_date, end_date):
    try:
        stock_data = ak.stock_zh_a_hist(
            symbol=ticker,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust="qfq"
        )
        stock_data.rename(columns={'日期': 'Date', '开盘': 'Open', '收盘': 'Close', '最高': 'High', '最低': 'Low',
                                   '成交量': 'Volume'}, inplace=True)
        stock_data['Date'] = pd.to_datetime(stock_data['Date'])
        stock_data.set_index('Date', inplace=True)
        return stock_data[['Open', 'Close', 'High', 'Low', 'Volume']].dropna()
    except Exception as e:
        print(f"获取A股数据失败：{e}")
        return pd.DataFrame()


# 计算技术指标
def calculate_technical_indicators(data):
    data['MA20'] = data['Close'].rolling(window=20).mean()
    data['MA50'] = data['Close'].rolling(window=50).mean()

    delta = data['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = 100 - (100 / (1 + rs))

    data['BB_Mid'] = data['Close'].rolling(window=20).mean()
    data['BB_Upper'] = data['BB_Mid'] + 2 * data['Close'].rolling(window=20).std()
    data['BB_Lower'] = data['BB_Mid'] - 2 * data['Close'].rolling(window=20).std()

    ema_12 = data['Close'].ewm(span=12, adjust=False).mean()
    ema_26 = data['Close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = ema_12 - ema_26
    data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()

    high_low = data['High'] - data['Low']
    high_close = (data['High'] - data['Close'].shift()).abs()
    low_close = (data['Low'] - data['Close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1)
    atr = tr.max(axis=1).rolling(window=14).mean()
    data['ATR'] = atr

    return data


# 准备特征和标签
def prepare_features(data, target_period=1):
    data['Target'] = (data['Close'].shift(-target_period) > data['Close']).astype(int)
    features = data[['MA20', 'MA50', 'RSI', 'BB_Mid', 'BB_Upper', 'BB_Lower', 'MACD', 'Signal_Line', 'ATR']].dropna()
    labels = data['Target'].loc[features.index]
    return features, labels


# 使用XGBoost和LightGBM进行训练
def train_models(features, labels):
    xgb_model = xgb.XGBClassifier(eval_metric='logloss', random_state=42)
    lgb_model = lgb.LGBMClassifier(random_state=42)

    xgb_model.fit(features, labels)
    lgb_model.fit(features, labels)

    return xgb_model, lgb_model


# 模型评估
def evaluate_model(model, features, labels):
    predictions = model.predict(features)
    accuracy = accuracy_score(labels, predictions)
    precision = precision_score(labels, predictions)
    recall = recall_score(labels, predictions)
    f1 = f1_score(labels, predictions)

    print(f"准确率: {accuracy:.4f}, 精确度: {precision:.4f}, 召回率: {recall:.4f}, F1分数: {f1:.4f}")
    print("分类报告：")
    print(classification_report(labels, predictions))


# 回测优化
def backtest(data, predictions, initial_balance=100000, stop_loss=0.05, take_profit=0.1, transaction_cost=0.001):
    balance = initial_balance
    position = 0
    buy_signals = []
    sell_signals = []
    stop_loss_price = None
    take_profit_price = None

    predictions = predictions[:len(data)]

    for i in range(len(predictions)):
        close_price = data['Close'].iloc[i]
        if predictions[i] == 1 and position == 0:
            position = balance / close_price
            balance = 0
            buy_signals.append(i)
            stop_loss_price = close_price * (1 - stop_loss)
            take_profit_price = close_price * (1 + take_profit)
        elif predictions[i] == 0 and position > 0:
            balance = position * close_price * (1 - transaction_cost)
            position = 0
            sell_signals.append(i)

        if position > 0:
            if close_price <= stop_loss_price or close_price >= take_profit_price:
                balance = position * close_price * (1 - transaction_cost)
                position = 0
                sell_signals.append(i)
                stop_loss_price = None
                take_profit_price = None

    if position > 0:
        balance = position * data['Close'].iloc[-1] * (1 - transaction_cost)

    final_balance = balance
    total_return = (final_balance - initial_balance) / initial_balance * 100
    print(
        f"回测结果：初始资金: {initial_balance:.2f} 元, 最终资金: {final_balance:.2f} 元, 总收益率: {total_return:.2f}%")


# 主函数
def main():
    # 获取数据
    ticker = '002456'  # 上证指数或其它股票
    start_date = '2020-01-01'
    end_date = '2024-11-28'
    data = fetch_stock_data_a(ticker, start_date, end_date)

    # 计算技术指标
    data = calculate_technical_indicators(data)

    # 准备特征和标签
    features, labels = prepare_features(data)

    # 训练模型
    xgb_model, lgb_model = train_models(features, labels)

    # 模型评估
    print("XGBoost模型评估：")
    evaluate_model(xgb_model, features, labels)

    print("\nLightGBM模型评估：")
    evaluate_model(lgb_model, features, labels)

    # 进行回测
    print("\n回测结果（XGBoost预测）：")
    backtest(data, xgb_model.predict(features), initial_balance=100000)

    print("\n回测结果（LightGBM预测）：")
    backtest(data, lgb_model.predict(features), initial_balance=100000)


# 运行主程序
if __name__ == "__main__":
    main()
