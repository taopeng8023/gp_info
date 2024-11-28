import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
import akshare as ak


# 加载数据
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


# 特征工程
def feature_engineering(data):
    # 增加更多的技术指标或衍生特征
    data['EMA20'] = data['Close'].ewm(span=20).mean()
    data['MACD'] = data['Close'].ewm(span=12).mean() - data['Close'].ewm(span=26).mean()
    data['Signal_Line'] = data['MACD'].ewm(span=9).mean()
    data['MACD_Histogram'] = data['MACD'] - data['Signal_Line']
    data['Momentum'] = data['Close'] / data['Close'].shift(5) - 1
    data.dropna(inplace=True)
    return data


# 数据预处理
def preprocess_data(data):
    # 创建目标列，涨为1，跌为0
    data['Target'] = (data['Close'].shift(-1) > data['Close']).astype(int)  # 下一天的收盘价大于当天为1，否则为0
    features = data[['Open', 'High', 'Low', 'Close', 'Volume', 'EMA20', 'MACD', 'Signal_Line', 'Momentum']]
    labels = data['Target']
    return features, labels


# XGBoost模型训练
def train_xgb_model(X_train, y_train):
    xgb_model = xgb.XGBClassifier(
        eval_metric='logloss',
        max_depth=6,  # 减少max_depth避免过拟合
        min_child_weight=5,  # 提高min_child_weight减少噪声影响
        gamma=0.1,  # 加入gamma来进行正则化
        learning_rate=0.05,  # 设置学习率
        n_estimators=100,  # 迭代次数
        subsample=0.9,  # 控制每棵树的样本比例
        colsample_bytree=1.0,  # 每棵树的特征比例
        random_state=42
    )
    xgb_model.fit(X_train, y_train)
    return xgb_model


# LightGBM模型训练
def train_lgb_model(X_train, y_train):
    lgb_model = lgb.LGBMClassifier(
        max_depth=6,
        min_child_samples=10,
        reg_alpha=0.1,  # L2正则化
        reg_lambda=0.1,  # L1正则化
        learning_rate=0.05,
        n_estimators=100,
        subsample=0.9,
        colsample_bytree=1.0,
        random_state=42
    )
    lgb_model.fit(X_train, y_train)
    return lgb_model


# 模型评估
def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"准确率: {accuracy:.4f}")
    print(classification_report(y_test, y_pred))


# 回测策略
def backtest_with_risk_management(data, predictions, initial_balance=100000, risk_per_trade=0.02):
    balance = initial_balance
    position = 0
    risk_amount = initial_balance * risk_per_trade
    for i in range(len(predictions)):
        close_price = data['Close'].iloc[i]
        if predictions[i] == 1 and position == 0:  # 买入信号
            position = risk_amount / close_price  # 根据风险比例决定购买量
            balance -= position * close_price
        elif predictions[i] == 0 and position > 0:  # 卖出信号
            balance += position * close_price  # 卖出并清空仓位
            position = 0
    if position > 0:  # 持仓到最后
        balance += position * data['Close'].iloc[-1]

    final_balance = balance
    total_return = (final_balance - initial_balance) / initial_balance * 100
    print(
        f"回测结果：初始资金: {initial_balance:.2f} 元, 最终资金: {final_balance:.2f} 元, 总收益率: {total_return:.2f}%")
    return final_balance


# 主函数
def main():
    # 加载数据
    ticker = '002456'  # 上证指数或其它股票
    start_date = '2020-01-01'
    end_date = '2024-11-28'
    data = fetch_stock_data_a(ticker, start_date, end_date)

    if data.empty:
        print("未能获取数据，退出程序。")
        return

    # 特征工程
    data = feature_engineering(data)

    # 数据预处理
    X, y = preprocess_data(data)

    # 保存原始数据的索引
    data_index = data.index

    # 分割数据集，并记录切分前的数据索引
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # 获取 X_test 的原始索引
    X_test_index = data_index[X_test.index.values]

    # 特征标准化
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    # 训练模型
    print("训练XGBoost模型...")
    xgb_model = train_xgb_model(X_train, y_train)
    print("XGBoost模型评估：")
    evaluate_model(xgb_model, X_test, y_test)

    print("训练LightGBM模型...")
    lgb_model = train_lgb_model(X_train, y_train)
    print("LightGBM模型评估：")
    evaluate_model(lgb_model, X_test, y_test)

    # 回测（XGBoost预测）
    print("回测结果（XGBoost预测）：")
    xgb_predictions = xgb_model.predict(X_test)
    # 使用iloc通过位置索引来获取数据
    backtest_with_risk_management(data.iloc[X_test.index], xgb_predictions)

    # 回测（LightGBM预测）
    print("回测结果（LightGBM预测）：")
    lgb_predictions = lgb_model.predict(X_test)
    # 使用iloc通过位置索引来获取数据
    backtest_with_risk_management(data.iloc[X_test.index], lgb_predictions)


if __name__ == "__main__":
    main()
