import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt

def get_stock():
    stock_symbol = '605366'
    df = ak.stock_zh_a_hist(symbol=stock_symbol, period="daily")
    return df
def _kdj_(df):
    N = 9
    M1 = 3

    # 计算RSV
    df['LOW_N'] = df['最低'].rolling(window=N).min()
    df['HIGH_N'] = df['最高'].rolling(window=N).max()
    df['RSV'] = (df['收盘'] - df['LOW_N']) / (df['HIGH_N'] - df['LOW_N']) * 100

    # 计算K、D、J值
    df['K'] = df['RSV'].ewm(span=M1, adjust=False).mean()
    df['D'] = df['K'].ewm(span=M1, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']

    # 产生进出场信号
    df['EnterLongSignal'] = df['J'] > 0
    df['ExitLongSignal'] = df['J'] > 100

    # 打印包含KDJ和交易信号的DataFrame
    print(df[['收盘', 'K', 'D', 'J', 'EnterLongSignal', 'ExitLongSignal']])


def calculate_bollinger_bands(data, n=20, k=2):
    # 计算中轨线
    data['MB'] = data['收盘'].rolling(window=n).mean()

    # 计算标准差
    data['std'] = data['收盘'].rolling(window=n).std()

    # 计算上轨线和下轨线
    data['UP'] = data['MB'] + k * data['std']
    data['DN'] = data['MB'] - k * data['std']
    return data

def _bool_():
    # 示例：假设你有一个包含交易数据的DataFrame叫做df
    df = pd.DataFrame({'收盘': [...], '最高': [...], '最低': [...]})
    df = get_stock()
    # 调用计算函数
    df = calculate_bollinger_bands(df)
    # 绘制图表
    plt.figure(figsize=(12, 6))
    plt.plot(df['收盘'], label='Close Price', color='black')
    plt.plot(df['MB'], label='Bollinger Bands - MB', color='blue')
    plt.plot(df['UP'], label='Bollinger Bands - UP', linestyle='--', color='red')
    plt.plot(df['DN'], label='Bollinger Bands - DN', linestyle='--', color='green')

    # 填充Bollinger Bands区域
    plt.fill_between(df.index, df['UP'], df['DN'], color='gray', alpha=0.2, label='Bollinger Bands Region')

    # 设置图例、标题等
    plt.legend()
    plt.title('Bollinger Bands')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.show()


_bool_()