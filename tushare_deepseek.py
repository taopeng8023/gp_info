import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 初始化tushare（需要先注册获取token）
ts.set_token('a7358a0255666758b3ef492a6c1de79f2447de968d4a1785b73716a4')
pro = ts.pro_api()

# 参数设置
start_date = '20200101'  # 回测开始日期
end_date = '20231231'  # 回测结束日期
capital = 1000000  # 初始资金
hold_days = 20  # 持有天数


# 获取基础股票数据
def get_stock_basic():
    df = pro.stock_basic(exchange='', list_status='L',
                         fields='ts_code,symbol,name,industry,list_date')
    return df[df['list_date'] < '20190101']  # 排除次新股


# 获取财务数据
def get_finance_data(date):
    # 获取最新财报日期
    q_date = (datetime.strptime(date, "%Y%m%d") - timedelta(days=90)).strftime("%Y%m%d")

    # 获取盈利能力数据
    income = pro.income(period=date, fields='ts_code,net_profit')

    # 获取估值数据
    daily_basic = pro.daily_basic(trade_date=date,
                                  fields='ts_code,pe_ttm,pb,dv_ttm')

    # 获取资产负债表
    balance = pro.balancesheet(period=q_date,
                               fields='ts_code,total_assets,total_hldr_eqy_exc_min')

    return income.merge(daily_basic, on='ts_code').merge(balance, on='ts_code')


# 技术指标筛选
def technical_filter(ts_code, start_date, end_date):
    try:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df.empty:
            return False

        # 计算均线
        df['MA5'] = df['close'].rolling(5).mean()
        df['MA20'] = df['close'].rolling(20).mean()

        # 计算MACD
        exp12 = df['close'].ewm(span=12, adjust=False).mean()
        exp26 = df['close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp12 - exp26
        df['SIGNAL'] = df['MACD'].ewm(span=9, adjust=False).mean()

        # 筛选条件
        last = df.iloc[-1]
        cond1 = last['MA5'] > last['MA20']  # 短期均线上穿长期均线
        cond2 = last['MACD'] > last['SIGNAL']  # MACD金叉
        cond3 = last['close'] > df['close'].mean()  # 股价在均线之上

        return cond1 and cond2 and cond3
    except:
        return False


# 策略筛选条件
def strategy_filter(row):
    try:
        # 财务指标筛选
        pe_cond = row['pe_ttm'] < 30 and row['pe_ttm'] > 0
        pb_cond = row['pb'] < 5 and row['pb'] > 0
        dv_cond = row['dv_ttm'] > 2  # 股息率大于2%
        roe = row['net_profit'] / row['total_hldr_eqy_exc_min'] * 4  # 年化ROE
        roe_cond = roe > 0.15

        # 技术指标筛选
        tech_cond = technical_filter(row['ts_code'],
                                     (datetime.strptime(end_date, "%Y%m%d") -
                                      timedelta(days=60)).strftime("%Y%m%d"),
                                     end_date)

        return pe_cond and pb_cond and dv_cond and roe_cond and tech_cond
    except:
        return False


# 回测函数
def backtest(selected_stocks, end_date):
    results = []
    future_date = (datetime.strptime(end_date, "%Y%m%d") +
                   timedelta(days=hold_days)).strftime("%Y%m%d")

    for ts_code in selected_stocks:
        try:
            df = pro.daily(ts_code=ts_code, start_date=end_date, end_date=future_date)
            if not df.empty:
                buy_price = df.iloc[0]['open']
                sell_price = df.iloc[-1]['close']
                returns = (sell_price - buy_price) / buy_price
                results.append(returns)
        except:
            continue

    return results


# 主程序
if __name__ == '__main__':
    # 获取基础数据
    stock_basic = get_stock_basic()
    finance_data = get_finance_data(end_date)

    # 合并数据
    merged_data = stock_basic.merge(finance_data, on='ts_code', how='inner')
    merged_data.dropna(inplace=True)

    # 应用筛选策略
    selected = merged_data[merged_data.apply(strategy_filter, axis=1)]
    selected_stocks = selected['ts_code'].tolist()

    print(f"筛选到 {len(selected_stocks)} 只股票：")
    print(selected[['ts_code', 'name', 'industry']])

    # 进行回测
    returns = backtest(selected_stocks, end_date)

    if returns:
        print(f"\n回测结果（持有期{hold_days}天）：")
        print(f"平均收益率：{np.mean(returns) * 100:.2f}%")
        print(f"胜率：{sum(np.array(returns) > 0) / len(returns) * 100:.2f}%")
        print(f"最大单笔收益：{max(returns) * 100:.2f}%")
        print(f"最大单笔亏损：{min(returns) * 100:.2f}%")
    else:
        print("没有有效回测数据")

# 注意事项：
# 1. 需要注册tushare并替换token
# 2. 需要安装必要库：pip install tushare pandas numpy
# 3. 参数可根据需要调整，建议在模拟盘验证后再实盘
# 4. 回测结果不代表未来表现，市场有风险投资需谨慎