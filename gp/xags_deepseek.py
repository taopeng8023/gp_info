import pandas as pd
import numpy as np
import akshare as ak
import datetime
import time
import os
import gc
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil  # 添加内存监控

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


# 获取所有A股股票列表
def get_all_a_stocks():
    """获取所有A股股票代码和名称"""
    try:
        stock_info = ak.stock_info_a_code_name()
        # 排除ST股票
        stock_info = stock_info[~stock_info['name'].str.contains('ST')]
        return stock_info[['code', 'name']].values.tolist()
    except Exception as e:
        print(f"获取股票列表失败: {str(e)}")
        return []


# 获取股票历史数据 - 更严格的内存控制
def get_stock_data(stock_code, start_date, end_date):
    """获取股票历史数据，更严格的内存控制"""
    try:
        # 处理股票代码格式
        if stock_code.startswith('6'):
            symbol = f"{stock_code}"
        else:
            symbol = f"{stock_code}"

        # 获取数据，只保留需要的列
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date)
        print(df)
        # 重命名列
        df = df.rename(columns={
            '日期': 'Date',
            '开盘': 'Open',
            '收盘': 'Close',
            '最高': 'High',
            '最低': 'Low',
            '成交量': 'Volume'
        })

        # 只保留必要的列
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # 设置日期索引
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        return df
    except Exception as e:
        print(f"获取股票 {stock_code} 数据失败: {str(e)}")
        return None
    finally:
        gc.collect()


# 计算相对强度指标 - 更高效的内存使用
def calculate_relative_strength(stock_data, benchmark_data):
    """
    计算股票相对于基准的相对强度，更高效的内存使用
    """
    try:
        # 只合并需要的日期范围
        merged = pd.merge(
            stock_data[['Close']],
            benchmark_data[['Close']],
            left_index=True,
            right_index=True,
            suffixes=('_stock', '_benchmark')
        )

        # 计算相对强度
        rs = merged['Close_stock'] / merged['Close_benchmark']
        rs_ma50 = rs.rolling(window=50, min_periods=1).mean()

        return pd.DataFrame({'RS': rs, 'RS_MA50': rs_ma50})
    finally:
        del merged, rs, rs_ma50
        gc.collect()


# 识别茶杯柄形态 - 最小化内存占用
def identify_cup_with_handle(data, window=30, cup_depth=0.15, handle_depth=0.08):
    """
    识别茶杯柄形态，最小化内存占用
    """
    if len(data) < window * 2:
        return data

    try:
        # 计算滚动最高点和最低点
        rolling_high = data['High'].rolling(window, min_periods=1).max()
        rolling_low = data['Low'].rolling(window, min_periods=1).min()

        # 识别茶杯形态
        cup_condition = (
                (data['Close'] < rolling_high * (1 - cup_depth)) &
                (data['Close'] > rolling_low * (1 + cup_depth)))

        # 识别柄部形态
        handle_condition = (
            (data['Close'] < rolling_high * (1 - handle_depth)) &
            (data['Close'] > rolling_low * (1 + cup_depth)))

        # 生成信号：柄部形成后突破高点时买入
        signal = pd.Series(0, index=data.index)
        for i in range(window, len(data)):
            if handle_condition.iloc[i] and data['Close'].iloc[i] > rolling_high.iloc[i - 1]:
                signal.iloc[i] = 1

        # 标记卖出信号：跌破50日移动平均线
        ma50 = data['Close'].rolling(window=50, min_periods=1).mean()
        sell_signal = (data['Close'] < ma50).astype(int)

        # 只添加必要的列到原始数据
        data = data.copy()  # 避免修改原始数据
        data['Signal'] = signal
        data['Sell_Signal'] = sell_signal
        data['MA50'] = ma50

        return data
    finally:
        # 强制释放内存
        del rolling_high, rolling_low, cup_condition, handle_condition, signal, ma50, sell_signal
        gc.collect()


# 分析单只股票 - 内存安全模式
def analyze_stock(stock_info, benchmark_data, end_date):
    """分析单只股票并返回信号，内存安全模式"""
    code, name = stock_info
    print(f"正在分析: {name}({code})")

    # 监控内存使用
    mem_info = psutil.virtual_memory()
    if mem_info.percent > 85:
        print(f"内存使用过高({mem_info.percent}%)，暂停处理释放内存...")
        time.sleep(5)
        gc.collect()

    # 获取数据（仅最近6个月）
    start_date = (pd.to_datetime(end_date) - pd.DateOffset(months=6)).strftime('%Y%m%d')
    stock_data = get_stock_data(code, start_date, end_date)

    if stock_data is None or len(stock_data) < 60:
        return None

    try:
        # 计算相对强度
        rs_data = calculate_relative_strength(stock_data, benchmark_data)
        stock_data = pd.concat([stock_data, rs_data], axis=1)

        # 识别技术形态
        stock_data = identify_cup_with_handle(stock_data)

        # 获取最近一天的信号
        last_day = stock_data.iloc[-1]

        # 检查买入信号
        buy_signal = False
        if last_day.get('Signal', 0) == 1:
            # 相对强度条件：RS > 1.0 且高于50日均线
            if last_day['RS'] > 1.0 and last_day['RS'] > last_day['RS_MA50']:
                # 成交量条件：最近5天成交量放大
                avg_volume = stock_data['Volume'].rolling(window=20, min_periods=1).mean().iloc[-1]
                if last_day['Volume'] > avg_volume * 1.5:
                    buy_signal = True

        # 检查卖出信号
        sell_signal = last_day.get('Sell_Signal', 0) == 1

        # 如果没有任何信号，返回None
        if not buy_signal and not sell_signal:
            return None

        result = {
            '代码': code,
            '名称': name,
            '当前价格': last_day['Close'],
            '买入信号': '是' if buy_signal else '否',
            '卖出信号': '是' if sell_signal else '否',
            '相对强度': last_day['RS'],
            '50日均线': last_day['MA50'],
            '分析日期': end_date
        }

        return result
    except Exception as e:
        print(f"分析股票 {name}({code}) 时出错: {str(e)}")
        return None
    finally:
        # 确保释放内存
        del stock_data, rs_data
        gc.collect()


# 批量分析所有A股 - 低内存模式
def batch_analyze_a_stocks(end_date):
    """批量分析所有A股股票，低内存模式"""
    # 获取沪深300指数作为基准
    print("获取沪深300指数数据...")
    try:
        benchmark_data = ak.stock_zh_index_daily(symbol="sh000300")
        benchmark_data = benchmark_data.rename(columns={
            'date': 'Date',
            'close': 'Close'
        })
        benchmark_data['Date'] = pd.to_datetime(benchmark_data['Date'])
        benchmark_data.set_index('Date', inplace=True)
        benchmark_data = benchmark_data[['Close']]  # 只保留需要的列
    except Exception as e:
        print(f"获取基准数据失败: {str(e)}")
        return pd.DataFrame()

    # 获取所有A股股票列表
    print("获取A股股票列表...")
    all_stocks = get_all_a_stocks()
    if not all_stocks:
        return pd.DataFrame()

    print(f"共获取 {len(all_stocks)} 只A股股票")

    # 创建结果列表
    results = []

    # 使用更小的批次和更少的线程
    print("开始批量分析股票(低内存模式)...")
    start_time = time.time()

    # 分批处理股票，每批30只
    batch_size = 30
    num_batches = (len(all_stocks) + batch_size - 1) // batch_size

    for batch_idx in range(num_batches):
        # 检查内存使用
        mem_info = psutil.virtual_memory()
        if mem_info.percent > 90:
            print(f"内存使用过高({mem_info.percent}%)，暂停处理释放内存...")
            time.sleep(10)
            gc.collect()

        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(all_stocks))
        current_batch = all_stocks[start_idx:end_idx]

        print(f"处理批次 {batch_idx + 1}/{num_batches} ({len(current_batch)}只股票)")

        # 使用串行处理替代线程池
        for stock in current_batch:
            result = analyze_stock(stock, benchmark_data, end_date)
            if result:
                results.append(result)
                print(
                    f"发现信号: {result['名称']}({result['代码']}) - 买入:{result['买入信号']} 卖出:{result['卖出信号']}")

        # 每批处理完后强制垃圾回收
        del current_batch
        gc.collect()

    # 计算耗时
    elapsed = time.time() - start_time
    print(f"分析完成! 共发现 {len(results)} 个交易信号, 耗时 {elapsed:.2f} 秒")

    # 转换为DataFrame
    if results:
        results_df = pd.DataFrame(results)
        # 排序：买入信号优先，然后按相对强度降序
        results_df = results_df.sort_values(by=['买入信号', '相对强度'], ascending=[False, False])
        return results_df
    else:
        return pd.DataFrame()


# 主函数 - 简化版本
def main():
    # 设置分析日期（默认为今天）
    end_date = datetime.datetime.now().strftime('%Y%m%d')

    print("=" * 80)
    print("基于《笑傲股市》的A股批量交易策略 (低内存模式)")
    print(f"分析日期: {end_date}")
    print("=" * 80)

    # 批量分析股票
    results_df = batch_analyze_a_stocks(end_date)

    # 保存结果到CSV
    if not results_df.empty:
        csv_file = f"stock_signals_{end_date}.csv"
        results_df.to_csv(csv_file, index=False, encoding='utf_8_sig')
        print(f"\n结果已保存到 {csv_file}")

        # 输出买入推荐股票
        buy_stocks = results_df[results_df['买入信号'] == '是']
        if not buy_stocks.empty:
            print("\n买入推荐股票:")
            print(buy_stocks[['代码', '名称', '当前价格', '相对强度']].to_string(index=False))

        # 输出卖出建议股票
        sell_stocks = results_df[results_df['卖出信号'] == '是']
        if not sell_stocks.empty:
            print("\n卖出建议股票:")
            print(sell_stocks[['代码', '名称', '当前价格', '50日均线']].to_string(index=False))

        print("\n分析完成! 请查看生成的CSV文件")
    else:
        print("\n今日未发现任何交易信号")


if __name__ == "__main__":
    # 设置内存限制（如果可能）
    try:
        import resource

        # 设置内存限制为2GB (仅适用于Linux/Mac)
        resource.setrlimit(resource.RLIMIT_AS, (2 * 1024 ** 3, 2 * 1024 ** 3))
    except:
        pass

    main()