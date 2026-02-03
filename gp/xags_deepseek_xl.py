import pandas as pd
import numpy as np
import datetime
import time
import os
import gc
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


# 获取所有A股股票列表 - 使用更可靠的来源
def get_all_a_stocks():
    """获取所有A股股票代码和名称"""
    try:
        # 使用东方财富网的数据源
        url = "http://80.push2.eastmoney.com/api/qt/clist/get"
        params = {
            'pn': '1',
            'pz': '10000',  # 获取足够大的数量
            'po': '1',
            'np': '1',
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': '2',
            'invt': '2',
            'fid': 'f3',
            'fs': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
            'fields': 'f12,f14',
            '_': str(int(time.time() * 1000))
        }

        response = requests.get(url, params=params)
        data = response.json()

        if data['data'] is None:
            logger.error("获取股票列表失败：数据为空")
            return []

        stocks = []
        for item in data['data']['diff']:
            code = item['f12']  # 股票代码
            name = item['f14']  # 股票名称
            # 排除ST股票
            if 'ST' not in name and '*' not in name:
                stocks.append((code, name))

        logger.info(f"成功获取 {len(stocks)} 只A股股票列表")
        return stocks

    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        return []


# 获取股票历史数据 - 使用可靠的API
def get_stock_data(stock_code, days=90):
    """获取股票历史数据，默认获取最近90天"""
    try:
        # 使用新浪财经API
        url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            'symbol': f'sh{stock_code}' if stock_code.startswith('6') else f'sz{stock_code}',
            'scale': '240',  # 日线
            'ma': 'no',  # 不需要均线
            'datalen': days
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            logger.warning(f"获取股票 {stock_code} 数据失败: HTTP {response.status_code}")
            return None

        # 解析JSON数据
        data = json.loads(response.text)

        if not data:
            logger.warning(f"股票 {stock_code} 返回数据为空")
            return None

        # 转换为DataFrame
        df = pd.DataFrame(data)

        # 转换数据类型
        df['day'] = pd.to_datetime(df['day'])
        df.set_index('day', inplace=True)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # 重命名列
        df = df.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        })

        return df

    except Exception as e:
        logger.error(f"获取股票 {stock_code} 数据失败: {str(e)}")
        return None


# 获取沪深300指数数据
def get_hs300_data(days=90):
    """获取沪深300指数历史数据"""
    try:
        url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            'symbol': 'sh000300',  # 沪深300指数代码
            'scale': '240',  # 日线
            'ma': 'no',  # 不需要均线
            'datalen': days
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            logger.warning(f"获取沪深300指数数据失败: HTTP {response.status_code}")
            return None

        # 解析JSON数据
        data = json.loads(response.text)

        if not data:
            logger.warning("沪深300指数返回数据为空")
            return None

        # 转换为DataFrame
        df = pd.DataFrame(data)

        # 转换数据类型
        df['day'] = pd.to_datetime(df['day'])
        df.set_index('day', inplace=True)
        df['close'] = df['close'].astype(float)

        # 重命名列
        df = df.rename(columns={'close': 'Close'})
        df = df[['Close']]  # 只保留收盘价

        return df

    except Exception as e:
        logger.error(f"获取沪深300指数数据失败: {str(e)}")
        return None


# 计算相对强度指标
def calculate_relative_strength(stock_data, benchmark_data):
    """计算股票相对于基准的相对强度"""
    if stock_data is None or benchmark_data is None or stock_data.empty or benchmark_data.empty:
        return None

    try:
        # 合并数据
        merged = pd.merge(
            stock_data[['Close']],
            benchmark_data[['Close']],
            left_index=True,
            right_index=True,
            how='left',
            suffixes=('_stock', '_benchmark')
        )

        # 前向填充缺失值
        merged.ffill(inplace=True)

        # 计算相对强度
        merged['RS'] = merged['Close_stock'] / merged['Close_benchmark']

        # 计算50日移动平均
        merged['RS_MA50'] = merged['RS'].rolling(window=50, min_periods=1).mean()

        return merged[['RS', 'RS_MA50']]

    except Exception as e:
        logger.error(f"计算相对强度失败: {str(e)}")
        return None


# 识别买入信号 - 简化版
def identify_buy_signal(data):
    """识别买入信号"""
    if data is None or len(data) < 30:
        return False

    try:
        # 条件1: 当前价格高于50日移动平均
        ma50 = data['Close'].rolling(window=50, min_periods=1).mean().iloc[-1]
        current_price = data['Close'].iloc[-1]
        condition1 = current_price > ma50

        # 条件2: 相对强度高于1.0且高于其50日均线
        rs = data.get('RS', pd.Series([1.0] * len(data))).iloc[-1]
        rs_ma50 = data.get('RS_MA50', pd.Series([1.0] * len(data))).iloc[-1]
        condition2 = rs > 1.0 and rs > rs_ma50

        # 条件3: 成交量放大 - 最近5天平均成交量高于30天平均
        if len(data) >= 30:
            vol_5d = data['Volume'].iloc[-5:].mean()
            vol_30d = data['Volume'].iloc[-30:].mean()
            condition3 = vol_5d > vol_30d * 1.2
        else:
            condition3 = True  # 如果数据不足，跳过此条件

        return condition1 and condition2 and condition3

    except Exception as e:
        logger.error(f"识别买入信号失败: {str(e)}")
        return False


# 识别卖出信号 - 简化版
def identify_sell_signal(data):
    """识别卖出信号"""
    if data is None or len(data) < 30:
        return False

    try:
        # 条件1: 当前价格低于50日移动平均
        ma50 = data['Close'].rolling(window=50, min_periods=1).mean().iloc[-1]
        current_price = data['Close'].iloc[-1]
        condition1 = current_price < ma50

        # 条件2: 相对强度低于1.0
        rs = data.get('RS', pd.Series([1.0] * len(data))).iloc[-1]
        condition2 = rs < 1.0

        return condition1 or condition2

    except Exception as e:
        logger.error(f"识别卖出信号失败: {str(e)}")
        return False


# 分析单只股票
def analyze_stock(stock_info, benchmark_data):
    """分析单只股票并返回信号"""
    code, name = stock_info
    logger.info(f"正在分析: {name}({code})")

    try:
        # 获取股票数据（最近90天）
        stock_data = get_stock_data(code, 90)

        if stock_data is None or len(stock_data) < 30:
            logger.warning(f"股票 {name}({code}) 数据不足，跳过分析")
            return None

        # 计算相对强度
        rs_data = calculate_relative_strength(stock_data, benchmark_data)
        if rs_data is not None:
            stock_data = pd.concat([stock_data, rs_data], axis=1)

        # 获取当前价格
        current_price = stock_data['Close'].iloc[-1]

        # 识别信号
        buy_signal = identify_buy_signal(stock_data)
        sell_signal = identify_sell_signal(stock_data)

        # 如果没有任何信号，返回None
        if not buy_signal and not sell_signal:
            return None

        result = {
            '代码': code,
            '名称': name,
            '当前价格': current_price,
            '买入信号': '是' if buy_signal else '否',
            '卖出信号': '是' if sell_signal else '否',
            '分析日期': datetime.datetime.now().strftime('%Y-%m-%d')
        }

        return result

    except Exception as e:
        logger.error(f"分析股票 {name}({code}) 时出错: {str(e)}")
        return None
    finally:
        # 确保释放内存
        del stock_data
        gc.collect()


# 批量分析所有A股 - 带重试机制
def batch_analyze_a_stocks():
    """批量分析所有A股股票"""
    logger.info("开始批量分析A股股票...")
    start_time = time.time()

    # 获取沪深300指数数据
    logger.info("获取沪深300指数数据...")
    benchmark_data = get_hs300_data(90)
    if benchmark_data is None:
        logger.error("无法获取基准数据，终止分析")
        return pd.DataFrame()

    # 获取所有A股股票列表
    logger.info("获取A股股票列表...")
    all_stocks = get_all_a_stocks()
    if not all_stocks:
        logger.error("无法获取股票列表，终止分析")
        return pd.DataFrame()

    logger.info(f"共获取 {len(all_stocks)} 只A股股票")

    # 创建结果列表
    results = []

    # 分批处理股票，每批50只
    batch_size = 50
    num_batches = (len(all_stocks) + batch_size - 1) // batch_size

    # 限制总处理股票数量（可选）
    # all_stocks = all_stocks[:200]  # 只处理前200只股票

    # 使用线程池（限制线程数）
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        batch_count = 0

        for i, stock in enumerate(all_stocks):
            # 提交任务
            future = executor.submit(analyze_stock, stock, benchmark_data)
            futures[future] = stock

            # 每批提交后等待
            if (i + 1) % batch_size == 0:
                batch_count += 1
                logger.info(f"已提交第 {batch_count} 批任务 ({batch_size} 只股票)")
                time.sleep(5)  # 避免请求过快

        # 等待所有任务完成
        logger.info("等待任务完成...")
        for future in as_completed(futures):
            stock = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    code, name = stock
                    logger.info(f"发现信号: {name}({code}) - 买入:{result['买入信号']} 卖出:{result['卖出信号']}")
            except Exception as e:
                code, name = stock
                logger.error(f"处理股票 {name}({code}) 时出错: {str(e)}")

    # 计算耗时
    elapsed = time.time() - start_time
    logger.info(f"分析完成! 共发现 {len(results)} 个交易信号, 耗时 {elapsed:.2f} 秒")

    # 转换为DataFrame
    if results:
        results_df = pd.DataFrame(results)
        # 排序：买入信号优先
        results_df = results_df.sort_values(by=['买入信号'], ascending=False)
        return results_df
    else:
        return pd.DataFrame()


# 主函数
def main():
    logger.info("=" * 80)
    logger.info("基于《笑傲股市》的A股交易策略")
    logger.info("=" * 80)

    # 批量分析股票
    results_df = batch_analyze_a_stocks()

    # 保存结果到CSV
    if not results_df.empty:
        today = datetime.datetime.now().strftime('%Y%m%d')
        csv_file = f"stock_signals_{today}.csv"
        results_df.to_csv(csv_file, index=False, encoding='utf_8_sig')
        logger.info(f"结果已保存到 {csv_file}")

        # 输出买入推荐股票
        buy_stocks = results_df[results_df['买入信号'] == '是']
        if not buy_stocks.empty:
            logger.info("\n买入推荐股票:")
            logger.info(buy_stocks[['代码', '名称', '当前价格']].to_string(index=False))

        # 输出卖出建议股票
        sell_stocks = results_df[results_df['卖出信号'] == '是']
        if not sell_stocks.empty:
            logger.info("\n卖出建议股票:")
            logger.info(sell_stocks[['代码', '名称', '当前价格']].to_string(index=False))

        logger.info("\n分析完成! 请查看生成的CSV文件")
    else:
        logger.info("\n今日未发现任何交易信号")


if __name__ == "__main__":
    main()