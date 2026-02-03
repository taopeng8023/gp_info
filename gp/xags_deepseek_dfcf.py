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
from scipy.signal import argrelextrema

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


# 获取所有A股股票列表
def get_all_a_stocks():
    """获取所有A股股票代码和名称"""
    try:
        # 使用东方财富网的数据源
        url = "http://80.push2.eastmoney.com/api/qt/clist/get"
        params = {
            'pn': '1',
            'pz': '10000',
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


# 获取股票历史数据
def get_stock_data(stock_code, days=180):
    """获取股票历史数据，默认获取最近180天"""
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

        # 按日期升序排列
        df = df.sort_index()

        return df

    except Exception as e:
        logger.error(f"获取股票 {stock_code} 数据失败: {str(e)}")
        return None


# 获取沪深300指数数据
def get_hs300_data(days=180):
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

        # 按日期升序排列
        df = df.sort_index()

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


# 识别茶杯柄形态 - 根据《笑傲股市》原则
def identify_cup_with_handle(stock_data, min_cup_days=30, min_handle_days=5, cup_depth=0.15, handle_depth=0.08):
    """
    识别茶杯柄形态，基于《笑傲股市》原则
    """
    if stock_data is None or len(stock_data) < min_cup_days + min_handle_days:
        return False

    try:
        # 1. 寻找左杯沿（局部高点）
        # 使用最近60天的数据寻找局部高点
        recent_data = stock_data.iloc[-60:] if len(stock_data) > 60 else stock_data
        high_idx = argrelextrema(recent_data['High'].values, np.greater, order=5)[0]

        if len(high_idx) < 1:
            return False

        # 取最近的一个局部高点作为左杯沿
        left_rim_idx = high_idx[-1]
        left_rim_price = recent_data.iloc[left_rim_idx]['High']

        # 2. 寻找杯底（局部低点）
        # 在左杯沿之后寻找局部低点
        after_left_rim = recent_data.iloc[left_rim_idx:]
        low_idx = argrelextrema(after_left_rim['Low'].values, np.less, order=5)[0]

        if len(low_idx) < 1:
            return False

        cup_bottom_idx = low_idx[0] + left_rim_idx
        cup_bottom_price = recent_data.iloc[cup_bottom_idx]['Low']

        # 3. 检查杯深（回撤幅度）
        cup_drop = (left_rim_price - cup_bottom_price) / left_rim_price
        if cup_drop < cup_depth or cup_drop > 0.5:  # 杯深在15%-50%之间
            return False

        # 4. 寻找右杯沿（突破点）
        # 在杯底之后寻找右杯沿（接近左杯沿高度的位置）
        after_cup_bottom = recent_data.iloc[cup_bottom_idx:]
        potential_break_idx = after_cup_bottom[after_cup_bottom['High'] >= left_rim_price * 0.95].index

        if len(potential_break_idx) == 0:
            return False

        # 取第一个突破点作为右杯沿
        right_rim_idx = potential_break_idx[0]
        right_rim_price = recent_data.loc[right_rim_idx]['High']

        # 5. 检查柄部形态
        # 柄部应该在右杯沿之前形成，通常是小幅回撤
        before_right_rim = recent_data.iloc[cup_bottom_idx:right_rim_idx]
        if len(before_right_rim) < min_handle_days:
            return False

        # 柄部最低点
        handle_low = before_right_rim['Low'].min()
        handle_drop = (right_rim_price - handle_low) / right_rim_price

        # 柄部回撤应在5%-15%之间
        if handle_drop < handle_depth or handle_drop > 0.15:
            return False

        # 6. 检查成交量形态
        # 杯底区域成交量应萎缩，突破时成交量应放大
        cup_bottom_vol = before_right_rim['Volume'].mean()
        recent_vol = recent_data.iloc[-5:]['Volume'].mean()

        if recent_vol < cup_bottom_vol * 1.2:  # 突破时成交量应比杯底高20%
            return False

        # 7. 确认当前价格突破右杯沿
        current_price = stock_data['Close'].iloc[-1]
        if current_price < right_rim_price:
            return False

        # 所有条件满足，形成茶杯柄形态
        return True

    except Exception as e:
        logger.error(f"识别茶杯柄形态失败: {str(e)}")
        return False


# 识别买入信号 - 根据《笑傲股市》原则增强
def identify_buy_signal(stock_data, benchmark_data):
    """识别买入信号，基于《笑傲股市》原则"""
    if stock_data is None or len(stock_data) < 50:
        return False

    try:
        # 1. 相对强度条件
        rs_data = calculate_relative_strength(stock_data, benchmark_data)
        if rs_data is not None:
            stock_data = pd.concat([stock_data, rs_data], axis=1)

        rs = stock_data['RS'].iloc[-1] if 'RS' in stock_data.columns else 1.0
        rs_ma50 = stock_data['RS_MA50'].iloc[-1] if 'RS_MA50' in stock_data.columns else 1.0

        # 条件1: 相对强度 > 1.0 且高于其50日均线
        rs_condition = rs > 1.0 and rs > rs_ma50

        # 2. 趋势条件
        ma50 = stock_data['Close'].rolling(window=50, min_periods=1).mean().iloc[-1]
        current_price = stock_data['Close'].iloc[-1]

        # 条件2: 当前价格高于50日移动平均线
        trend_condition = current_price > ma50

        # 3. 成交量条件
        if len(stock_data) >= 30:
            vol_5d = stock_data['Volume'].iloc[-5:].mean()
            vol_30d = stock_data['Volume'].iloc[-30:].mean()
            # 条件3: 最近5天成交量高于30天平均成交量的1.2倍
            volume_condition = vol_5d > vol_30d * 1.2
        else:
            volume_condition = True  # 如果数据不足，跳过此条件

        # 4. 茶杯柄形态条件
        cup_handle_condition = identify_cup_with_handle(stock_data)

        # 5. 盈利增长预期（简化版，实际应使用基本面数据）
        # 这里使用价格动量作为代理指标
        price_1m = stock_data['Close'].iloc[-20] if len(stock_data) >= 20 else stock_data['Close'].iloc[0]
        price_growth = (current_price - price_1m) / price_1m
        growth_condition = price_growth > 0.1  # 近1个月上涨超过10%

        # 买入信号需要满足所有核心条件 + 茶杯柄形态
        core_conditions = rs_condition and trend_condition and volume_condition
        buy_signal = core_conditions and cup_handle_condition and growth_condition

        return buy_signal

    except Exception as e:
        logger.error(f"识别买入信号失败: {str(e)}")
        return False


# 识别卖出信号
def identify_sell_signal(stock_data, benchmark_data):
    """识别卖出信号"""
    if stock_data is None or len(stock_data) < 50:
        return False

    try:
        # 1. 趋势反转条件
        ma50 = stock_data['Close'].rolling(window=50, min_periods=1).mean().iloc[-1]
        current_price = stock_data['Close'].iloc[-1]

        # 条件1: 当前价格低于50日移动平均线
        trend_condition = current_price < ma50

        # 2. 相对强度条件
        rs_data = calculate_relative_strength(stock_data, benchmark_data)
        if rs_data is not None:
            stock_data = pd.concat([stock_data, rs_data], axis=1)

        rs = stock_data['RS'].iloc[-1] if 'RS' in stock_data.columns else 1.0

        # 条件2: 相对强度低于1.0
        rs_condition = rs < 1.0

        # 3. 止损条件（从最高点回撤8%）
        recent_high = stock_data['High'].iloc[-20:].max()
        stop_loss_condition = (recent_high - current_price) / recent_high > 0.08

        # 满足任一条件即卖出
        return trend_condition or rs_condition or stop_loss_condition

    except Exception as e:
        logger.error(f"识别卖出信号失败: {str(e)}")
        return False


# 分析单只股票
def analyze_stock(stock_info, benchmark_data):
    """分析单只股票并返回信号"""
    code, name = stock_info
    logger.info(f"正在分析: {name}({code})")

    try:
        # 获取股票数据（最近180天）
        stock_data = get_stock_data(code, 180)

        if stock_data is None or len(stock_data) < 50:
            logger.warning(f"股票 {name}({code}) 数据不足，跳过分析")
            return None

        # 获取当前价格
        current_price = stock_data['Close'].iloc[-1]

        # 识别信号
        buy_signal = identify_buy_signal(stock_data, benchmark_data)
        sell_signal = identify_sell_signal(stock_data, benchmark_data)

        # 计算相对强度用于报告
        rs_data = calculate_relative_strength(stock_data, benchmark_data)
        rs = rs_data['RS'].iloc[-1] if rs_data is not None else 1.0

        # 计算50日均线
        ma50 = stock_data['Close'].rolling(window=50, min_periods=1).mean().iloc[-1]

        # 如果没有任何信号，返回None
        if not buy_signal and not sell_signal:
            return None

        result = {
            '代码': code,
            '名称': name,
            '当前价格': current_price,
            '50日均线': ma50,
            '相对强度': rs,
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


# 批量分析所有A股
def batch_analyze_a_stocks():
    """批量分析所有A股股票"""
    logger.info("开始批量分析A股股票...")
    start_time = time.time()

    # 获取沪深300指数数据
    logger.info("获取沪深300指数数据...")
    benchmark_data = get_hs300_data(180)
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

    # 分批处理股票，每批20只
    batch_size = 20
    num_batches = (len(all_stocks) + batch_size - 1) // batch_size

    # 限制总处理股票数量（可选）
    # all_stocks = all_stocks[:300]  # 只处理前300只股票

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
                time.sleep(3)  # 避免请求过快

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
        # 排序：买入信号优先，然后按相对强度降序
        results_df = results_df.sort_values(by=['买入信号', '相对强度'], ascending=[False, False])
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
            logger.info(buy_stocks[['代码', '名称', '当前价格', '相对强度']].to_string(index=False))
            logger.info(f"\n共发现 {len(buy_stocks)} 只买入推荐股票")

        # 输出卖出建议股票
        sell_stocks = results_df[results_df['卖出信号'] == '是']
        if not sell_stocks.empty:
            logger.info("\n卖出建议股票:")
            logger.info(sell_stocks[['代码', '名称', '当前价格', '50日均线']].to_string(index=False))
            logger.info(f"\n共发现 {len(sell_stocks)} 只卖出建议股票")

        logger.info("\n分析完成! 请查看生成的CSV文件")
    else:
        logger.info("\n今日未发现任何交易信号")


if __name__ == "__main__":
    # 设置内存限制（如果可能）
    try:
        import resource

        # 设置内存限制为2GB (仅适用于Linux/Mac)
        resource.setrlimit(resource.RLIMIT_AS, (2 * 1024 ** 3, 2 * 1024 ** 3))
    except:
        pass

    main()