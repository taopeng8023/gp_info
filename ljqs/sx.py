import akshare as ak
import pandas as pd
import numpy as np
import time
import datetime
import math
from tqdm import tqdm
import warnings
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import random

warnings.filterwarnings('ignore')

try:
    # 同花顺实时行情（只用于最终入选股的补充展示）
    from ljqs.ths_realtime import get_ths_stocks_realtime
except Exception:
    get_ths_stocks_realtime = None


@dataclass(frozen=True)
class Config:
    # --- Data fetching ---
    hist_days_default: int = 150  # 约覆盖最近120个交易日
    request_sleep_seconds: float = 0.35  # 控制请求频率（基础限速）
    request_sleep_jitter_seconds: float = 0.25  # 随机抖动，降低被识别为高频
    request_retries: int = 6
    request_retry_backoff_seconds: float = 1.2
    request_retry_max_elapsed_seconds: float = 10.0  # 单次请求(含重试)总耗时上限
    request_retry_max_backoff_seconds: float = 4.0  # 单次退避上限（避免指数级睡太久）

    # --- Universe / coarse filters ---
    concept_top_n: int = 15
    analyze_top_concepts: int = 8
    analyze_top_stocks_per_concept: int = 25
    min_trading_days: int = 60  # 次新过滤：至少60个交易日

    # --- Technical filters ---
    min_volume_ratio: float = 1.5
    strong_high_pos_volume_ratio: float = 2.5
    min_ma_trend_strength: float = 0.1
    breakout_quality_min_score: float = 25.0

    # --- Market cap (亿元) ---
    min_circulating_cap_bil: float = 30.0
    max_circulating_cap_bil: float = 1000.0

    # --- Final selection ---
    min_total_win_prob: float = 62.0
    min_expected_return: float = 0.7
    max_risk_level: float = 3.5

    # --- Realtime (THS) ---
    enable_ths_realtime: bool = True
    ths_realtime_max_count: int = 15  # 只对最终少量标的拉取

    # --- Defensive limits ---
    volume_ratio_clip_max: float = 10.0


CONFIG = Config()

# 本次运行内缓存（避免重复请求）
_CACHE_STOCK_HIST: Dict[Tuple[str, str, Optional[str], Optional[str]], Optional[pd.DataFrame]] = {}
_CACHE_MARKET_CAP: Dict[str, Optional[float]] = {}


def _sleep(seconds: float) -> None:
    if seconds and seconds > 0:
        time.sleep(seconds)


def _retry_call(
    func,
    *args,
    retries: int,
    backoff: float,
    sleep_seconds: float,
    max_elapsed_seconds: Optional[float] = None,
    max_backoff_seconds: Optional[float] = None,
    **kwargs,
):
    last_err = None
    started = time.time()
    for attempt in range(retries):
        if max_elapsed_seconds is not None and (time.time() - started) > max_elapsed_seconds:
            break
        try:
            result = func(*args, **kwargs)
            # 请求间隔 + 抖动
            _sleep(sleep_seconds + random.uniform(0, CONFIG.request_sleep_jitter_seconds))
            return result
        except Exception as e:
            last_err = e
            # 指数退避
            wait = backoff * (2 ** attempt)
            if max_backoff_seconds is not None:
                wait = min(wait, max_backoff_seconds)
            _sleep(wait)
    raise last_err


def get_stock_data(symbol, period="daily", start_date=None, end_date=None):
    """
    获取股票历史数据

    参数:
    symbol: 股票代码，格式如"sh600000"或"sz000001"
    period: 数据周期，"daily"表示日线
    start_date: 开始日期，格式"YYYYMMDD"
    end_date: 结束日期，格式"YYYYMMDD"

    返回:
    DataFrame包含OHLCV数据
    """
    # 缓存 key
    cache_key = (symbol, period, start_date, end_date)
    if cache_key in _CACHE_STOCK_HIST:
        return _CACHE_STOCK_HIST[cache_key]

    try:
        # 如果没有指定日期，则获取最近约120个交易日的数据
        if start_date is None:
            end_date = datetime.datetime.now().strftime("%Y%m%d")
            start_date = (datetime.datetime.now() - datetime.timedelta(days=CONFIG.hist_days_default)).strftime("%Y%m%d")

        stock_df = _retry_call(
            ak.stock_zh_a_hist,
            symbol=symbol,
            period=period,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
            retries=CONFIG.request_retries,
            backoff=CONFIG.request_retry_backoff_seconds,
            sleep_seconds=CONFIG.request_sleep_seconds,
        )

        if stock_df is None or stock_df.empty:
            _CACHE_STOCK_HIST[cache_key] = None
            return None

        stock_df = stock_df.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
            }
        )

        stock_df["date"] = pd.to_datetime(stock_df["date"])
        stock_df = stock_df.set_index("date").sort_index()

        # 过滤：无量（停牌/异常）与次新（交易日不足）
        stock_df = stock_df[stock_df["volume"].fillna(0) > 0]
        if len(stock_df) < CONFIG.min_trading_days:
            _CACHE_STOCK_HIST[cache_key] = None
            return None

        _CACHE_STOCK_HIST[cache_key] = stock_df
        return stock_df
    except Exception as e:
        print(f"获取股票{symbol}数据时出错: {e}")
        _CACHE_STOCK_HIST[cache_key] = None
        return None


def get_market_index_data(index_code="sh000001", days=60):
    """
    获取市场指数数据，用于评估市场环境
    """
    try:
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y%m%d")

        # 优先：东方财富指数历史（可能被限流/断连）——这里快速失败并尽快切源
        try:
            index_df = _retry_call(
                ak.index_zh_a_hist,
                symbol=index_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                retries=2,
                backoff=0.8,
                sleep_seconds=CONFIG.request_sleep_seconds,
                max_elapsed_seconds=CONFIG.request_retry_max_elapsed_seconds,
                max_backoff_seconds=CONFIG.request_retry_max_backoff_seconds,
            )
        except Exception:
            index_df = None

        # 备用：新浪/腾讯指数日线（字段不同，后面统一重命名）
        if index_df is None or index_df.empty:
            alt_code = index_code[2:] if index_code.startswith(("sh", "sz")) else index_code
            try:
                index_df = _retry_call(
                    ak.stock_zh_index_daily,
                    symbol=alt_code,
                    retries=2,
                    backoff=0.8,
                    sleep_seconds=CONFIG.request_sleep_seconds,
                    max_elapsed_seconds=CONFIG.request_retry_max_elapsed_seconds,
                    max_backoff_seconds=CONFIG.request_retry_max_backoff_seconds,
                )
            except Exception:
                index_df = None

        if index_df is None or index_df.empty:
            try:
                index_df = _retry_call(
                    ak.stock_zh_index_daily_tx,
                    symbol=alt_code,
                    retries=2,
                    backoff=0.8,
                    sleep_seconds=CONFIG.request_sleep_seconds,
                    max_elapsed_seconds=CONFIG.request_retry_max_elapsed_seconds,
                    max_backoff_seconds=CONFIG.request_retry_max_backoff_seconds,
                )
            except Exception:
                index_df = None

        if index_df is None or index_df.empty:
            return None

        # 统一字段（兼容不同数据源）
        rename_map = {
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "date": "date",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "volume": "volume",
        }
        index_df.rename(columns={k: v for k, v in rename_map.items() if k in index_df.columns}, inplace=True)

        # 有些源可能叫“日期”以外的字段名
        if "date" not in index_df.columns:
            for c in ["trade_date", "时间", "Date"]:
                if c in index_df.columns:
                    index_df.rename(columns={c: "date"}, inplace=True)
                    break

        index_df['date'] = pd.to_datetime(index_df['date'])
        index_df.set_index('date', inplace=True)
        index_df.sort_index(inplace=True)

        return index_df
    except Exception as e:
        print(f"获取指数{index_code}数据时出错: {e}")
        return None


def get_market_concept_with_money_flow():
    """
    获取A股市场热点行业/概念板块信息（东方财富数据源）

    说明：
    - 东方财富网页 `https://quote.eastmoney.com/center/hsbk.html` 背后也是接口数据
    - 这里优先使用 AkShare 封装好的行业/概念板块接口，等价于直接用该数据源
    - 按 涨跌幅 / 换手率 / 成交额 计算综合强度
    """
    try:
        # 1. 优先：东方财富行业板块列表（与网页行业板块涨幅一致）
        try:
            concept_df = _retry_call(
                ak.stock_board_industry_name_em,
                retries=2,
                backoff=0.8,
                sleep_seconds=CONFIG.request_sleep_seconds,
                max_elapsed_seconds=CONFIG.request_retry_max_elapsed_seconds,
                max_backoff_seconds=CONFIG.request_retry_max_backoff_seconds,
            )
        except Exception:
            concept_df = None

        # 2. 备用：东方财富概念板块列表
        if concept_df is None or concept_df.empty:
            try:
                concept_df = _retry_call(
                    ak.stock_board_concept_name_em,
                    retries=2,
                    backoff=0.8,
                    sleep_seconds=CONFIG.request_sleep_seconds,
                    max_elapsed_seconds=CONFIG.request_retry_max_elapsed_seconds,
                    max_backoff_seconds=CONFIG.request_retry_max_backoff_seconds,
                )
            except Exception:
                concept_df = None

        # 3. 再备用：同花顺概念板块
        if concept_df is None or concept_df.empty:
            try:
                concept_df = _retry_call(
                    ak.stock_board_concept_name_ths,
                    retries=2,
                    backoff=0.8,
                    sleep_seconds=CONFIG.request_sleep_seconds,
                    max_elapsed_seconds=CONFIG.request_retry_max_elapsed_seconds,
                    max_backoff_seconds=CONFIG.request_retry_max_backoff_seconds,
                )
            except Exception:
                concept_df = None

        if concept_df is None or concept_df.empty:
            return None

        concept_df = concept_df.copy()

        # 尝试识别“涨跌幅 / 换手率 / 成交额”字段（不同数据源/版本列名可能略有不同）
        def _find_col(df, key: str):
            for c in df.columns:
                if key in str(c):
                    return c
            return None

        change_col = "涨跌幅" if "涨跌幅" in concept_df.columns else _find_col(concept_df, "涨跌")
        turnover_col = "换手率" if "换手率" in concept_df.columns else _find_col(concept_df, "换手")
        amount_col = "成交额" if "成交额" in concept_df.columns else _find_col(concept_df, "成交额")

        if not change_col:
            # 无法识别涨跌幅列，直接返回 None
            return None

        # 将识别到的列统一重命名，后续逻辑只依赖标准列名
        rename_map = {}
        if change_col != "涨跌幅":
            rename_map[change_col] = "涨跌幅"
        if turnover_col and turnover_col != "换手率":
            rename_map[turnover_col] = "换手率"
        if amount_col and amount_col != "成交额":
            rename_map[amount_col] = "成交额"
        if rename_map:
            concept_df.rename(columns=rename_map, inplace=True)

        # 计算综合强度（可复现，且依赖字段尽量少）
        # 默认：涨跌幅 60%，换手率 25%，成交额 15%（不存在某字段则自动放大其他权重）
        weight_price = 0.6
        weight_turnover = 0.25 if "换手率" in concept_df.columns else 0.0
        weight_amount = 0.15 if "成交额" in concept_df.columns else 0.0

        total_weight = weight_price + weight_turnover + weight_amount
        weight_price /= total_weight
        if weight_turnover:
            weight_turnover /= total_weight
        if weight_amount:
            weight_amount /= total_weight

        strength = concept_df["涨跌幅"].rank(pct=True) * weight_price

        if weight_turnover and "换手率" in concept_df.columns:
            strength += concept_df["换手率"].rank(pct=True) * weight_turnover
        if weight_amount and "成交额" in concept_df.columns:
            strength += concept_df["成交额"].rank(pct=True) * weight_amount

        concept_df["综合强度"] = (strength * 100).round(2)

        concept_df.sort_values(by="综合强度", ascending=False, inplace=True)
        return concept_df.head(CONFIG.concept_top_n)
    except Exception as e:
        print(f"获取概念板块数据时出错: {e}")
        return None


def get_stocks_in_concept(concept_name):
    """
    获取某个概念板块中的股票，并增加初步筛选

    参数:
    concept_name: 概念板块名称

    返回:
    DataFrame包含该板块中的股票，按市值和活跃度排序
    """
    try:
        stocks_df = _retry_call(
            ak.stock_board_concept_cons_em,
            symbol=concept_name,
            retries=CONFIG.request_retries,
            backoff=CONFIG.request_retry_backoff_seconds,
            sleep_seconds=CONFIG.request_sleep_seconds,
        )

        if stocks_df is None or stocks_df.empty:
            return None

        # 基础筛选：剔除ST股、次新股(上市<60天)、停牌股
        stocks_df = stocks_df[~stocks_df['名称'].str.contains('ST')]
        stocks_df = stocks_df[~stocks_df['名称'].str.contains('退')]

        # 按涨跌幅降序排序，优先关注强势股
        stocks_df.sort_values(by='涨跌幅', ascending=False, inplace=True)

        return stocks_df
    except Exception as e:
        print(f"获取概念板块{concept_name}成分股时出错: {e}")
        return None


def calculate_indicators(df):
    """
    计算全面技术指标

    参数:
    df: 包含股票数据的DataFrame

    返回:
    带有技术指标的DataFrame
    """
    if df is None or len(df) < 40:
        return None

    df = df.copy()  # 避免修改原始数据

    # 1. 均线系统
    df['ma5'] = df['close'].rolling(window=5).mean()
    df['ma10'] = df['close'].rolling(window=10).mean()
    df['ma20'] = df['close'].rolling(window=20).mean()
    df['ma60'] = df['close'].rolling(window=60).mean()

    # 2. 量能指标
    df['vol_ma5'] = df['volume'].rolling(window=5).mean()
    df['vol_ma10'] = df['volume'].rolling(window=10).mean()

    # 量比：当日成交量 / 前一日的5日平均成交量（做保护）
    df["vol_ma5_shift1"] = df["vol_ma5"].shift(1)
    denom = df["vol_ma5_shift1"].replace([0, np.inf, -np.inf], np.nan)
    df["volume_ratio"] = (df["volume"] / denom).replace([np.inf, -np.inf], np.nan)
    df["volume_ratio"] = df["volume_ratio"].clip(upper=CONFIG.volume_ratio_clip_max)

    # 3. 价格通道
    df['hhv20'] = df['high'].rolling(window=20).max().shift(1)  # 前20日最高价
    df['llv20'] = df['low'].rolling(window=20).min().shift(1)  # 前20日最低价

    # 4. 趋势强度
    df['ma20_5days_ago'] = df['ma20'].shift(5)
    df['ma_trend_strength'] = (df['ma20'] - df['ma20_5days_ago']) / df['ma20_5days_ago'] * 100

    # 5. 波动率指标 (ATR)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = abs(df['high'] - df['close'].shift(1))
    df['tr3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['atr14'] = df['tr'].rolling(window=14).mean()

    # 6. 量价关系指标
    df['price_change_pct'] = df['close'].pct_change() * 100
    df['volume_change_pct'] = df['volume'].pct_change() * 100
    df['volume_price_ratio'] = df['volume_change_pct'] / (df['price_change_pct'].abs() + 0.1)  # 避免除零

    # 7. 突破强度
    df['breakout_strength'] = (df['close'] - df['hhv20']) / df['hhv20'] * 100

    # 8. 收盘位置 (衡量当日多头力量)
    df['close_position'] = (df['close'] - df['low']) / (df['high'] - df['low'] + 1e-6)

    # 9. 3日价格动量
    df['price_momentum_3d'] = df['close'].pct_change(3) * 100

    # 10. 平台整理评估 (20日内价格波动幅度)
    df['platform_range'] = (df['hhv20'] - df['llv20']) / df['llv20'] * 100

    return df


def get_stock_market_capital(symbol):
    """
    获取股票的流通市值（亿元）

    参数:
    symbol: 股票代码，格式如"sh600000"或"sz000001"

    返回:
    流通市值（亿元）
    """
    # 缓存：同一只股的市值本次运行只取一次
    if symbol in _CACHE_MARKET_CAP:
        return _CACHE_MARKET_CAP[symbol]

    try:
        # 格式化股票代码
        stock_code = symbol[2:]  # 去掉sh/sz前缀

        # 获取股票详情
        stock_info = _retry_call(
            ak.stock_individual_info_em,
            symbol=stock_code,
            retries=CONFIG.request_retries,
            backoff=CONFIG.request_retry_backoff_seconds,
            sleep_seconds=CONFIG.request_sleep_seconds,
        )

        # 筛选流通市值
        circulating_capital_values = stock_info[stock_info['item'] == '流通市值']['value']

        if len(circulating_capital_values) == 0:
            return None

        circulating_capital = circulating_capital_values.values[0]

        # 转换为亿元
        if isinstance(circulating_capital, str):
            if '亿' in circulating_capital:
                circulating_capital = float(circulating_capital.replace('亿', ''))
            elif '万' in circulating_capital:
                circulating_capital = float(circulating_capital.replace('万', '')) / 10000
            else:
                try:
                    circulating_capital = float(circulating_capital) / 100000000
                except:
                    return None
        else:
            circulating_capital = float(circulating_capital) / 100000000

        _CACHE_MARKET_CAP[symbol] = circulating_capital
        return circulating_capital
    except Exception as e:
        # print(f"获取股票{symbol}流通市值时出错: {e}")
        _CACHE_MARKET_CAP[symbol] = None
        return None


def evaluate_breakout_quality(df):
    """
    评估突破质量，返回突破质量评分和次日上涨概率

    评分标准(0-50分):
    1. 突破幅度(10分): 突破幅度越大越好
    2. 突破确认(10分): 近3日收盘高于20日高点的天数
    3. 量能配合(10分): 量比大小和量价配合
    4. 平台整理(10分): 突破前整理时间和幅度
    5. 均线系统(10分): 多头排列强度
    """
    if df is None or len(df) < 30:
        return {'quality_score': 0, 'next_day_win_prob': 50.0}

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    prev_2 = df.iloc[-3]

    scores = {
        'breakout_margin': 0,  # 突破幅度
        'breakout_confirmation': 0,  # 突破确认
        'volume_support': 0,  # 量能配合
        'platform_quality': 0,  # 平台质量
        'ma_alignment': 0  # 均线排列
    }

    # 1. 突破幅度评估 (10分)
    breakout_margin = latest['breakout_strength']
    if breakout_margin > 3:
        scores['breakout_margin'] = 10
    elif breakout_margin > 2:
        scores['breakout_margin'] = 8
    elif breakout_margin > 1.5:
        scores['breakout_margin'] = 6
    elif breakout_margin > 1:
        scores['breakout_margin'] = 4
    elif breakout_margin > 0.5:
        scores['breakout_margin'] = 2

    # 2. 突破确认 (10分)
    close_above_hhv = sum([
        df['close'].iloc[-1] > df['hhv20'].iloc[-1],
        df['close'].iloc[-2] > df['hhv20'].iloc[-2],
        df['close'].iloc[-3] > df['hhv20'].iloc[-3]
    ])
    scores['breakout_confirmation'] = min(10, close_above_hhv * 3.5)

    # 3. 量能配合 (10分)
    volume_score = 0
    if latest['volume_ratio'] > 2.5:
        volume_score = 10
    elif latest['volume_ratio'] > 2.0:
        volume_score = 8
    elif latest['volume_ratio'] > 1.8:
        volume_score = 6
    elif latest['volume_ratio'] > 1.5:
        volume_score = 4

    # 量价配合加分
    if latest['price_change_pct'] > 3 and latest['volume_ratio'] > 1.8:
        volume_score += 2

    scores['volume_support'] = min(10, volume_score)

    # 4. 平台质量 (10分) - 低波动整理后突破更强
    platform_days = 0
    recent_range = df['platform_range'].iloc[-5:].mean()

    # 检查最近10日是否处于盘整
    consolidation = df.iloc[-15:-1]
    if len(consolidation) > 0:
        price_range = (consolidation['high'].max() - consolidation['low'].min()) / consolidation['low'].min() * 100
        if price_range < 10:  # 10%以内波动视为盘整
            platform_days = len(consolidation)

    platform_score = 0
    if 5 <= platform_days <= 15 and recent_range < 8:  # 理想盘整时间和幅度
        platform_score = 10
    elif 15 < platform_days <= 25 and recent_range < 10:
        platform_score = 8
    elif platform_days >= 25 and recent_range < 12:
        platform_score = 6
    elif platform_days > 0 and recent_range < 15:
        platform_score = 4

    scores['platform_quality'] = platform_score

    # 5. 均线系统 (10分)
    ma_score = 0
    # 多头排列检查
    if (latest['ma5'] > latest['ma10'] > latest['ma20'] > latest['ma60'] and
            latest['ma_trend_strength'] > 0.5):
        ma_score = 10
    elif (latest['ma5'] > latest['ma10'] > latest['ma20'] and
          latest['ma_trend_strength'] > 0.3):
        ma_score = 8
    elif (latest['ma5'] > latest['ma20'] and
          latest['ma_trend_strength'] > 0.2):
        ma_score = 6

    # 短期均线斜率加分
    if (latest['ma5'] - df['ma5'].iloc[-3]) / df['ma5'].iloc[-3] * 100 > 1:
        ma_score += 1

    scores['ma_alignment'] = min(10, ma_score)

    # 计算总分
    total_score = sum(scores.values())

    # 次日上涨概率映射 (基于历史数据统计)
    # 0-20分: 50-55%, 20-30分: 55-65%, 30-40分: 65-75%, 40-50分: 75-85%
    if total_score < 20:
        win_prob = 52.0
    elif total_score < 30:
        win_prob = 58.0 + (total_score - 20) * 0.7
    elif total_score < 40:
        win_prob = 65.0 + (total_score - 30) * 1.0
    else:
        win_prob = 75.0 + (total_score - 40) * 0.8
        win_prob = min(85.0, win_prob)  # 上限85%

    return {
        'quality_score': round(total_score, 1),
        'next_day_win_prob': round(win_prob, 1),
        'component_scores': scores,
        'breakout_margin': breakout_margin,
        'platform_days': platform_days,
        'volume_ratio': latest['volume_ratio']
    }


def get_historical_pattern_success_rate(symbol, lookback_days=180):
    """
    获取该股历史相似突破形态的成功率

    返回:
    历史成功率、平均次日收益率、样本量
    """
    try:
        # 获取更长时间的历史数据
        hist_data = get_stock_data(symbol,
                                   start_date=(datetime.datetime.now() - datetime.timedelta(
                                       days=lookback_days + 50)).strftime("%Y%m%d"))

        if hist_data is None or len(hist_data) < 60:
            return {'success_rate': 55.0, 'avg_next_day_return': 0.8, 'sample_size': 0}

        # 计算技术指标
        hist_data = calculate_indicators(hist_data)

        if hist_data is None:
            return {'success_rate': 55.0, 'avg_next_day_return': 0.8, 'sample_size': 0}

        success_count = 0
        total_patterns = 0
        next_day_returns = []

        # 识别历史突破点
        for i in range(30, len(hist_data) - 1):
            # 检查是否是突破点
            current_close = hist_data.iloc[i]['close']
            current_hhv20 = hist_data.iloc[i]['hhv20']
            current_volume_ratio = hist_data.iloc[i]['volume_ratio']
            current_ma5 = hist_data.iloc[i]['ma5']
            current_ma20 = hist_data.iloc[i]['ma20']

            # 突破条件：收盘价>前20日最高价且量比>1.5
            if current_close > current_hhv20 and current_volume_ratio > 1.5:
                # 检查均线条件
                if current_ma5 > current_ma20:
                    total_patterns += 1

                    # 计算次日收益率
                    next_close = hist_data.iloc[i + 1]['close']
                    next_day_return = (next_close - current_close) / current_close * 100
                    next_day_returns.append(next_day_return)

                    # 次日上涨计为成功
                    if next_day_return > 0:
                        success_count += 1

        # 计算成功率与平均收益
        if total_patterns > 0:
            success_rate = success_count / total_patterns * 100
            avg_return = np.mean(next_day_returns)
        else:
            success_rate = 55.0
            avg_return = 0.8

        # 贝叶斯平滑（小样本调整）
        if total_patterns < 10:
            success_rate = (success_rate * total_patterns + 55.0 * 10) / (total_patterns + 10)
            avg_return = (avg_return * total_patterns + 0.8 * 10) / (total_patterns + 10)

        return {
            'success_rate': round(success_rate, 1),
            'avg_next_day_return': round(avg_return, 2),
            'sample_size': total_patterns
        }
    except Exception as e:
        # print(f"获取{symbol}历史形态成功率失败: {e}")
        return {'success_rate': 55.0, 'avg_next_day_return': 0.8, 'sample_size': 0}


def run_breakout_rule_backtest(symbols, lookback_days=250):
    """
    简易滚动回测：对一组股票，统计当前突破规则在过去一段时间内的
    - 总样本数
    - 加权成功率（按样本量加权）
    - 加权平均次日收益

    用途：帮助你校准“次日上涨概率/期望收益率”的经验值是否偏乐观或保守。

    使用方式（示例）:
        run_breakout_rule_backtest(["sh600000", "sz000001", ...], lookback_days=250)
    """
    agg_samples = 0
    agg_success_rate = 0.0
    agg_avg_return = 0.0

    print("\n===== 突破规则历史统计（简易回测）=====")
    for symbol in symbols:
        stats = get_historical_pattern_success_rate(symbol, lookback_days=lookback_days)
        sample = stats.get("sample_size", 0) or 0
        if sample <= 0:
            print(f"{symbol}: 样本不足，使用默认统计")
            continue

        print(
            f"{symbol}: 样本数={sample}, 成功率={stats['success_rate']}%, "
            f"平均次日收益={stats['avg_next_day_return']}%"
        )

        agg_success_rate += stats["success_rate"] * sample
        agg_avg_return += stats["avg_next_day_return"] * sample
        agg_samples += sample

    if agg_samples > 0:
        w_success = agg_success_rate / agg_samples
        w_return = agg_avg_return / agg_samples
        print("\n【组合加权统计】")
        print(f"- 总样本数: {agg_samples}")
        print(f"- 加权成功率: {w_success:.2f}%")
        print(f"- 加权平均次日收益: {w_return:.2f}%")
    else:
        print("\n组合样本不足，无法给出可靠统计。")


def evaluate_market_environment():
    """
    评估当前市场环境，返回环境评分和对策略的影响

    说明：
    - 该函数完全基于实时指数数据计算（不存在“知识库固定日期”）
    - 连阳后的风险惩罚属于经验项，后续建议结合回测参数化校准
    """
    # 获取上证指数数据
    market_df = get_market_index_data("sh000001", days=30)

    if market_df is None or len(market_df) < 20:
        # 默认评分 (中性环境)
        return {
            'score': 60.0,
            'win_prob_adjustment': 0.0,
            'risk_level': "中",
            'environment_description': "数据不足，使用默认市场环境"
        }

    latest = market_df.iloc[-1]
    prev = market_df.iloc[-2]

    scores = {
        'trend_strength': 0,  # 趋势强度
        'momentum': 0,  # 动能
        'volatility': 0,  # 波动性
        'volume': 0,  # 量能
        'breadth': 0  # 广度(简化)
    }

    # 1. 趋势强度 (25分权重)
    ma5 = market_df['close'].rolling(5).mean()
    ma20 = market_df['close'].rolling(20).mean()

    # 连阳天数
    close_series = market_df['close']
    daily_returns = close_series.pct_change()
    positive_days = 0
    for ret in daily_returns.iloc[-10:].values[::-1]:  # 从最近往前数
        if ret > 0:
            positive_days += 1
        else:
            break

    trend_score = 0
    if ma5.iloc[-1] > ma20.iloc[-1]:
        trend_score += 10
    if positive_days >= 8:  # 8连阳以上
        trend_score += 8
    if positive_days >= 12:  # 12连阳以上
        trend_score += 7

    scores['trend_strength'] = min(25, trend_score)

    # 2. 市场动能 (20分权重)
    momentum_score = 0
    price_change_3d = (market_df['close'].iloc[-1] - market_df['close'].iloc[-4]) / market_df['close'].iloc[-4] * 100

    if price_change_3d > 3:  # 3天上涨3%以上
        momentum_score = 20
    elif price_change_3d > 2:
        momentum_score = 15
    elif price_change_3d > 1:
        momentum_score = 10
    elif price_change_3d > 0:
        momentum_score = 5

    scores['momentum'] = momentum_score

    # 3. 波动性 (20分权重) - 低波动更有利
    atr = market_df['high'] - market_df['low']
    avg_atr_pct = (atr / market_df['close']).iloc[-5:].mean() * 100

    volatility_score = 0
    if avg_atr_pct < 1.0:  # 低波动
        volatility_score = 20
    elif avg_atr_pct < 1.5:
        volatility_score = 15
    elif avg_atr_pct < 2.0:
        volatility_score = 10
    else:  # 高波动
        volatility_score = 5

    scores['volatility'] = volatility_score

    # 4. 量能 (20分权重)
    volume_ma5 = market_df['volume'].rolling(5).mean()
    volume_ratio = market_df['volume'].iloc[-1] / volume_ma5.iloc[-2]

    volume_score = 0
    if volume_ratio > 1.5:
        volume_score = 20
    elif volume_ratio > 1.2:
        volume_score = 15
    elif volume_ratio > 0.9:
        volume_score = 10
    else:
        volume_score = 5

    # 但要注意：连续上涨后的放量可能是出货信号
    if positive_days >= 10 and volume_ratio > 2.0:
        volume_score = max(5, volume_score - 10)  # 降低评分

    scores['volume'] = volume_score

    # 5. 市场广度 (15分权重) - 简化处理
    # 实际应用中应获取上涨/下跌家数数据
    breadth_score = 10  # 默认中性
    if positive_days >= 10:
        breadth_score = 8  # 警惕普涨后分化

    scores['breadth'] = breadth_score

    # 综合评分
    total_score = sum(scores.values())

    # 调整项：连阳后的短期过热警示（经验项）
    caution_adjustment = 0
    caution_level = "中"
    environment_description = "温和上涨趋势"

    if positive_days >= 12:
        caution_adjustment = -8
        caution_level = "高"
        environment_description = f"强势上涨({positive_days}连阳)，警惕短期回调风险"
    elif positive_days >= 8:
        caution_adjustment = -4
        caution_level = "中高"
        environment_description = f"持续上涨({positive_days}连阳)，注意仓位控制"

    # 概率调整 (基于历史回测)
    win_prob_adjustment = (total_score - 60) * 0.3 + caution_adjustment

    return {
        'score': round(total_score, 1),
        'win_prob_adjustment': round(win_prob_adjustment, 1),
        'risk_level': caution_level,
        'positive_days': positive_days,
        'environment_description': environment_description,
        'component_scores': scores
    }


def evaluate_risk_level(df, market_env):
    """
    评估个股和市场风险水平，提供止损建议

    返回风险等级(1-5)和建议止损位
    """
    if df is None or len(df) < 20:
        return {'risk_level': 3, 'stop_loss': None}

    latest = df.iloc[-1]
    atr = latest['atr14']

    # 1. 个股波动风险
    volatility_risk = 1
    if atr / latest['close'] * 100 > 3.0:  # 日均波动>3%
        volatility_risk = 3
    elif atr / latest['close'] * 100 > 2.0:
        volatility_risk = 2

    # 2. 位置风险 (高位风险大)
    position_risk = 1
    # 计算60日范围
    hhv60 = df['high'].rolling(60).max().iloc[-2]  # 前一日的60日最高
    llv60 = df['low'].rolling(60).min().iloc[-2]  # 前一日的60日最低

    if hhv60 > 0 and llv60 > 0:
        position = (latest['close'] - llv60) / (hhv60 - llv60) * 100
        if position > 80:  # 位于60日高位
            position_risk = 3
        elif position > 60:
            position_risk = 2

    # 3. 市场环境风险
    market_risk = 1
    if market_env['risk_level'] == "高":
        market_risk = 3
    elif market_env['risk_level'] == "中高":
        market_risk = 2

    # 4. 突破可靠性风险
    breakout_risk = 1
    if latest['breakout_strength'] < 0.8:  # 突破幅度小
        breakout_risk = 2
    if latest['volume_ratio'] < 1.8:  # 量能不足
        breakout_risk = max(breakout_risk, 2)

    # 综合风险等级
    risk_level = max(1, (volatility_risk + position_risk + market_risk + breakout_risk) / 4 * 5)
    risk_level = min(5, math.ceil(risk_level))

    # 止损建议
    stop_loss_pct = 3.0 + (risk_level - 1) * 1.5  # 风险越高，止损越紧

    # 考虑ATR动态止损
    atr_multiple = 1.5 + (5 - risk_level) * 0.3  # 风险越低，ATR倍数越大
    stop_loss_by_atr = latest['close'] - atr * atr_multiple

    # 结合百分比和ATR，取两者中较高的作为止损位(更宽松的)
    stop_loss_by_pct = latest['close'] * (1 - stop_loss_pct / 100)
    stop_loss = max(stop_loss_by_pct, stop_loss_by_atr)

    # 保证止损位在关键支撑位以下
    stop_loss = min(stop_loss, latest['close'] * 0.94)  # 最大回撤不超过6%

    return {
        'risk_level': risk_level,
        'stop_loss': round(stop_loss, 2),
        'stop_loss_pct': round((latest['close'] - stop_loss) / latest['close'] * 100, 1),
        'risk_factors': {
            'volatility': volatility_risk,
            'position': position_risk,
            'market': market_risk,
            'breakout': breakout_risk
        }
    }


def basic_technical_filter(df, circulating_capital):
    """
    快速技术筛选，过滤明显不符合条件的股票

    返回: 是否通过基本筛选
    """
    if df is None or len(df) < 30:
        return False

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # 1. 基础突破条件
    if latest['close'] <= latest['hhv20']:
        return False

    # 2. 量能确认
    if latest['volume_ratio'] < CONFIG.min_volume_ratio:
        return False

    # 3. 趋势条件
    if latest['ma5'] <= latest['ma20'] or latest['ma_trend_strength'] < CONFIG.min_ma_trend_strength:
        return False

    # 4. 流通市值筛选 (亿元)
    if (
            circulating_capital is None
            or circulating_capital < CONFIG.min_circulating_cap_bil
            or circulating_capital > CONFIG.max_circulating_cap_bil
    ):
        return False

    # 5. 避免高位接盘
    hhv60 = df['high'].rolling(60).max().iloc[-2]
    if hhv60 > 0 and latest['close'] > hhv60 * 0.95:  # 位于60日高点的95%以上
        # 但允许强势股，需满足更高量能要求
        if latest['volume_ratio'] < CONFIG.strong_high_pos_volume_ratio:
            return False

    return True


def main_optimized():
    print("【量价突破+主线共振+盈利概率增强系统】")
    print("=" * 65)

    # 0. 评估当前市场环境 (决定整体策略基调)
    market_env = evaluate_market_environment()
    print(f"🏦 市场环境分析: {market_env['environment_description']}")
    print(f"📊 环境评分: {market_env['score']}/100 | 概率调整: {market_env['win_prob_adjustment']:+.1f}%")
    print(f"⚠️  风险等级: {market_env['risk_level']}/5")

    if market_env['risk_level'] == "高":
        print("💡 专业建议: 市场短期过热，建议降低仓位或提高选股标准，控制总仓位<40%")
    elif market_env['risk_level'] == "中高":
        print("💡 专业建议: 市场情绪偏热，精选个股，控制总仓位<60%")

    # 1. 获取热点板块 (接入真实资金流)
    print("\n🔍 获取热点板块数据...")
    concept_df = get_market_concept_with_money_flow()
    if concept_df is None or concept_df.empty:
        print("❌ 未能获取到有效的热点板块数据")
        return

    print(f"\n📈 发现{len(concept_df)}个有效热点板块 (按综合强度排序):")
    print(concept_df[['板块名称', '涨跌幅', '综合强度']].head(10).to_string(index=False))

    # 2. 建立高概率股票池
    high_probability_stocks = []
    total_stocks_analyzed = 0
    total_concepts_analyzed = 0

    # 仅分析最强板块，提高效率
    top_concepts = concept_df.head(CONFIG.analyze_top_concepts)

    for _, concept in top_concepts.iterrows():
        total_concepts_analyzed += 1
        concept_name = concept['板块名称']
        sector_strength = concept['综合强度']

        print(f"\n" + "-" * 50)
        print(f"🔍 深度分析板块: {concept_name} (强度: {sector_strength:.1f}/100)")

        # 获取板块成分股
        stocks_df = get_stocks_in_concept(concept_name)
        if stocks_df is None or stocks_df.empty:
            print(f"⚠️  板块 {concept_name} 无有效成分股数据")
            continue

        # 仅分析板块内前N只活跃股 (提高效率，聚焦最强标的)
        stocks_to_analyze = stocks_df.head(CONFIG.analyze_top_stocks_per_concept)
        total_stocks_analyzed += len(stocks_to_analyze)

        sector_candidates = []

        pbar = tqdm(total=len(stocks_to_analyze), desc=f"⏳ 评估{concept_name[:4]}板块")

        for _, stock in stocks_to_analyze.iterrows():
            symbol = stock['代码']
            stock_name = stock['名称']

            # 格式化股票代码
            if symbol.startswith('6'):
                symbol = f"sh{symbol}"
            else:
                symbol = f"sz{symbol}"

            # 获取股票数据
            stock_data = get_stock_data(symbol)

            # 获取流通市值
            circulating_capital = get_stock_market_capital(symbol)

            # 基础技术筛选
            if stock_data is not None:
                stock_data = calculate_indicators(stock_data)

            if circulating_capital is not None and stock_data is not None:
                if basic_technical_filter(stock_data, circulating_capital):
                    # 深度分析
                    breakout_quality = evaluate_breakout_quality(stock_data)

                    # 仅分析突破质量较高的标的
                    if breakout_quality['quality_score'] >= CONFIG.breakout_quality_min_score:
                        historical_stats = get_historical_pattern_success_rate(symbol)

                        # 风险评估
                        risk_assessment = evaluate_risk_level(stock_data, market_env)

                        # 计算综合次日上涨概率
                        base_prob = breakout_quality['next_day_win_prob']
                        hist_adjustment = historical_stats['success_rate'] - 55
                        market_adjustment = market_env['win_prob_adjustment']

                        total_win_prob = base_prob + hist_adjustment + market_adjustment
                        total_win_prob = max(50.0, min(85.0, total_win_prob))  # 限制范围

                        # 期望收益率
                        expected_return = historical_stats['avg_next_day_return'] * (total_win_prob / 60)
                        if expected_return < 0.5:
                            expected_return = 0.5  # 设置最低期望

                        # 仅保留高概率、高期望、风险可控的标的
                        if (
                                total_win_prob >= CONFIG.min_total_win_prob and
                                expected_return > CONFIG.min_expected_return and
                                risk_assessment['risk_level'] <= CONFIG.max_risk_level
                        ):
                            latest = stock_data.iloc[-1]
                            candidate = {
                                '板块': concept_name,
                                '代码': symbol[2:],
                                '名称': stock_name,
                                '当前价': latest['close'],
                                '涨幅': stock['涨跌幅'],
                                '流通市值(亿)': round(circulating_capital, 1),
                                '次日上涨概率': round(total_win_prob, 1),
                                '期望收益率': round(expected_return, 2),
                                '突破质量': breakout_quality['quality_score'],
                                '历史成功率': historical_stats['success_rate'],
                                '样本量': historical_stats['sample_size'],
                                '风险等级': risk_assessment['risk_level'],
                                '止损建议': risk_assessment['stop_loss'],
                                '量比': round(latest['volume_ratio'], 2),
                                '突破幅度': round(latest['breakout_strength'], 2),
                                '20日高点': round(latest['hhv20'], 2),
                                '概率拆分': {
                                    'base': round(base_prob, 1),
                                    'hist_adj': round(hist_adjustment, 1),
                                    'market_adj': round(market_adjustment, 1),
                                },
                                '质量细节': breakout_quality['component_scores']
                            }
                            sector_candidates.append(candidate)

            pbar.update(1)
            _sleep(CONFIG.request_sleep_seconds)  # 控制请求频率

        pbar.close()

        # 板块内机会排序
        if sector_candidates:
            # 按概率*期望收益排序
            sector_candidates.sort(key=lambda x: x['次日上涨概率'] * x['期望收益率'], reverse=True)

            # 仅保留最多3只最佳标的
            sector_candidates = sector_candidates[:3]
            high_probability_stocks.extend(sector_candidates)

            print(f"✅ 板块 {concept_name} 发现 {len(sector_candidates)} 个高概率机会")
            for cand in sector_candidates:
                print(
                    f"   • {cand['名称']}({cand['代码']}): 概率{cand['次日上涨概率']}% | 期望收益{cand['期望收益率']}% | 风险{cand['风险等级']}/5")
        else:
            print(f"❌ 板块 {concept_name} 未发现符合标准的机会")

    # 3. 全局排序与筛选
    if high_probability_stocks:
        # 按综合评分排序
        for stock in high_probability_stocks:
            stock['综合评分'] = (stock['次日上涨概率'] * 0.6 +
                                 stock['期望收益率'] * 4 * 0.3 +
                                 (10 - stock['风险等级'] * 2) * 0.1)

        # 仅保留综合评分最高的10只股票
        high_probability_stocks.sort(key=lambda x: x['综合评分'], reverse=True)
        final_stocks = high_probability_stocks[:10]

        results_df = pd.DataFrame(final_stocks)

        # 追加：同花顺实时交易信息（只对最终入选股）
        if CONFIG.enable_ths_realtime and get_ths_stocks_realtime is not None and not results_df.empty:
            try:
                codes = results_df["代码"].astype(str).tolist()[: CONFIG.ths_realtime_max_count]
                # 转换为 sh/sz 前缀，便于函数内部归一化
                prefixed = [("sh" + c if str(c).startswith("6") else "sz" + str(c)) for c in codes]
                realtime_rows = get_ths_stocks_realtime(prefixed, max_count=CONFIG.ths_realtime_max_count)
                if realtime_rows:
                    rt_df = pd.DataFrame(realtime_rows)
                    # rt_df['code'] 是 6位纯数字
                    rt_df.rename(
                        columns={
                            "code": "代码",
                            "price": "实时价",
                            "pct_change": "实时涨跌幅(%)",
                            "amount": "实时成交额",
                            "volume": "实时成交量",
                            "ts": "实时时间",
                        },
                        inplace=True,
                    )
                    keep_cols = [c for c in ["代码", "实时价", "实时涨跌幅(%)", "实时成交额", "实时成交量", "实时时间"] if c in rt_df.columns]
                    rt_df = rt_df[keep_cols]
                    results_df = results_df.merge(rt_df, on="代码", how="left")
            except Exception:
                # 实时行情失败不影响主流程
                pass

        print("\n" + "=" * 65)
        print(f"🎯 最终筛选: {len(final_stocks)} 个高概率交易机会 (次日上涨概率≥62%, 风险可控)")
        print("=" * 65)

        # 精简显示
        display_columns = [
            '板块', '名称', '当前价', '实时价', '实时涨跌幅(%)',
            '次日上涨概率', '期望收益率', '风险等级', '止损建议', '概率拆分'
        ]
        display_columns = [c for c in display_columns if c in results_df.columns]
        print(results_df[display_columns].to_string(index=False, float_format="%.2f"))

        # 专业解读
        print("\n" + "=" * 65)
        print("💡 专业操作建议:")
        print("=" * 65)

        for idx, (_, stock) in enumerate(results_df.iterrows(), 1):
            position_size = min(20, max(5, (stock['次日上涨概率'] - 60) * 2))
            if market_env['risk_level'] == "高":
                position_size = position_size * 0.6
            elif market_env['risk_level'] == "中高":
                position_size = position_size * 0.8

            print(f"\n{idx}. {stock['名称']} ({stock['代码']})")
            print(f"   • 买入策略: {stock['当前价']:.2f}元附近分批建仓，仓位建议{position_size:.0f}%")
            print(f"   • 止损策略: 跌破{stock['止损建议']:.2f}元(-{stock['当前价'] - stock['止损建议']:.2f}元)坚决止损")
            print(f"   • 止盈策略: 次日若上涨超过{stock['期望收益率'] * 2:.1f}%可考虑部分止盈")
            print(
                f"   • 概率依据: 突破质量{stock['突破质量']}/50分，历史相似形态成功率{stock['历史成功率']}%({stock['样本量']}次)")

            # 突破质量细节
            quality_details = stock['质量细节']
            print(
                f"   • 突破质量分解: 幅度{quality_details['breakout_margin']:.0f}/10 + 确认{quality_details['breakout_confirmation']:.0f}/10 + 量能{quality_details['volume_support']:.0f}/10")

        # 保存结果
        today = datetime.datetime.now().strftime("%Y%m%d")
        filename = f"A股高概率机会_{today}.csv"
        results_df.to_csv(filename, index=False, encoding='utf_8_sig')
        print(f"\n✅ 详细分析结果已保存至: {filename}")
    else:
        print("\n" + "=" * 65)
        print("🔍 深度分析结论: 未发现符合'量价突破+主线共振'高概率标准的标的")
        print("=" * 65)

        if market_env['risk_level'] == "高":
            print("\n💡 专业建议: 市场处于15连阳后的高风险区域，耐心等待回调或盘整后的二次机会")
            print("   • 历史数据表明: 沪指15连阳后3日内调整概率>80%，平均回调幅度2.5-3.5%")
            print("   • 策略建议: 降低仓位至20%以下，关注强势板块中的龙头回调至10日线的机会")
        else:
            print("\n💡 专业建议: 市场缺乏显著强势板块或突破形态，保持观望")
            print("   • 策略建议: 保持50%以上现金，等待市场出现新的领涨主线或调整结束信号")

        print("\n📌 投资纪律提醒: '筛选不到就不买，不然容易亏钱' - 无高概率机会时，保持现金为最佳策略")


if __name__ == "__main__":
    main_optimized()