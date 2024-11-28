import akshare as ak
import pandas as pd
import pandas_ta as ta


# 定义股票筛选函数
def stock_screening_a_shares_with_history(tickers, min_pe=0, max_pe=20, max_pb=3, min_gross_margin=0.2,
                                          min_current_ratio=1.5, min_rsi=30, max_rsi=70, min_macd=0,
                                          bollinger_band_threshold=1, atr_threshold=2, volume_change_pct=20,
                                          min_eps=0, min_net_margin=0.1, max_debt_to_equity=1.0, min_free_cash_flow=0,
                                          period="60d"):
    screened_stocks = []

    for ticker in tickers:
        # 获取A股股票数据
        stock = ak.stock_zh_a_spot()
        stock_data = stock[stock['代码'] == ticker]

        if stock_data.empty:
            continue

        # 获取股票的财务信息
        financials = ak.stock_financial_report_indicator_em(symbol=ticker)

        if financials.empty:
            continue

        # 提取财务指标
        pe_ratio = financials.iloc[0]['市盈率(TTM)']
        pb_ratio = financials.iloc[0]['市净率']
        gross_margin = financials.iloc[0]['毛利率']
        current_ratio = financials.iloc[0]['流动比率']
        eps = financials.iloc[0]['每股收益']
        net_margin = financials.iloc[0]['净利率']
        debt_to_equity = financials.iloc[0]['资产负债率']
        free_cash_flow = financials.iloc[0]['自由现金流']  # 如果数据缺失，设为0

        # 筛选条件：基本面财务指标
        if (pe_ratio is not None and min_pe <= pe_ratio <= max_pe) and \
                (pb_ratio is not None and pb_ratio <= max_pb) and \
                (gross_margin is not None and gross_margin >= min_gross_margin) and \
                (current_ratio is not None and current_ratio >= min_current_ratio) and \
                (eps is not None and eps >= min_eps) and \
                (net_margin is not None and net_margin >= min_net_margin) and \
                (debt_to_equity is not None and debt_to_equity <= max_debt_to_equity) and \
                (free_cash_flow is not None and free_cash_flow >= min_free_cash_flow):

            # 获取过去一段时间的历史数据（使用"qfq"前复权数据）
            hist = ak.stock_zh_a_hist(symbol=ticker, period="daily", adjust="qfq", end_date="2024-10-15")

            if hist.empty:
                continue

            # 根据历史数据计算技术指标
            hist['SMA_50'] = ta.sma(hist['close'], length=50)
            hist['SMA_200'] = ta.sma(hist['close'], length=200)
            hist['RSI'] = ta.rsi(hist['close'], length=14)
            macd = ta.macd(hist['close'], fast=12, slow=26, signal=9)
            hist['MACD'] = macd['MACD_12_26_9']
            hist['Signal'] = macd['MACDs_12_26_9']

            # 布林带 (Bollinger Bands)
            bollinger = ta.bbands(hist['close'], length=20, std=2)
            hist['BB_upper'] = bollinger['BBU_20_2.0']
            hist['BB_lower'] = bollinger['BBL_20_2.0']
            hist['BB_mid'] = bollinger['BBM_20_2.0']

            # 平均真实波幅 (ATR)
            hist['ATR'] = ta.atr(hist['high'], hist['low'], hist['close'], length=14)

            # 成交量变化百分比
            hist['Volume Change %'] = hist['volume'].pct_change() * 100

            # 获取最新一条的技术指标数据
            latest_data = hist.iloc[-1]
            sma_50 = latest_data['SMA_50']
            sma_200 = latest_data['SMA_200']
            rsi = latest_data['RSI']
            macd_value = latest_data['MACD']
            signal_value = latest_data['Signal']
            bb_upper = latest_data['BB_upper']
            bb_lower = latest_data['BB_lower']
            bb_mid = latest_data['BB_mid']
            atr = latest_data['ATR']
            volume_change = latest_data['Volume Change %']

            # 基于历史数据趋势筛选条件
            if (sma_50 > sma_200) and \
                    (min_rsi <= rsi <= max_rsi) and \
                    (macd_value > min_macd and macd_value > signal_value) and \
                    (latest_data['close'] < bb_upper and latest_data['close'] > bb_lower) and \
                    (atr <= atr_threshold) and \
                    (volume_change >= volume_change_pct):

                # 增加历史趋势判断：过去60天是否连续上涨，收盘价比前一个交易日高
                recent_price_trend = hist['close'][-int(period.strip('d')):].pct_change().mean()

                # 筛选符合条件的股票
                if recent_price_trend > 0.01:  # 如果过去60天的平均涨幅超过1%
                    screened_stocks.append({
                        'Ticker': ticker,
                        'P/E Ratio': pe_ratio,
                        'P/B Ratio': pb_ratio,
                        'Gross Margin': gross_margin,
                        'Current Ratio': current_ratio,
                        'EPS': eps,
                        'Net Profit Margin': net_margin,
                        'Debt to Equity': debt_to_equity,
                        'Free Cash Flow': free_cash_flow,
                        'SMA 50': sma_50,
                        'SMA 200': sma_200,
                        'RSI': rsi,
                        'MACD': macd_value,
                        'Signal': signal_value,
                        'BB Upper': bb_upper,
                        'BB Lower': bb_lower,
                        'ATR': atr,
                        'Volume Change %': volume_change,
                        'Price Trend (60 days avg)': recent_price_trend
                    })

    # 将结果转换为DataFrame并返回
    return pd.DataFrame(screened_stocks)


# 输入需要筛选的A股股票代码列表（A股代码以6开头）
all_stock_info_sh = ak.stock_info_sh_name_code(symbol="主板A股")
all_stock_info_sz = ak.stock_info_sz_name_code(symbol= "A股列表")
# 只选择需要的列
all_stock_info_sh = all_stock_info_sh[['证券代码']]
all_stock_info_sz = all_stock_info_sz[['A股代码']]
all_stock_codes = all_stock_info_sh['证券代码'].tolist() + all_stock_info_sz['A股代码'].tolist()

# 设置筛选条件并调用函数
result = stock_screening_a_shares_with_history(all_stock_codes)
print("筛选出的A股股票:")
print(result)
