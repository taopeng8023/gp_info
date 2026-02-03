import akshare as ak
import pandas as pd
from datetime import datetime


def enrich_with_industry(df):
    # """给股票列表增加行业字段"""
    industry_df = ak.stock_board_industry_cons_em()
    industry_map = dict(zip(industry_df["代码"], industry_df["板块名称"]))
    df["行业"] = df["代码"].map(industry_map).fillna("未知")
    return df


def get_top_10_volume_stocks():
    df = ak.stock_zh_a_spot_em()
    df = df[df["市场类型"] == "主板A股"]
    df = df.sort_values(by="成交额", ascending=False).head(10)
    df = enrich_with_industry(df)
    return df[["代码", "名称", "行业", "成交量", "成交额"]]


def get_limit_down_stocks():
    df = ak.stock_zh_a_daily_em()
    df = df[df["涨跌幅"] <= -9.9]  # 跌停板判断
    df = enrich_with_industry(df)
    return df[["代码", "名称", "行业", "成交量", "成交额"]]


def get_continuous_limit_up_stocks(days=2):
    df = ak.stock_zt_pool_em()
    df = df[df["连续涨停天数"] >= days]
    df = enrich_with_industry(df)
    return df[["代码", "名称", "行业", "连续涨停天数"]]


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"=== A股选股分析报告（{today}） ===\n")

    print("【主板成交额前10股票】：")
    try:
        top10 = get_top_10_volume_stocks()
        print(top10.to_string(index=False))
    except Exception as e:
        print("获取主板前10失败：", e)

    print("\n【跌停股票】：")
    try:
        down = get_limit_down_stocks()
        if down.empty:
            print("今日无跌停股票")
        else:
            print(down.to_string(index=False))
    except Exception as e:
        print("获取跌停股失败：", e)

    print("\n【2连板股票】：")
    try:
        up2 = get_continuous_limit_up_stocks(2)
        if up2.empty:
            print("今日无2连板股票")
        else:
            print(up2.to_string(index=False))
    except Exception as e:
        print("获取连板股失败：", e)


if __name__ == "__main__":
    main()