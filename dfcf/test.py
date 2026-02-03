import requests
import time
import pandas as pd

# ====== 配置参数 ======
BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"
PARAMS = {
    "np": 1,
    "fltt": 1,
    "invt": 2,
    "fs": "m:1+t:2+f:!2,m:1+t:23+f:!2",  # 沪市A股 + 科创板，排除ST
    "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f7,f15,f18,f16,f17,f10,f8,f9,f23",
    "fid": "f3",  # 按涨跌幅排序
    "po": 1,  # 1=升序（跌幅从大到小）
    "dect": 1,
    "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    "wbp2u": "|0|0|0|web"
}

# 字段映射表
FIELD_MAPPING = {
    "f12": "code",  # 股票代码
    "f14": "name",  # 股票名称
    "f2": "price",  # 最新价（分）
    "f3": "change_pct",  # 涨跌幅（万分比）
    "f4": "change_amount",  # 涨跌额（分）
    "f5": "volume",  # 成交量（手）
    "f6": "amount",  # 成交额（元）
    "f15": "high",  # 最高价
    "f16": "low",  # 最低价
    "f17": "open",  # 今开
    "f18": "prev_close",  # 昨收
    "f23": "turnover",  # 换手率（万分比）
    "f13": "market",  # 市场（1=沪市）
    "f152": "type"  # 股票类型
}


def fetch_page(page_num, page_size=20):
    """获取单页数据（返回纯JSON）"""
    params = PARAMS.copy()
    params["pn"] = page_num
    params["pz"] = page_size
    params["_"] = int(time.time() * 1000)  # 防缓存时间戳

    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"❌ 请求第 {page_num} 页失败: {e}")
        return None


def parse_stock_data(stock_list):
    """安全解析股票数据（处理字符串数值）"""
    parsed = []
    for item in stock_list:
        row = {}
        for key, col in FIELD_MAPPING.items():
            val = item.get(key)

            # 如果是字符串，尝试转为数字
            if isinstance(val, str):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = None  # 转换失败设为None

            # 数值单位转换
            if key in ["f2", "f4", "f15", "f16", "f17", "f18"]:
                # 价格类：分 → 元
                row[col] = round(val / 100.0, 2) if val is not None else None
            elif key in ["f3", "f23"]:
                # 百分比类：万分比 → %
                row[col] = round(val / 100.0, 2) if val is not None else None
            else:
                row[col] = val
        parsed.append(row)
    return parsed


def crawl_all_stocks(max_pages=500, page_size=100, delay=0.8):
    """
    轮询所有页面直到无数据

    Args:
        max_pages: 最大页数（防死循环）
        page_size: 每页条数（建议20-100）
        delay: 请求间隔（秒）
    """
    all_stocks = []
    page = 1

    while page <= max_pages:
        print(f"📥 正在获取第 {page} 页...")
        data = fetch_page(page, page_size)

        if not data:
            break

        rc = data.get("rc")
        stock_data = data.get("data", {})

        # 判断是否结束（rc=102 表示无数据）
        if rc == 102 or not stock_data or not stock_data.get("diff"):
            print("✅ 已到最后一页，停止抓取")
            break

        stocks = parse_stock_data(stock_data["diff"])
        all_stocks.extend(stocks)
        print(f"   → 获取 {len(stocks)} 条，累计 {len(all_stocks)} 条")

        # 防反爬：每页间隔
        time.sleep(delay)
        page += 1

    return all_stocks


# ====== 执行主程序 ======
if __name__ == "__main__":
    print("🚀 开始抓取东方财富全量股票数据（沪市非ST股）...")

    # 抓取全部数据（每页100条，间隔0.8秒）
    stocks = crawl_all_stocks(page_size=100, delay=0.8)

    if stocks:
        df = pd.DataFrame(stocks)
        print(f"\n📊 总共获取 {len(df)} 只股票")
        print(df.head())

        # 保存到CSV（UTF-8 with BOM，Excel友好）
        df.to_csv("eastmoney_stocks.csv", index=False, encoding="utf-8-sig")
        print("\n💾 数据已保存至 eastmoney_stocks.csv")
    else:
        print("⚠️ 未获取到任何数据，请检查网络或参数")