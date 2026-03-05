"""
脚本名称: main_force_ranking.py
功能: 抓取东方财富网个股主力排名数据（多周期综合排名，含所属板块）
数据命名: 主力排名_YYYY-MM-DD.csv
"""

import requests
import pandas as pd
import time
import logging
import os
import json
import re
from datetime import datetime

# ==================== 配置区域 ====================
# 主力排名接口参数（来自您提供的URL）
MAIN_FORCE_CONFIG = {
    'fid': 'f184',  # 按今日主力净占比排序
    'fields': [
        'f2', 'f3', 'f12', 'f13', 'f14', 'f62', 'f184', 'f225', 'f165', 'f263',
        'f109', 'f175', 'f264', 'f160', 'f100', 'f124', 'f265', 'f1'
    ],
    'field_map': {
        '代码': 'f12',
        '名称': 'f14',
        '最新价': 'f2',
        '所属板块': 'f100',          # 行业板块名称
        '板块代码': 'f265',          # 行业板块代码
        '今日主力净占比(%)': 'f184',
        '今日排名': 'f225',
        '今日涨跌幅(%)': 'f3',
        '5日主力净占比(%)': 'f165',
        '5日排名': 'f263',
        '5日涨跌幅(%)': 'f109',
        '10日主力净占比(%)': 'f175',
        '10日排名': 'f264',
        '10日涨跌幅(%)': 'f160',
        '市场标识': 'f13',           # 0=深市, 1=沪市
        '更新时间戳': 'f124',
    },
    # 注意：f62（今日主力净额）虽然请求了但未在页面显示，可根据需要添加
}

# 公共请求参数
BASE_URL = 'https://push2.eastmoney.com/api/qt/clist/get'
COMMON_PARAMS = {
    'po': '1',
    'pz': '50',        # 每页50条
    'pn': '1',         # 第一页
    'np': '1',
    'fltt': '2',
    'invt': '2',
    'ut': '8dec03ba335b81bf4ebdf7b29ec27d15',
    'fs': 'm:0 t:6,m:0 t:13,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:7,m:1 t:3',  # 覆盖沪深A股等
}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    'Referer': 'https://data.eastmoney.com/zjlx/detail.html',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}
# =================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_main_force_data(config, page_num=1, page_size=50):
    """获取主力排名数据"""
    params = COMMON_PARAMS.copy()
    params['fid'] = config['fid']
    params['fields'] = ','.join(config['fields'])
    params['pn'] = page_num
    params['pz'] = page_size
    params['_'] = str(int(time.time() * 1000))

    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
        response.encoding = 'utf-8'

        # 处理JSONP响应
        text = response.text
        if text.startswith('jQuery'):
            match = re.search(r'jQuery\d+_\d+\((.*)\)', text)
            if match:
                json_str = match.group(1)
                data_json = json.loads(json_str)
            else:
                logging.error("JSONP解析失败")
                return None
        else:
            data_json = response.json()

        # 检查返回码
        if data_json.get('rc') != 0:
            logging.error(f"API返回错误码: {data_json.get('rc')}, 错误信息: {data_json}")
            return None

        if data_json.get('data'):
            items = data_json['data'].get('diff', [])
            logging.info(f"成功获取 {len(items)} 条主力排名数据")
            return items
        else:
            logging.error(f"API返回数据为空: {data_json}")
            return None
    except Exception as e:
        logging.error(f"请求失败: {e}")
        return None

def parse_main_force_data(items, field_map):
    """根据字段映射解析数据"""
    parsed = []
    for item in items:
        try:
            record = {}
            for col, field in field_map.items():
                value = item.get(field)
                # 处理可能为 '-' 或 None 的情况
                if value == '-' or value is None:
                    record[col] = None
                else:
                    record[col] = value
            parsed.append(record)
        except Exception as e:
            logging.warning(f"解析单条数据出错: {e}")
            continue
    return parsed

def save_to_csv(data, filename):
    """保存数据到CSV"""
    if not data:
        logging.warning(f"无数据可保存至 {filename}")
        return False
    df = pd.DataFrame(data)
    # 将数值列转换为浮点数
    for col in df.columns:
        if col.endswith('(元)') or col.endswith('(%)') or col == '最新价' or '排名' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logging.info(f"数据已保存至 {filename}，共 {len(df)} 条记录")
    return True

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"主力排名_{today}.csv"

    # 检查文件是否已存在
    if os.path.exists(filename):
        logging.info(f"今日主力排名文件 {filename} 已存在，跳过抓取。")
        return

    logging.info("开始抓取主力排名数据...")
    items = fetch_main_force_data(MAIN_FORCE_CONFIG, page_size=50)

    if items:
        data = parse_main_force_data(items, MAIN_FORCE_CONFIG['field_map'])
        if data:
            print(f"\n主力排名前3条预览：")
            for i, d in enumerate(data[:3]):
                print(f"{i+1}. {d['代码']} {d['名称']} 今日排名: {d['今日排名']} 所属板块: {d['所属板块']}")
            save_to_csv(data, filename)
        else:
            logging.warning("解析后无数据")
    else:
        logging.error("获取数据失败，请检查接口有效性")

if __name__ == "__main__":
    main()