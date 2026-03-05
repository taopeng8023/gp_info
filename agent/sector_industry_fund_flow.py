"""
脚本名称: industry_sector_fund_flow.py
功能: 抓取东方财富网行业板块资金流向排行（今日、5日、10日）
数据命名: 板块资金流-行业资金流_周期_YYYY-MM-DD.csv
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
# 行业板块资金流周期配置
SECTOR_CONFIGS = {
    '今日': {
        'fid': 'f62',
        'fields': [
            'f12', 'f14', 'f2', 'f3', 'f62', 'f184', 'f66', 'f69', 'f72', 'f75',
            'f78', 'f81', 'f84', 'f87', 'f204', 'f205', 'f206', 'f124', 'f1', 'f13'
        ],
        'field_map': {
            '代码': 'f12',
            '名称': 'f14',
            '最新指数': 'f2',
            '涨跌幅(%)': 'f3',
            '主力净额(元)': 'f62',
            '主力净占比(%)': 'f184',
            '超大单净额(元)': 'f66',
            '超大单净占比(%)': 'f69',
            '大单净额(元)': 'f72',
            '大单净占比(%)': 'f75',
            '中单净额(元)': 'f78',
            '中单净占比(%)': 'f81',
            '小单净额(元)': 'f84',
            '小单净占比(%)': 'f87',
            '今日主力净流入最大股': 'f204',
            '最大股代码': 'f205',
            '最大股市场标识': 'f206',
        },
        'pct_change_field': 'f3'
    },
    '5日': {
        'fid': 'f164',
        'fields': [
            'f12', 'f14', 'f2', 'f109', 'f164', 'f165', 'f166', 'f167', 'f168',
            'f169', 'f170', 'f171', 'f172', 'f173', 'f257', 'f258', 'f259', 'f124', 'f1', 'f13'
        ],
        'field_map': {
            '代码': 'f12',
            '名称': 'f14',
            '最新指数': 'f2',
            '涨跌幅(%)': 'f109',
            '主力净额(元)': 'f164',
            '主力净占比(%)': 'f165',
            '超大单净额(元)': 'f166',
            '超大单净占比(%)': 'f167',
            '大单净额(元)': 'f168',
            '大单净占比(%)': 'f169',
            '中单净额(元)': 'f170',
            '中单净占比(%)': 'f171',
            '小单净额(元)': 'f172',
            '小单净占比(%)': 'f173',
            '5日主力净流入最大股': 'f257',
            '最大股代码': 'f258',
            '最大股市场标识': 'f259',
        },
        'pct_change_field': 'f109'
    },
    '10日': {
        'fid': 'f174',
        'fields': [
            'f12', 'f14', 'f2', 'f160', 'f174', 'f175', 'f176', 'f177', 'f178',
            'f179', 'f180', 'f181', 'f182', 'f183', 'f260', 'f261', 'f262', 'f124', 'f1', 'f13'
        ],
        'field_map': {
            '代码': 'f12',
            '名称': 'f14',
            '最新指数': 'f2',
            '涨跌幅(%)': 'f160',
            '主力净额(元)': 'f174',
            '主力净占比(%)': 'f175',
            '超大单净额(元)': 'f176',
            '超大单净占比(%)': 'f177',
            '大单净额(元)': 'f178',
            '大单净占比(%)': 'f179',
            '中单净额(元)': 'f180',
            '中单净占比(%)': 'f181',
            '小单净额(元)': 'f182',
            '小单净占比(%)': 'f183',
            '10日主力净流入最大股': 'f260',
            '最大股代码': 'f261',
            '最大股市场标识': 'f262',
        },
        'pct_change_field': 'f160'
    }
}

# 公共请求参数
BASE_URL = 'https://push2.eastmoney.com/api/qt/clist/get'
COMMON_PARAMS = {
    'po': '1',          # 降序
    'pz': '50',         # 每页50条
    'pn': '1',          # 第一页
    'np': '1',
    'fltt': '2',
    'invt': '2',
    'ut': '8dec03ba335b81bf4ebdf7b29ec27d15',  # 统一ut
    'fs': 'm:90 s:4',   # 行业板块
}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    'Referer': 'https://data.eastmoney.com/zjlx/detail.html',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}
# =================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_sector_data(period_name, config):
    """获取指定周期的行业板块数据"""
    params = COMMON_PARAMS.copy()
    params['fid'] = config['fid']
    params['fields'] = ','.join(config['fields'])
    params['_'] = str(int(time.time() * 1000))

    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
        response.encoding = 'utf-8'

        # 处理JSONP响应（去除jQuery回调）
        text = response.text
        if text.startswith('jQuery'):
            match = re.search(r'jQuery\d+_\d+\((.*)\)', text)
            if match:
                json_str = match.group(1)
                data_json = json.loads(json_str)
            else:
                logging.error(f"{period_name} JSONP解析失败")
                return None
        else:
            data_json = response.json()

        if data_json.get('rc') == 0 and data_json.get('data'):
            items = data_json['data'].get('diff', [])
            logging.info(f"{period_name} 成功获取 {len(items)} 条行业板块数据")
            return items
        else:
            logging.error(f"{period_name} API返回异常: {data_json}")
            return None
    except Exception as e:
        logging.error(f"{period_name} 请求失败: {e}")
        return None

def parse_sector_data(items, field_map):
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
    # 将数值列转换为浮点数（可选）
    for col in df.columns:
        if col.endswith('(元)') or col.endswith('(%)') or col == '最新指数':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logging.info(f"数据已保存至 {filename}，共 {len(df)} 条记录")
    return True

def main():
    today = datetime.now().strftime("%Y-%m-%d")

    for period_name, config in SECTOR_CONFIGS.items():
        # 文件名格式：板块资金流-行业资金流_周期_日期.csv
        filename = f"板块资金流-行业资金流_{period_name}_{today}.csv"

        # 检查文件是否已存在
        if os.path.exists(filename):
            logging.info(f"文件 {filename} 已存在，跳过 {period_name} 排行抓取。")
            continue

        logging.info(f"开始抓取行业板块 {period_name} 排行数据...")
        items = fetch_sector_data(period_name, config)

        if items:
            data = parse_sector_data(items, config['field_map'])
            if data:
                print(f"\n{period_name} 行业排行前3条预览：")
                for i, d in enumerate(data[:3]):
                    print(f"{i+1}. {d['代码']} {d['名称']} 主力净额: {d['主力净额(元)']}")
                save_to_csv(data, filename)
            else:
                logging.warning(f"{period_name} 排行解析后无数据")
        else:
            logging.error(f"获取 {period_name} 排行数据失败")

if __name__ == "__main__":
    main()