import requests

def get_real_fund_inflow(top_n=10):
    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    params = {
        'reportName': 'RPT_MUTUAL_DEAL_LATEST',
        'columns': 'ALL',
        'sortColumns': 'NET_BUY_AMT',
        'sortTypes': '-1',
        'pageSize': top_n,
        'pageNumber': 1
    }

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://data.eastmoney.com/zjlx/',
    }

    resp = requests.get(url, params=params, headers=headers)
    rows = resp.json()['result']['data']

    result = []
    for r in rows:
        result.append({
            '代码': r['SECUCODE'].split('.')[0],
            '名称': r['SECURITY_NAME_ABBR'],
            '主力净流入(亿)': round(r['NET_BUY_AMT'] / 1e8, 2),
            '收盘价': r['CLOSE_PRICE'],
            '涨跌幅': f"{r['CHANGE_RATE']}%"
        })

    return result

# 测试输出
if __name__ == '__main__':
    data = get_real_fund_inflow(10)
    for d in data:
        print(f"{d['代码']} {d['名称']} 主力净流入: {d['主力净流入(亿)']} 亿, 涨跌: {d['涨跌幅']}, 收盘: {d['收盘价']}")