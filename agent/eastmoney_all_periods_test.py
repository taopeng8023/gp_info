import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# 设置请求头，避免被反爬
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

# 目标URL
url = "https://data.eastmoney.com/zjlx/detail.html"

# 发送请求
response = requests.get(url, headers=headers)
response.raise_for_status()

# 解析HTML
soup = BeautifulSoup(response.content, 'html.parser')

# 找到表格
table = soup.find('table', class_='datatb')
if not table:
    raise ValueError("未找到数据表格")

# 提取表头（列名）
thead = table.find('thead')
if thead:
    headers = [th.get_text(strip=True) for th in thead.find_all('th')]
else:
    # 如果没有 thead，直接从第一行获取
    rows = table.find_all('tr')
    if len(rows) > 0:
        headers = [td.get_text(strip=True) for td in rows[0].find_all('td')]

# 提取数据行
data = []
rows = table.find_all('tr')[1:]  # 跳过表头

for row in rows:
    cols = row.find_all('td')
    if len(cols) >= 19:  # 确保有足够的列
        item = {}
        item['序号'] = cols[0].get_text(strip=True)
        item['代码'] = cols[1].get_text(strip=True)
        item['名称'] = cols[2].get_text(strip=True)
        item['最新价'] = cols[6].get_text(strip=True)
        item['今日涨跌幅'] = cols[7].get_text(strip=True)

        # 主力净流入
        item['主力净流入'] = cols[8].get_text(strip=True)
        item['主力净占比'] = cols[9].get_text(strip=True)

        # 超大单
        item['超大单净流入'] = cols[10].get_text(strip=True)
        item['超大单净占比'] = cols[11].get_text(strip=True)

        # 大单
        item['大单净流入'] = cols[12].get_text(strip=True)
        item['大单净占比'] = cols[13].get_text(strip=True)

        # 中单
        item['中单净流入'] = cols[14].get_text(strip=True)
        item['中单净占比'] = cols[15].get_text(strip=True)

        # 小单
        item['小单净流入'] = cols[16].get_text(strip=True)
        item['小单净占比'] = cols[17].get_text(strip=True)

        data.append(item)

# 转换为 DataFrame
df = pd.DataFrame(data)

# 清洗数字数据（转换为浮点数，支持“亿”、“万”）
def parse_money(text):
    if pd.isna(text) or text == '':
        return None
    text = text.replace('亿', '').replace('万', '')
    try:
        return float(text)
    except:
        return None

# 应用于所有金额列
amount_cols = ['主力净流入', '超大单净流入', '大单净流入', '中单净流入', '小单净流入']
for col in amount_cols:
    df[col] = df[col].apply(lambda x: parse_money(x))

# 占比列
ratio_cols = ['主力净占比', '超大单净占比', '大单净占比', '中单净占比', '小单净占比']
for col in ratio_cols:
    df[col] = df[col].str.rstrip('%').astype(float) / 100

# 输出结果
print(df.head(10))

# 可选：保存为CSV
df.to_csv('eastmoney_individual_fund_flow.csv', encoding='utf-8-sig', index=False)