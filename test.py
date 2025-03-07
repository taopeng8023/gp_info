import pandas as pd
import matplotlib.pyplot as plt

# 数据加载
data = pd.read_clipboard()  # 直接复制上述数据并用粘贴方式导入
data['order_cnt'] = pd.to_numeric(data['order_cnt'], errors='coerce')
data['月份'] = pd.to_datetime(data['月份'])

# 按月份和 source 汇总数据
pivot_data = data.pivot_table(
    index='月份', columns='source', values='order_cnt', aggfunc='sum', fill_value=0
)

# 绘制堆叠柱状图
plt.figure(figsize=(15, 8))
pivot_data.plot(kind='bar', stacked=True, figsize=(15, 8), colormap='tab20')
plt.title('Cash-In Source 分布（按月）', fontsize=16)
plt.xlabel('月份', fontsize=12)
plt.ylabel('订单数量', fontsize=12)
plt.legend(title='Source', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
