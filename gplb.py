# 导入tushare
import tushare as ts

# 初始化pro接口
pro = ts.pro_api('a7358a0255666758b3ef492a6c1de79f2447de968d4a1785b73716a4')

# 拉取数据
df = pro.stock_basic(**{
    "ts_code": "",
    "name": "",
    "exchange": "",
    "market": "",
    "is_hs": "",
    "list_status": "",
    "limit": "",
    "offset": ""
}, fields=[
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "cnspell",
    "market",
    "list_date",
    "act_name",
    "act_ent_type"
])
print(df)

