# 导入tushare
import tushare as ts

# 初始化pro接口
pro = ts.pro_api('a7358a0255666758b3ef492a6c1de79f2447de968d4a1785b73716a4')

# 拉取数据
df = pro.daily(**{
    "ts_code": "",
    "trade_date": "",
    "start_date": "",
    "end_date": "",
    "offset": "",
    "limit": ""
}, fields=[
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount"
])
df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')

