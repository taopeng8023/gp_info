import akshare as ak
import datetime
import pandas as pd


# stock_info_sh_name_code_df = ak.stock_info_sh_name_code(symbol="主板A股")
# all_stock_info_sz = ak.stock_info_sz_name_code(symbol="A股列表")
end_date = datetime.datetime.now().strftime('%Y%m%d')
start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')
print(end_date)
print(start_date)
data = ak.stock_zh_a_hist(symbol="600085", start_date=start_date, end_date=end_date, adjust="qfq")
print(len(data))