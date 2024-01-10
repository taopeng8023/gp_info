import akshare as ak

def get_gp_info():
   stock_zh_a_spot_df = ak.stock_zh_a_spot()
   c_len = stock_zh_a_spot_df.shape[0]
   for i in range(c_len):
      _data = stock_zh_a_spot_df.iloc[i]
      print(_data)

get_gp_info()