# import akshare as ak
# import json
#
# stock_zh_ah_name_dict = ak.stock_zh_ah_name()
# _index = 0;
# for key in stock_zh_ah_name_dict:
#     print(key ,":" , stock_zh_ah_name_dict[key])
#     _index = _index + 1;
#     print(_index)
import numpy as np
_nan = 'nan'
if isinstance(_nan,np.float64):
    print("true")
if isinstance(_nan,str):
    print("str")