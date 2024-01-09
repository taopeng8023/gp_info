import akshare as ak
import pymysql
import datetime

#数据库设置
db = pymysql.Connect(
    host = 'localhost',
        port = 3306,  #端口号
        user = 'root',  #服务器上mysql的用户名，安装时填写确认的
        password = '123456',  #服务器上mysql的密码，安装时填写确认的
        database = 'pandas_tushare'   #服务器上的数据库名之一，选择需要连接的那个数据库
        #charset = 'utf8'
    )
cursor = db.cursor()

#获取A历史行情
def get_gp_info(gpCode,start_time,end_time):
   stock_zh_a_spot_df = ak.stock_zh_a_hist(symbol=gpCode,period="daily",start_date=start_time,end_date=end_time,adjust="")
   c_len = stock_zh_a_spot_df.shape[0]
   for i in range(c_len):
      _data = stock_zh_a_spot_df.iloc[i]
      _marketTime = _data[0]
      _marketTimeNew = _marketTime.replace("-","")
      cursor.execute(f"insert into gp_info (market_time, gp_code,gp_name,today_open_price,latest_price,highest_price,minimum_price,turnover,transaction_volume,amplitude,rise_fall_rang,rise_fall_amount,turnover_rate) "
                     f"value ('{_marketTimeNew}','sh605577','龙版传媒',{_data[1]},{_data[2]},{_data[3]},{_data[4]},{_data[5]},{_data[6]},{_data[7]},{_data[8]},{_data[9]},{_data[10]});")
      cursor.rowcount
      db.commit()
   cursor.close()

get_gp_info('605577','20240101','20240108')