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

def get_gp_info():
   stock_zh_a_spot_df = ak.stock_zh_a_spot()
   c_len = stock_zh_a_spot_df.shape[0]
   for i in range(c_len):
      _data = stock_zh_a_spot_df.iloc[i]
      cursor.execute(f"insert into gp_info (market_time, gp_code,gp_name,latest_price,rise_fall_amount,rise_fall_rang,bug_in,sell_out,yesterday_close_price,today_open_price,highest_price,minimum_price,turnover,transaction_volume) "
                     f"value (getCurrentDay(), '{_data[0]}','{_data[1]}',{_data[2]},{_data[3]},{_data[4]},{_data[5]},{_data[6]},{_data[7]},{_data[8]},{_data[9]},{_data[10]},{_data[11]},{_data[12]});")
      cursor.rowcount
      db.commit()
   cursor.close()


def getCurrentDay():
   current_day = datetime.datetime.now()
   formatter_day = current_day.strftime("%Y%m%d")
   return formatter_day

get_gp_info()