import akshare as ak
import pymysql
import datetime
import numpy as np
import threading
from threading import current_thread

#数据库设置
def getDb():
    db = pymysql.Connect(
        host = 'localhost',
            port = 3306,  #端口号
            user = 'root',  #服务器上mysql的用户名，安装时填写确认的
            password = '123456',  #服务器上mysql的密码，安装时填写确认的
            database = 'pandas_tushare'   #服务器上的数据库名之一，选择需要连接的那个数据库
            #charset = 'utf8'
        )
    return db

#获取当前日期
def getCurrentDay():
   current_day = datetime.datetime.now()
   formatter_day = current_day.strftime("%Y%m%d")
   return formatter_day

#获取所有A+H股票代码
def getGpCode(startTime,endTime):
    _gpCodeData = ak.stock_zh_a_spot_em()
    _gpCodeLeng = _gpCodeData.shape[0]
    for i in range(_gpCodeLeng):
        _gpData = _gpCodeData.iloc[i]
        _xuhao = _gpData['序号']
        _marketTime = getCurrentDay()
        _gp_code = _gpData['代码']
        _gp_name = _gpData['名称']
        _latest_price = _gpData['最新价']
        print(_xuhao,":",_gp_code,":",_gp_name)
        if not np.isnan(_latest_price):
            getGpHisInfo(_gp_code, _gp_name, startTime, endTime)

def getGpHisInfo(gpCode,gpName,start_time,end_time):
    _gpHisData = ak.stock_zh_a_hist(symbol=gpCode,period='daily',start_date=start_time,end_date=end_time,adjust='hfq')
    _gpHisLeng = _gpHisData.shape[0]
    for i in range(_gpHisLeng):
        _gpData = _gpHisData.iloc[i]
        _marketTime = _gpData['日期']
        _marketTimeNew = _marketTime.replace("-","")
        _today_open_price = _gpData['开盘']
        _latest_price = _gpData['收盘']
        if not np.isnan(_latest_price):
            _highest_price = _gpData['最高']
            _minimum_price = _gpData['最低']
            _turnover = _gpData['成交量']
            _transaction_volume = _gpData['成交额']
            _amplitude = _gpData['振幅']
            _rise_fall_rang = _gpData['涨跌幅']
            _rise_fall_amount = _gpData['涨跌额']
            _turnover_rate = _gpData['换手率']
            db = getDb()
            cursor = db.cursor()
            cursor.execute(
                f"insert into gp_info (market_time, gp_code,gp_name,latest_price,rise_fall_rang,rise_fall_amount,turnover,transaction_volume,amplitude,highest_price,minimum_price,today_open_price,turnover_rate) "
                f"value ('{_marketTimeNew}','{gpCode}','{gpName}',{_latest_price},{_rise_fall_rang},{_rise_fall_amount},{_turnover},{_transaction_volume},{_amplitude},{_highest_price},{_minimum_price},{_today_open_price},{_turnover_rate});")
            cursor.rowcount
            db.commit()
class Thread1(threading.Thread):
    def run(self):
        getGpCode('20240110', '20240110')

class Thread2(threading.Thread):
    def run(self):
        getGpCode('20231211','20231220')

class Thread3(threading.Thread):
    def run(self):
        getGpCode('20231221','20231231')

def _main():
    t1 = Thread1()
    # t2 = Thread2()
    # t3 = Thread3()
    t1.start()
    # t2.start()
    # t3.start()



_main()

