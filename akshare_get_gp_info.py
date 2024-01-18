import akshare as ak
import pymysql
import datetime
import numpy as np

#数据库设置
db = pymysql.Connect(
    host = 'localhost',
        port = 3306,  #端口号
        user = 'root',  #服务器上mysql的用户名，安装时填写确认的
        password = '',  #服务器上mysql的密码，安装时填写确认的
        database = 'pandas_tushare'   #服务器上的数据库名之一，选择需要连接的那个数据库
        #charset = 'utf8'
    )
cursor = db.cursor()

#获取当前日期
def getCurrentDay():
   current_day = datetime.datetime.now()
   formatter_day = current_day.strftime("%Y%m%d")
   return formatter_day

#获取所有A+H股票代码
def getGpCode():
    _gpCodeData = ak.stock_zh_a_spot_em()
    _gpCodeLeng = _gpCodeData.shape[0]
    for i in range(_gpCodeLeng):
        _gpData = _gpCodeData.iloc[i]
        _xuhao = _gpData['序号']
        _marketTime = getCurrentDay()
        _gp_code = _gpData['代码']
        _gp_name = _gpData['名称']
        _latest_price = _gpData['最新价']
        _equivalent_ratio = _gpData['量比']
        if not np.isnan(_latest_price) and not np.isnan(_equivalent_ratio):
            print(_xuhao)
            _rise_fall_rang = _gpData['涨跌幅']
            _rise_fall_amount = _gpData['涨跌额']
            _turnover = _gpData['成交量']
            _transaction_volume = _gpData['成交额']
            _amplitude = _gpData['振幅']
            _highest_price = _gpData['最高']
            _minimum_price = _gpData['最低']
            _today_open_price = _gpData['今开']
            _yesterday_close_price = _gpData['昨收']
            _turnover_rate = _gpData['换手率']
            _pe_ratio_movement = _gpData['市盈率-动态']
            _pe_ratio = _gpData['市净率']
            _total_market_value = _gpData['总市值']
            _circulate_market_value = _gpData['流通市值']
            _speed_increase = _gpData['涨速']
            _five_speed_increase = _gpData['5分钟涨跌']
            _sixty_day_speed_increase = _gpData['60日涨跌幅']
            _year_to_date_speed_increase = _gpData['年初至今涨跌幅']
            cursor.execute(f"insert into gp_info (market_time, gp_code,gp_name,latest_price,rise_fall_rang,rise_fall_amount,turnover,transaction_volume,amplitude,highest_price,minimum_price,today_open_price,yesterday_close_price,equivalent_ratio,turnover_rate,pe_ratio_movement,pe_ratio,total_market_value,circulate_market_value,speed_increase,five_speed_increase,sixty_day_speed_increase,year_to_date_speed_increase) "
                             f"value ('{_marketTime}','{_gp_code}','{_gp_name}',{_latest_price},{_rise_fall_rang},{_rise_fall_amount},{_turnover},{_transaction_volume},{_amplitude},{_highest_price},{_minimum_price},{_today_open_price},{_yesterday_close_price},{_equivalent_ratio},{_turnover_rate},{_pe_ratio_movement},{_pe_ratio},{_total_market_value},{_circulate_market_value},{_speed_increase},{_five_speed_increase},{_sixty_day_speed_increase},{_year_to_date_speed_increase});")
            cursor.rowcount
            db.commit()

getGpCode()
