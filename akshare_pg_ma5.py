#MA5计算

import pymysql

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

#获取当日gp信息
def getCurrentGpInfo(currentDayStr):
    query = "SELECT gp_code,gp_name,turnover from gp_info WHERE market_time = %s"
    db = getDb()
    cursor = db.cursor()
    cursor.execute(query,[currentDayStr])
    result = cursor.fetchall()
    for row in result:
        getGpMa5Info(row[0],row[1],row[2],currentDayStr)
    cursor.close()
    db.cursor()



def getGpMa5Info(gpCode,gpName,turnover,currentDayStr):
    query = "SELECT gp_code,SUM(turnover) from gp_info WHERE gp_code = %s and market_time < %s ORDER BY id DESC LIMIT 5"
    db = getDb()
    cursor = db.cursor()
    cursor.execute(query, (gpCode,currentDayStr))
    result = cursor.fetchall()
    if result[0][1] is not None and turnover > float(result[0][1]) * 1.5:
        print(gpCode,":",gpName)
    cursor.close()
    db.cursor()



getCurrentGpInfo(20240109)