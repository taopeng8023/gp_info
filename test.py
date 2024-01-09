import mysql.connector
conn = mysql.connector.connect(user='root', password='123456', database='test2020')
cursor = conn.cursor()
# 打开并读取txt文件
file_path = "example.txt"
file = open(file_path,'r')
id = ""
name = ""
for (num,value) in enumerate(file):
    id, name = value.split(" ")
    cursor.execute(f"insert into user_testI (id, name) values ({id}, '{name}');")
    cursor.rowcount
    conn.commit()
    print(num+1)
# cursor.execute('create table user_testI (id varchar(20) primary key, name varchar(20))')
# f"update big_order_2312_2 set fee_amount = {fee} , fee_pattern = 'IN_DEDUCT',update_time=update_time where order_no = '{order_no}' and fee_amount=0;"
file.close()
cursor.close()