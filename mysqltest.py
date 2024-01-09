import mysql.connector
conn = mysql.connector.connect(user='root', password='123456', database='test2020')
cursor = conn.cursor()
# 打开并读取txt文件
file_path = "example.txt" # txt文件路径
with open(file_path, 'r') as file:
    lines = file.readlines() # 将文件内容存入变量content中
    id = ""
    name = ""
    index = 0
    for line in lines:
        id , name = line.replace('\n','').split(' ')
        cursor.execute(f"insert into user_testI (id, name) value ({id}, '{name}');")
        cursor.rowcount
        conn.commit()
        index = index + 1
        print(index)

# cursor.execute('create table user_testI (id varchar(20) primary key, name varchar(20))')


cursor.close()