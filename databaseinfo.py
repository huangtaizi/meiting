import pandas as pd
# import pymysql
import pymssql

if __name__ == '__main__':
    print('————————————————————start')
    conn = pymssql.connect(host='127.0.0.1', user='sa', password='admin', database='UserDatabase')
    cursor = conn.cursor()
    print('————————————————————end')
    if conn:
        print('连接成功')
    #sql0 = "show create table {}".format('data_2')
    #cursor.execute(sql0)
    #data = cursor.fetchone()
    #print(data.get('Create Table'))
    sql = 'delete from dbo.data_2 where "振动风选_1#振动X" > -10000'
    cursor.execute(sql)
    #sql3 = 'alter table data_2 drop time '
    #cursor.execute(sql3)
    conn.commit()
    sql2 = 'select * from dbo.data_2'
    cursor.execute(sql2)
    allData = cursor.fetchall()
    for item in allData:
        print(item)
    cursor.close() # 关闭游标
    conn.close() # 关闭连接



