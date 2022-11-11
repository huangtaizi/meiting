import pandas as pd
# import pymysql
import pymssql

if __name__ == '__main__':
    print('————————————————————start')
    # conn = pymysql.connect(
    # host='localhost',
    # user='sa',
    # password='admin',
    # db='UserDatabase',
    # charset='utf8'
    # )
    # conn = pymssql.connect(host='127.0.0.1',user='sa',password='admin',database='UserDatabase')
    conn = pymssql.connect(host='127.0.0.1', user='sa', password='admin', database='UserDatabase')
    cur = conn.cursor()
    print('————————————————————end')
    if conn:
        print('连接成功')
    # """
    # cursor = conn.cursor()
    # cursor.execute("select * from dbo.data_1")
    # for row in cursor.fetchall():
    #     print(row)
    # """
    # df = pd.read_sql(
    #     """
    #     select count(1) from dbo.data_1
    #     """, con=conn
    # )

    # ————————————————————————————————————————————————————————————————
    # 插入语句   pymssql官方例子：https://www.pymssql.org/pymssql_examples.html
    # cursor.executemany(
    #     "INSERT INTO persons VALUES (%d, %s, %s)",
    #     [(1, 'John Smith', 'John Doe'),
    #      (2, 'Jane Doe', 'Joe Dog'),
    #      (3, 'Mike T.', 'Sarah H.')])
    # # you must call commit() to persist your data if you don't set autocommit to True
    # conn.commit()

    cur.execute("select * from data_2")
    allData = cur.fetchall()
    for item in allData:
        print(item)
    # data = pd.read_sql(sql, conn)
    conn.close()
    # print(data)


