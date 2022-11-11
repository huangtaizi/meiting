import pymssql
def creat_table():
    sql = pymssql.connect(
        host='127.0.0.1',
        user='sa',
        password='admin',
        database='UserDatabase')
    if sql:
        print('连接sql数据库成功')

    cursor = sql.cursor()
    cursor.execute("""
                    CREATE TABLE import_data (
                    time datetime  PRIMARY KEY,
                    "振动风选_1#振动X" VARCHAR(255),
                    "振动风选_1#振动Y" VARCHAR(255),
                    "振动风选_1#振动Z" VARCHAR(255),
                    "振动风选_2#振动X" VARCHAR(255),
                    "振动风选_2#振动Y" VARCHAR(255), 
                    "振动风选_2#振动Z" VARCHAR(255),
                    "振动风选_3#振动X" VARCHAR(255),
                    "振动风选_3#振动Y" VARCHAR(255),
                    "振动风选_3#振动Z" VARCHAR(255),
                    "振动风选_4#振动X" VARCHAR(255),
                    "振动风选_4#振动Y" VARCHAR(255),
                    "振动风选_4#振动Z" VARCHAR(255),
                    "振动风选_5#振动X" VARCHAR(255),
                    "振动风选_5#振动Y" VARCHAR(255),
                    "振动风选_5#振动Z" VARCHAR(255),
                    ) 
                    """
                    )
    cursor.execute("select * from import_data")
    allData = sql.fetchall()
    for item in allData:
        print(item)
    # data = pd.read_sql(sql, conn)
    sql.close()
if __name__ =='__main__':
    creat_table()
