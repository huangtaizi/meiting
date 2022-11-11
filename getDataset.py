#!/usr/bin/python
# coding=utf-8
import datetime

from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import schedule
import time
import pandas as pd
import numpy as np

def get_InfluxDBClient():
    influxDBClient = InfluxDBClient(
        host='localhost',
        port=8086,
        username='admin',
        password='admin',
        database='DocData',
        timeout=100000,
    )
    return influxDBClient

def get_DataFrameClient():
    dataframeClient=DataFrameClient(
        host='localhost',
        port=8086,
        username='admin',
        password='admin',
        database='DocData',
        timeout=5,
    )
    return dataframeClient


# 连接数据库
def get_list_database():
    dataframeclient = get_DataFrameClient()
    result = dataframeclient.get_list_database()
    print('Dataframe:{0}'.format(result))
    print('连接成功')

def utc2local(timestr):
    import dateutil.parser
    import pytz
    from datetime import datetime
    local_time = dateutil.parser.parse(timestr).astimezone(pytz.timezone('Asia/Shanghai'))
    da = datetime.strftime(local_time,'%Y-%m-%d %H:%M:%S.%f')
    return da

def local2utc(local_st):
    time_struct = time.mktime(local_st.timetuple())
    utc_st = datetime.datetime.utcfromtimestamp(time_struct)
    utc_st = utc_st.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return utc_st

def query_dataset():
    import pytz
    influxDBClient = get_InfluxDBClient()
    utc_begin = local2utc(datetime.datetime(2022,9,12,0,0,0))
    print(utc_begin)
    utc_end = local2utc(datetime.datetime(2022,9,13,0,0,0))
    print(utc_end)
    #result = influxDBClient.query('select * from DocData limit 3000;')
    result = influxDBClient.query(" select * from DocData where time <'2022-09-29T13:00:00.000000Z' and time >= '2022-09-29T08:00:00.000000Z';")
    print(result)
    points = result.get_points()
    TIME = []
    QTY = []
    TAG = []
    FVALUE = []
    for item in points:
        Time = item[u'time']
        Time = utc2local(Time)
        Qty = item [u'Qty']
        Tag = item [u'Tag']
        Fvalue = item[u'fValue']
        TIME.append(Time)
        QTY.append(Qty)
        TAG.append(Tag)
        FVALUE.append(Fvalue)
    df_time = pd.DataFrame(TIME,columns= ['time'])
    df_Qty = pd.DataFrame(QTY, columns=['Qty'])
    df_Tag = pd.DataFrame(TAG, columns=['Tag'])
    df_fValue = pd.DataFrame(FVALUE, columns=['fValue'])
    df_result = pd.concat([df_time,df_Qty,df_Tag,df_fValue],axis=1, ignore_index= False)
    df_result.drop(df_result[df_result.Qty==-1].index,inplace=True)
    array_result = pd.pivot_table(df_result,index='time',columns='Tag',values='fValue')
    array_result.to_csv('D:/analysis/data/array_result.csv', encoding='utf_8_sig', index=True, sep=',')
    #print("array_result:", array_result)
    #print('输入变量名称进行筛选：',array_result.loc[:,'HMI_101_1_I'])
    #print('输入变量序号进行筛选：',array_result.iloc[:,71:74]) # 0:50
    #A = array_result.iloc[:, 71:74]
    #A.to_csv('D:/analysis/data/A.csv', encoding='utf_8_sig', index=True, sep=',')

    #print("运行完毕")
    #B = ['HMI_214_DL','振动风选_3#振动X','振动风选_3#振动Y','振动风选_3#振动Z','振动风选_4#振动X','振动风选_4#振动Y','振动风选_4#振动Z']
    #B = ['HMI_214_DL', '振动风选_3#振动X', '振动风选_3#振动Y', '振动风选_3#振动Z', '振动风选_4#振动X', '振动风选_4#振动Y', '振动风选_4#振动Z', '振动风选_5#振动X', '振动风选_5#振动Y', '振动风选_5#振动Z']
    B = ['HMI_316_DL']
    C = array_result.loc[:,B]  #loc名称筛选

    #C = array_result.iloc[:, 10:74] #iloc数值筛选
    C.to_csv('C:/Users/Lenovo/Desktop/316电流.csv', encoding='utf_8_sig', index=True, sep=',')
    print(C)
    print("运行完毕")



