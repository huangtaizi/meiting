#!/usr/bin/python
# coding=utf-8
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
        timeout=3,
    )
    return influxDBClient

def get_DataFrameClient():
    dataframeClient=DataFrameClient(
        host='localhost',
        port=8086,
        username='admin',
        password='admin',
        database='DocData',
        timeout=3,
    )
    return dataframeClient


# 连接数据库
# 数据库接口文档：https://docs.influxdata.com/influxdb/v1.8/#
def get_list_database():
    influxDBClient = get_InfluxDBClient()
    # result = influxDBClient.query('select * from DocData limit 50;')
    result = influxDBClient.get_list_database()
    print("Result: {0}".format(result))



    #=====================================
    dataframeclient=get_DataFrameClient()
    result_dataframe=dataframeclient.get_list_database()
    print('Dataframe:{0}'.format(dataframeclient))


def query_database():
    influxDBClient = get_InfluxDBClient()
    result = influxDBClient.query('select * from DocData limit 50;')
    print("{0}".format(result))

    dataframeclient = get_DataFrameClient()
    result_df = dataframeclient.query('select * from DocData limit 50;')
    print('result_df：', result_df)
    # result_df_1=np.array(result_df.values())
    result_df_1=result_df['DocData']
    result_df_1 = list(result_df.values())
    print(result_df_1)
    print(type(result_df_1))
    result_df_1=np.reshape(result_df_1,(50,3))
    result = pd.DataFrame(result_df_1)
    result.to_csv('D:/analysis/data/result.csv', encoding='utf-8',index=False, sep=',')

    """
        col_name=['time','Qty','Tag','fValue']
    df=pd.DataFrame()
    for col in col_name:
        df_aux=pd.DataFrame(result[col],columns=[col,'time'])
        df_aux.set_index('time',inplace=True)
        df[col]=df_aux[col]
    df.index=pd.to_datetime(df.index,errors='coerce')
    print(df.head())
    
    dataframeclient = get_DataFrameClient()
    result_df=dataframeclient.query('select * from DocData limit 50;')
    result_df=result_df.items()
    print(pd.DataFrame.from_dict(result_df).head())
    # result_df.to_csv('D:/analysis/data/result_df.csv',index=False,sep=',')
    """










