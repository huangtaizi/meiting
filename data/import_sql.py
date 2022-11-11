import time
import schedule
import datetime
from influxdb import InfluxDBClient
from sqlalchemy import create_engine
import pymssql
import pandas as pd
import copy


def get_InfluxDBClient():
    influxDBClient = InfluxDBClient(
        host = 'localhost',
        port = 8086,
        username = 'admin',
        password = 'admin',
        database = 'DocData',
        timeout=10000,
    )
    if influxDBClient:
        print('连接influxDB数据库成功')
    return influxDBClient

def get_Pymysql():
    sql = pymssql.connect(
        host='127.0.0.1',
        user='sa',
        password='admin',
        database='UserDatabase')
    if sql:
        print('连接sql数据库成功')
    return sql

def Centra(data,window_size):
    """
    :param data: The data is imputed and does not contain the time column
    :param window_size:the size of sliding window
    """
    for j in range(data.shape[1]):
        # 滑动平均值：通过顺序逐期增减新旧数据求算移动平均值
        # 通过min_periods=0将滑窗的最小值设置为0。当数据不足100时，会只使用实际数据个数来计算其滑动平均值
        data['mean'] = data.iloc[:,j].rolling(window=window_size,min_periods=0).mean()
        data.iloc[:,j] = data.iloc[:,j] - data['mean']
        data.drop(['mean'],axis=1,inplace=True)
    return data



def Integral(data, initial_value, time =None):
    """
    :param data: 多个加速度数值列，加速度值的单位设定为g
    :param time: 时间序列，单位可以选择ms或s
    :return: 位移数据
    """
    import scipy.integrate as it
    columns = data.columns
    integral_data = pd.DataFrame(None,columns = columns)
    for j in range(data.shape[1]):
        integral = it.cumtrapz(data.iloc[:,j],time,initial=initial_value[j]) # 原始加速度数据单位为g，速度单位为km/h,位移单位为m
        integral_data.iloc[:,j] = integral
    return integral_data



def job(begin,initial_ve,initial_lo,lag_time):
    influxDBClient = get_InfluxDBClient()
    shift_begin = begin.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # 示例
    end_time = datetime.datetime(2022, 9, 12, 7, 0, 0)
    end_time_struct = time.mktime(end_time.timetuple())
    end = datetime.datetime.utcfromtimestamp(end_time_struct)
    shift_end = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    integral_window = 100
    """
    # 可以正常使用
    end = datetime.datetime.utcnow()
    if (end-begin).total_seconds() > 1000:
        integral_window = 100
        shift_begin = begin.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        begin1 = end - datetime.timedelta(seconds = lag_time)
        shift_begin = begin1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        integral_window = 10
    """
    result = influxDBClient.query(" select * from DocData where time <'{}' and time >= '{}';".format(shift_end,shift_begin))
    points = result.get_points()
    TIME = []
    QTY = []
    TAG = []
    FVALUE = []
    for item in points:
        Time = item[u'time']
        Qty = item[u'Qty']
        Tag = item[u'Tag']
        Fvalue = item[u'fValue']
        TIME.append(Time)
        QTY.append(Qty)
        TAG.append(Tag)
        FVALUE.append(Fvalue)
    df_time = pd.DataFrame(TIME, columns=['time'])
    df_Qty = pd.DataFrame(QTY, columns=['Qty'])
    df_Tag = pd.DataFrame(TAG, columns=['Tag'])
    df_fValue = pd.DataFrame(FVALUE, columns=['fValue'])
    df_result = pd.concat([df_time, df_Qty, df_Tag, df_fValue], axis=1, ignore_index=False)
    df_result.drop(df_result[df_result.Qty == -1].index, inplace=True)
    array_result = pd.pivot_table(df_result, index='time', columns='Tag',values='fValue')
    col_name = ['HMI_214_DL','振动风选_1#振动X', '振动风选_1#振动Y', '振动风选_1#振动Z', '振动风选_2#振动X', '振动风选_2#振动Y',
                '振动风选_2#振动Z', '振动风选_3#振动X', '振动风选_3#振动Y', '振动风选_3#振动Z', '振动风选_4#振动X',
                '振动风选_4#振动Y', '振动风选_4#振动Z', '振动风选_5#振动X', '振动风选_5#振动Y', '振动风选_5#振动Z']
    index2 = array_result.index
    analy_data = array_result.loc[:, col_name]
    analy_data = analy_data[analy_data['HMI_214_DL']>2]
    analy_data.dropna(axis=0, how='all', inplace=True)
    analy_data_copy = pd.DataFrame(analy_data.values,columns=col_name)
    analy_data_index = analy_data.index
    print(analy_data_index)
    if analy_data_copy.isnull().any().sum() !=0:
        #analy_data_copy = analy_data_copy.fillna(method='pad')
        analy_data_copy = analy_data_copy.interpolate(method = 'linear')
    analy_data_copy.to_csv('D:/analysis/data/analy_data2.csv', encoding='utf_8_sig', index=True, sep=',')
    integral_data = analy_data_copy.iloc[:,1:]
    accele_cen = Centra(integral_data, window_size = integral_window)
    velocity_data = Integral(accele_cen, initial_value = initial_ve)
    velocity_cen = Centra(velocity_data, window_size=integral_window)
    location_data = Integral(velocity_cen, initial_value = initial_lo)
    ve_initial = velocity_data.iloc[-1, :]
    lo_initial = location_data.iloc[-1, :]
    conn = get_Pymysql()
    cursor = conn.cursor()
    cursor.execute("select * from dbo.data_1")
    # if_exists:当存在表格时选择数据以怎样的方式写入这张表格
    # fail: 当存在表格自动弹出错误ValueError
    # replace：将原表里面的数据给替换掉
    # append：将数据插入到原表的后面
    return ve_initial,lo_initial,end



def main():
    col = 15
    lag_time = 3
    beijing_time = datetime.datetime(2022,9,12,6,40,0)
    time_struct = time.mktime(beijing_time.timetuple())
    initial_time = datetime.datetime.utcfromtimestamp(time_struct)
    ve = [0 for _ in range(col)]
    lo = [0 for _ in range(col)]
    ve_next,lo_next, previous_time = job(begin = initial_time,initial_ve = ve, initial_lo = lo,lag_time = lag_time)
    initial_time = previous_time
    time.sleep(lag_time * 2)
    while True:
        ve_next, lo_next, previous_time = job(begin = initial_time, initial_ve=ve, initial_lo=lo,lag_time = lag_time)
        ve = ve_next
        lo = lo_next
        initial_time = previous_time
        time.sleep(lag_time)


if __name__=='__main__':
    main()
