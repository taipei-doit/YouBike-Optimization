# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 10:07:13 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
target_stop_ids = ['U112076', 'U112019']

# 202303 status
ym = '202303'
status_path = root_path+f'/DM/{ym}/prepared_data\status'
file_path = status_path+'/filled_missing_value_by_minute.csv'
# Load
data1 = pd.read_csv(file_path)
data1 = data1.loc[~data1['is_disabled']]
is_target_stop = data1['stop_id'].isin(target_stop_ids)
data1 = data1.loc[is_target_stop]
data1 = data1[['stop_id', 'data_time', 'available_rent_bikes']]
data1['data_time'] = pd.to_datetime(data1['data_time'])
data1['data_time_m6h'] = data1['data_time'] - pd.Timedelta(hours=6)
data1['data_day_m6h'] = data1['data_time_m6h'].dt.day
data1['time_m6h'] = data1['data_time_m6h'].dt.strftime('%H:%M:%S')
data1['weekday_m6h'] = data1['data_time_m6h'].dt.weekday + 1
data1['ym'] = ym

# 202307 status
ym = '202307'

status_path = root_path+f'/DM/{ym}/prepared_data\status'
file_path = status_path+'/filled_missing_value_by_minute.csv'
# Load
data2 = pd.read_csv(file_path)
data2 = data2.loc[~data2['is_disabled']]
is_target_stop = data2['stop_id'].isin(target_stop_ids)
data2 = data2.loc[is_target_stop]
data2 = data2[['stop_id', 'data_time', 'available_rent_bikes']]
data2['data_time'] = pd.to_datetime(data2['data_time'])
data2['data_time_m6h'] = data2['data_time'] - pd.Timedelta(hours=6)
data2['data_day_m6h'] = data2['data_time_m6h'].dt.day
data2['time_m6h'] = data2['data_time_m6h'].dt.strftime('%H:%M:%S')
data2['weekday_m6h'] = data2['data_time_m6h'].dt.weekday + 1 
data2['ym'] = ym

# merge
data = pd.concat([data1, data2])

data.to_csv(r'D:\iima\ubike分析\DM\202307\市長視察_20230907\target_stop_status.csv',
            index=False, encoding='UTF-8')