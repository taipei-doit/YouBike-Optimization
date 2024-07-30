# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 11:48:56 2023

@author: rz3881
"""

import pandas as pd
import os

CUR_PATH = r'D:\iima\ubike分析\DM\202307\全策略'

summary_info = pd.read_csv(CUR_PATH+'/Recommend.csv')
hourly_net_txn = []
files = os.listdir(CUR_PATH+'/Established_Net')
for file in files:
    temp = pd.read_csv(f'{CUR_PATH}/Established_Net/{file}')
    temp.columns = ['stop_id', 'net_txn']
    weekday_type, hour = file.split('.csv')[0].split('_')
    temp['weekday_type'] = 'week' + weekday_type
    if hour == '0':  # 0會被去掉，當成23:59
        temp['m6h_hour'] = '23:59:59'
    elif hour in ['1', '2', '3', '4', '5']:  # 0 ~ 05:59不看
        continue
    else:
        temp['m6h_hour'] = f'{hour.zfill(2)}:00:00'
    hourly_net_txn.append(temp)
hourly_net_txn = pd.concat(hourly_net_txn)

hourly_cum_net_txn = []
for _, gdata in hourly_net_txn.groupby(['stop_id', 'weekday_type']):
    gdata = gdata.sort_values('m6h_hour')
    gdata['cum_net_txn'] = gdata['net_txn'].cumsum()
    hourly_cum_net_txn.append(gdata)
hourly_cum_net_txn = pd.concat(hourly_cum_net_txn)

hourly_cum_net_txn.to_csv(
    CUR_PATH+'/predict_hourly_cum_net_txn.csv',
    index=False, encoding='UTF-8'
)