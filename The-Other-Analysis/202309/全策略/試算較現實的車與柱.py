# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 10:22:18 2023

@author: rz3881
"""

import pandas as pd
import time
from numpy import nan
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import load_ubike_stop


ym = '202307'
root_path = r'D:\iima\ubike分析'
dim_path = root_path+'/DIM'
txn_path = root_path+f'/DM/{ym}/prepared_data/txn'
dispatch_path = root_path+f'/DM/{ym}/prepared_data/dispatch'
status_path = root_path+f'/DM/{ym}/prepared_data/status'
init_hour = 6
weekday_type_map = {
    1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday',
    5: 'weekday', 6: 'weekend', 7: 'weekend'
}

# stop
stop = load_ubike_stop(ym, dim_path)

# txn
txn = pd.read_csv(txn_path+'/txn_only_ubike.csv')
txn['on_time'] = pd.to_datetime(txn['on_time']).dt.tz_localize(None)
txn['off_time'] = pd.to_datetime(txn['off_time']).dt.tz_localize(None)
# wide to long df
filter_on_col_name = ['card_id', 'route_name', 'on_stop_id', 'on_stop', 'on_time']
filter_off_col_name = ['card_id', 'route_name', 'off_stop_id', 'off_stop', 'off_time']
result_col_name = ['card_id', 'bike_id', 'stop_id', 'stop_name', 'txn_time']
on_txn = txn[filter_on_col_name]
on_txn.columns = result_col_name
on_txn['type'] = 'on'
off_txn = txn[filter_off_col_name]
off_txn.columns = result_col_name
off_txn['type'] = 'off'
txn_long = pd.concat([on_txn, off_txn])
# time cols
txn_long['txn_time_m6h'] = txn_long['txn_time'] - pd.Timedelta(hours=init_hour)
txn_long['date_m6h'] = txn_long['txn_time_m6h'].dt.date.astype(str)
txn_long['weekday_m6h'] = txn_long['txn_time_m6h'].dt.weekday + 1
txn_long['weekday_type_m6h'] = txn_long['weekday_m6h'].map(weekday_type_map)


# dispatch
dispatch = pd.read_csv(dispatch_path+'/cleaned_raw.csv')
dispatch['txn_time'] = pd.to_datetime(dispatch['txn_time']).dt.tz_localize(None)
dispatch['txn_time_m6h'] = dispatch['txn_time'] - pd.Timedelta(hours=init_hour)
dispatch['date_m6h'] = dispatch['txn_time_m6h'].dt.date.astype(str)
dispatch['weekday_m6h'] = dispatch['txn_time_m6h'].dt.weekday + 1
dispatch['weekday_type_m6h'] = dispatch['weekday_m6h'].map(weekday_type_map)


# 月調度
dispatch['dispatch_type'].value_counts()
# 日均調度
dis_daily_agg = dispatch.groupby(['weekday_type_m6h', 'date_m6h']).agg({
    'bike_id': ['count', 'nunique'],
    'stop_id': 'nunique'
}).reset_index()
dis_daily_agg.columns = [
    'weekday_type_m6h', 'date_m6h',
    'dispatch_count', 'unique_bikes', 'unique_stop_id'
]


# 月交易
txn_long['type'].value_counts()
# 日均交易
txn_daily_agg = txn_long.groupby(['weekday_type_m6h', 'date_m6h']).agg({
    'bike_id': ['count', 'nunique'],
    'card_id': 'nunique',
    'stop_id': 'nunique'
}).reset_index()
txn_daily_agg.columns = [
    'weekday_type_m6h', 'date_m6h',
    'txn_count', 'unique_bikes', 'unique_cards', 'unique_stop_id'
]