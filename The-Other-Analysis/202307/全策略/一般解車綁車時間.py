# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 16:53:13 2023

@author: rz3881
"""


import pandas as pd
from numpy import int64


def calcu_mode_hour(x):
    if type(x) is int64:
        if x == 6:
            return ''
        else:
            return f'{str(int(x-1))}~{str(int(x))}點'
    else:
        is_two_element = (len(x)==2)
        is_diff_one = ((x[1]-x[0])==1)
        if is_two_element & is_diff_one:
            if x[0] == 6:
                return ''
            else:
                return f'{str(int(x[0]-1))}~{str(int(x[0]))}點'
        else:
            return ''
        
        
ym = '202307'
root_path = r'D:\iima\ubike分析'
dim_path = root_path+'/DIM'
idle_path = root_path+f'/DM/{ym}/閒置車'
strategy_path = root_path+f'/DM/{ym}/全策略'
n_th = 2
init_hour = 6

# Load
compare_detail = pd.read_csv(idle_path + '/compare_detail.csv')

# agg by stop_id, weekday_type
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday',
               5: 'weekday', 6: 'weekend', 7: 'weekend'}
compare_detail['weekday_type'] = compare_detail['weekday_m6h'].map(weekday_map)

# Add columns
compare_detail['min_cum_txn_hour'] = pd.to_datetime(compare_detail['minct_time']).dt.hour
compare_detail['max_cum_txn_hour'] = pd.to_datetime(compare_detail['maxct_time']).dt.hour

# 一般性的借車、還車高峰
mode_cum_txn = compare_detail.groupby(['stop_id', 'weekday_type']).agg({
    'min_cum_txn_hour': pd.Series.mode,
    'max_cum_txn_hour': pd.Series.mode,
    }).reset_index()
mode_cum_txn['min_cum_txn_hour'] = mode_cum_txn['min_cum_txn_hour'].apply(calcu_mode_hour)
mode_cum_txn['max_cum_txn_hour'] = mode_cum_txn['max_cum_txn_hour'].apply(calcu_mode_hour)

# 
rich_setup = nlarge_bike.merge(mode_cum_txn, how='inner',
                               on=['stop_id','weekday_type'])