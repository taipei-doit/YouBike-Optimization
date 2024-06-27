# -*- coding: utf-8 -*-
"""
Created on Fri May 19 09:16:50 2023

@author: rz3881
"""

import pandas as pd

# config
ym = '202303'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'

# load
data = pd.read_excel(idle_path+'/redundancy_bike.xlsx')

# by stop by weekday, txn狀況
agg = data.groupby(['weekday', 'stop_id']).agg({
    'stop_name': 'first',
    'min_cum_txn': ['mean', 'std'],
    'max_cum_txn': ['mean', 'std'],
    'sum_txn_delta': ['mean', 'std']
    }).reset_index()
agg.columns = ['weekday', 'stop_id', 'stop_name',
               'mean_min_cum_txn', 'std_min_cum_txn',
               'mean_max_cum_txn', 'std_max_cum_txn',
               'mean_net_txn', 'std_net_txn']

# reshape
agg['upper_min_cum_txn'] = agg['mean_min_cum_txn'] + agg['std_min_cum_txn']
agg['lower_min_cum_txn'] = agg['mean_min_cum_txn'] - agg['std_min_cum_txn']
agg['upper_max_cum_txn'] = agg['mean_max_cum_txn'] + agg['std_max_cum_txn']
agg['lower_max_cum_txn'] = agg['mean_max_cum_txn'] - agg['std_max_cum_txn']
agg['upper_net_txn'] = agg['mean_net_txn'] + agg['std_net_txn']
agg['lower_net_txn'] = agg['mean_net_txn'] - agg['std_net_txn']

# save
file_path = idle_path+'/redundancy_bike_agg_by_weekday.csv'
agg.to_csv(file_path, index=False, encoding='UTF-8')
