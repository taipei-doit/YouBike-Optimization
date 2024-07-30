# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 09:32:15 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'

# load 
ubike_status_by_10m = pd.read_csv(root_path+'/DM/[status]ubike_202211_stop_by_10m.csv')
ubike_status_by_10m['standard_data_time'] = pd.to_datetime(ubike_status_by_10m['standard_data_time'])

# by stop 統計資訊
status_bystop = ubike_status_by_10m.groupby('stop_id').agg({
    'standard_data_time': ['min', 'max', 'count'],
    'raw_data_count': 'sum',
    'raw_data_disabled_count': 'sum',
    'is_disabled': 'sum',
    'is_empty': 'sum',
    'is_full': 'sum'
    }).reset_index()
status_bystop.columns = ['stop_id', 'earliest_data', 'latest_data',
                        'data_count_by10m',
                        'raw_data_count', 'raw_data_disabled_count',
                        'disabled_count_by10m',
                        'empty_count_by10m', 'full_count_by10m']
status_bystop = status_bystop[['stop_id', 'earliest_data', 'latest_data',
                               'raw_data_count', 'raw_data_disabled_count',
                               'data_count_by10m', 'disabled_count_by10m', 
                               'empty_count_by10m', 'full_count_by10m']]
status_bystop.to_csv(root_path+'/DM/[status]ubike_202211_by_stop_summary.csv',
                     index=False, encoding='UTF-8')