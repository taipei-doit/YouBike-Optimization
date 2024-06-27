# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 17:59:36 2023

@author: rz3881
"""

import pickle
import os
import pandas as pd

root_path = r'D:\iima\ubike分析'
ym = '202309'
missing_data_path = r'D:\iima\ubike分析\DM\202309\prepared_data\txn\missing_data\202309'

files = os.listdir(missing_data_path)
res = []
for file in files:
    # break
    temp = pd.read_pickle(os.path.join(missing_data_path, file))
    res.append(temp)
    

# from [status]unique_raw_to_stop_info.py
file_path = root_path+f'/DIM/ubike_stops_from_api_{ym}.csv'
ubike_stops = pd.read_csv(file_path, encoding='utf8')

# find missing data stop
missing_data = pd.concat(res)
missing_on = missing_data.loc[missing_data['on_lng'].isna(), 'on_stop_id']
missing_off = missing_data.loc[missing_data['off_lng'].isna(), 'off_stop_id']
missing_stop_id = set(missing_on) | set(missing_off)

# match if missing data stop can be found in stop list extract from status
ubike_stops.loc[ubike_stops['stop_id'].isin(missing_stop_id)]
