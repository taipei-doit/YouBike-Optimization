# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 09:57:33 2023

@author: rz3881
"""

import pandas as pd

_root_path = r'D:\iima\ubike分析'

data = pd.read_csv(_root_path+'/DATA/ubike_20230216.csv')
data = data.dropna()
# create loc mapping table
stop_loc_dict = {}
stop_loc = data[['station_uid', 'name', 'lng', 'lat']].drop_duplicates()

# define column type
data['data_time'] = pd.to_datetime(data['data_time'])
data['_ctime'] = pd.to_datetime(data['_ctime'])

# standardlize datetime
data['standard_data_time'] = data['data_time'].dt.floor('Min').copy()
round_minute = (round(data['standard_data_time'].dt.minute/10)*10).astype(int)
round_minute.loc[round_minute==60] = 0
for m in [0, 10, 20, 30, 40, 50]:
    data.loc[round_minute==m, 'standard_data_time'] = data.loc[round_minute==m, 'standard_data_time'].apply(lambda t: t.replace(minute=m))

# 計算被借走的車數
# 已知早上6點會回歸default，並在旁邊有綁車可隨時提供借車
# => 無法光靠車柱、車的量還原有多少人借車
# 以捷運公館站(2號出口)，總車位數99個，6點保持45台可借，但旁邊綁車200台
# 存在極端情況如: 一個早上被借走了100台，但每10分鐘的數據卻始終保持+45
