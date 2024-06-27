# -*- coding: utf-8 -*-
"""
Created on Wed May 10 18:18:55 2023

@author: rz3881
"""

import pandas as pd

# config
ym = '202309'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'

# load
data = pd.read_excel(idle_path+'/redundancy_bike.xlsx')

# output index
# date
dates = pd.DataFrame(data['date'].drop_duplicates())
dates['weekday'] = dates['date'].dt.weekday + 1
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
dates['weekday_type'] = dates['weekday'].map(weekday_map)
file_path = idle_path+'/dates_index.csv'
dates.to_csv(file_path, encoding='UTF-8', index=False)
# weekday type
weekday_types = dates.groupby('weekday_type').count().reset_index()
weekday_types = weekday_types.drop(columns=['date', 'weekday'])
file_path = idle_path+'/weekday_types.csv'
weekday_types.to_csv(file_path, encoding='UTF-8', index=False)
# stop
stops = data.groupby('stop_id').agg({'stop_name': 'first'}).reset_index()
file_path = idle_path+'/stops_index.csv'
stops.to_csv(file_path, encoding='UTF-8', index=False)
