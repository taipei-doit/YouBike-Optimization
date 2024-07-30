# -*- coding: utf-8 -*-
"""
Created on Wed May 10 18:18:55 2023

@author: rz3881
"""

import pandas as pd
import time

# config
ym = '202303'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'

# load
data = pd.read_excel(idle_path+'/redundancy_bike.xlsx')

# output index
dates = data['date'].drop_duplicates()
file_path = idle_path+'/dates_index.csv'
dates.to_csv(file_path, encoding='UTF-8')

stops = data.groupby('stop_id').agg({'stop_name': 'first'}).reset_index()
file_path = idle_path+'/stops_index.csv'
stops.to_csv(file_path, encoding='UTF-8')
