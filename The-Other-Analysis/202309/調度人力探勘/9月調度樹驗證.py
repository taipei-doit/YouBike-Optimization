# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 16:44:45 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
ym = '202309'
dispatch_path = root_path+f'/DM/{ym}/prepared_data/dispatch'

dispatch = pd.read_csv(dispatch_path+'/cleaned_raw.csv')
dispatch['txn_time'] = pd.to_datetime(dispatch['txn_time']).dt.tz_localize(None)
dispatch['date'] = dispatch['txn_time'].dt.date

date_agg = dispatch.groupby('date').agg({
    'bike_id': ['count', 'nunique'],
    'stop_id': 'nunique'
}).reset_index()
date_agg.columns = ['date', 'dis_bike_count', 'dis_unique_bike', 'unique_stop']

date_agg['dis_bike_count'].mean()
date_agg['dis_unique_bike'].mean()
date_agg['unique_stop'].mean()
