# -*- coding: utf-8 -*-
"""
Created on Sat Apr  1 18:32:12 2023

@author: rz3881
"""

import pandas as pd
root_path = r'D:\iima\ubike分析'

# load
file_path = '/DM/cleaned_stop_api/ubike_202211_stop_status.csv'
ubike_status = pd.read_csv(root_path+file_path)

# 檢查禁用情況，已知萬華國中有停止運行過
ubike_status['service_status'].value_counts()
# 共405244禁用記錄
# 萬華國中共有3站，2個2.0，1個1.0
ubike_status.loc[ubike_status['stop_id']==500113065, 'service_status'].value_counts()
ubike_status.loc[ubike_status['stop_id']==500113021, 'service_status'].value_counts()
temp = ubike_status.loc[ubike_status['stop_id']==500113021]