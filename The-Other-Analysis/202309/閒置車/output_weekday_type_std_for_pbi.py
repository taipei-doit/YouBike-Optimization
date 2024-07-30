# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 15:14:32 2023

@author: rz3881
"""

# 為了在powerbi 畫出周間/周末 一個站的交易是否穩定

import pandas as pd
import time
import datetime

# Load
detail = pd.read_csv(idle_path+'/simulation_results_detail_part.csv')
detail['adjust_api_time'] = pd.to_datetime(detail['adjust_api_time'])
detail['api_time_m6h'] = detail['adjust_api_time'] - datetime.timedelta(hours=6)
detail['time'] = detail['api_time_m6h'].dt.strftime("%H:%M:%S")
detail['weekday_m6h'] = detail['adjust_api_time'].dt.weekday + 1
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
detail['weekday_type'] = detail['weekday_m6h'].map(weekday_map)

# groupby
stime = time.time()
weekday_type_data = []
for stop_id in set(detail['stop_id']):
    temp_target_data = detail.loc[detail['stop_id']==stop_id]
    temp_simu_detail = temp_target_data.groupby(['weekday_type', 'time']).agg({
        'min_cum_txn': ['count', 'median', 'std']
        }).reset_index()    
    temp_simu_detail.columns = ['weekday_type', 'time', 'count', 'median_cumtxn', 'std_cumtxn']
    temp_simu_detail['stop_id'] = stop_id
    temp_simu_detail['upper_cumtxn'] = temp_simu_detail['median_cumtxn'] + temp_simu_detail['std_cumtxn']
    temp_simu_detail['lower_cumtxn'] = temp_simu_detail['median_cumtxn'] - temp_simu_detail['std_cumtxn']
    weekday_type_data.append(temp_simu_detail)
    # break
print(f'weekday_type_data cost {time.time()-stime} secs.')
# cost 70 mins
weekday_type_data = pd.concat(weekday_type_data)

# Save
weekday_type_data.to_csv(idle_path + '/weekday_type_cum_txn_agg.csv', index=False)
