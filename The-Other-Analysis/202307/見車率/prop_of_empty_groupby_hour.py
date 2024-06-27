# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 17:05:05 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
ym = '202303'
dim_path = root_path+'/DIM'
status_path = root_path+f'/DM/{ym}/prepared_data\status'
available_prob_path = root_path+'/DM/202307/見車率'

# Load 
# api status
hourly_status = pd.read_csv(status_path+'/aggregate_by_weekdaytype_by_hour.csv')
# stop
stops = pd.read_csv(root_path+f'/DIM/ubike_stops_from_api_{ym}.csv')
# 實驗50站
test50 = pd.read_csv(dim_path+'/投車50站.csv')
test50 = test50.rename(columns={'ID': 'stop_id', '主/衛星站': 'level'})
test50['is_test50'] = True

# Filter
# keep weekday
is_weekday = (hourly_status['weekday_type']=='weekday')
# keep 06:00~23:59
is_normal_hour = (hourly_status['hour']>=6)
# keep valid data
is_valid = ~hourly_status['available_rent_prob'].isna()
hourly_status_filtered = hourly_status.loc[is_weekday&is_normal_hour&is_valid]

# Pivot hourly table
status_hour_pivot = hourly_status_filtered.pivot_table(
    index='stop_id', columns='hour', values='available_rent_prob'
    ).reset_index()
hour_cols = ['stop_id'] + ['available_rent_prob_'+str(h) for h in range(6, 24)]
status_hour_pivot.columns = hour_cols

# Groupby stop_id
status_stop_agg = hourly_status_filtered.groupby(['stop_id']).agg({
    'available_rent_prob': 'mean',
    'raw_data_count': 'sum'
    }).reset_index()
status_stop_agg.columns = [
    'stop_id', 'available_rent_prob_mean', 'raw_data_count']

# Reshape
available_prob = stops.merge(status_stop_agg, how='outer', on='stop_id')
available_prob = available_prob.merge(status_hour_pivot, how='outer', on='stop_id')
# add test stop label
available_prob = available_prob.merge(
    test50[['stop_id', 'level', 'is_test50']],
    how='outer', on='stop_id'
    )
available_prob['is_test50'] = available_prob['is_test50'].fillna(False)

# Save
file_path = available_prob_path+f'/available_prob_{ym}.csv'
available_prob.to_csv(file_path, index=False)
