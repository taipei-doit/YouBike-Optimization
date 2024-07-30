# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 09:17:14 2023

@author: rz3881
"""

import pandas as pd

_root_path = r'D:\iima\ubike分析'

data = pd.read_csv(_root_path+'/DATA/ubike_20230216.csv')
data = data.dropna()
# create loc mapping table
stop_loc_dict = {}
stop_loc = data[['station_uid', 'name', 'bike_capacity', 'lng', 'lat']].drop_duplicates()
stop_loc.to_csv(_root_path+'/DM/stop_loc.csv', index=False)

# define column type
data['data_time'] = pd.to_datetime(data['data_time'])
data['_ctime'] = pd.to_datetime(data['_ctime'])

# standardlize datetime
data['standard_data_time'] = data['data_time'].dt.floor('Min').copy()
round_minute = (round(data['standard_data_time'].dt.minute/10)*10).astype(int)
round_minute.loc[round_minute==60] = 0
for m in [0, 10, 20, 30, 40, 50]:
    data.loc[round_minute==m, 'standard_data_time'] = data.loc[round_minute==m, 'standard_data_time'].apply(lambda t: t.replace(minute=m))

# to pivot matrix
unique_data = data.groupby(['standard_data_time', 'station_uid']).agg({
    'bike_capacity': 'max',
    'available_rent_general_bikes': 'min',
    'available_return_bikes': 'min',
    'lng': 'min',
    'lat': 'min'
    }).reset_index()
# 細分時分站表
# 可借數
rent_matrix = unique_data.pivot_table(index='standard_data_time', columns='station_uid', values='available_rent_general_bikes').reset_index()
rent_matrix = rent_matrix.fillna(axis='index', method='ffill')
rent_matrix.to_excel(_root_path+'/DM/available_rent_by10mins_bystop.xlsx', index=False)
# 繪製缺車分群直方圖
rent_df = rent_matrix.melt(id_vars='standard_data_time', var_name='station_uid', value_name='available_rent')
rent_df['is_available_rent'] = (rent_df['available_rent']!=0)
rent_df['period'] = '21-6點'
rent_df.loc[(rent_df['standard_data_time'].dt.hour>=7)&((rent_df['standard_data_time'].dt.hour<=10)), 'period'] = '7~10點'
rent_df.loc[(rent_df['standard_data_time'].dt.hour>=11)&((rent_df['standard_data_time'].dt.hour<=16)), 'period'] = '11~16點'
rent_df.loc[(rent_df['standard_data_time'].dt.hour>=17)&((rent_df['standard_data_time'].dt.hour<=20)), 'period'] = '17~20點'
rent_df = rent_df.merge(stop_loc, on='station_uid', how='left')
# 可還數
return_matrix = unique_data.pivot_table(index='standard_data_time', columns='station_uid', values='available_return_bikes').reset_index()
return_matrix = return_matrix.fillna(axis='index', method='ffill')
return_matrix.to_excel(_root_path+'/DM/available_return_by10mins_bystop.xlsx', index=False)
# 繪製缺車分群直方圖
return_df = return_matrix.melt(id_vars='standard_data_time', var_name='station_uid', value_name='available_return')
return_df['is_available_return'] = (return_df['available_return']!=0)
# merge 可借可還
bike_capacity = rent_df.merge(return_df, on=['standard_data_time', 'station_uid'], how='inner')
bike_capacity = bike_capacity[['standard_data_time', 'period',
                               'station_uid', 'name', 'bike_capacity', 'lng', 'lat',
                               'available_return', 'is_available_return',
                               'available_rent', 'is_available_rent']]
# 計算吞吐量
bike_capacity = bike_capacity.sort_values(['station_uid', 'standard_data_time']).reset_index(drop=True)
bike_capacity['delta'] = bike_capacity['available_rent'] - bike_capacity['available_rent'].shift()
bike_capacity['status'] = 0
bike_capacity.loc[~bike_capacity['is_available_return'], 'status'] = 1
bike_capacity.loc[~bike_capacity['is_available_rent'], 'status'] = -1
bike_capacity.loc[bike_capacity['status']!=0].to_csv(_root_path+'/DM/bike_capacity_by10mins_bystop.csv', index=False)

