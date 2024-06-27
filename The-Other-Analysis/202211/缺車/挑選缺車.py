# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 07:36:52 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'

# load
status = pd.read_csv(root_path+'/DM/站點分群_共用/[status]ubike_202211_stop_by_hour_by_weekday.csv')
stop = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')

# define
stop['stop_id'] = 'U' + stop['stop_id'].astype(str)

# filter
is_ntp_dist = stop['dist'].isin(['臺大專區', '臺大公館校區'])
is_ntp_name = stop['stop_name'].str.startswith('臺大') | stop['stop_name'].str.startswith('臺灣科技大學')
ntp_stop_id = set(stop.loc[is_ntp_dist | is_ntp_name, 'stop_id'])
status_filter = status.loc[~status['stop_id'].isin(ntp_stop_id)]
is_disabled = (status_filter['disabled_count_by1m'] > 0)
status_filter = status_filter.loc[~is_disabled]

# group by hour
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
status_filter['weekday_type'] = status_filter['weekday'].map(weekday_map)
status_by_hour = status_filter.groupby(['stop_id', 'weekday_type', 'hour']).agg({
    'raw_data_count': 'sum',
    'median_available_rent': 'mean',
    'median_available_return': 'mean',
    'available_rent_prob': 'mean'
    }).reset_index()
status_by_hour.columns = ['stop_id', 'weekday_type', 'hour', 'raw_data_count',
                          'mean_available_rent', 'mean_available_return',
                          'available_rent_prob']

# add info
status_by_hour = status_by_hour.merge(stop[['stop_id', 'stop_name', 'capacity']],
                                    how='left', on='stop_id')

# mark 交通站點
is_mrt = status_by_hour['stop_name'].str.contains('捷運')
is_station = status_by_hour['stop_name'].str.contains('車站')
status_by_hour['stop_type'] = '-'
status_by_hour.loc[is_mrt | is_station, 'stop_type'] = '捷運或車站'

# reshape
status_by_hour = status_by_hour[['stop_id', 'stop_name', 'capacity', 'stop_type',
                               'weekday_type', 'hour', 'raw_data_count',
                               'available_rent_prob',
                               'mean_available_rent', 'mean_available_return',
                               ]]

# save
file_path = '/DM/缺車/[status]available_rent_prob_by_hour.csv'
status_by_hour.to_csv(root_path+file_path, index=False, encoding='utf-8')
