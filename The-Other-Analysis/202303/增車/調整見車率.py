# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 07:36:52 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
status_path = root_path+r'\DM\202303\prepared_data\status'
addbike_path = root_path+r'\DM\202303\增車'

# load
status = pd.read_csv(status_path+'/aggregate_by_weekdaytype_by_hour.csv')
stop = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')

# filter
is_ntp_dist = stop['dist'].isin(['臺大專區', '臺大公館校區'])
is_ntp_name = stop['stop_name'].str.startswith('臺大') | stop['stop_name'].str.startswith('臺灣科技大學')
ntp_stop_id = set(stop.loc[is_ntp_dist | is_ntp_name, 'stop_id'])
status_filter = status.loc[~status['stop_id'].isin(ntp_stop_id)]
is_disabled = (status_filter['mean_disabled_minute'] > 0)
status_filter = status_filter.loc[~is_disabled]

# add info
status_by_hour = status_filter.merge(stop[['stop_id', 'stop_name', 'capacity']],
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
file_path = addbike_path+'/[status]available_rent_prob_by_hour.csv'
status_by_hour.to_csv(file_path, index=False, encoding='utf-8')


