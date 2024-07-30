# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 14:43:37 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
status_path = root_path+'/DM/202303/prepared_data/status'

# load
status_by_date_by_hour = pd.read_csv(status_path+'/aggregate_by_date_by_hour.csv')
stop = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')

# filter
is_weekday = status_by_date_by_hour['weekday']<=5
is_target_hour = status_by_date_by_hour['hour']>=6
status_filtered = status_by_date_by_hour.loc[is_weekday&is_target_hour]

# merge
data = status_filtered.merge(stop[['stop_id', 'stop_name']],
                             how='left', on='stop_id')


def count_txn_count_by_given_stop_id(stop_id, hour, output_level='weekday_type'):
    is_target_stop = data['stop_id']==stop_id
    is_target_hour = data['hour']==hour
    date_status = data.loc[is_target_stop&is_target_hour]
    
    
    if output_level=='date':
        return date_status
    elif output_level=='weekday':
        weekday_status = date_status.groupby(['stop_id', 'weekday', 'hour']).agg({
            'empty_minutes': 'median'
            }).reset_index()
        weekday_status['available_rent_prob'] = 1 - (weekday_status['empty_minutes']/60)
        return weekday_status
    elif output_level=='weekday_type':
        weekday_status = date_status.groupby(['stop_id', 'weekday', 'hour']).agg({
            'empty_minutes': 'median'
            }).reset_index()
        weekdaytype_status = weekday_status.groupby(['stop_id', 'hour']).agg({
            'empty_minutes': 'mean'
            }).reset_index()
        weekdaytype_status['available_rent_prob'] = 1 - (weekdaytype_status['empty_minutes']/60)
        return weekdaytype_status
    else:
        raise ValueError('output_level can only be one of [date, weekday, weekday_type]')

# check
count_txn_count_by_given_stop_id(stop_id='U101001', hour=8, output_level='weekday_type')
count_txn_count_by_given_stop_id(stop_id='U101016', hour=12, output_level='weekday_type')
count_txn_count_by_given_stop_id(stop_id='U109071', hour=17, output_level='weekday_type')
count_txn_count_by_given_stop_id(stop_id='U107102', hour=12, output_level='weekday_type')
count_txn_count_by_given_stop_id(stop_id='U111085', hour=6, output_level='weekday_type')
