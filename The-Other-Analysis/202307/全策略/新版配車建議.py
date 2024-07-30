# -*- coding: utf-8 -*-
"""
Created on Sun Sep 10 18:22:54 2023

@author: rz3881
"""

import pandas as pd
from numpy import int64


def calcu_mode_hour(x):
    if type(x) is int64:
        return f'{str(int(x-1))}~{str(int(x))}點'
    else:
        is_two_element = (len(x)==2)
        is_diff_one = ((x[1]-x[0])==1)
        if is_two_element & is_diff_one:
            return f'{str(int(x[0]-1))}~{str(int(x[0]))}點'
        
        
ym = '202307'
root_path = r'D:\iima\ubike分析'
dim_path = root_path+'/DIM'
idle_path = root_path+f'/DM/{ym}/閒置車'
strategy_path = root_path+f'/DM/{ym}/全策略'
n_th = 2
init_hour = 6

# Load
compare_detail = pd.read_csv(idle_path + '/compare_detail.csv')

# agg by stop_id, weekday_type
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday',
               5: 'weekday', 6: 'weekend', 7: 'weekend'}
compare_detail['weekday_type'] = compare_detail['weekday_m6h'].map(weekday_map)

# Add columns
compare_detail['min_cum_txn_hour'] = pd.to_datetime(compare_detail['minct_time']).dt.hour
compare_detail['max_cum_txn_hour'] = pd.to_datetime(compare_detail['maxct_time']).dt.hour
compare_detail['confidence_rate'] = 1- (
    (compare_detail['empty_minutes'] 
     + compare_detail['full_minutes']) 
    / ((24-init_hour)*60)
    )
compare_detail['simu_effect_by_dis_bikes'] = (
    compare_detail['abs_dis_suggest'] - compare_detail['sum_abs_dispatch'])
compare_detail['morning_total_bike'] = (
    compare_detail['best_init_bikes'] 
    + compare_detail['simu_morning_in']
    - compare_detail['dispatch_for_afternoon'])
compare_detail['afternoon_total_bike'] = (
    compare_detail['best_afternoon_bikes'] 
    + compare_detail['simu_afternoon_in']
    - compare_detail['dispatch_for_tomorrow'])
compare_detail['total_bike'] = compare_detail['morning_total_bike'] + compare_detail['afternoon_total_bike']

# Filter
# 取第二小的最低潮當做備車代表
nlarge_bike = []
for stop_id, group_data in compare_detail.groupby(['stop_id', 'weekday_type']):
    # break
    group_data = group_data.sort_values('total_bike', ascending=False)
    
    if group_data.shape[0] < n_th:
        print(stop_id)
    else:
        second_large_total_bike = group_data.iloc[[n_th-1]]
        second_large_total_bike['nsmall_morning_init'] = group_data['best_init_bikes'].sort_values().iloc[n_th-1]
        second_large_total_bike['nsmall_afternoon_init'] = group_data['best_afternoon_bikes'].sort_values().iloc[n_th-1]
        nlarge_bike.append(second_large_total_bike)
nlarge_bike = pd.concat(nlarge_bike).reset_index()
nlarge_bike['morning_prepare_bike'] = nlarge_bike['morning_total_bike'] - nlarge_bike['nsmall_morning_init']
nlarge_bike['afternoon_prepare_bike'] = nlarge_bike['afternoon_total_bike'] - nlarge_bike['nsmall_afternoon_init']

# Reshape
nlarge_bike = nlarge_bike[[
    'stop_id', 'stop_name', 'capacity', 'weekday_type', 'date_m6h',
    # 建議
    'confidence_rate', 'total_bike', 'simu_effect_by_dis_bikes',  # 總
    'morning_total_bike', 'nsmall_morning_init', 'morning_prepare_bike',  # 上午
    'afternoon_total_bike', 'nsmall_afternoon_init', 'afternoon_prepare_bike',  # 下午
    # 實際
    'empty_minutes', 'full_minutes', 
    'init_hour_available_bike', 'morning_in', 'morning_out',
    'afternoon_hour_available_bike', 'afternoon_in', 'afternoon_out',
    'sum_rent', 'sum_return', 'sum_txn_delta',
    'sum_in', 'sum_out', 'sum_dispatch_delta', 'sum_abs_dispatch'
    ]]

# 一般性的借車、還車高峰
mode_cum_txn = compare_detail.groupby(['stop_id', 'weekday_type']).agg({
    'min_cum_txn_hour': pd.Series.mode,
    'max_cum_txn_hour': pd.Series.mode,
    }).reset_index()
mode_cum_txn['min_cum_txn_hour'] = mode_cum_txn['min_cum_txn_hour'].apply(calcu_mode_hour)
mode_cum_txn['max_cum_txn_hour'] = mode_cum_txn['max_cum_txn_hour'].apply(calcu_mode_hour)

# 
rich_setup = nlarge_bike.merge(mode_cum_txn, how='inner',
                               on=['stop_id','weekday_type'])
                         
# Save
rich_setup.to_excel(strategy_path+'/rich_setup.xlsx',
                  index=False)
