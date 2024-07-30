# -*- coding: utf-8 -*-
"""
Created on Sat May 27 11:53:03 2023

@author: rz3881
"""

import pandas as pd
import pickle
from numpy import nan
import datetime
# pd.options.mode.chained_assignment = None


# load dict
file_path = idle_path+'/simulate_results_by_stop_by_date.pkl'
with open(file_path, 'rb') as f:
    results = pickle.load(f)

# extract static simulation results.
static_simulation = []
for stop_id, dates_data in results.items():
    for date, v in dates_data.items():
        if date == date_last_month:
            continue
        
        # raise ValueError('test')
        simu_result = v['simulat_best_result']
        sum_dispatch_bikes = simu_result['dis_suggest'].abs().sum()
        is_morning = (simu_result['adjust_api_time'].dt.hour < afternoon_hour) & (simu_result['adjust_api_time'].dt.hour >= init_hour)
        is_afternoon = ~is_morning
        is_positive = (simu_result['dis_suggest'] >= 0)
        is_negative = ~is_positive
        # 上午調度 in/out
        morning_in = simu_result.loc[is_morning&is_positive, 'dis_suggest'].sum()
        morning_out = simu_result.loc[is_morning&is_negative, 'dis_suggest'].abs().sum()
        # 下午調度 in/out
        afternoon_in = simu_result.loc[is_afternoon&is_positive, 'dis_suggest'].sum()
        afternoon_out = simu_result.loc[is_afternoon&is_negative, 'dis_suggest'].abs().sum()
        # save
        temp_static = [stop_id, date, v['reserve_number'],
                       v['best_init_bikes'], v['best_afternoon_bikes'], 
                       v['min_dispatch_sum'],  # 模擬不同初始值後，最小的調度車數
                       v['morning_final_bikes'], v['afternoon_final_bikes'],
                       v['tomorrow_best_init_bikes'], 
                       morning_in, morning_out, 
                       afternoon_in, afternoon_out,
                       sum_dispatch_bikes]
        static_simulation.append(temp_static)
static_simulation_col = ['stop_id', 'date', 'reserve_capacity',
                         'best_init_bikes', 'best_afternoon_bikes',
                         'min_dispatch_bike',
                         'morning_final_bikes', 'afternoon_final_bikes',
                         'tomorrow_best_init_bikes',
                         'morning_in', 'morning_out', 
                         'afternoon_in', 'afternoon_out','sum_dispatch_bikes']
static_simulation_df = pd.DataFrame(static_simulation,
                                    columns=static_simulation_col)
# save
file_path = idle_path+'/simulation_results_static_part.csv'
static_simulation_df.to_csv(file_path, encoding='UTF-8', index=False)
print('Finished simulation_results_static_part in [04]extract_df_from_simulation.py.')

# extract detail simulation results, including dispatch and availabel bikes
detail_simulation = []
for stop_id, dates_data in results.items():
    for date, v in dates_data.items():
        if date == date_last_month:
            continue

        detail_simulation.append(v['simulat_best_result'])
        # raise ValueError('test')
detail_simulation_df = pd.concat(detail_simulation)

# reshape
# 一天有可能早上+8晚上又-8卻是無效操作，因此除了淨值另外使用絕對相加
detail_simulation_df['sum_dispatch'] = (detail_simulation_df['in'] +
                                        detail_simulation_df['out'])
is_zero = (detail_simulation_df['dispatch_delta']==0)
detail_simulation_df.loc[is_zero, 'dispatch_delta'] = nan
detail_simulation_df['weekday_m6h'] = (
    detail_simulation_df['adjust_api_time_m6h'].dt.weekday + 1)
detail_simulation_df = detail_simulation_df[[
    'date_m6h', 'weekday_m6h', 'time',
    'stop_id', 'stop_name', 'service_status', 'capacity',
    'prev_adjust_api_time', 'adjust_api_time', 'available_rent_bikes',
    'txn_on', 'txn_off', 'txn_delta', 'min_cum_txn', 'max_cum_txn', 
    'in', 'out', 'dispatch_delta', 'sum_dispatch',
    'actual_delta', 'other_delta', 
    'available_rent_bikes_in_worst_case',
    'best_init', 'dis_suggest', 'simulat_best_result',
    'negative_capacity', 'zero', 'capacity1.5', 'negative_capacity1.5',
    'upper_bound', 'lower_bound']]

# save
file_path = idle_path + '/simulation_results_detail_part.csv'
detail_simulation_df.to_csv(file_path, encoding='UTF-8', index=False)
print('Finished [04]extract_df_from_simulation.py')

del detail_simulation_df