# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 10:38:48 2023

@author: rz3881
"""

import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt

# config
ym = '202303'
root_path = r'D:\iima\ubike分析'
idle_path = root_path+f'/DM/{ym}/閒置車'
init_hour = 6
target_percentile = 1

# load
compare_detail = pd.read_csv(idle_path + '/compare_detail.csv')

# agg by stop_id, weekday_type
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
compare_detail['weekday_type'] = compare_detail['weekday_m6h'].map(weekday_map)
compare_agg = compare_detail.groupby(['stop_id', 'weekday_type']).agg({
    'stop_name': 'first',
    'date_m6h': 'nunique',
    'capacity': 'first',
    # 固定無法模擬
    'sum_txn_delta': ['median', 'std'],  # 單日交易淨值
    'min_cum_txn': ['min', 'std'],  # 最小累積交易淨值
    'max_cum_txn': ['max', 'std'],  # 最大累積交易淨值
    'txn_range': ['max', 'std'],  # 累積交易淨值差=建議車柱數
    # 現況
    'min_bike_after6': ['median', 'std'],  # 6點後最小在站車數
    'empty_minutes': ['median', 'std'],  #  空車分鐘
    'full_minutes': ['median', 'std'],  #  滿車分鐘
    'init_hour_available_bike': ['median', 'std'],  # 6點初始車數
    'afternoon_hour_available_bike': ['median', 'std'],  # 16點初始車數
    'morning_in': 'median',  # 上午調入
    'morning_out': 'median',  # 上午調出
    'afternoon_in': 'median',  # 下午調入
    'afternoon_out': 'median',  # 下午調出
    'sum_abs_dispatch': ['median', 'std'],  # 實際調度車數加總
    'sum_dispatch_delta': ['median', 'std'],  # 實際調度淨值((tie-untie)+(load-unload))
    # 模擬/新結論
    # 車柱無限
    'best_init_if_docker_free': ['median', 'std'],
    'end_bike_if_docker_free': ['median', 'std'],
    # 車柱不變
    # 'simu_empty_minutes': ['median', 'std'],  #  模擬後空車分鐘
    # 'simu_full_minutes': ['median', 'std'],  #  模擬後滿車分鐘
    'best_init_bikes': ['median', 'std'],  # 模擬最佳初始車數
    'best_afternoon_bikes': ['median', 'std'],  # 模擬下午初始車數
    'simu_morning_in': 'median',  # 模擬上午調入
    'simu_morning_out': 'median',  # 模擬上午調出
    'simu_afternoon_in': 'median',  # 模擬下午調入
    'simu_afternoon_out': 'median',  # 模擬下午調出
    'abs_dis_suggest': ['median', 'std'],  # 模擬調度車數加總
    'dis_suggest': ['median', 'std'],  # 模擬調度淨值
    'idle': ['median', 'std']
    }).reset_index()
# rename
compare_agg.columns = ['stop_id', 'weekday_type', 'stop_name', 'day_count',
                       'capacity', 'net_txn_median', 'net_txn_std',
                       'min_cum_net_txn_min', 'min_cum_net_txn_std',
                       'max_cum_net_txn_max', 'max_cum_net_txn_std',
                       'cum_net_txn_range_max', 'cum_net_txn_range_std',
                       # 現況
                       'min_bike_after6_min', 'min_bike_after6_std',
                       'empty_minutes_median', 'empty_minutes_std',
                       'full_minutes_median', 'full_minutes_std',
                       'best_init_bikes_median', 'best_init_bikes_std',
                       'best_afternoon_bikes_median', 'best_afternoon_bikes_std',
                       'morning_in_median', 'morning_out_median',
                       'afternoon_in_median', 'afternoon_out_median',
                       'dispatch_bikes_median', 'dispatch_bikes_std',
                       'net_dispatch_median', 'net_dispatch_std',
                       # 模擬/新結論
                       # 車柱無限
                       'best_init_if_docker_free_median', 'best_init_if_docker_free_std',
                       'end_bike_if_docker_free_median', 'end_bike_if_docker_free_std',
                       # 車柱不變
                       # 'simu_empty_minutes_median', 'simu_empty_minutes_std',
                       # 'simu_full_minutes_median', 'simu_full_minutes_std',
                       'simu_best_init_bikes_median', 'simu_best_init_bikes_std',
                       'simu_best_afternoon_bikes_median', 'simu_best_afternoon_bikes_std',
                       'simu_morning_in_median', 'simu_morning_out_median',
                       'simu_afternoon_in_median', 'simu_afternoon_out_median',
                       'simu_dispatch_bikes_median', 'simu_dispatch_bikes_std',
                       'simu_net_dispatch_median', 'simu_net_dispatch_std',
                       'idle_median', 'idle_std']

# 計算最大潮差的n分位數(接近n不完全等於)
txn_range_df = []
for (stop_id, weekday_type), temp in compare_detail.groupby(['stop_id', 'weekday_type']):
    # break
    seq_len = temp.shape[0]
    locate_num = round(seq_len * target_percentile)
    nlarge_txn_range = temp['txn_range'].sort_values().iloc[locate_num-1]
    txn_range_df.append((stop_id, weekday_type, nlarge_txn_range))
txn_range_df = pd.DataFrame(
    txn_range_df, columns=['stop_id', 'weekday_type', 'nlarge_txn_range'])
# join
compare_agg = compare_agg.merge(txn_range_df, how='inner', on=['stop_id', 'weekday_type'])

# add column
compare_agg['night_dispatch_if_docker_free'] = compare_agg['end_bike_if_docker_free_median'] - compare_agg['best_init_if_docker_free_median']
compare_agg['confidence_rate'] = 1- ((compare_agg['empty_minutes_median'] + compare_agg['full_minutes_median']) / ((24-init_hour)*60))
compare_agg['capacity_diff'] = (compare_agg['cum_net_txn_range_max'] - compare_agg['capacity'])
compare_agg['dipatch_diff'] = compare_agg['simu_dispatch_bikes_median'] - compare_agg['dispatch_bikes_median']
# reshape
col_map = {
    # 基本資訊
    'stop_id': 'ID', 'weekday_type': '週間週末',
    'stop_name': '站名', 'day_count': '天數', 'capacity': '柱數',
    'net_txn_median': '淨交易', 'net_txn_std': '淨交易_std',
    'min_cum_net_txn_min': '潮汐最低點', 'min_cum_net_txn_std': '潮汐最低點_std',
    'max_cum_net_txn_max': '潮汐最高點', 'max_cum_net_txn_std': '潮汐最高點_std',
    'cum_net_txn_range_max': '理想車柱數', 'cum_net_txn_range_std': '理想車柱數_std',
    # 現況事實
    'empty_minutes_median': '空車分鐘', 'empty_minutes_std': '空車分鐘_std',
    'full_minutes_median': '滿車分鐘', 'full_minutes_std': '滿車分鐘_std',
    'confidence_rate': '資料可信度',
    'best_init_bikes_median': '實際6點在站車', 'best_init_bikes_std': '實際6點在站車_std',
    'best_afternoon_bikes_median': '實際16點在站車', 'best_afternoon_bikes_std': '實際16點在站車_std',
    'morning_in_median': '實際上午調入', 'morning_out_median': '實際上午調出',
    'afternoon_in_median': '實際下午調入', 'afternoon_out_median': '實際下午調出',
    'dispatch_bikes_median': '實際調度車數', 'dispatch_bikes_std': '實際調度車數_std',
    'net_dispatch_median': '淨調度', 'net_dispatch_std': '淨調度_std',
    # 簡單閒置
    'min_bike_after6_min': '閒置車', 'min_bike_after6_std': '閒置車_std',
    # 柱無限
    'capacity_diff': '建議調整柱數',
    'best_init_if_docker_free_median': '建議初始在站車_柱無限', 'best_init_if_docker_free_std': '建議初始在站車_柱無限std',
    'end_bike_if_docker_free_median': '調整後結尾在站車_柱無限', 'end_bike_if_docker_free_std': '調整後結尾在站車_柱無限std',
    'night_dispatch_if_docker_free': '夜間調度_柱無限',
    # 柱不變
    'simu_best_init_bikes_median': '建議6點在站車', 'simu_best_init_bikes_std': '建議6點在站車_std',
    'simu_best_afternoon_bikes_median': '建議16點在站車', 'simu_best_afternoon_bikes_std': '建議16點在站車_std',
    'simu_morning_in_median': '模擬上午調入', 'simu_morning_out_median': '模擬上午調出',
    'simu_afternoon_in_median': '模擬下午調入', 'simu_afternoon_out_median': '模擬下午調出',
    'simu_dispatch_bikes_median': '模擬調度車數', 'simu_dispatch_bikes_std': '模擬調度車數_std',
    'dipatch_diff': '模擬與現實調度差',
    'nlarge_txn_range': '理想車柱數_最大值'}
compare_agg = compare_agg.rename(columns = col_map)

# Save
compare_agg.to_csv(idle_path + '/compare_agg.csv', index=False)
                         
