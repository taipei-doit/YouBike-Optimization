# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 10:38:48 2023

@author: rz3881
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# config
root_path = r'D:\iima\ubike分析'
idle_path = root_path+r'\DM\202303\閒置車'

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
    'abs_dis_suggest': ['median', 'std'],  # 模擬調度車數加總
    'dis_suggest': ['median', 'std'],  # 模擬調度淨值
    'idle': ['median', 'std']
    }).reset_index()
# record count of days, and days that not work
compare_agg.columns = ['stop_id', 'weekday_type', 'stop_name', 'day_count',
                       'capacity', 'net_txn_median', 'net_txn_std',
                       'min_cum_net_txn_min', 'min_cum_net_txn_std',
                       'max_cum_net_txn_max', 'max_cum_net_txn_std',
                       'cum_net_txn_range_max', 'cum_net_txn_range_std',
                       # 現況
                       'min_bike_after6_min', 'min_bike_after6_std',
                       'empty_minutes_median', 'empty_minutes_std',
                       'full_minutes_median', 'full_minutes_std',
                       'init_hour_bikes_median', 'init_hour_bikes_std',
                       'dispatch_bikes_median', 'dispatch_bikes_std',
                       'net_dispatch_median', 'net_dispatch_std',
                       # 模擬/新結論
                       # 車柱無限
                       'best_init_if_docker_free_median', 'best_init_if_docker_free_std',
                       'end_bike_if_docker_free_median', 'end_bike_if_docker_free_std',
                       # 車柱不變
                       # 'simu_empty_minutes_median', 'simu_empty_minutes_std',
                       # 'simu_full_minutes_median', 'simu_full_minutes_std',
                       'simu_init_bikes_median', 'simu_init_bikes_std',
                       'simu_dispatch_bikes_median', 'simu_dispatch_bikes_std',
                       'simu_net_dispatch_median', 'simu_net_dispatch_std',
                       'idle_median', 'idle_std']
# add column
compare_agg['night_dispatch_if_docker_free'] = compare_agg['end_bike_if_docker_free_median'] - compare_agg['best_init_if_docker_free_median']
compare_agg['confidence_rate'] = 1- ((compare_agg['empty_minutes_median'] + compare_agg['full_minutes_median']) / (24*60))
compare_agg['capacity_diff'] = (compare_agg['cum_net_txn_range_max'] - compare_agg['capacity'])
# reshape
compare_agg.columns = ['ID', '週間週末', '站名', '天數',
                       '柱數', '淨交易', '淨交易std',
                       '潮汐最低點', '潮汐最低點std',
                       '潮汐最高點', '潮汐最高點std',
                       '理想車柱數', '理想車柱數std',
                       # 現況
                       '閒置車', '閒置車std',
                       '空車分鐘', '空車分鐘std',
                       '滿車分鐘', '滿車分鐘std',
                       '實際6點在站車', '實際6點在站車std',
                       '實際調度車數', '實際調度車數std',
                       '淨調度', '淨調度std',
                       # 模擬/新結論
                       '建議初始在站車_柱無限', '建議初始在站車_柱無限std',
                       '調整後結尾在站車_柱無限', '調整後結尾在站車_柱無限std',
                       '建議初始在站車_柱不變', '建議初始在站車_柱不變std',
                       '模擬調度車數_柱不變', '模擬調度車數_柱不變std', 
                       '模擬淨調度_柱不變', '模擬淨調度_柱不變std',
                       '模擬與現實差_柱不變', '模擬與現實差柱不變std',
                       '夜間調度_柱無限', '資料可信度', '建議調整柱數']

# 閒置車
simple_idle = compare_agg[['ID', '天數', '週間週末', '站名',
                           # 閒置
                           '閒置車', '閒置車std']]
# 柱無限
dokcer_free = compare_agg[['ID', '天數', '週間週末', '站名',
                           '柱數', '理想車柱數', '理想車柱數std', '建議調整柱數',
                           # 柱無限
                           '建議初始在站車_柱無限', '建議初始在站車_柱無限std',
                           '淨交易', '淨交易std',
                           '資料可信度']]
# use the same '理想車柱數' on the same stop, do not consider weekday or weekend
tdata = dokcer_free.groupby(['ID']).agg({
            '理想車柱數': 'max',
            }).reset_index()
dokcer_free = dokcer_free.drop('理想車柱數', axis=1).merge(tdata, how='left', on='ID')
dokcer_free['建議調整柱數'] = dokcer_free['理想車柱數'] - dokcer_free['柱數']

fig = plt.figure(figsize=(6, 12))
sns.kdeplot(y=dokcer_free.loc[dokcer_free['週間週末']=='weekday', '建議調整柱數'])
plt.show()
# 柱不變
no_change = compare_agg[['ID', '天數', '週間週末', '站名', '柱數',
                         '實際6點在站車', '實際6點在站車std',
                         '建議初始在站車_柱不變', '建議初始在站車_柱不變std',
                         '實際調度車數', '實際調度車數std',
                         '模擬調度車數_柱不變', '模擬調度車數_柱不變std', 
                         '模擬與現實差_柱不變', '模擬與現實差柱不變std',
                         '資料可信度']]
# save 
with pd.ExcelWriter(f'{idle_path}/idle_final_output.xlsx') as writer:
    simple_idle.to_excel(writer, sheet_name='簡單閒置', index=False)
    dokcer_free.to_excel(writer, sheet_name='柱無限', index=False)
    no_change.to_excel(writer, sheet_name='柱不變', index=False)
