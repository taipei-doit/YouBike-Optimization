# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 08:56:46 2023

@author: rz3881
"""

import pandas as pd
import time
import sys
sys.path.append(r'D:\iima\ubike分析\CODE\202304\閒置車')
from share_function import (
    generate_target_txn, dispatch_accumulator
    )

# Config
root_path = r'D:\iima\ubike分析'
valid_ym = '202304'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
valid_txn_path = root_path+f'/DM/{valid_ym}/prepared_data/txn'
valid_dis_path = root_path+f'/DM/{valid_ym}/prepared_data/dispatch'
exclude_date = ['2023-02-28', '2023-03-31']
init_hour = 6  # 一天初始時間(6 = 06:00)

# Load
# 3月結論
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal_add_idle.csv')
# 4月交易
val_txn = pd.read_csv(valid_txn_path+'/txn_only_ubike.csv')
val_txn = generate_target_txn(val_txn)

# Preprocess
# use the same '理想車柱數' on the same stop, do not consider weekday or weekend
temp = []
for _id, subdata in compare_agg.groupby(['ID']):
    subdata = subdata.sort_values('理想車柱數_最大值_3月', ascending=False)
    temp.append(subdata.head(1))
dokcer_free = pd.concat(temp)
dokcer_free['建議調整柱數'] = dokcer_free['理想車柱數_最大值_3月'] - dokcer_free['柱數_3月']
# 4月交易 取得-6 tz的date, filter weekday
val_txn['txn_time'] = pd.to_datetime(val_txn['txn_time'])
val_txn['txn_time_m6h'] = val_txn['txn_time'] - pd.Timedelta(hours=init_hour)
val_txn['date_m6h'] = val_txn['txn_time_m6h'].dt.date.astype(str)
val_txn['weekday_m6h'] = val_txn['txn_time_m6h'].dt.weekday + 1
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
val_txn['weekday_type'] = val_txn['weekday_m6h'].map(weekday_map)

# Simulation
# 如果這些站真的根據我們從3月得出的建議調整柱數，能帶來多少改善?
val_txn_dict = {}
for stop_id, sdata in val_txn.groupby('stop_id'):
    val_txn_dict[stop_id] = {}
    for weekday_type, swdata in sdata.groupby('weekday_type'):
        val_txn_dict[stop_id][weekday_type] = {}
        for date_m6h, sub_txn in swdata.groupby('date_m6h'):
            val_txn_dict[stop_id][weekday_type][date_m6h] = sub_txn
 
adj_docker_results_col = ('stop_id', 'date_m6h', 'april_capacity',
                          'real_in_dispatch', 'real_out_dispatch',
                          'simu_in_dispatch', 'simu_out_dispatch')
adj_docker_results = []
start_time = time.time()
for stop_id in set(compare_agg['ID']):
    if stop_id in ['U26']:
        continue
    
    # break
    # 3月資訊
    target_adj_docker_row = dokcer_free.loc[dokcer_free['ID']==stop_id]
    best_init_bike = target_adj_docker_row['建議初始在站車_柱無限_3月'].iloc[0]
    best_docker = target_adj_docker_row['理想車柱數_3月'].iloc[0]
    march_capacity = target_adj_docker_row['柱數_3月'].iloc[0]
    target_weekday_type = target_adj_docker_row['週間週末'].iloc[0]
    # 4月資訊
    april_capacity = target_adj_docker_row['柱數_4月'].iloc[0]
    stop_txn = val_txn_dict[stop_id][target_weekday_type]
    for date_m6h, sub_txn in stop_txn.items():
        if date_m6h in exclude_date:
            continue
        
        # raise ValueError('test')
        sub_txn = sub_txn.sort_values('txn_time')
        txn_delta = sub_txn['type'].map({'on': -1, 'off': 1}).tolist()
        # 根據3月建議使用4月交易模擬
        simu_result = dispatch_accumulator(best_init_bike, best_docker, txn_delta)
        # in
        is_positive = (simu_result['margin_dispatch_num'] > 0)
        simu_in_dispatch = simu_result.loc[is_positive, 'margin_dispatch_num'].sum()
        # out
        is_negative = (simu_result['margin_dispatch_num'] < 0)
        simu_out_dispatch = -simu_result.loc[is_negative, 'margin_dispatch_num'].sum()
        # 實際4月調度值
        real_in_dispatch = target_adj_docker_row['實際上午調入_3月'].iloc[0] + target_adj_docker_row['實際上午調入_4月'].iloc[0]
        real_out_dispatch = target_adj_docker_row['實際上午調出_3月'].iloc[0] + target_adj_docker_row['實際上午調出_4月'].iloc[0]
        # save
        adj_docker_results.append((stop_id, date_m6h, april_capacity,
                                   real_in_dispatch, real_out_dispatch,
                                   simu_in_dispatch, simu_out_dispatch))
print(time.time() - start_time) # 53 secs
adj_docker_results_df = pd.DataFrame(adj_docker_results,
                                     columns=adj_docker_results_col)

# Agg
adj_docker_results_agg = adj_docker_results_df.groupby('stop_id').agg({
    'date_m6h': 'count',
    'april_capacity': 'first',
    'real_in_dispatch': ['mean', 'std'],
    'real_out_dispatch': ['mean', 'std'],
    'simu_in_dispatch': ['mean', 'std'],
    'simu_out_dispatch': ['mean', 'std']
    }).reset_index()
adj_docker_results_agg.columns = [
    'stop_id', 'day_count', 'april_capacity',
    'real_in_dispatch_mean', 'real_in_dispatch_std',
    'real_out_dispatch_mean', 'real_out_dispatch_std',
    'simu_in_dispatch_mean', 'simu_in_dispatch_std',
    'simu_out_dispatch_mean', 'simu_out_dispatch_std']

# Join
adj_docker_verify = dokcer_free.merge(
    adj_docker_results_agg, how='outer', left_on='ID', right_on='stop_id')

# Extract
adj_docker_verify['實際調度車數'] = adj_docker_verify['real_in_dispatch_mean'] + adj_docker_verify['real_out_dispatch_mean']
adj_docker_verify['模擬調度車數'] = adj_docker_verify['simu_in_dispatch_mean'] + adj_docker_verify['simu_out_dispatch_mean']
adj_docker_verify['模擬-實際調度車數'] = adj_docker_verify['模擬調度車數'] - adj_docker_verify['實際調度車數']

# Filter
# 成效ok
is_useful = (adj_docker_verify['模擬-實際調度車數'] < -5)
useful_id = set(adj_docker_verify.loc[is_useful, 'stop_id'])
need_id = set(compare_agg.loc[compare_agg['類型']=='改善', 'ID'])
is_target = compare_agg['ID'].isin((useful_id & need_id))
is_positive = compare_agg['建議調整柱數_3月'] > 0
# is_zero = compare_agg['建議調整柱數_3月'] == 0
is_negative = compare_agg['建議調整柱數_3月'] < 0
compare_agg.loc[is_target & is_positive, '類型'] = '改善_增加車位'
compare_agg.loc[is_target & is_negative, '類型'] = '供過於求_車位'

# Save
compare_agg['類型'].value_counts()
compare_agg.to_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker.csv',
                   index=False)

# Reshape
dockerfree_clean = compare_agg[[
    'ID', '站名_3月', '週間週末', '柱數_3月', '建議調整柱數_3月',
    '建議初始在站車_柱無限_3月', '實際6點在站車_3月', '實際調度車數_3月']]
dockerfree_clean.columns = [
    'ID', '站名', '週間週末', '柱數', '建議調整柱數',
    '建議初始在站車_柱無限', '實際6點在站車', '實際調度車數']

# Save
dockerfree_add = compare_agg.loc[is_target & is_positive]
dockerfree_add.to_excel(strategy_path+'/擴位.xlsx', index=False)

# Save
dockerfree_reduce = compare_agg.loc[is_target & is_negative]
dockerfree_reduce.to_excel(strategy_path+'/閒置位.xlsx', index=False)