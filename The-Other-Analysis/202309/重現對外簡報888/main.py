# -*- coding: utf-8 -*-
"""
Created on Sun Oct 29 14:20:02 2023

@author: rz3881
"""

# 重現
# 202304資料
# 空滿用1輛車(閒置[05])
# 用改車位v1
# confidence level 0.8


import pandas as pd
import time
import sys
sys.path.append(r'D:\iima\ubike分析\CODE\202304\閒置車')
from share_function import (
    generate_target_txn, dispatch_accumulator
    )

def show_results(compare_agg):
    type_results = {}
    for weekday_type, temp in compare_agg.groupby('週間週末'):
        type_results[weekday_type] = temp['類型'].value_counts(dropna=False)
    print(type_results)
    print('.')
    print('.')


# Config
root_path = r'D:\iima\ubike分析'
ref_ym = '202303'
reference_idle_path = root_path+f'/DM/{ref_ym}/閒置車'
valid_ym = '202304'
valid_idle_path = root_path+f'/DM/{valid_ym}/閒置車'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
confidence_threshould = 0.7
valid_txn_path = root_path+f'/DM/{valid_ym}/prepared_data/txn'
valid_dis_path = root_path+f'/DM/{valid_ym}/prepared_data/dispatch'
exclude_date = ['2023-02-28', '2023-03-31']
init_hour = 6  # 一天初始時間(6 = 06:00)
efficient_threshould = -5


# Load
# 3月結論
march_agg = pd.read_csv(reference_idle_path + '/compare_agg.csv')
# 4月結論
april_agg = pd.read_csv(valid_idle_path + '/compare_agg.csv')
# Preprocess
# is idel stop in March still idel in April?
data = march_agg.merge(april_agg, how='outer', on=['ID', '週間週末'])
data = data.loc[data['ID']!='U26']
# Rename
cols = []
for col in data.columns:
    col = col.replace('_x', '_3月')
    col = col.replace('_y', '_4月')
    cols.append(col)
data.columns = cols

# 4月交易
raw_val_txn = pd.read_csv(valid_txn_path+'/txn_only_ubike.csv')
raw_val_txn = generate_target_txn(raw_val_txn)
# 4月交易 取得-6 tz的date, filter weekday
raw_val_txn['txn_time'] = pd.to_datetime(raw_val_txn['txn_time'])
raw_val_txn['txn_time_m6h'] = raw_val_txn['txn_time'] - pd.Timedelta(hours=init_hour)
raw_val_txn['date_m6h'] = raw_val_txn['txn_time_m6h'].dt.date.astype(str)
raw_val_txn['weekday_m6h'] = raw_val_txn['txn_time_m6h'].dt.weekday + 1
weekday_map = {
    1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
    6: 'weekend', 7: 'weekend'
}
raw_val_txn['weekday_type'] = raw_val_txn['weekday_m6h'].map(weekday_map)

# 問題=========================================================================
unqualified = data.copy()
val_txn = raw_val_txn.copy()
# Filter
unqualified['類型'] = None
# 柱數變動
is_docker_change = (
    unqualified['柱數_4月'] - unqualified['柱數_3月']
).abs() >= 2
unqualified.loc[is_docker_change, '類型'] = '問題_柱數變動'
# 可能有故障、停用、新設立，資料不齊全問題
is_weekday = (unqualified['週間週末']=='weekday')
is_weekend = ~is_weekday
is_normal_weekday = (
    is_weekday 
    & (unqualified['天數_3月']==23) 
    & (unqualified['天數_4月']==20)
)
is_normal_weekend = (
    is_weekend 
    & (unqualified['天數_3月']==8) 
    & (unqualified['天數_4月']==10)
)
is_not_normal_day = (~is_normal_weekday) & (~is_normal_weekend)
unqualified.loc[is_not_normal_day, '類型'] = '問題_天數不正常'
# 正常時間過少
is_low_confidence = (
    (unqualified['資料可信度_4月'] <= confidence_threshould) 
    | (unqualified['資料可信度_3月'] <= confidence_threshould)
)
unqualified.loc[is_low_confidence, '類型'] = '問題_需求未知'

show_results(compare_agg)
# =============================================================================


# 閒置=========================================================================
compare_agg = unqualified

# Extract
compare_agg['3、4月平均閒置車數'] = (
    compare_agg['閒置車_3月'] + compare_agg['閒置車_4月']
)/2

# Filter
# 閒置過少，無檢視意義
is_idel = compare_agg['3、4月平均閒置車數'] >= 2
is_useable = compare_agg['類型'].isna()
compare_agg.loc[is_idel & is_useable, '類型'] = '供過於求_車輛'

show_results(compare_agg)
# ============================================================================= 


# 車柱=========================================================================
# Preprocess
# use the same '理想車柱數' on the same stop, do not consider weekday or weekend
temp = []
for _id, subdata in compare_agg.groupby(['ID']):
    subdata = subdata.sort_values('理想車柱數_最大值_3月', ascending=False)
    temp.append(subdata.head(1))
dokcer_free = pd.concat(temp)
dokcer_free['建議調整柱數'] = (
    dokcer_free['理想車柱數_最大值_3月'] - dokcer_free['柱數_3月']
)

# Simulation
# 如果這些站真的根據我們從3月得出的建議調整柱數，能帶來多少改善?
val_txn = raw_val_txn.copy()
val_txn_dict = {}
for stop_id, sdata in val_txn.groupby('stop_id'):
    val_txn_dict[stop_id] = {}
    for weekday_type, swdata in sdata.groupby('weekday_type'):
        val_txn_dict[stop_id][weekday_type] = {}
        for date_m6h, sub_txn in swdata.groupby('date_m6h'):
            val_txn_dict[stop_id][weekday_type][date_m6h] = sub_txn
 
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
        simu_result = dispatch_accumulator(
            best_init_bike, best_docker, txn_delta
        )
        # in
        is_positive = (simu_result['margin_dispatch_num'] > 0)
        simu_in_dispatch = (
            simu_result.loc[is_positive, 'margin_dispatch_num'].sum()
        )
        # out
        is_negative = (simu_result['margin_dispatch_num'] < 0)
        simu_out_dispatch = (
            -simu_result.loc[is_negative, 'margin_dispatch_num'].sum()
        )
        # 實際4月調度值
        real_in_dispatch = (
            target_adj_docker_row['實際上午調入_3月'].iloc[0] 
            + target_adj_docker_row['實際上午調入_4月'].iloc[0]
        )
        real_out_dispatch = (
            target_adj_docker_row['實際上午調出_3月'].iloc[0] 
            + target_adj_docker_row['實際上午調出_4月'].iloc[0]
        )
        # save
        adj_docker_results.append((stop_id, date_m6h, april_capacity,
                                   real_in_dispatch, real_out_dispatch,
                                   simu_in_dispatch, simu_out_dispatch))
print(time.time() - start_time) # 53 secs
adj_docker_results_col = (
    'stop_id', 'date_m6h', 'april_capacity',
    'real_in_dispatch', 'real_out_dispatch',
    'simu_in_dispatch', 'simu_out_dispatch'
)
adj_docker_results_df = pd.DataFrame(
    adj_docker_results, columns=adj_docker_results_col
)

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
adj_docker_verify['實際調度車數'] = (
    adj_docker_verify['real_in_dispatch_mean'] 
    + adj_docker_verify['real_out_dispatch_mean']
)
adj_docker_verify['模擬調度車數'] = (
    adj_docker_verify['simu_in_dispatch_mean'] 
    + adj_docker_verify['simu_out_dispatch_mean']
)
adj_docker_verify['模擬-實際調度車數'] = (
    adj_docker_verify['模擬調度車數'] - adj_docker_verify['實際調度車數']
)

# Filter
# 成效ok
is_useful = (adj_docker_verify['模擬-實際調度車數'] < efficient_threshould)
useful_id = set(adj_docker_verify.loc[is_useful, 'stop_id'])
need_id = set(compare_agg.loc[compare_agg['類型'].isna(), 'ID'])
is_target = compare_agg['ID'].isin((useful_id & need_id))
is_positive = compare_agg['建議調整柱數_3月'] > 5
# is_zero = compare_agg['建議調整柱數_3月'] == 0
is_negative = compare_agg['建議調整柱數_3月'] < 0
compare_agg.loc[is_target & is_positive, '類型'] = '改善_增加車位'
compare_agg.loc[is_target & is_negative, '類型'] = '供過於求_車位'

show_results(compare_agg)
# =============================================================================

# 調度=========================================================================
# Extract
# 模擬與驗證差
compare_agg['成效(日模擬-現實_調度車數差_兩月平均)'] = (
    compare_agg['模擬與現實調度差_3月'] + compare_agg['模擬與現實調度差_4月']
) / 2

# Filter
# 效益不彰
is_need_change = compare_agg['類型'].isna()
is_useful = (
    compare_agg['成效(日模擬-現實_調度車數差_兩月平均)'] <= efficient_threshould
)
compare_agg.loc[is_need_change & is_useful, '類型'] = '改善_最佳化調度'
show_results(compare_agg)
# =============================================================================



# # 彙總========================================================================
# 未知、閒置、良好、需改善比例
is_type_na = compare_agg['類型'].isna()
compare_agg.loc[is_type_na, '類型'] = '良好'
is_problem = compare_agg['類型'].str.startswith('問題_')
compare_agg.loc[is_problem, '類型'] = '待改善'
is_need_prove = compare_agg['類型'].str.startswith('改善')
compare_agg.loc[is_need_prove, '類型'] = '待改善'
is_too_much = compare_agg['類型'].str.startswith('供過於求_')
compare_agg.loc[is_too_much, '類型'] = '供過於求'
# =============================================================================

type_results = {}
for weekday_type, temp in compare_agg.groupby('週間週末'):
    type_results[weekday_type] = temp['類型'].value_counts(dropna=False)
