# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 08:56:46 2023

@author: rz3881
"""

# only consider weekday

import pandas as pd
import time

def filter_solid_results(data, only_weekday, confidence_ratio_threshould,
                         is_confidence_ratio = True):
    filtered_data = data.copy()
    if only_weekday:
        is_weekday = (filtered_data['週間週末']=='weekday')
        filtered_data = filtered_data.loc[is_weekday]        
    if is_confidence_ratio:
        is_cl_big_enough = (data['資料可信度']>=confidence_ratio_threshould)
        filtered_data = data.loc[is_cl_big_enough]
    return filtered_data

def generate_target_txn(txn, target_date=None, target_stop_id=None):
    filter_on_col_name = ['card_id', 'route_name', 'on_stop_id', 'on_stop', 'on_time']
    filter_off_col_name = ['card_id', 'route_name', 'off_stop_id', 'off_stop', 'off_time']
    result_col_name = ['card_id', 'bike_id', 'stop_id', 'stop_name', 'txn_time']
    # build filter
    if target_date:
        is_target_on_date = (txn['on_time'].dt.date==target_date)
        is_target_off_date = (txn['off_time'].dt.date==target_date)
    if target_stop_id:
        is_target_on_stop = (txn['on_stop_id']==target_stop_id)
        is_target_off_stop = (txn['off_stop_id']==target_stop_id)
    # filter
    if target_date:
        if target_stop_id:
            on_txn = txn.loc[is_target_on_date & is_target_on_stop, filter_on_col_name]
            off_txn = txn.loc[is_target_off_date & is_target_off_stop, filter_off_col_name]
        else:
            on_txn = txn.loc[is_target_on_date, filter_on_col_name]
            off_txn = txn.loc[is_target_off_date, filter_off_col_name]
    else:
        if target_stop_id:
            on_txn = txn.loc[is_target_on_stop, filter_on_col_name]
            off_txn = txn.loc[is_target_off_stop, filter_off_col_name]
        else:
            on_txn = txn[filter_on_col_name]
            off_txn = txn[filter_off_col_name]
    # reshape
    on_txn.columns = result_col_name
    on_txn['type'] = 'on'
    off_txn.columns = result_col_name
    off_txn['type'] = 'off'
    target_txn = pd.concat([on_txn, off_txn])
    return target_txn

def dispatch_accumulator(init_num, capacity, numbers):
    '累加數列後，若超出上下界線，紀錄超出值並抹去'
    zero = 0
    delta = 0
    result = {'avaliabe_bike': [], 'margin_dispatch_num': []}
    for num in numbers:
        # 累加值
        init_num += num
        result['avaliabe_bike'].append(init_num)
        # 調度值
        if init_num > capacity:
            delta = init_num - capacity
            init_num = capacity
        elif init_num < zero:
            delta = init_num
            init_num = zero
        else:
            delta = 0
        result['margin_dispatch_num'].append(delta)
    return pd.DataFrame(result)


# Config
root_path = r'D:\iima\ubike分析'
ref_ym = '202303'
ref_idle_path = root_path+f'/DM/{ref_ym}/閒置車'
valid_ym = '202304'
valid_idle_path = root_path+f'/DM/{valid_ym}/閒置車'
valid_txn_path = root_path+f'/DM/{valid_ym}/prepared_data/txn'
exclude_date = ['2023-02-08', '2023-03-31']

# Load
# 3月結論
simple_idel = pd.read_excel(ref_idle_path+'/idle_final_output.xlsx',
                           sheet_name='簡單閒置')
adj_docker = pd.read_excel(ref_idle_path+'/idle_final_output.xlsx',
                           sheet_name='柱無限')
adj_dispatch = pd.read_excel(ref_idle_path+'/idle_final_output.xlsx',
                           sheet_name='柱不變')
# 4月結論
april_idel = pd.read_excel(valid_idle_path+'/idle_final_output.xlsx',
                           sheet_name='簡單閒置')
# 4月調度
april_dispatch = pd.read_csv(valid_idle_path+'/compare_detail.csv')
# 4月交易
april_txn = pd.read_csv(valid_txn_path+'/txn_only_ubike.csv')
april_txn = generate_target_txn(april_txn)

# Preprocess
# 3月策略 filter solid results
simple_idel = filter_solid_results(simple_idel, False, None, False)
adj_docker = filter_solid_results(adj_docker, True, 0.8)
adj_docker = adj_docker.rename(columns={'理想車柱數_最大值': '理想車柱數'})
adj_dispatch = filter_solid_results(adj_dispatch, True, 0.8)
# 4月交易 取得-7 tz的date, filter weekday
init_hour = 6  # 一天初始時間(7 = 07:00)
april_txn['txn_time'] = pd.to_datetime(april_txn['txn_time'])
april_txn['txn_time_m7h'] = april_txn['txn_time'] - pd.Timedelta(hours=init_hour)
april_txn['date_m7h'] = april_txn['txn_time_m7h'].dt.date.astype(str)
april_txn['weekday_m7h'] = april_txn['txn_time_m7h'].dt.weekday + 1
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
april_txn['weekday_type'] = april_txn['weekday_m7h'].map(weekday_map)
is_weekday = (april_txn['weekday_type']=='weekday')
april_txn = april_txn.loc[is_weekday]



# 成效分兩部份: 調柱、調車柱
# Simulation
# 如果這些站真的根據我們從3月得出的建議調整柱數，能帶來多少改善?
adj_docker_results_col = ('stop_id', 'date_m7h', 'april_capacity',
                          'real_in_dispatch', 'real_out_dispatch',
                          'simu_in_dispatch', 'simu_out_dispatch')
adj_docker_results = []
start_time = time.time()
for stop_id in set(adj_docker['ID']):
    # break
    # 3月資訊
    target_adj_docker_row = adj_docker.loc[adj_docker['ID']==stop_id]
    best_init_bike = target_adj_docker_row['建議初始在站車_柱無限'].iloc[0]
    best_docker = target_adj_docker_row['理想車柱數'].iloc[0]
    march_capacity = target_adj_docker_row['柱數'].iloc[0]
    # 4月資訊
    is_target_stop = (april_dispatch['stop_id']==stop_id)
    april_capacity = april_dispatch.loc[is_target_stop, 'capacity'].max()
    stop_txn = april_txn.loc[april_txn['stop_id']==stop_id]
    for date_m7h, sub_txn in stop_txn.groupby('date_m7h'):
        if date_m7h in exclude_date:
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
        is_target = (april_dispatch['stop_id']==stop_id) & (april_dispatch['date_m7h']==date_m7h)
        if is_target.sum() >= 1:
            real_in_dispatch = april_dispatch.loc[is_target, 'sum_in'].iloc[0]
            real_out_dispatch = april_dispatch.loc[is_target, 'sum_out'].iloc[0]
        else:
            real_in_dispatch = None
            real_out_dispatch = None
        # save
        adj_docker_results.append((stop_id, date_m7h, april_capacity,
                                   real_in_dispatch, real_out_dispatch,
                                   simu_in_dispatch, simu_out_dispatch))
print(time.time() - start_time) # 437 secs
adj_docker_results_df = pd.DataFrame(adj_docker_results,
                                     columns=adj_docker_results_col)
# agg
adj_docker_results_agg = adj_docker_results_df.groupby('stop_id').agg({
    'date_m7h': 'count',
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
# join
adj_docker_compare = adj_docker.merge(adj_docker_results_agg, how='outer',
                                      left_on='ID', right_on='stop_id')
adj_docker_compare['real_dipatch_bikes'] = adj_docker_compare['real_in_dispatch_mean'] + adj_docker_compare['real_out_dispatch_mean']
adj_docker_compare['simu_dipatch_bikes'] = adj_docker_compare['simu_in_dispatch_mean'] + adj_docker_compare['simu_out_dispatch_mean']
adj_docker_compare['simu_m_real_bikes'] = adj_docker_compare['simu_dipatch_bikes'] - adj_docker_compare['real_dipatch_bikes']
adj_docker_compare['benefit'] = (adj_docker_compare['simu_m_real_bikes'] 
                                 / adj_docker_compare['建議調整柱數'])
# Extract
adj_docker_compare['m6h_bikes_cv'] = adj_docker_compare['實際7點在站車std'] / adj_docker_compare['實際7點在站車']
adj_docker_compare['adj_docker_cv'] = adj_docker_compare[] / adj_docker_compare[]
adj_docker_compare['real_dispatch_cv'] = adj_docker_compare[] / adj_docker_compare[]
adj_docker_compare['simu_dispatch_cv'] = adj_docker_compare[] / adj_docker_compare[]
# Filter
# 柱數變動太多，無法參考
# 結果浮動太大，不值得信任
# 不切實際
# 成效不佳
# Reshape
adj_docker_compare = adj_docker_compare[[
    'ID', '站名', '週間週末', '天數', 'day_count', '資料可信度',
    '柱數', 'april_capacity', '理想車柱數', '建議調整柱數', '建議初始在站車_柱無限',
    'real_in_dispatch_mean', 'real_out_dispatch_mean',
    'simu_in_dispatch_mean', 'simu_out_dispatch_mean',
    'real_dipatch_bikes', 'simu_dipatch_bikes',
    'simu_m_real_bikes', 'benefit']]
adj_docker_compare.columns = [
    'ID', '站名', '週間週末', '天數', '4月天數', '資料可信度',
    '柱數', '4月柱數', '理想車柱數', '建議調整柱數', '建議初始在站車_柱無限',
    '4月實際入站調度', '4月實際出站調度',
    '4月模擬入站調度(根據TUIC建議)', '4月模擬出站調度(根據TUIC建議)',
    '4月實際調度車輛數(入+出)', '4月模擬調度車輛數(入+出)',
    '4月模擬-實際調度車數', '日效益(調度車數/調整柱數)']
# Save
adj_docker_compare.to_excel(valid_idle_path+'/柱無限驗證結果.xlsx')



# is idel stop in March still idel in April?
idel_verify = simple_idel.merge(april_idel, how='left', on=['ID', '週間週末'])
# Rename
cols = []
for col in idel_verify.columns:
    col = col.replace('_x', '_3月')
    col = col.replace('_y', '_4月')
    cols.append(col)
idel_verify.columns = cols
# Extract
idel_verify['3、4月平均閒置車數'] = (idel_verify['閒置車_3月'] + idel_verify['閒置車_4月'])/2
idel_verify['3月閒置車_cv'] = idel_verify['閒置車std_3月'] / idel_verify['閒置車_3月']
idel_verify['4月閒置車_cv'] = idel_verify['閒置車std_4月'] / idel_verify['閒置車_4月']
# Filter
# 可能有故障、停用、新設立，資料不齊全問題
is_weekday = (idel_verify['週間週末']=='weekday')
is_weekend = ~is_weekday
is_normal_weekday = is_weekday & (idel_verify['天數_3月']==23) & (idel_verify['天數_4月']==20)
is_normal_weekend = is_weekend & (idel_verify['天數_3月']==8) & (idel_verify['天數_4月']==10)
idel_verify = idel_verify.loc[is_normal_weekday | is_normal_weekend]
# 閒置過少，無檢視意義
idel_verify = idel_verify.loc[idel_verify['閒置車_3月']>2]
idel_verify = idel_verify.loc[idel_verify['閒置車_4月']>2]
# 變動程度太大，結果存疑
idel_verify = idel_verify.loc[idel_verify['3月閒置車_cv']<1]
idel_verify = idel_verify.loc[idel_verify['4月閒置車_cv']<1]
# Reshape
idel_verify = idel_verify[['ID', '站名_3月', '週間週末', '閒置車_3月',
                           '閒置車_4月', '3、4月平均閒置車數']]
idel_verify.columns = ['ID', '站名', '週間週末', '閒置車_3月中位數',
                           '閒置車_4月中位數', '3、4月平均閒置車數']
# Save
idel_verify.to_excel(valid_idle_path+'/簡單閒置車驗證結果.xlsx')


# 如果根據3月來調度，4月會適用嗎