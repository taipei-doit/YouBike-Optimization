# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 14:07:59 2022

@author: rz3881
"""

# 檢視交易資料，辨識轉乘行為
# 轉乘 = 本次下車與下次上車的行為是一趟旅途，是為了達到某個目的地的連續行為

import shutil
import pandas as pd
import geopandas as gpd
import os
import time
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import delete_input_data, make_sure_folder_exist
import warnings
warnings.filterwarnings("ignore")
from numpy import nan


def calcu_distance(data, is_transfer):
    '算距離來排除過快的移動，距離 = 上次下車->下次上車'
    # save previous loc to calcu distance
    data['pre_off_stop'] = data['off_stop'].shift(1)
    data['pre_off_lng'] = data['off_lng'].shift(1)
    data['pre_off_lat'] = data['off_lat'].shift(1)
    # 這邊只把有轉乘的拿出來，不然跑太久。
    tdata = data.loc[is_transfer].copy() 
    # 上次下車
    tdata['pre_off_lng'] = pd.to_numeric(tdata['pre_off_lng'], errors='coerce')
    tdata['pre_off_lat'] = pd.to_numeric(tdata['pre_off_lat'], errors='coerce')
    pre_off_points = gpd.points_from_xy(tdata['pre_off_lng'], tdata['pre_off_lat'], crs='EPSG:4326')
    tdata['pre_off_points'] = pre_off_points.to_crs(epsg='3826')
    tdata['pre_off_lat_dis'] = tdata['pre_off_points'].map(lambda point: point.x if point else nan)
    tdata['pre_off_lng_dis'] = tdata['pre_off_points'].map(lambda point: point.y if point else nan)
    # 本次上車
    tdata['on_lng'] = pd.to_numeric(tdata['on_lng'], errors='coerce')
    tdata['on_lat'] = pd.to_numeric(tdata['on_lat'], errors='coerce')
    on_points = gpd.points_from_xy(tdata['on_lng'], tdata['on_lat'], crs='EPSG:4326')
    tdata['on_points'] = on_points.to_crs(epsg='3826')
    tdata['on_lat_dis'] = tdata['on_points'].map(lambda point: point.x if point else nan)
    tdata['on_lng_dis'] = tdata['on_points'].map(lambda point: point.y if point else nan)
    # 計算距離
    tdata['distance'] = ((tdata['on_lng_dis']-tdata['pre_off_lng_dis'])**2 + (tdata['on_lat_dis']-tdata['pre_off_lat_dis'])**2) ** 0.5 # 公尺
    return tdata['distance']


def calcu_diff_time(data):
    prev_off_time = data['off_time'].shift(1)
    time_diff_mins = round((data['on_time'] - prev_off_time).dt.total_seconds()/60, 1)
    return time_diff_mins


def calcu_speed(data):
    '檢測速度，只留下合理速度=低於speed_threshould'
    speed = data['distance_m'] / data['time_diff_mins']
    return speed


def identify_transfer(data, check_mrt_to_mrt=True, drop_speed_too_fast=True):
    t = time.time()
    print(f'  {data.shape[0]} rows data.')
    # identify continue txn by time
    max_trans_time = 30*60 # seconds
    min_trans_time = 5
    # rule 0: same card_id 
    pre_card_id = data['card_id'].shift(1).copy()
    is_same_id = (data['card_id'] == pre_card_id)
    # rule 1: short time diff between off stop and next on stop
    pre_off_time = data['off_time'].shift(1).copy()
    time_delta = (data['on_time'] - pre_off_time).dt.total_seconds()
    is_in_30mins = ((time_delta<max_trans_time) & (time_delta>min_trans_time))
    # summary
    is_transfer = is_in_30mins & is_same_id
    # 要不要排除捷運轉捷運
    if check_mrt_to_mrt:
        pre_trans_type = data['trans_type'].shift(1).copy()
        not_both_mrt = ~((data['trans_type']=='mrt') & (pre_trans_type=='mrt'))
        is_transfer = is_transfer & not_both_mrt
    # 要不要排除移動過快的行為
    # 平均老年人步行的速度是3.2 km/h ~ 3.9 km/h，年輕人則為3.75 km/h ~ 5.43 km/h，這邊採用最寬鬆的5.43
    speed_threshould = 5.43 * 1000 / 60  # km/hr => m/minute
    if drop_speed_too_fast:
        data['distance_m'] = 1e-8
        data.loc[is_transfer, 'distance_m'] = calcu_distance(data, is_transfer) # 轉乘站間的距離計算
        data['time_diff_mins'] = calcu_diff_time(data)
        data['speed'] = calcu_speed(data) # 剔除速度過快的轉乘行為
        is_reasonable_speed = (data['speed']<speed_threshould) & (data['speed']>0.1)
        # 如果轉乘時間<5分鐘，不檢查速度
        is_time_short = (data['time_diff_mins'] < 5*60)
        is_transfer = is_transfer & (is_reasonable_speed|is_time_short)
        
    print(f'  Identify transfer cost: {time.time() - t} seconds.')
    return is_transfer


def mark_journey_id(input_data): 
    # 添加旅程ID 與 流程 起點O、轉乘T、終點D，用作未來以旅程為主體做分析
    # 旅程ID 命名邏輯: card_id + 日期 + 第幾趟旅程, ex: 106167857_20220201_001, 106167857_202200201_002
    t = time.time()
    data = input_data.copy()
    # transfer_mark
    next_card_id = data['card_id'].shift(-1)
    is_next_new_cardid = (data['card_id'] != next_card_id)
    
    next_is_transfer = data['is_transfer'].shift(-1)
    next_is_transfer.iloc[-1] = False
    next_is_transfer = next_is_transfer.astype(bool)
    is_next_new_jour = ~next_is_transfer
    # O
    data['transfer_mark_test'] = 'O'
    # T
    data.loc[data['is_transfer'], 'transfer_mark_test'] = 'T'
    data.loc[next_is_transfer, 'transfer_mark_test'] += 'T'
    # D
    is_end = (is_next_new_cardid|is_next_new_jour)
    data.loc[is_end, 'transfer_mark_test'] += 'D'
    
    # seq
    pre_card_id = data['card_id'].shift(1)
    is_new_card_id = (data['card_id'] != pre_card_id)
    seq = []
    a = 1
    for is_new, mark in zip(is_new_card_id, data['transfer_mark_test']):
        if is_new:
            a = 1
        else:
            if mark[0]=='O':
                a += 1
        seq.append(a)
    seq = pd.Series(seq)
    
    # id
    seq_str = seq.astype(str).str.zfill(3)
    data['journey_ids'] = data['card_id'] +'_'+ str_date +'_'+ seq_str
    
    print(f'  Mark journey id cost: {time.time() - t} seconds.')
    return data['journey_ids'], data['transfer_mark_test']


# 優化選項之一，因為group data的迴圈似乎無法避免
# 也許只能把不需要迴圈的無轉乘交易拉出來加速
def extract_journey_df_from_not_trans(not_tdata, journey_data):
    '無轉乘資料處理'
    not_tdata['txn_count'] = 1
    not_tdata['first_on_time'] = not_tdata['on_time']
    not_tdata['first_on_lng'] = not_tdata['on_lng']
    not_tdata['first_on_lat'] = not_tdata['on_lat']
    not_tdata['last_off_time'] = not_tdata['off_time']
    not_tdata['last_off_lng'] = not_tdata['off_lng']
    not_tdata['last_off_lat'] = not_tdata['off_lat']
    
    not_tdata['first_on_time'] = not_tdata['on_time']
    not_tdata['first_on_time'] = not_tdata['on_time']
    not_tdata['first_on_time'] = not_tdata['on_time']
    
    not_tdata['journey_stop_name_chain'] = not_tdata['on_stop'] + '-' + not_tdata['off_stop']
    not_tdata['journey_stop_id_chain'] = not_tdata['on_stop_id'] + '-' + not_tdata['off_stop_id']
    not_tdata['vehicle_chain'] = not_tdata['trans_type']
    
    select_col = list(journey_data.keys())
    not_tdata = not_tdata[select_col]
    return not_tdata


def extract_journey_df_from_trans(tdata, journey_data):
    ''
    t = time.time()
    a = 0
    for journey_id, group_data in tdata.groupby('journey_id'):
        # break
        journey_data['journey_id'].append(journey_id)
        
        card_id = group_data['card_id'].iloc[0]
        journey_data['card_id'].append(card_id)
        
        txn_count = group_data.shape[0]
        journey_data['txn_count'].append(txn_count)
        
        first_on_time = group_data['on_time'].iloc[0]
        journey_data['first_on_time'].append(first_on_time)
        
        first_on_lng = group_data['on_lng'].iloc[0]
        journey_data['first_on_lng'].append(first_on_lng)
        
        first_on_lat = group_data['on_lat'].iloc[0]
        journey_data['first_on_lat'].append(first_on_lat)
        
        last_off_time = group_data['off_time'].iloc[-1]
        journey_data['last_off_time'].append(last_off_time)
        
        last_off_lng = group_data['off_lng'].iloc[-1]
        journey_data['last_off_lng'].append(last_off_lng)
        
        last_off_lat = group_data['off_lat'].iloc[-1]
        journey_data['last_off_lat'].append(last_off_lat)
        
        on_to_off_stop_name = group_data['on_stop'] + '-' + group_data['off_stop']
        journey_stop_name_chain = ','.join(on_to_off_stop_name)
        journey_data['journey_stop_name_chain'].append(journey_stop_name_chain)
        
        on_to_off_stop_id = group_data['on_stop_id'] + '-' + group_data['off_stop_id']
        journey_stop_id_chain = ','.join(on_to_off_stop_id)
        journey_data['journey_stop_id_chain'].append(journey_stop_id_chain)
        
        vehicle_chain = ','.join(group_data['trans_type'])
        journey_data['vehicle_chain'].append(vehicle_chain)
        
        # on off stop and location
        #...
        
        if (a % 100000) == 0:
            print(f'  Loop {a}, cost {time.time()-t} secs')
        a += 1
    
    jdata = pd.DataFrame(journey_data)
    print(f'  trans data {jdata.shape[0]} rows, cost {time.time()-t} secs')
    return jdata


def extract_journey_df(data): # 計算瓶頸 需優化
    '將原始資料轉換成以journey_id為pk的agg表格'
    t = time.time()
    
    journey_data = {'journey_id': [], 'card_id': [], 'txn_count': [],
                    'first_on_time': [], 'first_on_lng': [], 'first_on_lat': [],
                    'last_off_time': [], 'last_off_lng': [], 'last_off_lat': [],
                    'journey_stop_name_chain': [], 'journey_stop_id_chain': [],
                    'vehicle_chain': []}
    # 分離有無轉乘再後續合併，為了優化速度
    not_tdata = data.loc[~data['is_transfer']].copy()
    journey_data_not_trans = extract_journey_df_from_not_trans(not_tdata, journey_data)
    
    tdata = data.loc[data['is_transfer']].copy()
    journey_data_is_trans = extract_journey_df_from_trans(tdata, journey_data)
        
    journey_df = pd.concat([journey_data_not_trans, journey_data_is_trans])
    print(f'  extract journey df cost: {time.time() - t} seconds.')
    return journey_df
    

# Config
root_path = r'D:\iima\ubike分析'
ym = '202403'
txn_path = f'{root_path}/DM/{ym}/prepared_data/txn'
input_path = f'{txn_path}/cleaned_daily_concated'
transfer_df_path = f'{txn_path}/identified_transfer'
journey_df_path = f'{txn_path}/journey_df'

is_delete_input = False
is_clean_output = False

# clean output folder before save data
if is_clean_output:
    shutil.rmtree(transfer_df_path)
    os.mkdir(transfer_df_path)
    shutil.rmtree(journey_df_path)
    os.mkdir(journey_df_path)

# output
dates = os.listdir(input_path)
for str_date in dates:
    # break
    # if int(str_date) < 20221122:
    #     continue
    
    t = time.time()
    print(f'\nLoad {str_date} data......')
    input_file_path = f'{input_path}/{str_date}/ubike_bus_mrt.pkl'
    data = pd.read_pickle(input_file_path)
    data = data.sort_values(['card_id', 'off_time'])
    data = data.reset_index(drop=True)
    data.loc[data['on_stop'].isna(), 'on_stop'] = ''
    data.loc[data['off_stop'].isna(), 'off_stop'] = ''
    data['is_transfer'] = identify_transfer(data)
    data['journey_id'], data['transfer_mark'] = mark_journey_id(data)
    data['is_transfer'] = data['transfer_mark'].str.contains('T')
    
    journey_df = extract_journey_df(data)
    journey_df['is_transfer'] = (journey_df['txn_count'] > 1)
    journey_df = journey_df.sort_values(['journey_id'])
    journey_df = journey_df.reset_index(drop=True)
    
    data = data.drop(columns=['pre_off_stop', 'pre_off_lng', 'pre_off_lat'])
    make_sure_folder_exist(transfer_df_path)
    data.to_pickle(f'{transfer_df_path}/{str_date}.pkl')
    make_sure_folder_exist(journey_df_path)
    journey_df.to_pickle(f'{journey_df_path}/{str_date}.pkl')
    if is_delete_input:
        delete_input_data(input_file_path)
    print(f'  Saved data. Total cost: {time.time() - t} seconds.')





