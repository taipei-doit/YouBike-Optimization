# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:28:40 2023

@author: rz3881
"""

import pandas as pd
import geopandas as gpd
from sklearn.cluster import DBSCAN
import time

root_path = r'D:\iima\ubike分析'

# load
ubike = pd.read_csv(root_path+'/DM/[txn]202211_txn_only_bike.csv')
ubike_stops = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
ubike_stops['stop_id'] = 'U' + ubike_stops['stop_id'].astype(str)

# 轉換成TWD97，限定距離在200 m之間
geometry = gpd.points_from_xy(ubike_stops['lng'], ubike_stops['lat'], crs=4326)
geometry = geometry.to_crs(crs=3826)
ubike_stops['x_twd97'] = [g.x for g in geometry]
ubike_stops['y_twd97'] = [g.y for g in geometry]
x = ubike_stops[['x_twd97', 'y_twd97']]

# # 找到自己
# my_card = 'A99FB6B51DD7FFC12F61D2D73A3D24BC'
# is_my_card = ubike['card_id']==my_card
# temp = ubike.loc[is_my_card]

# 對每個card_id遍歷，尋找替代的OD
dbscan = DBSCAN(eps=200, min_samples=2)
stops_correlation = pd.DataFrame(0, index=ubike_stops['stop_id'], columns=ubike_stops['stop_id'])
a = 0
time_split_hour = 12
ubike['on_hour'] = pd.to_datetime(ubike['on_time']).dt.hour
start_time = time.time()
for card_id, card_data in ubike.groupby('card_id'):
    break
    # 上午/下午、OD分開找群比較精確
    morning_o = set(card_data.loc[ubike['on_hour'] < time_split_hour, 'on_stop_id'])
    morning_d = set(card_data.loc[ubike['on_hour'] < time_split_hour, 'off_stop_id'])
    afternoon_o = set(card_data.loc[ubike['on_hour'] >= time_split_hour, 'on_stop_id'])
    afternoon_d = set(card_data.loc[ubike['on_hour'] >= time_split_hour, 'off_stop_id'])
    
    all_s = []
    for target_stops in [morning_o, morning_d, afternoon_o, afternoon_d]:
        if len(target_stops) < 2:
            continue
        # 僅2.0站點被計算
        is_target = ubike_stops['stop_id'].isin(target_stops)
        not_more_than_two_v2_stops = is_target.sum() < 2
        if not_more_than_two_v2_stops:
            continue
        
        # 利用DBSCAN分群
        target_x = x.loc[is_target]
        labels = dbscan.fit_predict(target_x)
        stop_ids = ubike_stops.loc[is_target, 'stop_id']
        stop_ids.index = labels
        
        # 紀錄結果
        # 先整合所有要記的，避免重複計入
        for label in set(labels):
            if label != -1:
                stops = set(stop_ids.loc[label])
                for s1 in stops:
                    for s2 in stops:
                        if s1 != s2:
                            all_s.append((s1, s2))
    # 實際錄入
    unique_all_s = set(all_s)
    # print(unique_all_s)
    if len(unique_all_s) >= 1:
        for s1, s2 in unique_all_s:
            stops_correlation.loc[s1, s2] += 1
    
    # print(card_id)
    # print(labels)
    if (a % 10000) == 0:
        print(a)
        print(time.time() - start_time)
    a += 1
# 跑一次112分鐘
stops_correlation.to_csv(root_path+'/DM/[cor]stops_dbscan_within_same_group_times_by_cardid_v2.csv',
                         encoding='UTF-8')

# 雨露均霑式分配，每次最多分配一個站，從關係最強的站開始分配，分後不放回
min_cortime = 150
groups = {} # 結果{picker: [been_picked, been_picked]}
# 沒有夠強關聯的不用挑選
stop_cor = stops_correlation.max(axis=0).sort_values(ascending=False)
is_weak_cor = stop_cor < min_cortime
picker_with_no_cor_stop = stop_cor.loc[is_weak_cor].index.tolist()
for s in picker_with_no_cor_stop:
    groups[s] = []
# 挑選
picker_list = stop_cor.loc[~is_weak_cor].index.tolist()
all_been_picked = [] # 已經被選中的人
iter_num = 0
while True:
    # 記錄每輪挑選成功次數
    pick_count = 0
    # 遍歷所有的人，讓每個人依次選擇
    for picker in picker_list:
        not_valid_picker = (picker in all_been_picked)
        if not_valid_picker:
            continue
        
        # 產生候選站名單
        candidate = stops_correlation[picker]
        candidate = candidate.loc[candidate>=min_cortime]
        candidate = candidate.sort_values(ascending=False).index
        i = 0
        # 挑選合格站
        while True:
            no_candidate = (i >= len(candidate))
            if no_candidate:
                been_picked = None
                break
            else:
                been_picked = candidate[i]
                not_been_picked = (been_picked not in all_been_picked)
                not_picker = (groups.get(been_picked) is None)
                is_valid = not_been_picked & not_picker
                if is_valid:
                    break
                else:
                    i += 1
        # 紀錄挑選成功的結果，並刪除已被選擇的人
        if been_picked:
            pick_count += 1
            all_been_picked.append(been_picked)
            if groups.get(picker) is None:
                groups[picker] = []
            groups[picker].append(been_picked)
        else: # 有候選，但被選完了
            if groups.get(picker) is None:
                groups[picker] = []

        
    iter_num += 1
    # 本輪無任何挑選
    if pick_count == 0:
        break

def check_results_number(ubike_stops, groups):
    right_num = ubike_stops.shape[0]
    results_num = 0
    for k, v in groups.items():
        results_num += (len(v)+1)
    print(right_num, results_num)
    return right_num==results_num
check_results_number(ubike_stops, groups)

# save
labels = {'stop_id': [], 'label': []}
label = 0
ungroup_label = '未分群'
for picker, been_picked in groups.items():
    no_picked = len(been_picked)==0
    if no_picked:
        labels['stop_id'].append(picker)
        labels['label'].append(ungroup_label)
    else:
        temp_label = 'cor_' + str(label).zfill(3)
        labels['stop_id'].append(picker)
        labels['label'].append(temp_label)
        for picked in been_picked:
            labels['stop_id'].append(picked)
            labels['label'].append(temp_label)
        label += 1
labels = pd.DataFrame(labels)
labels.to_csv(root_path+'/DM/[PBI][cor]kmean_label_v2.csv',
              index=False, encoding='UTF-8')
