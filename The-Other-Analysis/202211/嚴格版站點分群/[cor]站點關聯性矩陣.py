# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 22:53:11 2023

@author: rz3881
"""

import pandas as pd
import geopandas as gpd
from sklearn.cluster import DBSCAN
import time

root_path = r'D:\iima\ubike分析'

# load
file_path = '/DM/站點分群_共用/[txn]202211_txn_only_bike.csv'
ubike = pd.read_csv(root_path+file_path)
ubike_stops = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
ubike_stops['stop_id'] = 'U' + ubike_stops['stop_id'].astype(str)

# 轉換成TWD97，限定距離在200 m之間
geometry = gpd.points_from_xy(ubike_stops['lng'], ubike_stops['lat'], crs=4326)
geometry = geometry.to_crs(crs=3826)
ubike_stops['x_twd97'] = [g.x for g in geometry]
ubike_stops['y_twd97'] = [g.y for g in geometry]
x = ubike_stops[['x_twd97', 'y_twd97']]

# 對每個card_id遍歷，尋找替代的OD
dbscan = DBSCAN(eps=200, min_samples=2)
stops_coexistence = pd.DataFrame(0, index=ubike_stops['stop_id'], columns=ubike_stops['stop_id'])
a = 0
time_split_hour = 12
ubike['on_hour'] = pd.to_datetime(ubike['on_time']).dt.hour
start_time = time.time()
for card_id, card_data in ubike.groupby('card_id'):
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
            stops_coexistence.loc[s1, s2] += 1
    
    # print(card_id)
    # print(labels)
    if (a % 10000) == 0:
        print(a)
        print(time.time() - start_time)
    a += 1
# 跑一次112分鐘
file_path = '/DM/站點分群_嚴格/[cor]stops_coexistence_matrix.csv'
stops_coexistence.to_csv(root_path+file_path, encoding='UTF-8')


# 站點總共現
stops_sum_cor = stops_coexistence.sum().sort_values(ascending=False).reset_index()
stops_sum_cor.columns = ['stop_id', 'sum_cor']
file_path = '/DM/站點分群_嚴格/[cor]stops_sum_coexistence.csv'
stops_sum_cor.to_csv(root_path+file_path, encoding='UTF-8', index=False)
