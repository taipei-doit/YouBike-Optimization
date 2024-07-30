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

# 對每個card_id遍歷
dbscan = DBSCAN(eps=200, min_samples=2)
stops_correlation = pd.DataFrame(0, index=ubike_stops['stop_id'], columns=ubike_stops['stop_id'])
a = 0
t = time.time()
for card_id, card_data in ubike.groupby('card_id'):
    # 找到交易涉及到的所有站點
    target_stops = set(card_data['on_stop_id']) | set(card_data['off_stop_id'])
    is_target = ubike_stops['stop_id'].isin(target_stops)
    not_more_than_two_v2_stops = is_target.sum() < 2
    if not_more_than_two_v2_stops:
        continue
    target_x = x.loc[is_target]
    # 利用DBSCAN分群
    labels = dbscan.fit_predict(target_x)
    labels = pd.Series(labels)
    labels.index = ubike_stops.loc[is_target, 'stop_id']
    labels = labels.reset_index()
    labels.columns = ['stop_id', 'label']
    # 紀錄結果
    for label, label_data in labels.groupby('label'):
        if label != -1:
            stops = set(label_data['stop_id'])
            for s1 in stops:
                for s2 in stops:
                    if s1 != s2:
                        stops_correlation.loc[s1, s2] += 1
    # print(card_id)
    # print(labels)
    if a == 10000:
        print(a)
        print(time.time() - t)
    a += 1
# 跑一次約40分鐘
stops_correlation.to_csv(root_path+'/DM/[cor]stops_dbscan_within_same_group_times_by_cardid.csv')

# 選定capacity較大做為主站
# 設定門檻將各站分群
stop_capacity = ubike_stops.sort_values('capacity', ascending=False)
labels = {}
label = 0
ungroup_label = -1
# freq_threshould = 100
max_number = 5
for stop_id in stop_capacity['stop_id']:
    if labels.get(stop_id):
        continue
    else:
        # break
        temp = stops_correlation.loc[stop_id]
        temp = temp.sort_values(ascending=False).iloc[0:max_number]
        # is_frequently = temp >= freq_threshould
        fit_stops = set(temp.index) | set([stop_id])
        num_of_ele_already_grouped = len([0 for fs in fit_stops if labels.get(fs)])
        is_outlier = (len(fit_stops)-num_of_ele_already_grouped)<=1
        if is_outlier:
            labels[stop_id] = ungroup_label
            continue
        else:
            for fs in fit_stops:
                if labels.get(fs):
                    continue
                else:
                    labels[fs] = label
            label += 1

labels = pd.DataFrame(labels, index=[0]).T.reset_index()
labels.columns = ['stop_id', 'label']
labels.to_csv(root_path+'/DM/[PBI][cor]kmean_label.csv', index=False)
