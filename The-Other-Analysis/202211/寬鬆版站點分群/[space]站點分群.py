# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:28:40 2023

@author: rz3881
"""

import pandas as pd
import geopandas as gpd
from sklearn.cluster import DBSCAN
root_path = r'D:\iima\ubike分析'

# 加入捷運站作為關鍵節點

# load
ubike_stops = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
ubike_stops['stop_id'] = 'U' + ubike_stops['stop_id'].astype(str)

# 只需要2.0
ubike_stops = ubike_stops.loc[ubike_stops['service_type']==2.0]

# 轉換成TWD97，限定距離在250 m之間
geometry = gpd.points_from_xy(ubike_stops['lng'], ubike_stops['lat'], crs=4326)
geometry = geometry.to_crs(crs=3826)
ubike_stops['x_twd97'] = [g.x for g in geometry]
ubike_stops['y_twd97'] = [g.y for g in geometry]

# reshape data
x = ubike_stops[['x_twd97', 'y_twd97']]

# DBSCAN
dbscan = DBSCAN(eps=200, min_samples=2)
labels = dbscan.fit_predict(x)
labels = pd.Series(labels)
labels.index = ubike_stops['stop_id']
labels = labels.reset_index()
labels.columns = ['stop_id', 'label']
labels['label'].value_counts()
is_ungrouped = (labels['label']==-1)
labels.loc[is_ungrouped, 'label'] = '未分群'
labels.loc[~is_ungrouped, 'label'] = 'space_' + labels.loc[~is_ungrouped, 'label'].astype(str).str.zfill(3)

len(labels)
len(set(labels['label']))
sum(labels.value_counts() > 10)

# save
file_path = '/DM/站點分群_寬鬆/[PBI][space]dbscan_label.csv'
labels.to_csv(root_path+file_path, index=False, encoding='UTF-8')
