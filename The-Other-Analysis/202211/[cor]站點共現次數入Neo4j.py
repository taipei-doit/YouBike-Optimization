# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 15:23:38 2023

@author: rz3881
"""

import pandas as pd
from py2neo import Graph, Node, Relationship, Subgraph

# 需先手動啟動DB，才連得到
# local
graph = Graph("bolt://localhost:7687", auth=('ubike', 'ubike0323'))
root_path = r'D:\iima\ubike分析'

# load
stops_correlation = pd.read_csv(root_path+'/DM/[cor]stops_dbscan_within_same_group_times_by_cardid_v2.csv')
ubike_stops = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
ubike_stops['stop_id'] = 'U' + ubike_stops['stop_id'].astype(str)

# 將關係映射到graph
s = Subgraph()
# add nodes
nodes = {}
for _, row in ubike_stops.iterrows():
    n = Node(row['stop_id'], name=row['stop_name'], dist=row['dist'],
               capacity=row['capacity'], lat=row['lat'], lng=row['lng'],
               service_type=row['service_type'])
    nodes[row['stop_id']] = n
# add edges
df_len = stops_correlation.shape[0]
stop_ids = stops_correlation.columns.tolist()
for i in range(df_len):
    for j in range(i+1, df_len):
        # print(i, j)
        coexistence_times_by_card = stops_correlation.iloc[i, j]
        if coexistence_times_by_card != 0:
            e = Relationship(nodes[stop_ids[i]], 'coexistence', nodes[stop_ids[j]],
                             weight=coexistence_times_by_card)
            s = s | e

# truncate then insert
# delete all graph
graph.delete_all()
# 將節點和關係通過關係運算符合併為一個子圖，再寫入資料庫
graph.create(s)