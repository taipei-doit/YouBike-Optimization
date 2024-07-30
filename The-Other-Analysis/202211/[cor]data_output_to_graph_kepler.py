# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 19:36:01 2023

@author: rz3881
"""

import pandas as pd
import networkx as nx

root_path = r'D:\iima\ubike分析'

# load
ubike_stops = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
ubike_stops['stop_id'] = 'U' + ubike_stops['stop_id'].astype(str)
file_path = '/DM/[cor]stops_dbscan_within_same_group_times_by_cardid_v2.csv'
stops_correlation = pd.read_csv(root_path+file_path)
stops_correlation.index = stops_correlation.iloc[:, 0]
stops_correlation = stops_correlation.iloc[:, 1:]
# 將關係映射到graph
G = nx.Graph()
# add nodes
for _, row in ubike_stops.iterrows():
    G.add_node(row['stop_id'], name=row['stop_name'], dist=row['dist'],
               capacity=row['capacity'], lat=row['lat'], lng=row['lng'],
               service_type=row['service_type'])
# add edges
# 僅遍歷下三角部份矩陣，但因為無方向性，誰O誰D不重要
df_len = stops_correlation.shape[0]
stop_ids = stops_correlation.columns.tolist()
for i in range(df_len):
    for j in range(i+1, df_len):
        # print(i, j)
        coexistence_times_by_card = stops_correlation.iloc[i, j]
        if coexistence_times_by_card != 0:
            G.add_edge(stop_ids[i], stop_ids[j], weight=coexistence_times_by_card)
# save
nx.write_gpickle(G, root_path+'/DM/coexistence_times_graph.gpickle')

# 輸出給kepler畫圖
# 站點替代強度
relat = {'origin': [], 'destination': [], 'times': []}
for o, d in G.edges:
    # print(o, d)
    relat['origin'].append(o)
    relat['destination'].append(d)
    relat['times'].append(G[o][d]['weight'])
relat_df = pd.DataFrame(relat)
relat_df = relat_df.merge(ubike_stops[['stop_id', 'lng', 'lat']], how='left',
                          right_on='stop_id', left_on='origin')
relat_df = relat_df.merge(ubike_stops[['stop_id', 'lng', 'lat']], how='left',
                          right_on='stop_id', left_on='destination')
relat_df.columns = ['origin', 'destination', 'times',
                    'stop_id_o', 'lng_o', 'lat_o',
                    'stop_id_o', 'lng_d', 'lat_d']
relat_df.to_csv(root_path+'/DM/[kepler][cor]coexistence_times_in_df.csv',
                index=False, encoding='UTF-8')
# 站點總替代指標
cor_index = stops_correlation.sum(axis=0).reset_index()
cor_index.columns = ['stop_id', 'sum_cor']
stop_info = ubike_stops.merge(cor_index, how='outer', on='stop_id')
stop_info.to_csv(root_path+'/DM/[kepler][cor]coexistence_sum_by_stop.csv',
                index=False, encoding='UTF-8')