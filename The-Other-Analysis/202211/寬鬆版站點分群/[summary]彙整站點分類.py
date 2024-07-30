# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 16:34:53 2023

@author: rz3881
"""

import pandas as pd 

root_path = r'D:\iima\ubike分析'

# 根據已分好的
# 把未分的根據DBSCAN分一分

# load  
file_path = '/DM/站點分群_嚴格/[PBI][summary]clustering_results.csv'
stops_label = pd.read_csv(root_path+file_path)
file_path = '/DM/站點分群_寬鬆/[PBI][space]dbscan_label.csv'
space_labels = pd.read_csv(root_path+file_path)
stops_label['stop_id']

# 
stops_label = stops_label.merge(space_labels, on='stop_id', how='left')
stops_label.columns = ['stop_id', 'stop_name', 'dist', 'capacity',
                        'lat', 'lng', 'service_type',
                        'label_cor', 'label_txn', 'label_status', 'label_space',
                        'sum_cor', 'label_txn_refactor', 'label_strict',
                        'label_space_200']

stops_label['stop_id']

# ungrouped根據label_space_200再聚一點
# config
individual_label = '獨立站'
critical_label = '需關注站'
ungrouped_space_label = '未分群'
is_special_group = stops_label['label_strict'].isin([individual_label, critical_label])
normal_label = stops_label.loc[~is_special_group, 'label_strict']
normal_label_seq = normal_label.str.split('_').apply(lambda x: x[1]).astype(int)
max_group_label = normal_label_seq.max()
label_seq = max_group_label+1
stops_label.index = stops_label['stop_id']

# cluster
stops_label['label_loose'] = stops_label['label_strict'].copy()
is_individual = (stops_label['label_loose']==individual_label)
individual_stop = stops_label.loc[is_individual]
individual_stop_len = individual_stop.shape[0]
results = {} # {group: label}
for stop_id in individual_stop['stop_id']:
    # break
    is_grouped = results.get(stop_id)
    if is_grouped:
        continue
    
    # find space label
    sl = individual_stop.loc[stop_id, 'label_space_200']
    is_sl_grouped = (sl!=ungrouped_space_label)
    if is_sl_grouped:
        is_same_sl = (individual_stop['label_space_200']==sl)
    
        target_stop_id = individual_stop.loc[is_same_sl].index.tolist()
        is_not_only_self = (len(target_stop_id)>1)
        if is_not_only_self:
            for sid in target_stop_id:
                results[sid] = 'group_' + str(label_seq).zfill(3)
            label_seq += 1
stops_label.loc[is_individual, 'label_loose'] = stops_label.loc[is_individual, 'stop_id'].map(results)
stops_label.loc[stops_label['label_loose'].isna(), 'label_loose'] = individual_label

temp = stops_label['label_loose'].value_counts()

# save
file_path = '/DM/站點分群_寬鬆/[PBI][summary]clustering_results.csv'
stops_label.to_csv(root_path+file_path, index=False, encoding='UTF-8')
