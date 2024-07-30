# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 16:34:53 2023

@author: rz3881
"""

import pandas as pd 

root_path = r'D:\iima\ubike分析'

# load
stops_info = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
stops_info['stop_id'] = 'U' + stops_info['stop_id'].astype(str)
label_cor = pd.read_csv(root_path+'/DM/[PBI][cor]kmean_label.csv')
label_txn = pd.read_csv(root_path+'/DM/[PBI][txn]kmean_label_by_weekdayhour.csv')
label_status = pd.read_csv(root_path+'/DM/[PBI][status]kmean_label_by_weekdayperiod.csv')
label_space = pd.read_csv(root_path+'/DM/[PBI][space]kmean_label.csv')

# merge
stops_label = stops_info.merge(label_cor, how='left', on='stop_id')
stops_label = stops_label.merge(label_txn, how='left', on='stop_id')
stops_label = stops_label.merge(label_status, how='left', on='stop_id')
stops_label = stops_label.merge(label_space, how='left', on='stop_id')
stops_label.columns = ['stop_id', 'stop_name', 'dist', 'capacity',
                       'lat', 'lng', 'service_type',
                       'label_cor', 'label_txn', 'label_status', 'label_space']
stops_label.to_csv(root_path+'/DM/[PBI][summary]clustering_results.csv', index=False)

# # 聚焦松山區
# stops_label = stops_label.loc[stops_label['dist']=='松山區']

# rename label
txn_label_map = {0: '偏居住', 1: '明顯工作', 2: '明顯居住', 3: '混和', 4: '偏工作'}
stops_label['label_txn'] = stops_label['label_txn'].map(txn_label_map)
status_label_map = {0: '小空', 1: '早滿晚空', 3: '超級空', 2: '早空晚滿', 4: '早滿晚微空', 5: '微微空'}
stops_label['label_status'] = stops_label['label_status'].map(status_label_map)
stops_label['label_cor'] = stops_label['label_cor'].astype(str)
stops_label['label_space'] = stops_label['label_space'].astype(str)

# 規則
# 確定的 = 借還時間類似且有可替代性
txn_label_map = {'偏工作': 'work', '明顯工作': 'work', '明顯居住': 'home', '混和': 'mix', '偏居住': 'home'}
stops_label['label_txn_refactor'] = stops_label['label_txn'].map(txn_label_map)
stops_label.index = stops_label['stop_id']

stops_label['group'] = -1
g_index = 0
for stop_id in stops_label['stop_id']:
    # break
    not_grouped = stops_label.loc[stop_id, 'group'] == -1
    if not_grouped:
        txn_label = stops_label.loc[stop_id, 'label_txn_refactor']
        is_same_txn_label = stops_label['label_txn_refactor']==txn_label
        cor_label = stops_label.loc[stop_id, 'label_cor']
        if cor_label == '-1':
            continue
        is_same_cor_label = stops_label['label_cor']==cor_label
        is_same_group = is_same_txn_label & is_same_cor_label
        if (is_same_group).sum() < 2:
            continue
        else:
            stops_label.loc[is_same_group, 'group'] = g_index
            g_index += 1
stops_label.to_csv(root_path+'/DM/[PBI][summary]final_clustering_results.csv', index=False, encoding='UTF-8')
    