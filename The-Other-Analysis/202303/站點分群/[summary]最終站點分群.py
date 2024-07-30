# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 15:24:20 2023

@author: rz3881
"""

import pandas as pd 

def show_group_results(labels):
    num_of_groups = len(set(labels))
    group_size = labels.value_counts()
    print(f'num_of_groups: {num_of_groups}')
    print(group_size)

root_path = r'D:\iima\ubike分析'
cor_path = root_path+r'\DM\202303\prepared_data\cor'
cluster_path = root_path+r'\DM\202303\站點分群'

# load
stops_label = pd.read_csv(cluster_path+'/[summary][PBI]sub_clustering_renaming.csv')

# 建議版分群
# config
ungrouped_label = ''
individual_label = '獨立站'
critical_label = '需關注站'
stops_label['label_suggest'] = ungrouped_label
label_seq = 0
ungrouped_space_label = '未分群'
ungrouped_cor_label = '未分群'
txn_neutral_label = '借還平衡'
# 特別站特別處理
is_txn_strange = (stops_label['label_txn'] == '只借不還')
is_status_strange = (stops_label['label_status'].isin(['全時段無車也無位', '全時段無車']))
is_critical = (is_txn_strange | is_status_strange)
stops_label.loc[is_critical, 'label_suggest'] = critical_label
# 其他分群判斷
is_ungrouped = (stops_label['label_suggest']==ungrouped_label)
ungrouped_stop = stops_label.loc[is_ungrouped]
ungrouped_stop.index = ungrouped_stop['stop_id']
ungrouped_stop_len = ungrouped_stop.shape[0]
results = {} # {group: label}
for stop_id in ungrouped_stop['stop_id']:
    # break
    is_grouped = results.get(stop_id)
    if is_grouped:
        continue
    
    # find cor label
    cl = ungrouped_stop.loc[stop_id, 'label_cor']
    is_cl_ungrouped = (cl==ungrouped_cor_label)
    if is_cl_ungrouped:
        is_same_cl = [False] * ungrouped_stop_len
    else:
        is_same_cl = (ungrouped_stop['label_cor']==cl)
    
    # find space label
    sl = ungrouped_stop.loc[stop_id, 'label_space_suggest']
    is_sl_ungrouped = (sl==ungrouped_space_label)
    if is_sl_ungrouped:
        is_same_sl = [False] * ungrouped_stop_len
    else:
        is_same_sl = (ungrouped_stop['label_space_suggest']==sl)

    # find txn label
    tl = ungrouped_stop.loc[stop_id, 'label_txn_refactor']
    is_neutral_label = (tl == txn_neutral_label)
    if is_neutral_label: # 無特徵站可以與任何站聚群
        is_same_tl = [True] * ungrouped_stop_len
    else:
        is_same_label = (ungrouped_stop['label_txn_refactor']==tl)
        is_neutral = (ungrouped_stop['label_txn_refactor']==txn_neutral_label)
        is_same_tl = is_same_label | is_neutral
    
    # 有實際交易中的替代性行為
    # 超級近(因為是stop_id，有些分兩個id實際上很近。且有可能某站狀良好，實際交易無替代行為)
    # 調度時間類似，減少調度才有可行性(有的是早上要補、有的是晚上，無明顯特徵不受限)
    # 若cor且space無任何同群站，推定獨立站
    is_individual_group = is_cl_ungrouped and is_sl_ungrouped
    if is_individual_group:
        results[stop_id] = individual_label
    else:
        is_grouped = ungrouped_stop['label_suggest']
        is_target = (is_same_cl | is_same_sl) & is_same_tl
        is_only_self = (is_target.sum()==1)
        if is_only_self:
            results[stop_id] = individual_label
        else:
            target_stop_id = is_target.loc[is_target].index.tolist()
            for sid in target_stop_id:
                results[sid] = 'group_' + str(label_seq).zfill(3)
            label_seq += 1
stops_label.loc[is_ungrouped, 'label_suggest'] = stops_label.loc[is_ungrouped, 'stop_id'].map(results)
show_group_results(stops_label['label_suggest'])


# 折衷版分群，ungrouped根據label_space_compromise再聚一點
# config
individual_label = '獨立站'
critical_label = '需關注站'
ungrouped_space_label = '未分群'
is_special_group = stops_label['label_suggest'].isin([individual_label, critical_label])
normal_label = stops_label.loc[~is_special_group, 'label_suggest']
normal_label_seq = normal_label.str.split('_').apply(lambda x: x[1]).astype(int)
max_group_label = normal_label_seq.max()
label_seq = max_group_label+1
stops_label.index = stops_label['stop_id']
# cluster
stops_label['label_compromise'] = stops_label['label_suggest'].copy()
is_individual = (stops_label['label_compromise']==individual_label)
individual_stop = stops_label.loc[is_individual]
individual_stop_len = individual_stop.shape[0]
results = {} # {group: label}
for stop_id in individual_stop['stop_id']:
    # break
    is_grouped = results.get(stop_id)
    if is_grouped:
        continue
    
    # find space label
    sl = individual_stop.loc[stop_id, 'label_space_compromise']
    is_sl_grouped = (sl!=ungrouped_space_label)
    if is_sl_grouped:
        is_same_sl = (individual_stop['label_space_compromise']==sl)
    
        target_stop_id = individual_stop.loc[is_same_sl].index.tolist()
        is_not_only_self = (len(target_stop_id)>1)
        if is_not_only_self:
            for sid in target_stop_id:
                results[sid] = 'group_' + str(label_seq).zfill(3)
            label_seq += 1
stops_label.loc[is_individual, 'label_compromise'] = stops_label.loc[is_individual, 'stop_id'].map(results)
stops_label.loc[stops_label['label_compromise'].isna(), 'label_compromise'] = individual_label

# see group results
temp = stops_label['label_compromise'].value_counts()

# save
file_path = cluster_path+'/[summary]final_tuic_clustering_results.csv'
stops_label.to_csv(file_path, index=False, encoding='UTF-8')
