# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:28:40 2023

@author: rz3881
"""

import pandas as pd

# config
root_path = r'D:\iima\ubike分析'
cor_path = root_path+r'\DM\202303\prepared_data\cor'
cluster_path = root_path+r'\DM\202303\站點分群'
min_coexistence = 150 # threshould of coexistence time
max_member_size = 3 
isolate_group_label = '未分群'

def add_to_dict(target, key, value):
    if target.get(key) is None:
        target[key] = set()
    target[key].add(value)


def filter_low_con(stops_coexistence, min_cor_threshould=min_coexistence):
    '沒有夠強關聯的不用聚合'
    stop_cor = stops_coexistence.max(axis=0).sort_values(ascending=False)
    is_weak_cor = stop_cor < min_cor_threshould
    stops_with_weak_cor = stop_cor.loc[is_weak_cor].index.tolist()
    for s in stops_with_weak_cor:
        id_to_group[s] = isolate_group_label
        add_to_dict(group_to_id, isolate_group_label, s)
    return stops_with_weak_cor


def delete_weak_cor_stops(stops_coexistence, stops_with_weak_cor):
    '共現矩陣刪除獨立站，減少後續運算量'
    strong_stops_coexistence = stops_coexistence.drop(columns=stops_with_weak_cor)
    strong_stops_coexistence = strong_stops_coexistence.drop(index=stops_with_weak_cor)
    return strong_stops_coexistence


def generate_cor_candidate_list(strong_stops_coexistence, min_cor_threshould=min_coexistence):
    '產生候選列表，依站共現次數排列'
    candidate_list = {'stop_id1': [], 'stop_id2': [], 'coexistence': []}
    matrix_len = strong_stops_coexistence.shape[0]
    for i in range(1, matrix_len):
        for j in range(0, i):
            cor = strong_stops_coexistence.iloc[i, j]
            if cor >= min_cor_threshould:
                candidate_list['stop_id1'].append(strong_stops_coexistence.index[i])
                candidate_list['stop_id2'].append(strong_stops_coexistence.columns[j])
                candidate_list['coexistence'].append(cor)
    candidate_df = pd.DataFrame(candidate_list)
    candidate_df = candidate_df.sort_values('coexistence', ascending=False)
    candidate_df = candidate_df.reset_index(drop=True)
    return candidate_df


def check_group_status(stop_id, max_size_threshould=max_member_size):
    '''
    檢查stop是否已分組，又該組是否已滿
    若未分組 or 已有組但超過size，回傳結果
    若有組別可加入，回傳組號
    
    ----example
    find_proper_group('U101181')
    find_proper_group('U105062')
    '''
    target_group = id_to_group.get(stop_id)
    is_grouped = target_group is not None
    if is_grouped:
        target_group_size = len(group_to_id[target_group])
        is_full = (target_group_size >= max_size_threshould)
        if is_full:
            return 'full'
        else:
            return target_group
    else:
        return 'ungrouped'


def cluster_candidate(candidate_df, init_group=1):
    '''
    依共現次數順序聚合
    當一組超過3個，即可無視該stop_id
    因此若任一id的群滿了，忽略
    兩個都分群都沒有滿，忽略，代表他有其他更是合組群的站
    若都未分群，一起建群
    若其中一個未分群，另一個已分群但未滿，則入未滿群
    '''
    for _, row in candidate_df.iterrows():
        id1_status = check_group_status(row['stop_id1'])
        id2_status = check_group_status(row['stop_id2'])
        if (id1_status=='full'): # 任一id的群滿了，忽略
            continue
        elif (id1_status=='ungrouped'):
            if (id2_status=='full'): # 任一id的群滿了，忽略
                continue
            elif (id2_status=='ungrouped'): # 都未分群，一起建群
                group_label = 'cor_' + str(init_group).zfill(3)
                id_to_group[row['stop_id1']] = group_label
                add_to_dict(group_to_id, group_label, row['stop_id1'])
                id_to_group[row['stop_id2']] = group_label
                add_to_dict(group_to_id, group_label, row['stop_id2'])
                init_group += 1
            else: # id1未分群，id2已分群但未滿，則id1入id2群
                group_label = id2_status
                id_to_group[row['stop_id1']] = group_label
                add_to_dict(group_to_id, group_label, row['stop_id1'])
        else: 
            if (id2_status=='full'): # 任一id的群滿了，忽略
                continue
            elif (id2_status=='ungrouped'): # id2未分群，id1已分群但未滿，則id2入id1群
                group_label = id1_status
                id_to_group[row['stop_id2']] = group_label
                add_to_dict(group_to_id, group_label, row['stop_id2'])
            else: # 兩個都分群都沒有滿，忽略，代表他有其他更適合對象
                continue


# load
stops_coexistence = pd.read_csv(cor_path+'/coexistence_matrix.csv')
stops_coexistence.index = stops_coexistence.columns

# 分群
# 從關聯性強的開始合併，一群最多3個stop
id_to_group = {}
group_to_id = {}
stops_with_weak_cor = filter_low_con(stops_coexistence)
strong_stops_coexistence = delete_weak_cor_stops(stops_coexistence, stops_with_weak_cor)
candidate_df = generate_cor_candidate_list(strong_stops_coexistence)
cluster_candidate(candidate_df)

# reshape results
results = {'stop_id': id_to_group.keys(), 'label': id_to_group.values()}
labels = pd.DataFrame(results)
all_stops = pd.DataFrame(stops_coexistence.index)
all_stops.columns = ['stop_id']
labels = all_stops.merge(labels, how='left', on='stop_id')
labels.loc[labels['label'].isna(), 'label'] = isolate_group_label

# fill na, na代表有站但沒交易，也許是廢站
is_no_txn = labels['label'].isna()
labels.loc[is_no_txn, 'label'] = isolate_group_label

# save
file_path = cluster_path+r'/[cor]label.csv'
labels.to_csv(file_path, index=False, encoding='UTF-8')
