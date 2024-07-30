# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 00:41:19 2023

@author: rz3881
"""

import pandas as pd 

def add_to_dict(target_dict, key, value):
    '''
    如果key已經存在於target中，就把這個key對應的value增加value；
    如果這個key不存在於target中，就在target中新增一個key-value pair
    """
    '''
    if target_dict.get(key) is None:
        target_dict[key] = 0
    target_dict[key] += value

def calcu_cor(stop_id_list):
    '''
    計算所有id之間的共現數，
    回傳一個dict(stop_id, id與其他id的共現值總和)
    '''
    coexistence = {} # 
    for id1 in stop_id_list:
        for id2 in stop_id_list:
            is_same = (id1 == id2)
            if is_same:
                continue
            cor = stops_coexistence.loc[id1, id2]
            add_to_dict(coexistence, id1, cor)
            add_to_dict(coexistence, id2, cor)
    return coexistence

def find_max_cor_stop(coexistence):
    '''
    找到所有共現值中最大的那個
    若有多個id與最大值相同，就返回一個包含這些id的list
    '''
    max_value = max(coexistence.values()) # 找到最大的value
    max_keys = []  # 儲存最大value對應的key
    
    # 找到所有與最大value相對應的key
    for key, value in coexistence.items():
        if value > max_value:
            max_value = value
            max_keys = [key]
        elif value == max_value:
            max_keys.append(key)
    return max_keys
    
def find_max_sum_cor_stop(stop_id_list):
    '回傳id list中共現值總和最大者'
    is_target = stops_label['stop_id'].isin(stop_id_list)
    max_sum_cor_id = stops_label.loc[is_target, 'stop_id'].iloc[0]
    return max_sum_cor_id

root_path = r'D:\iima\ubike分析'
cluster_path = root_path+r'\DM\202303\站點分群'
cor_path = root_path+r'\DM\202303\prepared_data\cor'

# load
stops_label = pd.read_csv(cluster_path+'/[summary]final_tuic_clustering_results.csv')
stops_coexistence = pd.read_csv(cor_path+'/coexistence_matrix.csv')
stops_coexistence.index = stops_coexistence.columns

# 主站 = 群內各站的交集 = 群內總共現數最高者
# 直接對寬鬆版做，因為寬鬆版是嚴格版的擴充
main_stops = {} # group: stop_id
for group, group_data in stops_label.groupby('label_compromise'):
    # break
    # 非正常群跳過
    if not group.startswith('group'):
        continue
    
    #!!! 群內只有一個站是一個bug，但暫時不處理
    is_group_only_one_stop = (group_data.shape[0]<2)
    if is_group_only_one_stop:
        print(group_data)
        main_stops[group] = group_data['stop_id'].iloc[0]
        continue

    # 遍歷倆倆比較並累計
    all_id = set(group_data['stop_id'])
    coexistence = calcu_cor(all_id)
    # 挑選最大的站
    max_cor_stop_id = find_max_cor_stop(coexistence)
    is_multi_stop = (len(max_cor_stop_id) > 1)
    if is_multi_stop:
        main_stop_id = find_max_sum_cor_stop(max_cor_stop_id)
    else:
        main_stop_id = max_cor_stop_id[0]
    # record result
    main_stops[group] = main_stop_id

# mark
stops_label['stop_role'] = '衛星站'
is_isolate = (stops_label['label_compromise']=='獨立站')
stops_label.loc[is_isolate, 'stop_role'] = '主站'
is_critical = (stops_label['label_compromise']=='需關注站')
stops_label.loc[is_critical, 'stop_role'] = '主站'
main_stops_set = set(main_stops.values())
is_main_stop = stops_label['stop_id'].isin(main_stops_set)
stops_label.loc[is_main_stop, 'stop_role'] = '主站'

# save
file_path = cluster_path+'/final_tuic_clustering_results_with_role.csv'
stops_label.to_csv(file_path, index=False, encoding='UTF-8')


