# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 16:34:53 2023

@author: rz3881
"""

import pandas as pd 

def show_group_results(labels):
    num_of_groups = len(set(labels))
    group_size = labels.value_counts()
    print(f'num_of_groups: {num_of_groups}')
    print(group_size)
    

root_path = r'D:\iima\ubike分析'

# load
stops_info = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
stops_info['stop_id'] = 'U' + stops_info['stop_id'].astype(str)
label_cor = pd.read_csv(root_path+'/DM/站點分群_嚴格/[PBI][cor]customer_clustering_label.csv')
label_txn = pd.read_csv(root_path+'/DM/站點分群_嚴格/[PBI][txn]kmean_label_by_weekday_by_period.csv')
label_status = pd.read_csv(root_path+'/DM/站點分群_嚴格/[PBI][status]kmean_label_by_weekday_by_period.csv')
label_space = pd.read_csv(root_path+'/DM/站點分群_嚴格/[PBI][space]dbscan_label.csv')
stops_sum_cor = pd.read_csv(root_path+'/DM/站點分群_嚴格/[cor]stops_sum_coexistence.csv')

# merge
stops_label = stops_info.merge(label_cor, how='left', on='stop_id')
stops_label = stops_label.merge(label_txn, how='left', on='stop_id')
stops_label = stops_label.merge(label_status, how='left', on='stop_id')
stops_label = stops_label.merge(label_space, how='left', on='stop_id')
stops_label = stops_label.merge(stops_sum_cor, how='left', on='stop_id')
stops_label.columns = ['stop_id', 'stop_name', 'dist', 'capacity',
                       'lat', 'lng', 'service_type',
                       'label_cor', 'label_txn', 'label_status', 'label_space',
                       'sum_cor']
stops_label.to_csv(root_path+'/DM/站點分群_嚴格/[PBI][summary]sub_clustering_results.csv',
                   index=False, encoding='UTF-8')

# 分群
# 分群的目的在減少調度次數，減少調度的前提是民眾體驗不變糟
# 因此群之中的站，必然要可以互相替代，A站走到B站也可以接受，如此才有一群的意義

# rename label
txn_label_map = {'txn_0': '借還平衡', 'txn_1': '早還晚借',
                 'txn_2': '超早借晚還', 'txn_3': '早借晚還', 
                 'txn_4': '公館特殊站', 'txn_5': '超早還晚借'
                 } # 超是因為這些站是少數，特殊的
stops_label['label_txn'] = stops_label['label_txn'].map(txn_label_map)
status_label_map = {'status_0': '有位有車', 'status_1': '晚上缺車',
                    'status_2': '早上缺車', 'status_3': '常態缺車'
                    } 
stops_label['label_status'] = stops_label['label_status'].map(status_label_map)
stops_label['label_cor'] = stops_label['label_cor'].astype(str)
stops_label['label_space'] = stops_label['label_space'].astype(str)
# refactor，txn幅度不重要
stops_label['label_txn_refactor'] = stops_label['label_txn'].str.replace('超', '')

# reshape
stops_label = stops_label.sort_values('sum_cor', ascending=False)
stops_label.index = stops_label['stop_id']

# config
ungrouped_label = ''
individual_label = '獨立站'
critical_label = '需關注站'
stops_label['label'] = ungrouped_label
label_seq = 0
ungrouped_space_label = '未分群'
ungrouped_cor_label = '未分群'
txn_neutral_label = '借還平衡'
# 1.特別站特別處理
is_critical_problem = (stops_label['label_txn']=='公館特殊站')
stops_label.loc[is_critical_problem, 'label'] = critical_label
# 其他分群判斷
is_ungrouped = (stops_label['label']==ungrouped_label)
ungrouped_stop = stops_label.loc[is_ungrouped]
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
    sl = ungrouped_stop.loc[stop_id, 'label_space']
    is_sl_ungrouped = (sl==ungrouped_space_label)
    if is_sl_ungrouped:
        is_same_sl = [False] * ungrouped_stop_len
    else:
        is_same_sl = (ungrouped_stop['label_space']==sl)

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
        is_grouped = ungrouped_stop['label']
        is_target = (is_same_cl | is_same_sl) & is_same_tl
        is_only_self = (is_target.sum()==1)
        if is_only_self:
            results[stop_id] = individual_label
        else:
            target_stop_id = is_target.loc[is_target].index.tolist()
            for sid in target_stop_id:
                results[sid] = 'group_' + str(label_seq).zfill(3)
            label_seq += 1
stops_label.loc[is_ungrouped, 'label'] = stops_label.loc[is_ungrouped, 'stop_id'].map(results)
show_group_results(stops_label['label'])

# save
file_path = '/DM/站點分群_嚴格/[PBI][summary]clustering_results.csv'
stops_label.to_csv(root_path+file_path, index=False, encoding='UTF-8')


