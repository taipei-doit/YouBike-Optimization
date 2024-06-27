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
label_cor = pd.read_csv(root_path+'/DM/[PBI][cor]kmean_label_v2.csv')
label_txn = pd.read_csv(root_path+'/DM/[PBI][txn]kmean_label_by_weekdayhour_v2.csv')
label_status = pd.read_csv(root_path+'/DM/[PBI][status]kmean_label_by_weekdayperiod_v2.csv')
label_space = pd.read_csv(root_path+'/DM/[PBI][space]kmean_label.csv')

# merge
stops_label = stops_info.merge(label_cor, how='left', on='stop_id')
stops_label = stops_label.merge(label_txn, how='left', on='stop_id')
stops_label = stops_label.merge(label_status, how='left', on='stop_id')
stops_label = stops_label.merge(label_space, how='left', on='stop_id')
stops_label.columns = ['stop_id', 'stop_name', 'dist', 'capacity',
                       'lat', 'lng', 'service_type',
                       'label_cor', 'label_txn', 'label_status', 'label_space']
stops_label.to_csv(root_path+'/DM/[PBI][summary]clustering_results_v2.csv',
                   index=False, encoding='UTF-8')

# rename label
txn_label_map = {0: '早還晚借', 1: '微早還晚借', 2: '無明顯特徵',
                 3: '超早還晚借', 4: '早借晚還', 5: '微早借晚還'}
stops_label['label_txn'] = stops_label['label_txn'].map(txn_label_map)
status_label_map = {0: '特別空', 1: '微早滿晚空', 2: '特別滿',
                    3: '無明顯特徵', 4: '微早空晚滿', 5: '早滿晚空',
                    6: '早空晚滿'}
stops_label['label_status'] = stops_label['label_status'].map(status_label_map)
stops_label['label_cor'] = stops_label['label_cor'].astype(str)
stops_label['label_space'] = stops_label['label_space'].astype(str)


# 分群的目的在減少調度次數，減少調度的前提是民眾體驗不變糟
# 因此群之中的站，必然要可以互相替代，A站走到B站也可以接受，如此才有一群的意義
# 1.特別空或滿嚴重的，要挑出來確保民怨不會升級
# 2.調度時間類似，減少調度才有可行性(有的是早上要補、有的是晚上，無明顯特徵不受限)
# 3.有實際交易中的替代性行為
# 4.超級近(因為是stop_id，有些分兩個id實際上很近。且有可能某站狀良好，實際交易無替代行為)
# refactor，沒有必要分那麼細
stops_label['label_txn_refactor'] = stops_label['label_txn'].str.replace('微', '')
stops_label['label_status_refactor'] = stops_label['label_status'].str.replace('微', '')
stops_label.index = stops_label['stop_id']
# 最後分群
ungrouped_label = ''
individual_label = '獨立站'
critical_label = '需關注站'
stops_label['label'] = ungrouped_label
label = 0
# 先排除特別空或滿
# is_critical_problem = stops_label['label_status_refactor'].isin(['特別空', '特別滿'])
# stops_label.loc[is_critical_problem, 'label'] = critical_label
# 從鬆往嚴做，下面的迴圈就是一個分類樹狀結構
is_ungrouped = stops_label['label']==ungrouped_label
ungrouped_stop = stops_label.loc[is_ungrouped]
results = {}
for stop_id in ungrouped_stop['stop_id']:
    is_grouped = results.get(stop_id)
    if is_grouped:
        continue
    # 檢查cor、spcae條件
    cl = ungrouped_stop.loc[stop_id, 'label_cor']
    sl = ungrouped_stop.loc[stop_id, 'label_space']
    is_individual_group = (cl=='未分群') or (sl=='未分群')
    # 若無任何cor、space相關，直接推定獨立站
    if is_individual_group:
        results[stop_id] = individual_label
        continue
    is_same_cl = (ungrouped_stop['label_cor']==cl)
    is_same_sl = (ungrouped_stop['label_space']==sl)
    # 檢查txn條件
    tl = ungrouped_stop.loc[stop_id, 'label_txn_refactor']
    if tl == '無明顯特徵':
        is_same_tl = [True] * ungrouped_stop.shape[0]
    else:
        is_same_tl = (ungrouped_stop['label_txn_refactor']==tl) | (ungrouped_stop['label_txn_refactor']=='無明顯特徵')
    # 
    is_target = (is_same_cl | is_same_sl) & is_same_tl
    is_only_self = (is_target.sum()==1)
    if is_only_self:
        results[stop_id] = individual_label
        continue
    else:
        target_stop_id = is_target.loc[is_target].index.tolist()
        for sid in target_stop_id:
            results[sid] = 'group_' + str(label).zfill(3)
        label += 1
stops_label.loc[is_ungrouped, 'label'] = stops_label.loc[is_ungrouped, 'stop_id'].map(results)
stops_label.to_csv(root_path+'/DM/[PBI][summary]final_clustering_results_v2.csv', index=False, encoding='UTF-8')
    