# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 10:27:16 2023

@author: rz3881
"""

import pandas as pd 

root_path = r'D:\iima\ubike分析'
cor_path = root_path+r'\DM\202303\prepared_data\cor'
cluster_path = root_path+r'\DM\202303\站點分群'

# load
stops_info = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')
label_cor = pd.read_csv(cluster_path+'/[cor]label.csv')
label_txn = pd.read_csv(cluster_path+'/[txn]kmean_label.csv')
label_status = pd.read_csv(cluster_path+'/[status]kmean_label.csv')
label_space_compromise = pd.read_csv(cluster_path+'/[space][compromise]dbscan_label.csv')
label_space_suggest = pd.read_csv(cluster_path+'/[space][suggest]dbscan_label.csv')
stops_sum_cor = pd.read_csv(cor_path+'/coexistence_sum_by_stop.csv')

# merge
stops_label = stops_info.merge(label_cor, how='left', on='stop_id')
stops_label = stops_label.merge(label_txn, how='left', on='stop_id')
stops_label = stops_label.merge(label_status, how='left', on='stop_id')
stops_label = stops_label.merge(label_space_suggest, how='left', on='stop_id')
stops_label = stops_label.merge(label_space_compromise, how='left', on='stop_id')
stops_label = stops_label.merge(stops_sum_cor, how='left', on='stop_id')
stops_label.columns = ['stop_id', 'stop_name', 'dist', 'capacity',
                       'lat', 'lng', 'service_type',
                       'label_cor', 'label_txn', 'label_status',
                       'label_space_suggest', 'label_space_compromise',
                       'sum_cor']
# drop no txn stop
is_no_txn = stops_label['label_txn'].isna()
stops_label = stops_label.loc[~is_no_txn]

# Export data to PowerBI for result inspection, in order to facilitate renaming later
file_path = cluster_path+'/[summary][PBI]sub_clustering_raw.csv'
stops_label.to_csv(file_path, index=False, encoding='UTF-8')


# rename label
txn_label_map = {'txn_0': '借還平衡', 'txn_1': '早還晚借',
                 'txn_2': '早借晚還', 'txn_3': '微早借晚還',
                 'txn_4': '超早借晚還', 'txn_5': '超早還晚借'
                 } # 超是因為這些站是少數，特殊的
stops_label['label_txn'] = stops_label['label_txn'].map(txn_label_map)
status_label_map = {'status_0': '全時段無車也無位',
                    'status_1': '晚微空',
                    'status_2': '晚空',
                    'status_3': '全時段無車',
                    'status_4': '早午空',
                    'status_5': '早午特空'
                    }
stops_label['label_status'] = stops_label['label_status'].map(status_label_map)
stops_label['label_cor'] = stops_label['label_cor'].astype(str)
stops_label['label_space_suggest'] = stops_label['label_space_suggest'].astype(str)
stops_label['label_space_compromise'] = stops_label['label_space_compromise'].astype(str)
# refactor，txn幅度不重要
stops_label['label_txn_refactor'] = stops_label['label_txn'].str.replace('超', '')

# save
file_path = cluster_path+'/[summary][PBI]sub_clustering_renaming.csv'
stops_label.to_csv(file_path, index=False, encoding='UTF-8')