# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 15:25:11 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
cluster_path = root_path+r'\DM\202303\站點分群'

# load
ubike_group_results = pd.read_csv(root_path+'/DIM/台北分群0324v3.csv')
stops_info = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')

# process for merge
ubike_group_results['場站編號'] = ubike_group_results['場站編號'].astype(str)
is_start_with_500 = ubike_group_results['場站編號'].str.startswith('500')
ubike_group_results.loc[is_start_with_500, '場站編號'] = ubike_group_results.loc[is_start_with_500, '場站編號'].str.slice(3, )
ubike_group_results['場站編號'] = 'U' + ubike_group_results['場站編號']

# merge
group_results = ubike_group_results.merge(stops_info[['stop_id', 'lng', 'lat']],
                                          how='left', left_on='場站編號',
                                          right_on='stop_id')

# mark isolate stop
group_size_count = {}
for stop_id, temp in group_results.groupby('責任區群'):
    group_size_count[stop_id] = temp.shape[0]
group_results['group_size'] = group_results['責任區群'].map(group_size_count)
is_isolate = (group_results['group_size']==1)
group_results['group_refactor'] = group_results['責任區群']
group_results.loc[is_isolate, 'group_refactor'] = '獨立站'

# save
file_path = cluster_path+'/[you]ubike_group.csv'
group_results.to_csv(file_path, index=False, encoding='UTF-8')


