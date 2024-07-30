# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 18:28:53 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'

# load
# 微單分群結果
file_path = '/DIM/ubike站點分群清單_long.xlsx'
ubike_group_results = pd.read_excel(root_path+file_path)

# 與TUIC結果比較
# TUIC分群結果
file_path = '/DM/站點分群_嚴格/[PBI][summary]clustering_results.csv'
stops_label = pd.read_csv(root_path+file_path)
# 為了ubike結果做調整
is_need_adjust = (stops_label['stop_id']=='U110047' )
stops_label.loc[is_need_adjust, 'stop_name'] = '捷運小巨蛋站(5號出口)_1'
# merge
group_results = ubike_group_results.merge(stops_label,
                                          how='left', on='stop_name')

# mark isolate stop
group_member_count = {}
for stop_id, temp in group_results.groupby('group'):
    group_member_count[stop_id] = temp.shape[0]
group_results['group_size_count'] = group_results['group'].map(
    group_member_count)
is_isolate = (group_results['group_size_count']==1)
group_results['group_refactor'] = group_results['group']
group_results.loc[is_isolate, 'group_refactor'] = '獨立站'

# reshape
group_results = group_results[['dist', 'stop_id', 'stop_name',
                               'lat', 'lng', 'stop_type',
                               'capacity_x', 'capacity_y',
                               'label', 'group',
                               'group_size_count', 'group_refactor']]
group_results = group_results.rename({'capacity_x': 'capacity_202303',
                                      'capacity_y': 'capacity_202211'})