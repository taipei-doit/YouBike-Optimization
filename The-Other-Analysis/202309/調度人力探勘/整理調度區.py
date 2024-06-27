# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 13:58:26 2023

@author: rz3881
"""

import pandas as pd

ym = '202309'
root_path = r'D:\iima\ubike分析'

# load
dispatch_operate = pd.read_csv(
    root_path+f'/DM/{ym}/prepared_data/dispatch/dispatch_operation_log.csv'
)

# filter
dispatch_operate = dispatch_operate.loc[dispatch_operate['工作狀態'].isin(
    ['綁車', '解綁車', '調入', '調出']
)]
dispatch_operate['date'] = pd.to_datetime(dispatch_operate['更新時間']).dt.date
is_tie = (dispatch_operate['工作狀態']=='綁車')
dispatch_operate['tie_count'] = is_tie.astype(int)
dispatch_operate['tie_bike'] = dispatch_operate.loc[is_tie, '車輛數']
is_untie = (dispatch_operate['工作狀態']=='解綁車')
dispatch_operate['untie_count'] = is_untie.astype(int)
dispatch_operate['untie_bike'] = dispatch_operate.loc[is_untie, '車輛數']
is_in = (dispatch_operate['工作狀態']=='調入')
dispatch_operate['in_count'] = is_in.astype(int)
dispatch_operate['in_bike'] = dispatch_operate.loc[is_in, '車輛數']
is_out = (dispatch_operate['工作狀態']=='調出')
dispatch_operate['out_count'] = is_out.astype(int)
dispatch_operate['out_bike'] = dispatch_operate.loc[is_out, '車輛數']

# agg by date
operate_date_agg = dispatch_operate.groupby(
    ['責任區', '責任群', '場站代號', 'date']
).agg({
    '城市': 'count',
    '車輛數': 'sum',
    'tie_count': 'sum',
    'untie_count': 'sum',
    'in_count': 'sum',
    'out_count': 'sum',
    'tie_bike': 'sum',
    'untie_bike': 'sum',
    'in_bike': 'sum',
    'out_bike': 'sum',
}).reset_index()

# agg by stop_id
operate_agg = operate_date_agg.groupby(['責任區', '責任群', '場站代號']).agg({
    '城市': 'sum',
    'date': 'nunique',
    '車輛數': ['sum', 'mean'],
    'tie_count': 'sum',
    'untie_count': 'sum',
    'in_count': 'sum',
    'out_count': 'sum',
    'tie_bike': ['sum', 'mean'],
    'untie_bike': ['sum', 'mean'],
    'in_bike': ['sum', 'mean'],
    'out_bike': ['sum', 'mean'],
}).reset_index()
operate_agg.columns = [
    '責任區', '責任群', 'stop_id',
    '調度次數(調入調出綁車解綁)', '有調度天數',
    '月調度車次(對稱調度須注意)', '日均調度車次',
    '月綁次數', '月解綁次數', '月調入次數', '月調出次數',
    '月綁車車次', '日均綁車車次',
    '月解綁車次', '日均解綁車次',
    '月調入車次', '日均調入車次',
    '月調出車次', '日均調出車次',
]

# save
operate_agg.to_csv(
    root_path+f'/DM/{ym}/調度人力探勘/dis_log_agg.csv',
    index=False, encoding='UTF-8'
)
