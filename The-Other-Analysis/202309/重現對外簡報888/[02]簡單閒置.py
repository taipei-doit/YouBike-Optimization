# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 15:47:39 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
valid_ym = '202304'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
type_name = '供過於求_車輛'

# Load
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal.csv')

# Extract
compare_agg['3、4月平均閒置車數'] = (compare_agg['閒置車_3月'] + compare_agg['閒置車_4月'])/2

# Filter
# 閒置過少，無檢視意義
is_idel = compare_agg['3、4月平均閒置車數'] >= 2
is_useable = ~compare_agg['類型'].isna()
compare_agg.loc[is_idel & is_useable, '類型'] = type_name
# # 變動程度太大，結果存疑
# idel_verify = idel_verify.loc[idel_verify['閒置車_std_3月']<3]
# idel_verify = idel_verify.loc[idel_verify['閒置車_std_4月']<3]

# Save
compare_agg['類型'].value_counts()
compare_agg.to_csv(strategy_path+'/strategy_filter_abnormal_add_idle.csv',
                   index=False)

# Reshape
idel_clean = compare_agg[is_idel]
idel_clean = idel_clean[[
    'ID', '站名_3月', '週間週末', '類型', 
    '閒置車_3月', '閒置車_4月', '3、4月平均閒置車數'
    ]]
idel_clean.columns = [
    'ID', '站名', '週間週末', '類型', 
    '閒置車_3月中位數', '閒置車_4月中位數', '3、4月平均閒置車數'
    ]

# Save
idel_clean.to_excel(strategy_path+'/閒置車.xlsx', index=False)
