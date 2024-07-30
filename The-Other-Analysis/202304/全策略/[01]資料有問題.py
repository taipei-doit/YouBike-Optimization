# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 15:04:24 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
ref_ym = '202303'
reference_idle_path = root_path+f'/DM/{ref_ym}/閒置車'
valid_ym = '202304'
valid_idle_path = root_path+f'/DM/{valid_ym}/閒置車'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
confidence_ratio_threshould = 0.8

# Load
# 3月結論
march_agg = pd.read_csv(reference_idle_path + '/compare_agg.csv')
# 4月結論
april_agg = pd.read_csv(valid_idle_path + '/compare_agg.csv')

# Preprocess
# is idel stop in March still idel in April?
data = march_agg.merge(april_agg, how='outer', on=['ID', '週間週末'])

# Rename
cols = []
for col in data.columns:
    col = col.replace('_x', '_3月')
    col = col.replace('_y', '_4月')
    cols.append(col)
data.columns = cols
unqualified = data.copy()

# Filter
unqualified['類型'] = None
# 柱數變動
is_docker_change = (unqualified['柱數_4月'] - unqualified['柱數_3月']).abs() >= 2
unqualified.loc[is_docker_change, '類型'] = '問題_柱數變動'
# 可能有故障、停用、新設立，資料不齊全問題
is_weekday = (unqualified['週間週末']=='weekday')
is_weekend = ~is_weekday
is_normal_weekday = is_weekday & (unqualified['天數_3月']==23) & (unqualified['天數_4月']==20)
is_normal_weekend = is_weekend & (unqualified['天數_3月']==8) & (unqualified['天數_4月']==10)
unqualified.loc[(~is_normal_weekday) & (~is_normal_weekend), '類型'] = '問題_天數不正常'
# 正常時間過少
is_low_confidence = (unqualified['資料可信度_4月'] <= confidence_ratio_threshould) | (unqualified['資料可信度_3月'] <= confidence_ratio_threshould)
unqualified.loc[is_low_confidence, '類型'] = '問題_需求未知'

# Save
unqualified['類型'].value_counts()
unqualified.to_csv(strategy_path+'/strategy_filter_abnormal.csv',
                   index=False)

# Reshape
unqualified_clean = unqualified[~unqualified['類型'].isna()]
unqualified_clean = unqualified_clean[[
    'ID', '站名_3月', '週間週末', '類型',
    '天數_3月', '天數_4月',
    '空車分鐘_3月', '滿車分鐘_3月', '資料可信度_3月',
    '空車分鐘_4月', '滿車分鐘_4月', '資料可信度_4月',
    ]]
unqualified_clean.columns = [
    'ID', '站名', '週間週末', '類型',
    '天數_3月', '天數_4月',
    '空車分鐘_3月', '滿站分鐘_3月', '資料可信度_3月',
    '空車分鐘_4月', '滿站分鐘_4月', '資料可信度_4月',
    ]

# Save
unqualified_clean.to_excel(strategy_path+'/異常.xlsx', index=False)
