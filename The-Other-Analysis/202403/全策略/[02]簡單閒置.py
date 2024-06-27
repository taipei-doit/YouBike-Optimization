# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 15:47:39 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
ref_yms = ['202311', '202402']
valid_ym = '202403'
yms = ref_yms + [valid_ym]
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
strategy_type = '閒置車'
idle_threshould = 5

# Load
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal.csv')

# Extract
sum_idle_bike = None
for ym in yms:
    if sum_idle_bike is None:
        sum_idle_bike = compare_agg[f'閒置車_{ym}'].copy()
    else:
        sum_idle_bike += compare_agg[f'閒置車_{ym}']
compare_agg['月平均閒置車數'] = sum_idle_bike / len(yms)

# Filter
# 閒置過少，無檢視意義
is_idel = (compare_agg[f'閒置車_{valid_ym}']>=idle_threshould)
is_trustable = compare_agg['類型'].isna()
compare_agg.loc[is_idel & is_trustable, '類型'] = strategy_type
# # 變動程度太大，結果存疑
# idel_verify = idel_verify.loc[idel_verify['閒置車_std_3月']<3]
# idel_verify = idel_verify.loc[idel_verify['閒置車_std_4月']<3]

# Save
compare_agg['類型'].value_counts(dropna=False)
compare_agg.to_csv(strategy_path+'/strategy_filter_abnormal_add_idle.csv',
                   index=False)

# Reshape
is_idle_bike = (compare_agg['類型']==strategy_type)
idel_clean = compare_agg.loc[is_idle_bike]
cols = ['ID', f'站名_{valid_ym}', '週間週末', '類型']
for ym in yms:
    temp_col = [f'閒置車_{ym}']
    cols.extend(temp_col)
cols.append('月平均閒置車數')
idel_clean = idel_clean[cols]

# Save
idel_clean.to_excel(strategy_path+'/閒置車.xlsx', index=False)
