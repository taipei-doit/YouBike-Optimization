# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 19:07:47 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
ref_yms = ['202311', '202402']
valid_ym = '202403'
yms = ref_yms + [valid_ym]
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
strategy_type = '優化調度'
na_type = '無須調整'
efficient_threshould = -5

# Load 
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker.csv')

# Extract
# 模擬與驗證差
sum_effect = None
for ym in yms:
    compare_agg[f'模擬上午淨調度_{ym}'] = compare_agg[f'模擬上午調入_{ym}'] - compare_agg[f'模擬上午調出_{ym}']
    compare_agg[f'模擬下午淨調度_{ym}'] = compare_agg[f'模擬下午調入_{ym}'] - compare_agg[f'模擬下午調出_{ym}']
    compare_agg[f'實際上午淨調度_{ym}'] = compare_agg[f'實際上午調入_{ym}'] - compare_agg[f'實際上午調出_{ym}']
    compare_agg[f'實際下午淨調度_{ym}'] = compare_agg[f'實際下午調入_{ym}'] - compare_agg[f'實際下午調出_{ym}']
    if sum_effect is None:
        sum_effect = compare_agg[f'模擬與現實調度差_{ym}']
    else:
        sum_effect += compare_agg[f'模擬與現實調度差_{ym}']
compare_agg['月均_調度車數差(模擬-現實)'] = sum_effect / len(yms)
compare_agg['月比_建議6點在站'] = compare_agg[f'建議6點在站車_{valid_ym}'] - compare_agg[f'建議6點在站車_{ref_yms[1]}']
compare_agg['月比_模擬調度數'] = compare_agg[f'模擬調度車數_{valid_ym}'] - compare_agg[f'模擬調度車數_{ref_yms[1]}']
compare_agg['上午'] = compare_agg[f'模擬調度車數_{valid_ym}'] - compare_agg[f'模擬調度車數_{ref_yms[1]}']
compare_agg['月比_模擬調度數'] = compare_agg[f'模擬調度車數_{valid_ym}'] - compare_agg[f'模擬調度車數_{ref_yms[1]}']


compare_agg[f'實際上午合計_{valid_ym}'] = (
    compare_agg[f'實際6點在站車_{valid_ym}'] 
    + compare_agg[f'實際上午調入_{valid_ym}']
)

compare_agg[f'實際下午合計_{valid_ym}'] = (
    compare_agg[f'實際16點在站車_{valid_ym}'] 
    + compare_agg[f'實際下午調入_{valid_ym}']
)

compare_agg[f'模擬上午合計_{valid_ym}'] = (
    compare_agg[f'建議6點在站車_{valid_ym}'] 
    + compare_agg[f'模擬上午調入_{valid_ym}']
)

compare_agg[f'模擬下午合計_{valid_ym}'] = (
    compare_agg[f'建議16點在站車_{valid_ym}'] 
    + compare_agg[f'模擬下午調入_{valid_ym}']
)

# Filter
# 效益不彰
is_need_change = compare_agg['類型'].isna()
is_efficient = (compare_agg[f'模擬與現實調度差_{valid_ym}'] <= efficient_threshould)
compare_agg.loc[is_need_change & is_efficient, '類型'] = strategy_type

# Save
compare_agg['類型'] = compare_agg['類型'].fillna(na_type)
compare_agg['類型'].value_counts(dropna=False)
compare_agg.to_csv(
    strategy_path+'/strategy_filter_abnormal_add_idle_docker_dispatch.csv',
    index=False
)

# Reshape
is_need_optimize = (compare_agg['類型']==strategy_type)
dispatch_clean = compare_agg.loc[is_need_optimize]
cols = ['ID', f'站名_{valid_ym}', '週間週末']
for ym in yms:
    temp_cols = [
        f'建議6點在站車_{ym}', f'實際6點在站車_{ym}', 
        f'模擬上午淨調度_{ym}', f'實際上午淨調度_{ym}', 
        f'建議16點在站車_{ym}', f'實際16點在站車_{ym}', 
        f'模擬下午淨調度_{ym}', f'實際下午淨調度_{ym}', 
        f'模擬調度車數_{ym}', f'實際調度車數_{ym}'
        ]
    cols.extend(temp_cols)
for ym in yms:
    cols.append(f'模擬與現實調度差_{ym}')
cols.append('月均_調度車數差(模擬-現實)')
dispatch_clean = dispatch_clean[cols]

# Save
dispatch_clean.to_excel(strategy_path+'/優化調度.xlsx', index=False)
