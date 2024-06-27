# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 10:18:29 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
ref_yms = ['202309', '202311']
valid_ym = '202402'
yms = ref_yms + [valid_ym]
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
strategy_type = '閒置柱'
reduce_threshould = -5

# Load
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal_add_idle.csv')

# Extract
compare_agg['月比_實際初始在站車'] = compare_agg[f'實際6點在站車_{valid_ym}'] - compare_agg[f'實際6點在站車_{ref_yms[1]}']
compare_agg['月比_建議初始在站車_柱無限'] = compare_agg[f'建議初始在站車_柱無限_{valid_ym}'] - compare_agg[f'建議初始在站車_柱無限_{ref_yms[1]}']
compare_agg['月比_理想車柱數'] = compare_agg[f'理想車柱數_{valid_ym}'] - compare_agg[f'理想車柱數_{ref_yms[1]}']

# Filter
# 差異夠大
is_stable = (compare_agg['月比_理想車柱數'].abs() < 5)
stable_id = set(compare_agg.loc[is_stable, 'ID'])
problem_id = set(compare_agg.loc[~compare_agg['類型'].isna(), 'ID'])
is_target = compare_agg['ID'].isin((stable_id - problem_id))
# is_target = compare_agg['ID'].isin(( problem_id))
# is_positive = compare_agg[f'建議調整柱數_{valid_ym}'] > 0
# is_zero = compare_agg['建議調整柱數_3月'] == 0
is_negative = compare_agg[f'建議調整柱數_{valid_ym}'] <= reduce_threshould
# compare_agg.loc[is_target & is_positive, '類型'] = '增加車柱'
compare_agg.loc[is_target & is_negative, '類型'] = strategy_type

# Save
compare_agg['類型'].value_counts(dropna=False)
compare_agg.to_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker.csv',
                   index=False)

# Reshape
# define columns
temp_col = ['ID', f'站名_{valid_ym}', f'柱數_{valid_ym}']
temp_col += [f'理想車柱數_{ym}' for ym in yms]
temp_col += ['月均理想車柱數']
for ym in yms:
    temp_col += [f'建議初始在站車_周間中位數_{ym}']
    temp_col += [f'實際6點在站車_周間中位數_{ym}']
    temp_col += [f'實際調度車數_周間中位數_{ym}']
    temp_col += [f'建議初始在站車_周末中位數_{ym}']
    temp_col += [f'實際6點在站車_周末中位數_{ym}']
    temp_col += [f'實際調度車數_周末中位數_{ym}']
# reshape structure from weektype-stop as a row to a stop a row
results = []
for stop_id, temp in compare_agg.groupby('ID'):
    # break
    is_only_one_type = (temp.shape[0] == 1)
    if is_only_one_type:
        # give it to weekday no matter it is weekday or weekend
        weekday_row = temp.iloc[0]
    else:
        is_weekday = (temp['週間週末']=='weekday')
        weekday_row = temp.loc[is_weekday].iloc[0]
        weekend_row = temp.loc[~is_weekday].iloc[0]
    
    row = [
        weekday_row['ID'],
        weekday_row[f'站名_{valid_ym}'],
        weekday_row[f'柱數_{valid_ym}']
    ]
    
    sum_max_docker = 0
    for ym in yms:
        max_docker = max(weekday_row[f'理想車柱數_{ym}'], weekend_row[f'理想車柱數_{ym}'])
        row.append(max_docker)
        sum_max_docker += max_docker
    mean_max_docker = sum_max_docker / len(yms)
    row.append(mean_max_docker)
    
    for ym in yms:
        temp_row = [
            weekday_row[f'建議初始在站車_柱無限_{ym}'],
            weekend_row[f'建議初始在站車_柱無限_{ym}'],
            weekday_row[f'實際6點在站車_{ym}'],
            weekend_row[f'實際6點在站車_{ym}'],
            weekday_row[f'實際調度車數_{ym}'],
            weekend_row[f'實際調度車數_{ym}']
            ]
        row.extend(temp_row)
    df = pd.DataFrame([row], columns=temp_col)
    results.append(df)
dockerfree_clean = pd.concat(results)
dockerfree_clean[f'建議調整柱數_{valid_ym}'] = dockerfree_clean[f'理想車柱數_{valid_ym}'] - dockerfree_clean[f'柱數_{valid_ym}']

# Save
# all stop
dockerfree_clean.to_excel(strategy_path+'/理想站體與配置.xlsx', index=False)
# idle docker
is_idle_docker  = (dockerfree_clean[f'建議調整柱數_{valid_ym}']<=reduce_threshould)
dockerfree_reduce = dockerfree_clean.loc[is_idle_docker]
dockerfree_reduce.to_excel(strategy_path+'/閒置柱.xlsx', index=False)
