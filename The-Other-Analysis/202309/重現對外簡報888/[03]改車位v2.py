# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 10:18:29 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
valid_ym = '202304'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
valid_txn_path = root_path+f'/DM/{valid_ym}/prepared_data/txn'
valid_dis_path = root_path+f'/DM/{valid_ym}/prepared_data/dispatch'
exclude_date = ['2023-02-28', '2023-03-31']
init_hour = 6  # 一天初始時間(6 = 06:00)

# Load
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal_add_idle.csv')

# Extract
compare_agg['月比_實際初始在站車'] = compare_agg['實際6點在站車_4月'] - compare_agg['實際6點在站車_3月']
compare_agg['月比_建議初始在站車_柱無限'] = compare_agg['建議初始在站車_柱無限_4月'] - compare_agg['建議初始在站車_柱無限_3月']
compare_agg['月比_理想車柱數'] = compare_agg['理想車柱數_4月'] - compare_agg['理想車柱數_3月']

# Filter
# 成效ok
is_useful = (compare_agg['月比_理想車柱數'].abs() < 5)
useful_id = set(compare_agg.loc[is_useful, 'ID'])
problem_id = set(compare_agg.loc[~compare_agg['類型'].isna(), 'ID'])
is_target = compare_agg['ID'].isin((useful_id - problem_id))
is_positive = compare_agg['建議調整柱數_3月'] > 0
# is_zero = compare_agg['建議調整柱數_3月'] == 0
is_negative = compare_agg['建議調整柱數_3月'] < 0
compare_agg.loc[is_target & is_positive, '類型'] = '改善_增加車位'
compare_agg.loc[is_target & is_negative, '類型'] = '供過於求_車位'

# Save
compare_agg['類型'].value_counts()
compare_agg.to_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker.csv',
                   index=False)

# Reshape
results = []
temp_col = ['ID', '站名', '柱數_3月',
            '建議調整柱數_兩月平均', '理想車柱數_3月', '理想車柱數_4月',
            '建議初始在站車_3月周間中位數', '建議初始在站車_3月周末中位數',
            '實際6點在站車_3月周間中位數', '實際6點在站車_3月周末中位數',
            '實際調度車數_3月周間中位數', '實際調度車數_3月周末中位數',
            '建議初始在站車_4月周間中位數', '建議初始在站車_4月周末中位數',
            '實際6點在站車_4月周間中位數', '實際6點在站車_4月周末中位數',
            '實際調度車數_4月周間中位數', '實際調度車數_4月周末中位數',
            ]
for stop_id, temp in compare_agg.loc[is_target].groupby('ID'):
    # break
    is_weekday = (temp['週間週末']=='weekday')
    weekday_row = temp.loc[is_weekday].iloc[0]
    weekend_row = temp.loc[~is_weekday].iloc[0]
    max_march_docker = max(weekday_row['理想車柱數_3月'], weekend_row['理想車柱數_3月'])
    max_april_docker = max(weekday_row['理想車柱數_4月'], weekend_row['理想車柱數_4月'])
    mean_adj_docker_two_month = (max_march_docker+max_april_docker)/2 - weekday_row['柱數_3月']
    
    row = [weekday_row['ID'], weekday_row['站名_3月'], weekday_row['柱數_3月'],
           mean_adj_docker_two_month, max_march_docker, max_april_docker,
           weekday_row['建議初始在站車_柱無限_3月'], weekend_row['建議初始在站車_柱無限_3月'],
           weekday_row['實際6點在站車_3月'], weekend_row['實際6點在站車_3月'],
           weekday_row['實際調度車數_3月'], weekend_row['實際調度車數_3月'],
           weekday_row['建議初始在站車_柱無限_4月'], weekend_row['建議初始在站車_柱無限_4月'],
           weekday_row['實際6點在站車_4月'], weekend_row['實際6點在站車_4月'],
           weekday_row['實際調度車數_4月'], weekend_row['實際調度車數_4月']
           ]
    df = pd.DataFrame([row], columns=temp_col)
    results.append(df)
dockerfree_clean = pd.concat(results)

# Save
is_positive = dockerfree_clean['建議調整柱數_3月'] > 0
dockerfree_add = dockerfree_clean.loc[is_positive]
dockerfree_add.to_excel(strategy_path+'/擴位.xlsx', index=False)

# Save
is_negative = dockerfree_clean['建議調整柱數_3月'] < 0
dockerfree_reduce = dockerfree_clean.loc[is_negative]
dockerfree_reduce.to_excel(strategy_path+'/閒置位.xlsx', index=False)