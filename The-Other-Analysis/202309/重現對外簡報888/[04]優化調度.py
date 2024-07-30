# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 19:07:47 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
valid_ym = '202304'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'

# Load 
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker.csv')

# Extract
# 模擬與驗證差
compare_agg['模擬上午淨調度_3月'] = compare_agg['模擬上午調入_3月'] - compare_agg['模擬上午調出_3月']
compare_agg['模擬下午淨調度_3月'] = compare_agg['模擬下午調入_3月'] - compare_agg['模擬下午調出_3月']
compare_agg['模擬上午淨調度_4月'] = compare_agg['模擬上午調入_4月'] - compare_agg['模擬上午調出_4月']
compare_agg['模擬下午淨調度_4月'] = compare_agg['模擬下午調入_4月'] - compare_agg['模擬下午調出_4月']
compare_agg['實際上午淨調度_3月'] = compare_agg['實際上午調入_3月'] - compare_agg['實際上午調出_3月']
compare_agg['實際下午淨調度_3月'] = compare_agg['實際下午調入_3月'] - compare_agg['實際下午調出_3月']
compare_agg['實際上午淨調度_4月'] = compare_agg['實際上午調入_4月'] - compare_agg['實際上午調出_4月']
compare_agg['實際下午淨調度_4月'] = compare_agg['實際下午調入_4月'] - compare_agg['實際下午調出_4月']
compare_agg['成效(日模擬-現實_調度車數差_兩月平均)'] = (compare_agg['模擬與現實調度差_3月'] + compare_agg['模擬與現實調度差_4月']) / 2
compare_agg['月比_建議6點在站'] = compare_agg['建議6點在站車_4月'] - compare_agg['建議6點在站車_3月']
compare_agg['月比_模擬調度數'] = compare_agg['模擬調度車數_4月'] - compare_agg['模擬調度車數_3月']

# Filter
# 效益不彰
is_need_change = compare_agg['類型']=='改善'
is_useful = (compare_agg['成效(日模擬-現實_調度車數差_兩月平均)'] <= -5)
compare_agg.loc[is_need_change & is_useful, '類型'] = '改善_最佳化調度'

# Save
compare_agg['類型'].value_counts()
compare_agg.to_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker_dispatch.csv',
                   index=False)

# Reshape
dispatch_clean = compare_agg.loc[is_useful]
dispatch_clean = dispatch_clean[[
    'ID', '站名_3月', '週間週末',
    '建議6點在站車_3月', '實際6點在站車_3月', 
    '模擬上午淨調度_3月', '實際上午淨調度_3月', 
    '建議16點在站車_3月', '實際16點在站車_3月', 
    '模擬下午淨調度_3月', '模擬下午淨調度_3月', 
    '模擬調度車數_3月', '實際調度車數_3月',
    '建議6點在站車_4月', '實際6點在站車_4月', 
    '模擬上午淨調度_4月', '實際上午淨調度_4月', 
    '建議16點在站車_4月', '實際16點在站車_4月', 
    '模擬下午淨調度_4月', '模擬下午淨調度_4月', 
    '模擬調度車數_4月', '實際調度車數_4月',
    '模擬與現實調度差_3月', '模擬與現實調度差_4月', '成效(日模擬-現實_調度車數差_兩月平均)',
    ]]
dispatch_clean.columns = [
    'ID', '站名_3月', '週間週末',
    '建議6點在站車_3月', '實際6點在站車_3月', 
    '模擬上午淨調度_3月', '實際上午淨調度_3月', 
    '建議16點在站車_3月', '實際16點在站車_3月', 
    '模擬下午淨調度_3月', '模擬下午淨調度_3月', 
    '模擬調度車數_3月', '實際調度車數_3月',
    '模擬與現實調度差_3月', '成效(日模擬-現實_調度車數差_兩月平均)',
    ]
# Save
dispatch_clean.to_excel(strategy_path+'/最佳化調度.xlsx', index=False)
