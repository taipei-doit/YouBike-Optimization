# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 21:45:21 2023

@author: rz3881
"""

import pandas as pd

# Config
root_path = r'D:\iima\ubike分析'
valid_ym = '202304'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'

# =============================================================================
# 比例
# Load 
compare_agg = pd.read_csv(strategy_path+'/strategy_filter_abnormal_add_idle_docker_dispatch.csv')
compare_agg = compare_agg.loc[compare_agg['ID']!='U26']

# 統計
# 未知、閒置、良好、需改善比例
is_type_na = compare_agg['類型'].isna()
compare_agg.loc[is_type_na, '類型'] = '良好'
is_problem = compare_agg['類型'].str.startswith('問題_')
compare_agg.loc[is_problem, '類型'] = '待改善'
is_need_prove = compare_agg['類型'].str.startswith('改善')
compare_agg.loc[is_need_prove, '類型'] = '待改善'
is_too_much = compare_agg['類型'].str.startswith('供過於求_')
compare_agg.loc[is_too_much, '類型'] = '供過於求'

type_results = {}
for weekday_type, temp in compare_agg.groupby('週間週末'):
    type_results[weekday_type] = temp['類型'].value_counts()
# =============================================================================


# =============================================================================
# 成效
# Load
idel_clean = pd.read_excel(strategy_path+'/閒置車.xlsx')
dockerfree_reduce = pd.read_excel(strategy_path+'/閒置位.xlsx')
dispatch_clean = pd.read_excel(strategy_path+'/最佳化調度.xlsx')
dockerfree_add = pd.read_excel(strategy_path+'/擴位.xlsx')

# 閒置，將閒置量能移至缺處
# 閒置車
idel_clean.groupby('週間週末').agg({'3、4月平均閒置車數': 'sum'})
# 閒置位
dockerfree_reduce.agg({'建議調整柱數_3月': 'sum'})

# 需改善，短期- 投車、最佳化調度、多出來的車再投入
# 投車 5月底 1500已做
# 最佳化調度
dispatch_clean['6點在站差'] = (dispatch_clean['建議6點在站車_3月'] - dispatch_clean['實際6點在站車_3月'])
dispatch_clean.groupby('週間週末').agg({'成效(日模擬-現實_調度車數差_兩月平均)': 'sum'})
dispatch_clean.groupby('週間週末').agg({'6點在站差': 'sum'})

# 需改善-長期- 增車、增位
dockerfree_add.agg({'建議調整柱數_3月': 'sum'})
dockerfree_add.groupby('週間週末').agg({'實際調度車數_3月': 'sum'})

# =============================================================================



