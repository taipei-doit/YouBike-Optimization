# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 15:04:24 2023

@author: rz3881
"""

import pandas as pd
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import make_sure_folder_exist

# Config
root_path = r'D:\iima\ubike分析'
dim_path = root_path+'/DIM'
ref_yms = ['202304', '202307']
reference_idle_paths = [root_path+f'/DM/{ym}/閒置車' for ym in ref_yms]
valid_ym = '202309'
valid_idle_path = root_path+f'/DM/{valid_ym}/閒置車'
strategy_path = root_path+f'/DM/{valid_ym}/全策略'
confidence_ratio_threshould = 0.9
dock_adj_threshould = 5
yms = ref_yms + [valid_ym]
weekday_days = {'202303': 23, '202304': 20, '202307': 21, '202309': 21}
weekend_days = {'202303': 8, '202304': 10, '202307': 10, '202309': 9}
strategy_type = '缺車站點'
error_type1 = '異常_柱數變動'
error_type2 = '異常_天數不正常'


# Load
# 實驗50站
test50 = pd.read_excel(
    dim_path+'/加強調度FINALv3+見車率(更新至112.06.26).xlsx',
    sheet_name='50站見車率', skiprows=2
)
test50 = set(test50['站名'])
# 3月結論
ref_aggs = {}
for ref_ym in ref_yms:
    ref_idle_path = root_path+f'/DM/{ref_ym}/閒置車'
    ref_aggs[ref_ym] = pd.read_csv(ref_idle_path+'/compare_agg.csv')
# 4月結論
valid_agg = pd.read_csv(valid_idle_path + '/compare_agg.csv')

# Preprocess
data = None
for ym, agg in ref_aggs.items():
    if data is None:
        data = valid_agg.merge(agg, how='outer', on=['ID', '週間週末'])
    else:
        data = data.merge(agg, how='outer', on=['ID', '週間週末'])

# Rename
cols = list(valid_agg.columns[0:2])
valid_col = [f'{c}_{valid_ym}' for c in valid_agg.columns[2:]]
cols.extend(valid_col)
for ref_ym in ref_yms:
    ref_col = [f'{c}_{ref_ym}' for c in ref_aggs[ref_ym].columns[2:]]
    cols.extend(ref_col)
data.columns = cols

# Filter
unqualified = data.loc[data['ID']!='U26']
is_test50 = unqualified['站名_202307'].isin(test50)
unqualified['是否為測試50站'] = '非測試50站'
unqualified.loc[is_test50, '是否為測試50站'] = '測試50站'

unqualified['類型'] = None
# 正常時間過少
is_low_confidence = (unqualified[f'見車率_{valid_ym}'] < confidence_ratio_threshould)
unqualified.loc[is_low_confidence, '類型'] = strategy_type
# 柱數變動
dock_adj = (unqualified[f'柱數_{valid_ym}'] - unqualified[f'柱數_{ref_yms[0]}'])
is_dock_change = (dock_adj.abs()>=dock_adj_threshould)
unqualified.loc[is_dock_change, '類型'] = error_type1
# 可能有故障、停用、新設立，資料不齊全問題
is_weekday = (unqualified['週間週末']=='weekday')
is_weekend = ~is_weekday
for seq in range(len(yms)):
    ym = yms[seq]
    if seq == 0:
        is_normal_weekday = is_weekday & (unqualified[f'天數_{ym}']==weekday_days[ym])
        is_normal_weekend = is_weekend & (unqualified[f'天數_{ym}']==weekend_days[ym])
    else: 
        is_normal_weekday = is_normal_weekday & (unqualified[f'天數_{ym}']==weekday_days[ym])
        is_normal_weekend = is_normal_weekend & (unqualified[f'天數_{ym}']==weekend_days[ym])
unqualified.loc[(~is_normal_weekday) & (~is_normal_weekend), '類型'] = error_type2


# Save
unqualified['類型'].value_counts(dropna=False)
make_sure_folder_exist(strategy_path)
unqualified.to_csv(strategy_path+'/strategy_filter_abnormal.csv', index=False)

# Reshape
is_abnormal = ~unqualified['類型'].isna()
unqualified_clean = unqualified.loc[is_abnormal]
cols = ['ID', f'站名_{valid_ym}', '週間週末', '類型']
for ym in yms:
    temp_col = [f'天數_{ym}', f'柱數_{ym}',
                f'空車分鐘_{ym}', f'滿車分鐘_{ym}', f'資料可信度_{ym}']
    cols.extend(temp_col)
unqualified_clean = unqualified_clean[cols]

# Save
# error
is_error = (unqualified_clean['類型']==error_type1) | (unqualified_clean['類型']==error_type2)
error_clean = unqualified_clean.loc[is_error]
error_clean.to_excel(strategy_path+'/異常.xlsx', index=False)
# need bike
is_lack = (unqualified_clean['類型']==strategy_type)
lack_clean = unqualified_clean.loc[is_lack]
lack_clean.to_excel(strategy_path+'/缺車滿車.xlsx', index=False)
