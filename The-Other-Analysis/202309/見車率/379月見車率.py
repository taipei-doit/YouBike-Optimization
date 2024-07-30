# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 10:15:27 2023

@author: rz3881
"""

import pandas as pd
import datetime

root_path = r'D:\iima\ubike分析'

# 50站
test50 = pd.read_csv(root_path+'/DIM/投車50站.csv')
test50 = test50.rename(columns={'ID': 'stop_id', '主/衛星站': 'level'})
test50['is_test50'] = True

available_prob = available_prob.merge(
    test50[['stop_id', 'level', 'is_test50']],
    how='outer', on='stop_id'
    )


# 202303
# Load
rb_202303 = pd.read_excel(
    root_path+'/DM/202303/閒置車/redundancy_bike.xlsx',
    sheet_name='redundancy_bike'
)
rb_202303 = rb_202303.loc[rb_202303['date'] != datetime.date(2023, 2, 28)]
rb_202303['見車率'] = 1 - (rb_202303['empty_minutes'] / (18*60))
# rb_202303['交易數'] = rb_202303['sum_rent'] + rb_202303['sum_return']
rb_202303 = rb_202303.merge(test50[['stop_id', 'is_test50']], how='outer', on='stop_id')
rb_202303['is_test50'] = rb_202303['is_test50'].fillna(False)
# agg
agg_202303 = rb_202303.groupby('stop_id').agg({
    '見車率': 'mean',
    'is_work_today': 'sum',
    'is_test50': 'first'
}).reset_index()
agg_202303 = agg_202303.rename(columns={'is_work_today': '營運天數'})
agg_202303 = agg_202303.loc[agg_202303['營運天數']>=31]
# 僅50站
agg_202303_test50 = agg_202303.loc[agg_202303['is_test50']]
agg_202303_test50['見車率'].mean()
len(agg_202303_test50['stop_id'].unique())
# 全北市
agg_202303['見車率'].mean()
len(agg_202303['stop_id'].unique())
rb_202303['交易數'].sum()/2

# 202307
rb_202307 = pd.read_excel(
    root_path+'/DM/202307/閒置車/redundancy_bike.xlsx',
    sheet_name='redundancy_bike'
)
rb_202307 = rb_202307.loc[rb_202307['date'] != datetime.date(2023, 6, 30)]
rb_202307['見車率'] = 1 - (rb_202307['empty_minutes'] / (18*60))
rb_202307['交易數'] = rb_202307['sum_rent'] + rb_202307['sum_return']
rb_202307 = rb_202307.merge(test50[['stop_id', 'is_test50']], how='outer', on='stop_id')
rb_202307['is_test50'] = rb_202307['is_test50'].fillna(False)
# agg
agg_202307 = rb_202307.groupby('stop_id').agg({
    '見車率': 'mean',
    'is_work_today': 'sum',
    'is_test50': 'first'
}).reset_index()
agg_202307 = agg_202307.rename(columns={'is_work_today': '營運天數'})
agg_202307 = agg_202307.loc[agg_202307['營運天數']>=31]
# 僅50站
agg_202307_test50 = agg_202307.loc[agg_202307['is_test50']]
agg_202307_test50['見車率'].mean()
len(agg_202307_test50['stop_id'].unique())
# 全北市
agg_202307['見車率'].mean()
len(agg_202307['stop_id'].unique())
rb_202307['交易數'].sum()/2

# 202309
rb_202309 = pd.read_excel(
    root_path+'/DM/202309/閒置車/redundancy_bike.xlsx',
    sheet_name='redundancy_bike'
)
rb_202309 = rb_202309.loc[rb_202309['date'] != datetime.date(2023, 8, 31)]
rb_202309['見車率'] = 1 - (rb_202309['empty_minutes'] / (18*60))
rb_202309['交易數'] = rb_202309['sum_rent'] + rb_202309['sum_return']
rb_202309 = rb_202309.merge(test50[['stop_id', 'is_test50']], how='outer', on='stop_id')
rb_202309['is_test50'] = rb_202309['is_test50'].fillna(False)
# agg
agg_202309 = rb_202309.groupby('stop_id').agg({
    '見車率': 'mean',
    'is_work_today': 'sum',
    'is_test50': 'first'
}).reset_index()
agg_202309 = agg_202309.rename(columns={'is_work_today': '營運天數'})
agg_202309 = agg_202309.loc[agg_202309['營運天數']>=30]
# 僅50站
agg_202309_test50 = agg_202309.loc[agg_202309['is_test50']]
agg_202309_test50['見車率'].mean()
len(agg_202309_test50['stop_id'].unique())
# 全北市
agg_202309['見車率'].mean()
len(agg_202309['stop_id'].unique())
rb_202309['交易數'].sum()/2

# 交易數必須用原始交易，拚上50站標籤，才能算出準確的
