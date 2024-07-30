# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 17:49:09 2023

@author: rz3881
"""

import pandas as pd
import seaborn as sns

# load
ubike = pd.read_csv(r'D:\iima\ubike分析\DM\output_for_roy\202211.csv')

# filter to test
target_stop_id = 'U105015'
is_on_stop = (ubike['on_stop_id']==target_stop_id)
on_stop_ubike = ubike.loc[is_on_stop]
is_off_stop = (ubike['off_stop_id']==target_stop_id)
off_stop_ubike = ubike.loc[is_off_stop]

# extract hour info
on_stop_ubike['hour'] = pd.to_datetime(on_stop_ubike['on_time']).dt.hour.copy()
off_stop_ubike['hour'] = pd.to_datetime(off_stop_ubike['off_time']).dt.hour.copy()

# by stop by date by hour agg data
hourly_on_ubike = on_stop_ubike.groupby(['data_date', 'hour']).agg({'card_id': 'count'}).reset_index()
hourly_off_ubike = off_stop_ubike.groupby(['data_date', 'hour']).agg({'card_id': 'count'}).reset_index()
hourly_ubike = hourly_on_ubike.merge(hourly_off_ubike, how='outer', on=['data_date', 'hour'])
hourly_ubike.columns = ['date', 'hour', 'rent', 'return']
hourly_ubike = hourly_ubike.fillna(0)

# extract weekday info
hourly_ubike['weekday'] = pd.to_datetime(hourly_ubike['date']).dt.weekday + 1

# Comparing the number within the same hour on different weekday
# all weekday
sns.boxplot(x='hour', y='rent', data=hourly_ubike)
sns.boxplot(x='hour', y='return', data=hourly_ubike)
# weekday
is_weekday = (hourly_ubike['weekday']<=5)
sns.boxplot(x='hour', y='rent', data=hourly_ubike.loc[is_weekday])
sns.boxplot(x='hour', y='return', data=hourly_ubike.loc[is_weekday])
# weekend
is_weekend = (hourly_ubike['weekday']>5)
sns.boxplot(x='hour', y='rent', data=hourly_ubike.loc[is_weekend])
sns.boxplot(x='hour', y='return', data=hourly_ubike.loc[is_weekend])

# 結論
'''
無法衡量平不平穩，建議by weekday用中位數(平均可能受離群值影響)
根據圖案可知，每個時段都會有跳動，也許可以檢驗是不是同分佈
因為不是同分佈的話，好像也沒更好的方式去簡化?
而且中位數應該可以排除離群值
'''
