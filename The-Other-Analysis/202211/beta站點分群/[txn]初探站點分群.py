# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import math

def convert_to_z_distribution(stop_series):
    mean = stop_series.mean()
    std = stop_series.std()
    normalized_stop_series = (stop_series - mean) / std
    return normalized_stop_series

def sqrt_with_negatives(x, is_print_err=False, value_when_err=0):
    '開根號，當負值時，轉為正值開根號再加上負號'
    try:
        if x >=0:
            return math.sqrt(x)
        else:
            return -math.sqrt(-x)
    except Exception as e:
        if is_print_err:
            print(e)
        return value_when_err

root_path = r'D:\iima\ubike分析\DM'

# load
ubike_byweekday = pd.read_csv(root_path+'/[txn]ubike_202211_agg_by_stop_weekday_hour.csv')

# 每小時無必要，合併為數個時段
hour_to_period = {}
hour_to_period.update({h: '0twlight' for h in range(0, 7)})
hour_to_period.update({h: '1morning' for h in range(7, 10)})
hour_to_period.update({h: '2noon' for h in range(10, 16)})
hour_to_period.update({h: '3afternoon' for h in range(16, 20)})
hour_to_period.update({h: '4evening' for h in range(20, 24)})
ubike_byweekday['period'] = ubike_byweekday['hour'].map(hour_to_period)
ubike_byperiod = ubike_byweekday.groupby(['stop_id', 'stop', 'weekday', 'period']).agg({
    'rent': 'sum',
    'return': 'sum',
    'net_profit': 'sum'
    }).reset_index()
# add index for powerbi present
is_weekend = ubike_byperiod['weekday'] > 5
ubike_byperiod['weekday_type'] = 'weekday'
ubike_byperiod.loc[is_weekend, 'weekday_type'] = 'weekend'
ubike_byperiod['wp'] = ubike_byperiod['weekday'].astype(str) + ubike_byperiod['period'].astype(str) 
x_mapping = {}
c = 0
for w in range(1, 8):
    for p in range(0, 5):
        w = str(w)
        p = str(p)
        x_mapping[f'{w}{p}'] = c
        c += 1
ubike_byperiod['wp_index'] = ubike_byperiod['wp'].str.slice(0, 2).map(x_mapping)

# shape data
x = ubike_byperiod.pivot_table(index='stop_id', columns='wp', values='net_profit')
x = x.fillna(0)

# 不在乎原始值多少，只在乎高峰在哪
# 先log10(壓平)，再轉Z分配
# x_sqrt = x.applymap(sqrt_with_negatives)
# x_normalized = x.apply(convert_to_z_distribution, axis=1)
# x_normalized = x_normalized.fillna(0)
# 做不做對結果幾乎沒影響，刪除

# clustering
k=7
kmeans = KMeans(init="random", n_clusters=k, random_state=42)
kmeans.fit(x)

labels = kmeans.fit_predict(x)
labels = pd.Series(labels)
labels.index = x.index
labels = labels.reset_index()
labels.columns = ['stop_id', 'group']
labels.value_counts()
len(labels)
sum(labels.value_counts() > 10)

labels.to_csv(root_path+'/[PBI][txn]kmean_label_by_weekdayhour.csv', index=False)
ubike_byperiod.to_csv(root_path+'/[PBI][txn]202211_by_weekdayperiod.csv', index=False)
