# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans

root_path = r'D:\iima\ubike分析'

# load
ubike_byweekday = pd.read_csv(root_path+'/DM/[txn]ubike_202211_agg_by_stop_weekday_hour.csv')
ubike_stops = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
ubike_stops['stop_id'] = 'U' + ubike_stops['stop_id'].astype(str)
ubike_stops = ubike_stops[['stop_id', 'capacity']]

# 每小時無必要，合併為5個時段
hour_to_period = {}
hour_to_period.update({h: '0twlight' for h in range(0, 6)})
hour_to_period.update({h: '1morning' for h in range(6, 10)})
hour_to_period.update({h: '2noon' for h in range(10, 15)})
hour_to_period.update({h: '3afternoon' for h in range(15, 20)})
hour_to_period.update({h: '4evening' for h in range(20, 24)})
ubike_byweekday['period'] = ubike_byweekday['hour'].map(hour_to_period)
ubike_byperiod = ubike_byweekday.groupby(['stop_id', 'stop', 'weekday', 'period']).agg({
    'rent': 'sum',
    'return': 'sum',
    'net_profit': 'sum'
    }).reset_index()

# 只要2.0，且拚上capacity
ubike_byperiod = ubike_byperiod.merge(ubike_stops, how='right', on='stop_id')
# 正規化，借還量/總站量，能體現這個時段借走、還了幾個站的量
ubike_byperiod['net_capacity'] = ubike_byperiod['net_profit'] / ubike_byperiod['capacity']

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
x = ubike_byperiod.pivot_table(index='stop_id', columns='wp', values='net_capacity')
x = x.fillna(0)

# 不在乎原始值多少，只在乎高峰在哪


# clustering
k = 6
kmeans = KMeans(init="random", n_clusters=k, random_state=42)
kmeans.fit(x)

labels = kmeans.fit_predict(x)
labels = pd.Series(labels)
labels.index = x.index
labels = labels.reset_index()
labels.columns = ['stop_id', 'label']
labels['label'].value_counts()
len(labels)
sum(labels.value_counts() > 10)

labels.to_csv(root_path+'/DM/[PBI][txn]kmean_label_by_weekdayhour_v2.csv', index=False)
ubike_byperiod.to_csv(root_path+'/DM/[PBI][txn]202211_by_weekdayperiod_v2.csv', index=False)
