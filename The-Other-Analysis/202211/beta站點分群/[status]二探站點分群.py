# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans
root_path = r'D:\iima\ubike分析'

# load
ubike_status = pd.read_csv(root_path+'/DM/[status]ubike_202211_stop_by_hour.csv')
ubike_status['standard_data_time'] = pd.to_datetime(ubike_status['standard_data_time'])
ubike_status['weekday'] = ubike_status['standard_data_time'].dt.weekday + 1
ubike_status['hour'] = ubike_status['standard_data_time'].dt.hour
ubike_status['stop_id'] = 'U' + ubike_status['stop_id'].astype(str)

# by period
# 每小時無必要，合併為數個時段
hour_to_period = {}
hour_to_period.update({h: '0twlight' for h in range(0, 6)})
hour_to_period.update({h: '1morning' for h in range(6, 10)})
hour_to_period.update({h: '2noon' for h in range(10, 15)})
hour_to_period.update({h: '3afternoon' for h in range(15, 20)})
hour_to_period.update({h: '4evening' for h in range(20, 24)})
ubike_status['period'] = ubike_status['hour'].map(hour_to_period)
ubike_byperiod = ubike_status.groupby(['stop_id', 'weekday', 'period']).agg({
    'raw_data_count': 'sum',
    'raw_data_disabled_count': 'sum',
    'hour': 'count',
    'empty_count_by10m': 'sum',
    'is_once_empty': 'sum',
    'full_count_by10m': 'sum',
    'is_once_full': 'sum',
    'disabled_count_by10m': 'sum',
    'is_once_disable': 'sum',
    }).reset_index()
ubike_byperiod['hour'] = ubike_byperiod['hour'] * 6
ubike_byperiod.columns = ['stop_id', 'weekday', 'period',
                          'raw_data_count', 'raw_data_disabled_count',
                          'data_count_by10m',
                          'empty_count_by10m', 'empty_count_byhour',
                          'full_count_by10m', 'full_count_byhour',
                          'disabled_count_by10m', 'disabled_count_byhour']
# 沒車沒位合併計算太複雜了，分開兩個結果
# 並且考慮不要分群了，直接用機率篩選
ubike_byperiod['empty_prob_by10m'] = ubike_byperiod['empty_count_by10m'] / ubike_byperiod['data_count_by10m']
ubike_byperiod['full_prob_by10m'] = ubike_byperiod['full_count_by10m'] / ubike_byperiod['data_count_by10m']
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
ubike_byperiod.to_csv(root_path+'/DM/[PBI][status]202211_by_weekdayperiod_v2.csv',
                      index=False, encoding='UTF-8')


# clustering
# shape data
x = ubike_byperiod.pivot_table(index='stop_id', columns='wp',
                               values=['empty_prob_by10m', 'full_prob_by10m'])
x = x.fillna(0)
k = 7
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

labels.to_csv(root_path+'/DM/[PBI][status]kmean_label_by_weekdayperiod_v2.csv',
              index=False, encoding='UTF-8')
