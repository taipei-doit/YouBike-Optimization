# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans
root_path = r'D:\iima\ubike分析\DM'

# load
ubike_status = pd.read_csv(root_path+'/[status]ubike_202211_stop_by_hour.csv')
ubike_status['standard_data_time'] = pd.to_datetime(ubike_status['standard_data_time'])
ubike_status['weekday'] = ubike_status['standard_data_time'].dt.weekday + 1
ubike_status['hour'] = ubike_status['standard_data_time'].dt.hour
ubike_status['stop_id'] = 'U' + ubike_status['stop_id'].astype(str)
# 每小時無必要，合併為數個時段
hour_to_period = {}
hour_to_period.update({h: '0twlight' for h in range(0, 7)})
hour_to_period.update({h: '1morning' for h in range(7, 10)})
hour_to_period.update({h: '2noon' for h in range(10, 16)})
hour_to_period.update({h: '3afternoon' for h in range(16, 20)})
hour_to_period.update({h: '4evening' for h in range(20, 24)})
ubike_status['period'] = ubike_status['hour'].map(hour_to_period)
ubike_byperiod = ubike_status.groupby(['stop_id', 'weekday', 'period']).agg({
    'hour': 'count',
    'empty_times_by10m': 'sum',
    'is_once_empty': 'sum',
    'full_times_by10m': 'sum',
    'is_once_full': 'sum'
    }).reset_index()
ubike_byperiod.columns = ['stop_id', 'weekday', 'period', 'row_counts_byhour',
                          'empty_times_by10m', 'empty_times_byhour',
                          'full_times_by10m', 'full_times_byhour']

# 沒車與沒位有可能同時存在，暫時不知道是不是因為調度造成的大量變異
# 這邊暫時只考慮單一指標，取沒車或沒位的次數較多者
# (原本打算沒位-沒車，但這樣可能稀釋問題發生的次數)
# = 計算此時段最有可能發生情況的機率
is_empty_more_than_full = (ubike_byperiod['empty_times_by10m'] > ubike_byperiod['full_times_by10m'])
ubike_byperiod['problem_times'] = ubike_byperiod['full_times_by10m']
ubike_byperiod.loc[is_empty_more_than_full, 'problem_times'] = -ubike_byperiod.loc[is_empty_more_than_full, 'empty_times_by10m']
row_counts_by10m = ubike_byperiod['row_counts_byhour'] * 6
ubike_byperiod['problem_prob'] = ubike_byperiod['problem_times'] / row_counts_by10m

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
x = ubike_byperiod.pivot_table(index='stop_id', columns='wp', values='problem_prob')
x = x.fillna(0)

# clustering
k=6
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

labels.to_csv(root_path+'/[PBI][status]kmean_label_by_weekdayperiod.csv', index=False)
ubike_byperiod.to_csv(root_path+'/[PBI][status]202211_by_weekdayperiod.csv', index=False)
