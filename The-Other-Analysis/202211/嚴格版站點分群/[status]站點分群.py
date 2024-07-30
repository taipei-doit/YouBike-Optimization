# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans
root_path = r'D:\iima\ubike分析'

# load
file_path = '/DM/站點分群_共用/[status]ubike_202211_stop_by_hour_by_weekday.csv'
ubike_status = pd.read_csv(root_path+file_path)

# by period
# 每小時無必要，合併為數個時段
hour_to_period = {}
hour_to_period.update({h: '0twilight' for h in range(0, 6)})
hour_to_period.update({h: '1morning' for h in range(6, 10)})
hour_to_period.update({h: '2noon' for h in range(10, 15)})
hour_to_period.update({h: '3afternoon' for h in range(15, 20)})
hour_to_period.update({h: '4evening' for h in range(20, 24)})
ubike_status['period'] = ubike_status['hour'].map(hour_to_period)
ubike_byperiod = ubike_status.groupby(['stop_id', 'weekday', 'period']).agg({
    'raw_data_count': 'sum',
    'raw_data_disabled_count': 'sum',
    'total_minutes': 'sum',
    'disabled_count_by1m': 'sum',
    'median_available_rent': 'median',
    'median_available_return': 'median',
    'empty_minutes': 'sum',
    'full_minutes': 'sum',
    'max_continuous_empty_minutes': 'max',
    'max_continuous_full_minutes': 'max',
    'continuous_empty_minutes': 'sum',
    'continuous_full_minutes': 'sum'
    }).reset_index()
# 沒車沒位合併計算太複雜了，分開兩個結果(考慮不要分群了，直接用機率篩選)
# 見車率/見位率 = (總分鐘-缺車分鐘)/總分鐘 = 1-(缺車分鐘/總分鐘)
ubike_byperiod['available_rent_prob'] = 1 - (ubike_byperiod['empty_minutes'] / ubike_byperiod['total_minutes'])
ubike_byperiod['available_return_prob'] = 1 - (ubike_byperiod['full_minutes'] / ubike_byperiod['total_minutes'])
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
file_path = '/DM/站點分群_嚴格/[status]202211_by_weekday_by_period.csv'
ubike_byperiod.to_csv(root_path+file_path, index=False, encoding='UTF-8')


# clustering
# filter，因凌晨、晚上不重要，分群時刪除
keep_period = ['1morning', '2noon', '3afternoon']
is_keep_period = ubike_byperiod['period'].isin(keep_period)
ubike_byperiod = ubike_byperiod.loc[is_keep_period]
# shape data
x = ubike_byperiod.pivot_table(index='stop_id', columns='wp',
                               values=['available_rent_prob', 'available_return_prob'])
x = x.fillna(0)
k = 4
kmeans = KMeans(init="random", n_clusters=k, random_state=42)
kmeans.fit(x)

labels = kmeans.fit_predict(x)
labels = pd.Series(labels)
labels.index = x.index
labels = labels.reset_index()
labels.columns = ['stop_id', 'label']
labels['label'] = 'status_' + labels['label'].astype(str)
labels['label'].value_counts()
len(labels)
sum(labels.value_counts() > 10)

file_path = '/DM/站點分群_嚴格/[PBI][status]kmean_label_by_weekday_by_period.csv'
labels.to_csv(root_path+file_path, index=False, encoding='UTF-8')
