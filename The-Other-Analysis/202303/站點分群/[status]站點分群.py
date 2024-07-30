# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans

root_path = r'D:\iima\ubike分析'
status_path = root_path+r'\DM\202303\prepared_data\status'
cluster_path = root_path+r'\DM\202303\站點分群'

# load
file_path = status_path+'/aggregate_by_weekday_by_period.csv'
status_byperiod = pd.read_csv(file_path)

# filter，因凌晨、晚上不重要，分群時刪除
keep_period = ['1morning', '2noon', '3afternoon']
is_keep_period = status_byperiod['period'].isin(keep_period)
status_byperiod = status_byperiod.loc[is_keep_period]
# shape data
x = status_byperiod.pivot_table(index='stop_id', columns='wp',
                                values=['available_rent_prob',
                                        'available_return_prob'])
x = x.fillna(0)

# clustering
k = 6
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

# save
file_path = cluster_path+'/[status]kmean_label.csv'
labels.to_csv(file_path, index=False, encoding='UTF-8')
