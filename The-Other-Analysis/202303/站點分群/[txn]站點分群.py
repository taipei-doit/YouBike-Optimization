# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:58:47 2023

@author: rz3881
"""

import pandas as pd
from sklearn.cluster import KMeans

root_path = r'D:\iima\ubike分析'
txn_path = root_path+r'\DM\202303\prepared_data\txn'
cluster_path = root_path+r'\DM\202303\站點分群'

# load
file_path = txn_path+'/aggregate_by_weekday_by_period.csv'
txn_byperiod = pd.read_csv(file_path)

# shape data
x = txn_byperiod.pivot_table(index='stop_id', columns='wp', values='net_profit')
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
labels['label'] = 'txn_' + labels['label'].astype(str)
labels['label'].value_counts()
len(labels)
sum(labels.value_counts() > 10)

# save
file_path = cluster_path+'/[txn]kmean_label.csv'
labels.to_csv(file_path, index=False, encoding='UTF-8')

