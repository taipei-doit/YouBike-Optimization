# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 12:13:29 2023

@author: rz3881
"""

import pandas as pd

input_path = r'D:\iima\ubike分析\DM\output_for_roy'

txn = pd.read_csv(input_path+'/202211.csv')
txn['on_time'] = pd.to_datetime(txn['on_time'])
txn['on_hour'] = txn['on_time'].dt.hour
txn['on_weekday'] = txn['on_time'].dt.weekday + 1

# 單次騎乘的人佔比
card_txn_count = txn['card_id'].value_counts().reset_index()
card_txn_count.columns = ['card_id', 'txn_count']
is_once = card_txn_count['txn_count']==1
once_txn_card_count = is_once.sum()
once_txn_card_count/card_txn_count.shape[0]

# 騎乘次數一次的人有都在什麼時候騎?
once_txn_card = set(card_txn_count.loc[is_once, 'card_id'])
once_txn = txn.loc[txn['card_id'].isin(once_txn_card)]
once_txn['on_weekday'].value_counts()
# 比較其他騎乘
not_once_txn = txn.loc[~txn['card_id'].isin(once_txn_card)]
not_once_txn['on_weekday'].value_counts()

# 單次騎乘行為佔所有交易的比例
once_txn.shape[0] / txn.shape[0]
