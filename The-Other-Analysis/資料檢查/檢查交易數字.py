# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 16:59:03 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
txn_path = root_path+r'\DM\202303\prepared_data\txn'

# load
txn_by_date_by_hour = pd.read_csv(txn_path+'/aggregate_by_date_by_hour.csv')

# filter
is_weekday = txn_by_date_by_hour['weekday']<=5
is_target_hour = txn_by_date_by_hour['hour']>=6
target_txn = txn_by_date_by_hour.loc[is_weekday&is_target_hour]

def count_txn_count_by_given_stop_id(stop_id):
    is_target = target_txn['stop_id']==stop_id
    rent_txn = target_txn.loc[is_target, 'rent'].sum()
    return_txn = target_txn.loc[is_target, 'return'].sum()
    total_txn = rent_txn + return_txn
    print(total_txn)
    return total_txn

# check
count_txn_count_by_given_stop_id('U101001')
count_txn_count_by_given_stop_id('U101004')
count_txn_count_by_given_stop_id('U101019')
