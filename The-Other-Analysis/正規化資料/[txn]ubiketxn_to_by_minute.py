# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 14:16:41 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
ym = '202309'
txn_path = root_path+f'/DM/{ym}/prepared_data/txn'

# load
txn = pd.read_csv(txn_path+'/txn_only_ubike.csv')

# extract time info
txn['on_minute'] = pd.to_datetime(txn['on_time']).dt.floor('Min')
txn['off_minute'] = pd.to_datetime(txn['off_time']).dt.floor('Min')

# filter out 1.0
# The earliest data records ubike version using column "route_name"
# Since there is no 1.0 version after 202212, the column "route_name" is now used to track "bike_id".
is_route_name_were_version = (txn['route_name'].iloc[0]==1) or (txn['route_name'].iloc[0]==2)
if is_route_name_were_version:
    is_2dot0 = (txn['route_name'] == 2)
    txn_v2 = txn.loc[is_2dot0]
else:
    txn_v2 = txn

# by stop by date by hour agg
ubike_rent = txn_v2.groupby(['on_minute', 'on_stop_id']).agg({
    'card_id': 'count',
    'is_transfer': 'sum'
    }).reset_index()
ubike_rent.columns = ['txn_time', 'stop_id', 'rent', 'on_transfer_count']
ubike_return = txn_v2.groupby(['off_minute', 'off_stop_id']).agg({
    'card_id': 'count',
    'is_transfer': 'sum'
    }).reset_index()
ubike_return.columns = ['txn_time', 'stop_id', 'return', 'off_transfer_count']
ubike_by1m = ubike_rent.merge(ubike_return,
                                on=['txn_time', 'stop_id'],
                                how='outer')

# save
file_path = txn_path+'/txn_only_ubike2_agg_by_minute.csv'
ubike_by1m.to_csv(file_path, index=False, encoding='utf8')
