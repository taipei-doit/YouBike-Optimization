# -*- coding: utf-8 -*-
"""
Created on Mon May  8 14:07:59 2023

@author: rz3881
"""

# 彙整各種資料，檢視閒置車可能性與定義

import pandas as pd
import datetime

root_path = r'D:\iima\ubike分析'
txn_path = root_path+r'\DM\202303\prepared_data\txn'
dispatch_path = root_path+r'\DM\202303\prepared_data\dispatch'
status_path = root_path+r'\DM\202303\prepared_data\status'

# load 
txn = pd.read_csv(txn_path+'/txn_only_ubike.csv')
txn['on_time'] = pd.to_datetime(txn['on_time']).dt.tz_localize(None)
txn['off_time'] = pd.to_datetime(txn['off_time']).dt.tz_localize(None)
status = pd.read_csv(status_path+'/unique_raw.csv')
status['source_update_time'] = pd.to_datetime(status['source_update_time'])
dispatch = pd.read_csv(dispatch_path+'/cleaned_raw.csv')
dispatch['arrival_time'] = pd.to_datetime(dispatch['arrival_time'])
stop = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')

# filter config
target_date = datetime.date(2023, 3, 17)
target_stop = '捷運公館站(2號出口)'
# 交易
col_name = ['card_id', 'bike_id', 'stop_id', 'stop_name', 'txn_time']
is_date = (txn['on_time'].dt.date==target_date)
is_stop = (txn['on_stop']==target_stop)
on_txn = txn.loc[is_date & is_stop, ['card_id', 'route_name', 'on_stop_id', 'on_stop', 'on_time']]
on_txn.columns = col_name
on_txn['type'] = 'on'
is_date = (txn['off_time'].dt.date==target_date)
is_stop = (txn['off_stop']==target_stop)
off_txn = txn.loc[is_date & is_stop, ['card_id', 'route_name', 'off_stop_id', 'off_stop', 'off_time']]
off_txn.columns = col_name
off_txn['type'] = 'off'
target_txn = pd.concat([on_txn, off_txn])
# 站點回報
is_date = (status['source_update_time'].dt.date==target_date)
is_stop = (status['stop_name']==target_stop)
target_status = status.loc[is_date & is_stop]
target_status = target_status[['stop_id', 'stop_name', 'service_status',
                              'capacity', 'available_rent_bikes',
                              'available_return_bikes', 'source_update_time']]
# 調度
is_date = (dispatch['arrival_time'].dt.date==target_date)
is_stop = (dispatch['stop_name']==target_stop)
target_dispatch = dispatch.loc[is_stop&is_date]


# 結合status、txn來釐清閒置車輛，調度時間不可用，因此非交易的變動全都算到調度上
# 站點回傳會delay，目前觀察delay時間4~n秒不固定，但原則上我們只要確認不會差太多
target_status['source_update_time'] = target_status['source_update_time'] - pd.Timedelta(seconds=4)
target_status['actual_delta'] = target_status['available_rent_bikes'] - target_status['available_rent_bikes'].shift(1)
# 將
ts = target_status[['source_update_time', 'available_rent_bikes']]
ts.columns = ['txn_time', 'type']
ts['type'] = 'status'
tt = target_txn[['txn_time', 'type']]
joined_txn = pd.concat([ts, tt])
joined_txn = joined_txn.sort_values(['txn_time', 'type'])
# # status join txn 的原始樣貌
# ts['status'] = 'api'
# status_join_txn = ts.merge(tt, how='outer', on='txn_time')
on_count = 0
off_count = 0
res = {'txn_time': [], 'txn_on': [], 'txn_off': []}
for _, row in joined_txn.iterrows():
    # break
    is_api_return = (row['type']=='status')
    is_txn = (row['type']=='on')|(row['type']=='off')
    # count txn type
    if is_txn:
        if row['type'] == 'on':
            on_count += 1
        else:
            off_count += 1
    # api return, summit txn count
    if is_api_return:
        res['txn_time'].append(row['txn_time'])
        res['txn_on'].append(on_count)
        res['txn_off'].append(off_count)
        on_count = 0
        off_count = 0
res1 = pd.DataFrame(res)
target_status = target_status.merge(res1, how='left',
                                    left_on='source_update_time',
                                    right_on='txn_time')
target_status['txn_delta'] = target_status['txn_off'] - target_status['txn_on']
target_status['dispatch_delta'] = target_status['actual_delta'] - target_status['txn_delta']
target_status = target_status.drop(columns='txn_time')
