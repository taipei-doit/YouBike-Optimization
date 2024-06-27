# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 23:33:26 2023

@author: rz3881
"""

import pandas as pd
import time
import pickle

root_path = r'D:\iima\ubike分析'
txn_path = root_path+r'\DM\202303\prepared_data\txn'
addbike_path = root_path+r'\DM\202303\增車'

# load txn
txn = pd.read_csv(txn_path+'/txn_only_ubike.csv')
txn['on_hour'] = pd.to_datetime(txn['on_time']).dt.hour
txn['off_hour'] = pd.to_datetime(txn['off_time']).dt.hour
txn['weekday'] = pd.to_datetime(txn['on_time']).dt.weekday + 1
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
txn['weekday_type'] = txn['weekday'].map(weekday_map)

# filter out 1.0
is_2dot0 = (txn['route_name'] == 2)
txn_v2 = txn.loc[is_2dot0]

# 逐站、分OD、分weekday_type、找出各時段使用人數母集
total_loop = 0
all_stop_id = set(txn_v2['on_stop_id']) | set(txn_v2['off_stop_id'])
all_weekday_type = ['weekday', 'weekend']
all_hour = range(0, 24)
# {stop_id: {weekday_type: {hour: {OD: set(card_id)}}}}
potential_demand_idlist = {}
for _id in all_stop_id:
    potential_demand_idlist[_id] = {}
    for weekday_type in all_weekday_type:
        potential_demand_idlist[_id][weekday_type] = {}
        for h in all_hour:
            potential_demand_idlist[_id][weekday_type][h] = {}
            total_loop += 1
            for od in ['O', 'D']:
                potential_demand_idlist[_id][weekday_type][h][od] = None
# on
t = time.time()
a = 0
for (stop_id, weekday_type, hour), subset in txn_v2.groupby(['on_stop_id', 'weekday_type', 'on_hour']):
    a += 1
    card_statistic = subset['card_id'].value_counts()
    card_statistic.name = 'txn_count'
    potential_demand_idlist[stop_id][weekday_type][hour]['O'] = card_statistic
    if (a%1000) == 0:
        print(f'O part, at {a}/{total_loop}, cost {time.time() - t} seconds.')
# off
a = 0
for (stop_id, weekday_type, hour), subset in txn_v2.groupby(['off_stop_id', 'weekday_type', 'off_hour']):
    a += 1
    card_statistic = subset['card_id'].value_counts()
    card_statistic.name = 'txn_count'
    potential_demand_idlist[stop_id][weekday_type][hour]['D'] = card_statistic
    if (a%1000) == 0:
        print(f'D part, at {a}/{total_loop}, cost {time.time() - t} seconds.')
# save
file_path = addbike_path+'/[demand]potential_demand_idlist.pickle'
with open(file_path, 'wb') as f:
    pickle.dump(potential_demand_idlist, f)


# 根據使用者母體計算使用人數
potential_demand_count = {'stop_id': [],
                          'weekday_type': [],
                          'hour': [],
                          'rent_demand_onece': [],
                          'return_demand_onece': [],
                          'rent_demand_multi': [],
                          'return_demand_multi': []}
for stop_id, v1 in potential_demand_idlist.items():
    for weekday_type, v2 in v1.items():
        for hour, v3 in v2.items():
            potential_demand_count['stop_id'].append(stop_id)
            potential_demand_count['weekday_type'].append(weekday_type)
            potential_demand_count['hour'].append(hour)
            for od, idlist in v3.items():
                # raise ValueError('123')
                if idlist is not None:
                    onece = idlist.shape[0]
                    is_at_least_twice = (idlist >= 2)
                    multi = is_at_least_twice.sum()
                else:
                    onece = 0
                    multi = 0
                    
                if od == 'O':
                    potential_demand_count['rent_demand_onece'].append(onece)
                    potential_demand_count['rent_demand_multi'].append(multi)
                else:
                    potential_demand_count['return_demand_onece'].append(onece)
                    potential_demand_count['return_demand_multi'].append(multi)
potential_demand_count_df = pd.DataFrame(potential_demand_count)
# save
file_path = addbike_path+'/[demand]potential_demand_count_by_weekdaytype_by_hour.csv'
potential_demand_count_df.to_csv(file_path, index=False, encoding='utf8')
