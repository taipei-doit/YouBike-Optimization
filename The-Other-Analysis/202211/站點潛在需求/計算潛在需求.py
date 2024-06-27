# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 19:10:34 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'

# 2.與潛在需求差異大

# load
txn_hourly = pd.read_csv(root_path+'/DM/站點分群_共用/[txn]ubike_202211_agg_by_stop_weekday_hour.csv')
demand_hourly = pd.read_csv(root_path+'/DM/站點潛在需求/[txn]potential_demand_count_by_weekday_type_by_hour.csv')
stop_info = pd.read_csv(root_path+'/DIM/ubike_stops_from_api.csv')
stop_info['stop_id'] = 'U' + stop_info['stop_id'].astype(str)
status_hourly = pd.read_csv(root_path+'/DM/缺車/[status]available_rent_prob_by_hour.csv')

# txn agg by weekday_type, hour
weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
               6: 'weekend', 7: 'weekend'}
txn_hourly['weekday_type'] = txn_hourly['weekday'].map(weekday_map)
txn_hourly = txn_hourly.groupby(['stop_id', 'weekday_type', 'hour']).agg({
    'rent': 'mean',
    'return': 'mean',
    'net_profit': 'mean'
    }).reset_index()

# merge
td_hourly = demand_hourly.merge(txn_hourly, how='left', on=['stop_id', 'weekday_type', 'hour'])
td_hourly = td_hourly.merge(status_hourly, how='left', on=['stop_id', 'weekday_type', 'hour'])

# 根據沒缺車的地方，計算多次的使用者數量與真實交易的比例
is_never_empty = (td_hourly['available_rent_prob'] == 1)
td_hourly_not_empty = td_hourly.loc[is_never_empty]
td_hourly_not_empty['rent_ratio'] = td_hourly_not_empty['rent'] / (td_hourly_not_empty['rent_demand_multi'] + 1)
td_hourly_not_empty['return_ratio'] = td_hourly_not_empty['return'] / (td_hourly_not_empty['return_demand_multi'] + 1)
hourly_ratio = td_hourly_not_empty.groupby(['weekday_type', 'hour']).agg({
    'rent_ratio': 'mean',
    'return_ratio': 'mean',
    })
# to dict
ratio_map = {}
for (weekday_type, hour), row in hourly_ratio.iterrows():
    if ratio_map.get(weekday_type):
        ratio_map[weekday_type][hour] = {'rent': row['rent_ratio'], 'return': row['return_ratio']}
    else:
        ratio_map[weekday_type] = {}
        ratio_map[weekday_type][hour] = {'rent': row['rent_ratio'], 'return': row['return_ratio']}

# predict
rent_predicts = []
return_predicts = []
net_profit_predicts = []
for _, row in td_hourly.iterrows():
    temp_r = ratio_map[row['weekday_type']][row['hour']]
    rent_predict = row['rent_demand_multi'] * temp_r['rent']
    return_predict = row['return_demand_multi'] * temp_r['return']
    net_profit_predict = (return_predict - rent_predict)
    rent_predicts.append(rent_predict)
    return_predicts.append(return_predict)
    net_profit_predicts.append(net_profit_predict)
td_hourly['rent_predict'] = rent_predicts
td_hourly['return_predict'] = return_predicts
td_hourly['net_profit_predict'] = net_profit_predicts
td_hourly['rent_bias'] = td_hourly['rent'] - td_hourly['rent_predict']
td_hourly['return_bias'] = td_hourly['return'] - td_hourly['return_predict']
td_hourly['net_profit_bias'] = td_hourly['net_profit'] - td_hourly['net_profit_predict']
# 
td_hourly = td_hourly.merge(stop_info[['stop_id', 'stop_name', 'capacity']],
                                how='left', on='stop_id')
# reshape
td_hourly = td_hourly[['stop_id', 'stop_name', 'capacity', 'weekday_type', 'hour',
                       'rent_demand_multi', 'rent', 'rent_predict','rent_bias', 
                       'return_demand_multi', 'return', 'return_predict', 'return_bias',
                       'net_profit', 'net_profit_predict', 'net_profit_bias']]

# save
file_path = '/DM/站點潛在需求/[txn]potential_demand_predict.csv'
td_hourly.to_csv(root_path+file_path, index=False, encoding='utf8')
