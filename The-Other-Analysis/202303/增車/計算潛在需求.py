# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 19:10:34 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
txn_path = root_path+r'\DM\202303\prepared_data\txn'
status_path = root_path+r'\DM\202303\prepared_data\status'
addbike_path = root_path+r'\DM\202303\增車'

# 2.與潛在需求差異大
# load
txn_hourly = pd.read_csv(txn_path+'/aggregate_by_weekdaytype_by_hour.csv')
demand_hourly = pd.read_csv(addbike_path+'/[demand]potential_demand_count_by_weekdaytype_by_hour.csv')
stop_info = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')
status_hourly = pd.read_csv(addbike_path+'/[status]available_rent_prob_by_hour.csv')

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
# reshape
td_hourly = td_hourly[['stop_id', 'stop_name', 'capacity', 'weekday_type', 'hour',
                       'raw_rent_count', 'rent_demand_multi', 'rent', 'rent_predict','rent_bias', 
                       'raw_return_count', 'return_demand_multi', 'return', 'return_predict', 'return_bias',
                       'net_profit', 'net_profit_predict', 'net_profit_bias']]

# save
file_path = addbike_path+'/[demand]potential_demand_predict.csv'
td_hourly.to_csv(file_path, index=False, encoding='utf8')
