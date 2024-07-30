# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 08:29:42 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
addbike_path = root_path+r'\DM\202303\增車'
txn_path = root_path+r'\DM\202303\prepared_data\txn'
status_path = root_path+r'\DM\202303\prepared_data\status'
dispatch_path = root_path+r'\DM\202303\prepared_data\dispatch'
trans_path = root_path+r'\DM\202303\prepared_data\站點附近大眾交通工具'

# load
# status統計，已排除台大、台科大、1.0與任何停用
status = pd.read_csv(addbike_path+'/[status]available_rent_prob_by_hour.csv')
# 推估需求人數
demand = pd.read_csv(addbike_path+'/[demand]potential_demand_predict.csv')
# 調度情況
dispatch = pd.read_csv(dispatch_path+'/aggregate_by_weekdaytype_by_hour.csv')
# 附近的其他大眾運輸交通工具
transportation = pd.read_csv(trans_path+'/public_transport_within_200m.csv')

# merge all data
whole = status.merge(demand, how='left', on=['stop_id', 'weekday_type', 'hour'])
whole = whole.merge(dispatch, how='left', on=['stop_id', 'weekday_type', 'hour'])
whole = whole.merge(transportation, how='left', on=['stop_id'])
# 去掉status沒有資料的部分，基本上是台大特區
is_no_status = whole['available_rent_prob'].isna()
whole = whole.loc[~is_no_status]
# reshape
whole['suggest_add'] = whole['rent_predict'] - whole['mean_available_rent']
whole['total_txn'] = whole['raw_rent_count'] + whole['raw_return_count']


# 排除周末，因周末難預測，且相對不是日常
is_weekday = (whole['weekday_type']=='weekday')
# 去掉00:00~05:59點
is_normal_hour = (whole['hour']>=6)
whole_filtered = whole.loc[is_weekday&is_normal_hour]


# reshape
# 哪個站、哪個時段、見車率多少、是不是故意調走的、附近有沒有替代工具、要加多少車
output_whole = whole_filtered[['stop_id', 'stop_name_x', 'stop_type', 'capacity_x', 
               'weekday_type', 'hour',
               'available_rent_prob', 'suggest_add', 'add', 'remove', 
               'ubike_within_200m', 'mrt_within_200m', 'bus_within_200m',
               'mean_available_rent', 'mean_available_return',
               'net_profit_predict', 'net_profit', 'net_profit_bias',
               'rent_demand_multi', 'rent', 'rent_predict', 'rent_bias',
               'return_demand_multi', 'return', 'return_predict', 'return_bias',
               'raw_data_count', 'total_txn', 'lng', 'lat']]
output_whole.columns = ['ID', '站名', '類別', '車柱數',
                 '周間/周末', '時間',
                 '見車率', '建議增加車數', '平均調度放入數', '平均調度移走數',
                 '200公尺內ubike站數', '200公尺內捷運站數', '200公尺內公車站數',
                 '平均可借車數', '平均可還位數',
                 '淨增減(預測)', '淨增減(實際)', '淨增減(誤差)', 
                 '該時段借車常客數', '平均實際借車數', '預測借車數', '借車誤差',
                 '該時段還車常客數', '平均實際還車數', '預測還車數', '還車誤差',
                 'API回傳資料筆數', '總交易數', 'lng', 'lat']
# save
file_path = addbike_path+'/[summary]bike_empty_whole_view.xlsx'
output_whole.to_excel(file_path, index=False)

# agg by stop_id

# agg
whole_agg = whole_filtered.groupby(['stop_id']).agg({
    'stop_name_x': 'first',
    'stop_type': 'first',
    'available_rent_prob': 'mean',
    'capacity_y': 'first',
    'raw_data_count': 'sum',
    'total_txn': 'sum'
    }).reset_index()
whole_agg.columns = ['ID', '站名', '類別', '見車率', '車柱數',
                     'API回傳資料筆數', '總交易數']
# save
file_path = addbike_path+'/[summary]empty_whole_view_agg_by_stopid.xlsx'
whole_agg.to_excel(file_path, index=False)
