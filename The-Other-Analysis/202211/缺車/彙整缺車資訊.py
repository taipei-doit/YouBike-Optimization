# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 08:29:42 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'

# 缺車情況
status = pd.read_csv(root_path+'/DM/缺車/[status]available_rent_prob_by_hour.csv')
# 已排除台大、台科大、1.0與任何停用

# 交易數、推估需求人數
demand = pd.read_csv(root_path+'/DM/站點潛在需求/[txn]potential_demand_predict.csv')

# 調度情況
dispatch = pd.read_csv(root_path+'/DM/缺車/[dispatch]number_by_weekday_type_by_hour.csv')

# 附近的其他大眾運輸交通工具
transportation = pd.read_csv(root_path+'/DM/站點附近大眾交通工具/YB_Buffer200m.csv', encoding='big5')
# 排除新北
is_tpe = transportation['station_uid'].str.startswith('TPE')
transportation = transportation.loc[is_tpe]
transportation['stop_id'] = transportation['station_uid'].str.replace('TPE', '')
# 修正資料
is_start_with_500 = transportation['stop_id'].str.startswith('500')
transportation.loc[is_start_with_500, 'stop_id'] = transportation.loc[is_start_with_500, 'stop_id'].str.slice(3, )
transportation['stop_id'] = 'U' + transportation['stop_id']
transportation['buffer200m_YB'] = transportation['buffer200m_YB'] - 1 # 把自己算進去了

# 彙總
whole = status.merge(demand, how='outer', on=['stop_id', 'weekday_type', 'hour'])
whole = whole.merge(dispatch, how='outer', on=['stop_id', 'weekday_type', 'hour'])
whole = whole.merge(transportation, how='right', on=['stop_id'])

# filter
# 排除周末，因周末難預測，且相對不是日常
is_weekday = (whole['weekday_type']=='weekday')
whole = whole.loc[is_weekday]
# 去掉status沒有資料的部分，基本上次台大特區
is_no_status = whole['available_rent_prob'].isna()
whole = whole.loc[~is_no_status]

# reshape
whole['suggest_add'] = whole['rent_predict'] - whole['mean_available_rent']
whole['total_txn'] = whole['rent'] + whole['return']
# 哪個站、哪個時段、見車率多少、是不是故意調走的、附近有沒有替代工具、要加多少車
whole = whole[['stop_id', 'name', 'stop_type', 'capacity_y', 
               'weekday_type', 'hour',
               'available_rent_prob', 'suggest_add', 'add', 'remove', 
               'buffer200m_YB', 'buffer200m_MRT', 'buffer200m_BUS',
               'mean_available_rent', 'mean_available_return',
               'net_profit_predict', 'net_profit', 'net_profit_bias',
               'rent_demand_multi', 'rent', 'rent_predict', 'rent_bias',
               'return_demand_multi', 'return', 'return_predict', 'return_bias',
               'raw_data_count', 'total_txn', 'lng', 'lat']]
whole.columns = ['ID', '站名', '類別', '車柱數',
                 '周間/周末', '時間',
                 '見車率', '建議增加車數', '平均調度放入數', '平均調度移走數',
                 '200公尺內ubike站數', '200公尺內捷運站數', '200公尺內公車站數',
                 '平均可借車數', '平均可還位數',
                 '淨增減(預測)', '淨增減(實際)', '淨增減(誤差)', 
                 '該時段借車常客數', '平均實際借車數', '預測借車數', '借車誤差',
                 '該時段還車常客數', '平均實際還車數', '預測還車數', '還車誤差',
                 'API回傳資料筆數', '總交易數', 'lng', 'lat']

# save
file_path = '/DM/缺車/[summary]bike_empty_whole_view.xlsx'
whole.to_excel(root_path+file_path, index=False)

# agg by stop_id
whole_agg = whole.groupby(['ID']).agg({
    '見車率': 'mean',
    '總交易數': 'sum'
    }).reset_index()
# save
file_path = '/DM/缺車/[summary]empty_whole_view_agg_by_stopid.xlsx'
whole_agg.to_excel(root_path+file_path, index=False)
