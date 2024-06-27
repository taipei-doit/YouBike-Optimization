# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 11:40:38 2023

@author: rz3881
"""

import pandas as pd

root_path = r'D:\iima\ubike分析'
trans_path = root_path+r'\DM\202303\prepared_data\站點附近大眾交通工具'

# load
transportation = pd.read_csv(trans_path+'/YB_Buffer200m.csv', encoding='big5')

# 排除新北
is_tpe = transportation['station_uid'].str.startswith('TPE')
transportation = transportation.loc[is_tpe]
transportation['stop_id'] = transportation['station_uid'].str.replace('TPE', '')
# 修正資料
is_start_with_500 = transportation['stop_id'].str.startswith('500')
transportation.loc[is_start_with_500, 'stop_id'] = transportation.loc[is_start_with_500, 'stop_id'].str.slice(3, )
transportation['stop_id'] = 'U' + transportation['stop_id']
transportation['buffer200m_YB'] = transportation['buffer200m_YB'] - 1 # 把自己算進去了

# reshape
col_map = {'name': 'stop_name', 'buffer200m_YB': 'ubike_within_200m',
           'buffer200m_MRT': 'mrt_within_200m',
           'buffer200m_BUS': 'bus_within_200m'}
transportation = transportation.rename(columns=col_map)
transportation = transportation[['stop_id', 'stop_name',
                                 'lng', 'lat',
                                 'ubike_within_200m','mrt_within_200m',
                                 'bus_within_200m']]


# save
file_path = trans_path+'/public_transport_within_200m.csv'
transportation.to_csv(file_path, index=False, encoding='utf8')
