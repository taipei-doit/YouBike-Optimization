# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 23:00:38 2023

@author: rz3881
"""

import pandas as pd
import os

root_path = r'D:\iima\ubike分析'
ym = '202403'
txn_path = f'{root_path}/DM/{ym}/prepared_data/txn'
input_path = f'{txn_path}/identified_transfer'
output_path = txn_path

# # 單日sample data
# data = pd.read_pickle(input_path+'/20221109.pkl')
# data.to_csv(output_path+'/20221109.csv', index=False)

# 整月只要ubike
monthly_data = []
files = os.listdir(input_path)
for file in files:
    # break
    data = pd.read_pickle(input_path+'/'+file)
    data = data.loc[data['trans_type']=='ubike']
    monthly_data.append(data)
month_data = pd.concat(monthly_data)
month_data = month_data.drop_duplicates(
    ['card_id', 'on_stop_id', 'on_time', 'off_stop_id', 'off_time','data_date']
)
month_data['card_type'] = ''

# 添加時間資訊
month_data['weekday'] = month_data['on_time'].dt.weekday + 1
weekday_type_map = {
    1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
    6: 'weekend', 7: 'weekend'
}
month_data['weekday_type'] = month_data['weekday'].map(weekday_type_map)
month_data['on_hour'] = month_data['on_time'].dt.hour
month_data['off_hour'] = month_data['off_time'].dt.hour

# save
month_data.to_csv(output_path+'/txn_only_ubike.csv', index=False, encoding='UTF-8')
