# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 08:39:51 2023

@author: rz3881
"""

import pandas as pd
import os
import time
from numpy import nan
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import convert_str_to_time_format
import pyodbc
import datetime

def read_accbe(input_file_path):
    conn = pyodbc.connect(f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={input_file_path};')
    df = pd.read_sql("SELECT * FROM 0_原文檔", conn)
    conn.close()
    return df

# init config
raw_data_path = r'D:\iima\ubike分析\DW\raw_transactions\202303'
output_path = r'D:\iima\ubike分析\DM\202303\周轉次數'
all_ym = ['202303']
# txn_disc = 折扣金額， txn_amt = 實際支付金額， txn_amt+txn_disc=該交易總金額
# data_type = 卡種，如悠遊卡、一卡通， card_type才是卡別，如學生卡

# ubike========================================================================
# define
ubike_column = ['txn_id', 'card_id', # 內碼
                 'txn_time', 'txn_amt',
                 'on_time', 'on_stop_id', 'on_stop', 'on_stand',
                 'bike_id',
                 'off_time', 'off_stop_id', 'off_stop', 'off_stand',
                 'duration', 'phone_id',
                 'card_type', 'fee_type', 'sp_id']
'''
column = 車柱, sp_id=業者代碼
'''

raw = []
card_type = {1:'一般', 2:'敬老1', 3:'敬老2', 4:'愛心', 5:'愛陪', 6:'學生', 9:'優待'}
data_type = {'ECC': '悠遊卡', 'IPASS': '一卡通'}
use_col = [s for s in range(23) if s != 17]
for ym in all_ym:
    for version in ['2.0']:
        file_path = f'{raw_data_path}\\ubike'
        files = os.listdir(file_path)
        for file in files:
            t = time.time()
            if file.endswith('xlsb'):
                continue
            # load
            input_file_path = f'{file_path}\\{file}'
            print(f'\nLoading {input_file_path}......')
            raw = read_accbe(input_file_path)
            ubike = raw.copy()
            # exception with not expected columns
            if 'PERSONAL_DISC' in ubike.columns:
                txn_disc = ubike['PERSONAL_DISC']
                ubike = ubike.drop(columns='PERSONAL_DISC')
            else:
                txn_disc = nan
            if 'XFER_CODE' in ubike.columns:
                ubike = ubike.drop(columns='XFER_CODE')
            # define column type
            ubike.columns = ubike_column
            ubike['on_time'] = convert_str_to_time_format(ubike['on_time'], on_error='coerce')
            ubike['off_time'] = convert_str_to_time_format(ubike['off_time'], on_error='coerce')
            ubike['on_stop_id'] = 'U' + ubike['on_stop_id'].astype(int).astype(str)
            ubike['off_stop_id'] = 'U' + ubike['off_stop_id'].astype(int).astype(str)
            ubike['card_type'] = ubike['card_type'].map(card_type)
            ubike['data_type'] = file.split('_')[1].upper()
            ubike['data_type'] = ubike['data_type'].map(data_type)
            # add in necessary column
            ubike['trans_type'] = 'ubike'
            ubike['route_name'] = version
            ubike['txn_disc'] = txn_disc
            # save
            raw.append(ubike)
txn = pd.concat(raw)
txn = txn.drop_duplicates()

# time info
txn['txn_time'] = pd.to_datetime(txn['txn_time'])
txn['date'] = txn['txn_time'].dt.date

# filter
is_normal_bike_id = (txn['bike_id']!='')
is_normal_date = (txn['date'] >= datetime.date(2023, 3, 1))
txn_filtered = txn.loc[is_normal_bike_id & is_normal_date]

dispatch_card = '1712048816'
is_dispatch = (txn['card_id']==dispatch_card)
is_dispatch.sum()

# 統計有幾輛車
len(set(txn['bike_id']))

# 車子的日平均交易次數分佈
txn_agg_daily = txn_filtered.groupby(['date']).agg({
    'bike_id': 'nunique',
    'txn_id': 'count' # txn_id是該卡的第幾次交易，不能用nunique
    }).reset_index()
txn_agg_daily.columns = ['date', 'bike_count', 'txn_count']
# save
file_path = output_path+r'\txn_agg_daily.csv'
txn_agg_daily.to_csv(file_path, index=False, encoding='UTF-8')

# 車子的交易次數分佈圖
# by bike by date
bike_daily_txn_count = txn_filtered.groupby(['bike_id', 'date']).agg({
    'txn_id': 'count'
    }).reset_index()
bike_daily_txn_count.columns = ['bike_id', 'date', 'txn_count']
# by date
daily_freq_dist = []
for date, temp in bike_daily_txn_count.groupby('date'):
    freq_dist = temp['txn_count'].value_counts().reset_index()
    freq_dist.columns = ['freq', 'bike_count']
    freq_dist = freq_dist.sort_values('freq')
    freq_dist['date'] = date
    daily_freq_dist.append(freq_dist)
daily_freq_dist = pd.concat(daily_freq_dist)
# by month mean
month_freq_dist = daily_freq_dist.groupby('freq').agg(
    {'bike_count': ['mean', 'count']}).reset_index()
month_freq_dist.columns = ['freq', 'mean_bike_count', 'date_count']
# save
with pd.ExcelWriter(output_path+'/daily_freq_dist.xlsx') as writer:
    temp = month_freq_dist.copy()
    temp.columns = ['周轉次數', '至少一次交易的ubike車數', '有此周轉次數的天數']
    temp.to_excel(writer, sheet_name='monthly', index=False)
    for date, temp in daily_freq_dist.groupby('date'):
        temp = temp[['freq', 'bike_count']]
        temp.columns = ['周轉次數', '至少一次交易的ubike車數']
        temp.to_excel(writer, sheet_name=str(date), index=False)