# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 16:17:13 2022

@author: rz3881
"""

import pandas as pd
import os
import time
from numpy import nan
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import convert_str_to_time_format, make_sure_folder_exist
import pyodbc

def read_accdb(input_file_path):
    conn = pyodbc.connect(f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={input_file_path};')
    cursor = conn.cursor()
    tables = cursor.tables(tableType='TABLE')
    target_table = []
    for table in tables:
        name = table.table_name
        is_target = name.endswith('_請款資料') or name.endswith('_請款資料_基北北桃定期票') or name.endswith('_北北基桃定期票')
        if is_target:
            target_table.append(name)
    if len(target_table) == 1:
        df = pd.read_sql(f"SELECT * FROM {target_table[0]}", conn)
    else:
        raise ValueError('accdb table name error')
    cursor.close()
    conn.close()
    return df

# Config
ym = '202403'
raw_data_path = f'D:/iima/ubike分析/DW/raw_transactions/{ym}'
normalized_data_path = f'D:/iima/ubike分析/DM/{ym}/prepared_data/txn/cleaned_raw'
is_delete_input = False

standard_column = [
    'trans_type', 'route_name',
    'card_id', 'card_type', 'data_type', 
    'on_stop_id', 'on_stop', 'on_time',
    'off_stop_id', 'off_stop', 'off_time',
    'txn_amt', 'txn_disc'
]
# txn_disc = 折扣金額， txn_amt = 實際支付金額， txn_amt+txn_disc=該交易總金額
# data_type = 卡種，如悠遊卡、一卡通， card_type才是卡別，如學生卡

# ubike========================================================================
# define
ubike_column = {
    '交易序號': 'txn_id', '卡號': 'card_id', '扣款時間': 'txn_time',
    '扣款金額': 'txn_amt', '借車時間': 'on_time', '借車站代號': 'on_stop_id',
    '借車場站': 'on_stop', '借車車柱': 'on_stand', '自行車編號': 'bike_id',
    '還車時間': 'off_time', '還車站代號': 'off_stop_id', '還車場站': 'off_stop',
    '還車車柱': 'off_stand', '租用': 'duration', '手機序號': 'phone_id',
    '卡種': 'card_type', '費率別': 'fee_type', 'SPID': 'sp_id'
}

'''
column = 車柱, sp_id=業者代碼
'''
card_type = {1:'一般', 2:'敬老1', 3:'敬老2', 4:'愛心', 5:'愛陪', 6:'學生', 9:'優待'}
data_type = {'ECC': '悠遊卡', 'IPASS': '一卡通', 'TapPay': 'TapPay'}
use_col = [s for s in range(23) if s != 17]

for version in ['2.0']:
    file_path = f'{raw_data_path}\\ubike'
    files = os.listdir(file_path)
    for file in files:
        # raise 'test'
        t = time.time()
        is_not_accdb_file = not file.endswith('.accdb')
        if is_not_accdb_file:
            continue
        
        # load
        input_file_path = f'{file_path}\\{file}'
        print(f'\nLoading {input_file_path}......')
        raw = read_accdb(input_file_path)
        ubike = raw.copy()
        
        # exception with not expected columns
        if 'PERSONAL_DISC' in ubike.columns:
            txn_disc = ubike['PERSONAL_DISC']
            ubike = ubike.drop(columns='PERSONAL_DISC')
        else:
            txn_disc = nan
            
        if 'XFER_CODE' in ubike.columns:
            ubike = ubike.drop(columns='XFER_CODE')
        
        ubike = ubike.rename(columns=ubike_column)
        # temp = ubike.iloc[0:5]
        # define column type
        ubike['on_time'] = convert_str_to_time_format(ubike['on_time'], on_error='coerce')
        ubike['off_time'] = convert_str_to_time_format(ubike['off_time'], on_error='coerce')
        
        ubike['on_stop_id'] = ubike['on_stop_id'].astype(int).astype(str)
        is_start_with_500 = ubike['on_stop_id'].str.startswith('500')
        ubike.loc[is_start_with_500, 'on_stop_id'] = ubike.loc[is_start_with_500, 'on_stop_id'].str.slice(3, )
        ubike['on_stop_id'] = 'U' + ubike['on_stop_id']
        ubike['off_stop_id'] = ubike['off_stop_id'].astype(int).astype(str)
        is_start_with_500 = ubike['off_stop_id'].str.startswith('500')
        ubike.loc[is_start_with_500, 'off_stop_id'] = ubike.loc[is_start_with_500, 'off_stop_id'].str.slice(3, )
        ubike['off_stop_id'] = 'U' + ubike['off_stop_id']

        ubike['card_type'] = ubike['card_type'].map(card_type)
        ubike['data_type'] = file.split('_')[1].upper()
        ubike['data_type'] = ubike['data_type'].map(data_type)
        # addin necessary column
        ubike['trans_type'] = 'ubike'
        ubike['route_name'] = ubike['bike_id']
        ubike['txn_disc'] = txn_disc
        # select column
        ubike = ubike[standard_column]
        ubike = ubike.drop_duplicates()
        
        # save
        output_folder_path = f'{normalized_data_path}\\{ym}'
        output_file_path = f"{output_folder_path}\\{file.replace('.accdb', '.pkl')}"
        make_sure_folder_exist(output_folder_path)
        ubike.to_pickle(output_file_path)
        print(f'  {ubike.shape[0]} rows have been saved. cost {round(time.time()-t)} seconds.')

# problem
# 不知道fee_type 類別對應的意義
# card_type = 0是什麼?

