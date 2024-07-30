# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 14:07:59 2022

@author: rz3881
"""

# 將各交通工具資料切割成日的資料
# 再合併成不分交通工具的日資料

import pandas as pd
import os
import time
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import delete_input_data, make_sure_folder_exist

# Config
root_path = r'D:\iima\ubike分析'
ym = '202403'
txn_path = f'{root_path}/DM/{ym}/prepared_data/txn'
input_path = f'{txn_path}/cleaned_daily'
output_path = f'{txn_path}/cleaned_daily_concated'
is_delete_input = False
is_clean_output = False

# clean output  before save data
if is_clean_output:
    dirs = os.listdir(input_path)
    for _dir in dirs:
        files = os.listdir(f'{input_path}/{_dir}')
        for file in files:
            is_concated_output = (file=='ubike_bus_mrt.pkl')
            if is_concated_output:
                output_file_path = f'{output_path}/{_dir}/{file}'
                delete_input_data(output_file_path)
                print(f'Delete output file {output_file_path}')

# concat cleaned daily
dirs = os.listdir(input_path)
for _dir in dirs:
    # break
    t = time.time()
    print(f'\nInto {_dir}......')
    # load
    files = os.listdir(f'{input_path}/{_dir}')

    datas = []
    for file in files:
        # break
        input_file_path = f'{input_path}/{_dir}/{file}'
        data = pd.read_pickle(input_file_path)
        data['data_date'] = _dir[0:4] + '-' + _dir[4:6] + '-' + _dir[6:8]
        datas.append(data)
        if is_delete_input:
            delete_input_data(input_file_path)
            print(f'  {len(files)} files in {_dir} have been removed.')
    concated_data = pd.concat(datas)
    # save
    output_folder_path = f'{output_path}/{_dir}'
    output_file_path = f'{output_path}/{_dir}/ubike_bus_mrt.pkl'
    make_sure_folder_exist(output_folder_path)
    concated_data.to_pickle(output_file_path)
    print(f'({output_file_path}) {data.shape[0]} rows have been saved. cost {round(time.time()-t)} seconds.')
