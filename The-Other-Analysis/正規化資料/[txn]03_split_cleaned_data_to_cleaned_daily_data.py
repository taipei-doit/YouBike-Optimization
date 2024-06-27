# 日期切割是為了未來大量數據的計算，不可能全放入RAM計算
# 以04:00為切割點，這時應該沒有什麼人用交通工具，也是我們作息習慣的分割點
# 以搭車的時間為準，就算他m月n+1日21:00才到到目的地，只要在m月n日03:59上車，歸到n日
# 12月31日 04:00 ~ 1月1日 03:59 期間上車都算是 12/31

# 將每天的所有交易整合成一個檔案
# 此處僅先把各交通工具切割成日資料，後續步驟再合併不同交通工具

import pandas as pd
import os
import time
import datetime
import shutil
import sys
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import make_sure_folder_exist, generate_next_month_firstday
import pytz
taipei_tz = pytz.timezone('Asia/Taipei')

root_path = r'D:\iima\ubike分析'
ym = '202403'
txn_path = f'{root_path}/DM/{ym}/prepared_data/txn'
input_folder = f'{txn_path}/cleaned_raw_with_geo'
output_folder = f'{txn_path}/cleaned_daily'
all_ym = [ym]

is_clean_output = False

def generate_date_seq(data:pd.DataFrame, y:int, m:int) -> list:
    '''
    根據原始資料所包含的所有交易日()，檢查並回傳符合預期的日期。
    比如傳入的y, m = (2022, 2)，則預期日期包含 20220201~20220301。
    符合預期的才回傳，不符合則不回傳且print。
    '''
    dates = list(set(data['on_time'].dt.date))
    earliest_date = datetime.date(y, m, 1)
    lastest_date = generate_next_month_firstday(earliest_date)
    vaild_dates = []
    for date in dates:
        is_vaild = (date>=earliest_date) & (date<=lastest_date)
        if is_vaild:
            vaild_dates.append(str(date))
        else:
            print(f'  {input_file_path} found invaild date {str(date)}')
    return vaild_dates


# clean folder before save data
if is_clean_output:
    shutil.rmtree(output_folder)
    os.mkdir(output_folder)
    print(f'Clean output folder {output_folder}')

yms = os.listdir(input_folder)
for ym in yms: # by年月
    if ym not in all_ym:
        continue
    # break
    y = int(ym[0:4])
    m = int(ym[4:6])
    input_ym_folder = f'{input_folder}/{ym}'
    files = os.listdir(input_ym_folder)
    for file in files: # by原始檔案
        # break
        t = time.time()
        print(f'\nLoading {file}......')
        # load
        input_file_path = f'{input_ym_folder}/{file}'
        data = pd.read_pickle(input_file_path)
        vaild_dates = generate_date_seq(data, y, m)
        if len(vaild_dates) == 0:
            print(f"Can't find any vaild date in data, check your input data {input_file_path}.")
        for date in vaild_dates: # by日
            # break
            tt = time.time()
            y, m, d = date.split('-')
            y, m, d = int(y), int(m), int(d)
            earliest_time = datetime.datetime(y, m, d, hour=4, tzinfo=taipei_tz)
            lastest_time = earliest_time + datetime.timedelta(days=1)
            is_target = (data['on_time'] >= earliest_time) & (data['on_time'] < lastest_time)
            target_data = data.loc[is_target]
            # if no such directory create directory
            folder_name = str(y) + str(m).zfill(2) + str(d).zfill(2)
            # save raw file to daily file, like 20220201/bus_od1
            output_folder_path = f'{output_folder}/{folder_name}'
            output_file_path = f'{output_folder_path}/{ym}_{file}'
            make_sure_folder_exist(output_folder_path)
            target_data.to_pickle(output_file_path)
            # print(f'  ({output_file_path}) saved. {target_data.shape[0]} rows, cost {round(time.time()-tt)} seconds.')
        print(f'({input_file_path}) done. {data.shape[0]} rows have been saved. cost {round(time.time()-t)} seconds.')

