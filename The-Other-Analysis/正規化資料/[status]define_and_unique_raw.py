import pandas as pd
import os
import sys
import time
sys.path.append(r'D:\iima\ubike分析\code')
from udf_function import load_ubike_stop, make_sure_folder_exist


def correct_encoding_error(data, stop):
    '''
    202307的資料有encoding error導致name, lng, lat有誤
    乾脆利用TDX的stop清單，全部重新取一次
    '''
    data = data.merge(stop, how='left', on='stop_id')
    is_na = data['lng'].isna()
    print(f'{sum(is_na)} rows not match stop_id.')
    data = data.loc[~is_na]
    return data


def check_data(data):
    for col in data.columns:
        print(f'{col} have {data[col].isna().sum()} rows NA')


# Config
root_path = r'D:\iima\ubike分析'
ym = '202403'
status_path = f'{root_path}/DM/{ym}/prepared_data/status'
dim_path = f'{root_path}/DIM'
min_delay_secs = 4
is_header = False  # 看到資料才知道，False代表沒表頭
col_map = {
    'sno': 'stop_id',
    'sna': 'stop_name',
    'tot': 'capacity',  # 總停車格
    'sbi': 'available_rent_bikes',  # 車輛數量
    'sarea': 'dist',
    'mday': 'source_update_time1',  # 微笑單車各場站來源資料更新時間
    'lat': 'lat',
    'lng': 'lng',
    'ar': 'addr',
    'sareaen': 'dist_eng',
    'snaen': 'stop_name_eng',
    'aren': 'addr_eng',
    'bemp': 'available_return_bikes',  # 空位數量
    'act': 'service_status',  # 啟用狀態(0:禁用、1:啟用)
    'srcupdatetime': 'data_time',  # 微笑單車系統發布資料更新的時間
    'updatetime': 'tpegov_update_time',  # 北市府交通局處理後存入DB時間
    'infotime': 'source_update_time',  # 各場站來源資料更新時間
    'infodate': 'source_update_date'  # 各場站來源資料更新日期
}

# load
stop = load_ubike_stop(ym, dim_path)
input_path = f'{root_path}/DW/raw_stop_api_response/{ym}/'
files = os.listdir(input_path)
ubike_status = []
for file in files:
    t = time.time()
    if file.startswith('欄位'):
        continue

    if is_header:
        temp_raw = pd.read_csv(input_path+file, encoding_errors='replace')
        temp_raw = temp_raw.rename(columns=col_map)
    else:
        temp_raw = pd.read_csv(
            input_path+file,
            names=col_map.values(),
            encoding_errors='replace'
        )

    # Cleansing
    temp_raw['service_type'] = (
        temp_raw['stop_name'].str.replace('YouBike', '').str.split('_').str[0]
    )
    # temp_raw['stop_name'] = temp_raw['stop_name'].str.split('_').str[1]
    temp_raw['stop_id'] = temp_raw['stop_id'].astype(str)
    is_start_with_500 = temp_raw['stop_id'].str.startswith('500')
    temp_raw.loc[is_start_with_500, 'stop_id'] = (
        temp_raw.loc[is_start_with_500, 'stop_id'].str.slice(3,)
    )
    temp_raw['stop_id'] = (
        'U' + temp_raw['stop_id'].str.replace('.0', '', regex=False)
    )
    # temp_raw['lng'] = pd.to_numeric(temp_raw['lng'], errors='coerce')
    # temp_raw['lat'] = pd.to_numeric(temp_raw['lat'], errors='coerce')
    temp_raw['data_time'] = (
        pd.to_datetime(temp_raw['data_time'], errors='coerce')
    )
    # 202307、202402的source_update_time有錯誤，目前全面使用data_time
    # 並取代source_update_time，以避免誤用
    # temp_raw['source_update_time'] = (
    #     pd.to_datetime(temp_raw['source_update_time'], errors='coerce')
    # )
    temp_raw['available_rent_bikes'] = (
        pd.to_numeric(temp_raw['available_rent_bikes'], errors='coerce')
    )
    temp_raw['available_return_bikes'] = (
        pd.to_numeric(temp_raw['available_return_bikes'], errors='coerce')
    )
    temp_raw = temp_raw.drop(columns=[
        'source_update_time1', 'addr', 'dist_eng', 'stop_name_eng',
        'addr_eng', 'tpegov_update_time', 'source_update_date',
        'stop_name', 'lng', 'lat'
    ])
    temp_raw = correct_encoding_error(temp_raw, stop)
    check_data(temp_raw)
    temp_raw = temp_raw.dropna()

    ubike_status.append(temp_raw)
    print(f'Cleaned {file}, cost {time.time()-t} seconds.')
ubike_status = pd.concat(ubike_status)


# to show 
# # show 某站點在同一個source_update_time有多少不同在站車數，
# temp = ubike_status.groupby('stop_id').first()
# # same source_update_time different availabe bike
# ubike_status_unique = ubike_status.groupby(['source_update_time', 'stop_id']).agg({
#     'available_rent_bikes': 'nunique',
#     'available_return_bikes': 'nunique'
#     }).reset_index()
# temp = ubike_status.loc[ubike_status['stop_id']=='U110065']
# temp1 = ubike_status_unique.loc[ubike_status_unique['stop_id']=='U110065']

# 站點回傳會delay，目前觀察交易系統與站點資料有delay
# delay時間4~n秒不固定，目前統一減4秒
ubike_status['data_time'] = ubike_status['data_time'] - pd.Timedelta(seconds=min_delay_secs)

# drop duplicate
# data_time是平台更新時間，但常見來源資料未變平台一直在吐
# 因此以source_update_time當key去重
#! 202307的source_update_time發生錯誤，202402也有錯誤，目前以data_time去重
ubike_status_unique = ubike_status.groupby(
    ['data_time', 'stop_id', 'stop_name']
).agg({
    'service_status': 'first',
    'capacity': 'first',
    'available_rent_bikes': 'first',
    'available_return_bikes': 'first',
    'dist': 'first',
    'lat': 'first',
    'lng': 'first',
    'service_type': 'first'
}).reset_index()
make_sure_folder_exist(status_path)
file_path = status_path+'/unique_raw.csv'
ubike_status_unique.to_csv(file_path, index=False, encoding='utf8')
