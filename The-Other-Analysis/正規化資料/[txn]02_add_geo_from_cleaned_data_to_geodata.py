# =============================================================================
# 把原始資料添加地理資訊
# 其中捷運還要加上站名
# =============================================================================

import os
import sys
import time

import pandas as pd

sys.path.append('D:/iima/ubike分析/code')
from udf_function import load_ubike_stop, make_sure_folder_exist

ym = '202403'
ym_trim = ym[2:]
root_path = 'D:/iima/ubike分析'
txn_path = root_path+f'/DM/{ym}/prepared_data/txn'
input_folder = txn_path+'/cleaned_raw'
output_folder = txn_path+'/cleaned_raw_with_geo'
dim_path = root_path+'/DIM'
missing_data_folder = txn_path+'/missing_data'


# 讀取站點資訊
# mrt
mrt_stop = pd.read_excel(dim_path+'/mrt_id_name.xlsx')
mrt_stop['stop_id'] = 'M' + mrt_stop['stop_id'].astype(str)
# bus
# 公車站有分站牌、站位。站位是將靠近的數個站牌合併，比如台北車站幾百根全部收成一個台北車站站位
bus_stop = pd.read_csv(dim_path+'/ntpe_bus_stop_20230411.csv')
bus_stop = bus_stop.drop_duplicates()
bus_stop = bus_stop[['stop_id', 'lng', 'lat']]
# ubikea
ubike_stop = load_ubike_stop(ym, dim_path)


# 處理資料
standard_column = [
    'trans_type', 'route_name',
    'card_id', 'card_type', 'data_type', 
    'on_stop_id', 'on_stop', 'on_lng', 'on_lat', 'on_time',
    'off_stop_id', 'off_stop', 'off_lng', 'off_lat', 'off_time',
    'txn_amt', 'txn_disc'
]
yms = os.listdir(input_folder)
for ym in yms: # 年月
    # break
    input_ym_folder = f'{input_folder}/{ym}'
    files = os.listdir(input_ym_folder)
    for file in files:
        # raise 'test'
        t = time.time()
        # load
        input_file_path = f'{input_ym_folder}/{file}'
        print(f'\nLoading {input_file_path}......')
        data = pd.read_pickle(input_file_path)
        file = file.replace('taipei_', '')
        if file.startswith('mrt'):
            # continue
            gdata = data.merge(mrt_stop, how='left', left_on='on_stop_id', right_on='stop_id', suffixes=('', '_on'))
            gdata = gdata.merge(mrt_stop, how='left', left_on='off_stop_id', right_on='stop_id', suffixes=('', '_off'))
            route = gdata['line'] + '+' + gdata['line_off']
            route.loc[route.isna()] = ''
            gdata['route_name'] = route.map(lambda x: '+'.join(list(set(x.split('+')))))
            gdata = gdata.drop(columns=['on_stop', 'off_stop', 'line', 'line_off'])
            gdata = gdata.rename(columns={'stop_name': 'on_stop', 'stop_name_off': 'off_stop',
                                        'lng': 'on_lng', 'lat': 'on_lat',
                                        'lng_off': 'off_lng', 'lat_off': 'off_lat'})
            # 捷運有一個站分屬兩個ID的情況，如台北車站有M51、M52兩個ID，但西門只有一個ID
            # 目前看不出區分必要，反而會造成一個捷運站的數量被分成兩半，因此合併
            # 合併邏輯= 有兩個ID的站，一律改為數字較小的ID，如台北車站的的M52交易全改為M51
            stop_id_mappin = {
                'M52': 'M51', # 台北車站
                'M102': 'M11', # 大安
                'M200': 'M36', # 大坪林
                'M106': 'M53', # 中山
                'M129': 'M55', # 民權西路
                'M90': 'M10', # 忠孝復興
                'M133': 'M89', # 忠孝新生
                'M132': 'M107', # 松江南京
                'M209': 'M82', # 板橋
                'M108': 'M9', # 南京復興
                'M98': 'M31', # 南港展覽館
                'M204': 'M47', # 景安
                'M211': 'M123' # 頭前庄
            }
            is_target = gdata['on_stop_id'].isin(list(stop_id_mappin.keys()))
            gdata.loc[is_target, 'on_stop_id'] = gdata.loc[is_target, 'on_stop_id'].map(stop_id_mappin)
            is_target = gdata['off_stop_id'].isin(list(stop_id_mappin.keys()))
            gdata.loc[is_target, 'off_stop_id'] = gdata.loc[is_target, 'off_stop_id'].map(stop_id_mappin)
            
        elif file.startswith('bus'):
            # continue
            gdata = data.merge(bus_stop, how='left', left_on='on_stop_id', right_on='stop_id', suffixes=('', '_on'))
            gdata = gdata.merge(bus_stop, how='left', left_on='off_stop_id', right_on='stop_id', suffixes=('', '_off'))
            gdata = gdata.drop(columns=['stop_id', 'stop_id_off'])
            gdata = gdata.rename(columns={
                'stop_name': 'on_stop',
                'stop_name_off': 'off_stop',
                'lng': 'on_lng',
                'lat': 'on_lat',
                'lng_off': 'off_lng',
                'lat_off': 'off_lat'
            })
        elif file.startswith('ubike') or file.startswith(ym_trim) or file.endswith:
            # 資料問題手動修正
            data['on_stop'] = data['on_stop'].str.replace('@', '')
            data['off_stop'] = data['off_stop'].str.replace('@', '')
            data['on_stop'] = data['on_stop'].str.replace(' ', '')
            data['off_stop'] = data['off_stop'].str.replace(' ', '')
            
            count_of_on_stop_prev_brackets = data['on_stop'].str.count('\\(')
            count_of_on_stop_suff_brackets = data['on_stop'].str.count('\\)')
            is_on_unequal = (count_of_on_stop_prev_brackets - count_of_on_stop_suff_brackets) > 0
            data.loc[is_on_unequal, 'on_stop'] += ')'
            
            count_of_off_stop_prev_brackets = data['off_stop'].str.count('\\(')
            count_of_off_stop_suff_brackets = data['off_stop'].str.count('\\)')
            is_off_unequal = (count_of_off_stop_prev_brackets - count_of_off_stop_suff_brackets) > 0
            data.loc[is_off_unequal, 'off_stop'] += ')'
    
            gdata = data.merge(ubike_stop, how='left', left_on='on_stop_id', right_on='stop_id', suffixes=('', '_on'))
            gdata = gdata.merge(ubike_stop, how='left', left_on='off_stop_id', right_on='stop_id', suffixes=('', '_off'))
            gdata = gdata.rename(columns={'lng': 'on_lng', 'lat': 'on_lat', 'lng_off': 'off_lng', 'lat_off': 'off_lat'})
            gdata = gdata.drop(columns=['stop_id', 'stop_id_off'])
        else:
            raise ValueError(f'Can only process bus, mrt, ubike. {file} is unexpected.')
        print(f'  ETL Done, cost {time.time()-t} secs.')
        
        # save
        output_folder_path = f'{output_folder}/{ym}'
        output_file_path = f'{output_folder_path}/{file}'

        gdata = gdata[standard_column]
        make_sure_folder_exist(output_folder_path)
        gdata.to_pickle(output_file_path)

        # save missing data for future research
        is_geometry_missing = (gdata['on_lng'].isna()|gdata['off_lng'].isna())
        is_time_missing = (gdata['on_time'].isna()|gdata['off_time'].isna())
        is_missing = (is_geometry_missing|is_time_missing)
        print(f'  {is_geometry_missing.sum()} Geomissing, {is_time_missing.sum()} Timemissing at {ym}/{file}')
        misdata = gdata.loc[is_missing]
        missing_folder_path = f'{missing_data_folder}/{ym}'
        make_sure_folder_exist(missing_folder_path)
        misdata.to_pickle(f'{missing_folder_path}/{file}')
        print(f'  Saved data, cost {time.time()-t} secs.')
