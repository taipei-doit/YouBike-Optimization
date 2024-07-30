# -*- coding: utf-8 -*-
"""
Created on Tue May  9 13:35:46 2023

@author: rz3881
"""

import pandas as pd
import time
from numpy import nan
pd.options.mode.chained_assignment = None

# def
def generate_target_txn(txn, target_date=None, target_stop_id=None):
    filter_on_col_name = ['card_id', 'route_name', 'on_stop_id', 'on_stop', 'on_time']
    filter_off_col_name = ['card_id', 'route_name', 'off_stop_id', 'off_stop', 'off_time']
    result_col_name = ['card_id', 'bike_id', 'stop_id', 'stop_name', 'txn_time']
    # build filter
    if target_date:
        is_target_on_date = (txn['on_time'].dt.date==target_date)
        is_target_off_date = (txn['off_time'].dt.date==target_date)
    if target_stop_id:
        is_target_on_stop = (txn['on_stop_id']==target_stop_id)
        is_target_off_stop = (txn['off_stop_id']==target_stop_id)
    # filter
    if target_date:
        if target_stop_id:
            on_txn = txn.loc[is_target_on_date & is_target_on_stop, filter_on_col_name]
            off_txn = txn.loc[is_target_off_date & is_target_off_stop, filter_off_col_name]
        else:
            on_txn = txn.loc[is_target_on_date, filter_on_col_name]
            off_txn = txn.loc[is_target_off_date, filter_off_col_name]
    else:
        on_txn = txn.loc[is_target_on_stop, filter_on_col_name]
        off_txn = txn.loc[is_target_off_stop, filter_off_col_name]
    # reshape
    on_txn.columns = result_col_name
    on_txn['type'] = 'on'
    off_txn.columns = result_col_name
    off_txn['type'] = 'off'
    target_txn = pd.concat([on_txn, off_txn])
    return target_txn

def generate_target_status(status, target_date=None, target_stop_id=None):
    result_col_name = ['stop_id', 'stop_name', 'service_status', 'capacity',
                       'available_rent_bikes', 'available_return_bikes',
                       'source_update_time', 'adjust_api_time']
    # build filter
    if target_date:
        is_date = (status['adjust_api_time'].dt.date==target_date)
    if target_stop_id:
        is_stop = (status['stop_id']==target_stop_id)
    # filter
    if target_date:
        if target_stop_id:
            target_status = status.loc[is_date & is_stop]
        else:
            target_status = status.loc[is_date]
    else:
        target_status = status.loc[is_stop]
    # reshape
    target_status = target_status[result_col_name]
    return target_status

def generate_target_dispatch(dispatch, target_date=None, target_stop_id=None):
    result_col_name = ['stop_id', 'stop_name',  'dispatch_type', 'txn_time']
    # build filter
    if target_date:
        is_date = (dispatch['adjust_api_time'].dt.date==target_date)
    if target_stop_id:
        is_stop = (dispatch['stop_id']==target_stop_id)
    # filter
    if target_date:
        if target_stop_id:
            target_dispatch = dispatch.loc[is_date & is_stop]
        else:
            target_dispatch = dispatch.loc[is_date]
    else:
        target_dispatch = dispatch.loc[is_stop]
    # reshape
    target_dispatch = target_dispatch[result_col_name]
    return target_dispatch

def combine_status_and_txn(target_txn, target_status, is_return_view=False):
    '''
    is_return_view = True, 會回傳另一種格式，適合用人眼了解API回傳與交易關係的格式
    '''
    ts = target_status[['adjust_api_time', 'available_rent_bikes']]
    ts.columns = ['time', 'type']
    ts['type'] = 'status' # 為了確保排序的時候，同樣時間下status會在交易後面
    tt = target_txn[['txn_time', 'type']]
    tt.columns = ['time', 'type']
    joined_txn = pd.concat([ts, tt])
    joined_txn = joined_txn.sort_values(['time', 'type'])
    if is_return_view:
        ts['status'] = 'api'
        ts = ts.drop(columns='type')
        status_join_txn = ts.merge(tt, how='outer', on='time')
        return status_join_txn
    else:
        return joined_txn

def combine_status_and_dispatch(target_dispatch, target_status, is_return_view=False):
    '''
    is_return_view = True, 會回傳另一種格式，適合用人眼了解API回傳與交易關係的格式
    '''
    ts = target_status[['adjust_api_time', 'available_rent_bikes']]
    ts.columns = ['time', 'type']
    ts['type'] = 'z_status' # 為了確保排序的時候，同樣時間下status會在調度後面
    td = target_dispatch[['txn_time', 'dispatch_type']]
    td.columns = ['time', 'type']
    joined_dispatch = pd.concat([ts, td])
    joined_dispatch = joined_dispatch.sort_values(['time', 'type'])
    if is_return_view:
        ts['status'] = 'api'
        ts = ts.drop(columns='type')
        status_join_dispatch = ts.merge(td, how='outer', on='time')
        return status_join_dispatch
    else:
        return joined_dispatch

def count_txn_between_api_return(joined_txn, init_hour):
    '計算 借還 & 累加借還'
    on_count = 0
    off_count = 0
    cum_txn_delta = 0
    temp_cum_txn_delta_list = []
    # 使用者、營運的一天，{init_hour}點視為一天開始，所以比如3/28 01:00，被放在3/27
    joined_txn['date'] = (joined_txn['time']-pd.Timedelta(hours=init_hour)).dt.date
    joined_txn['prev_date'] = joined_txn['date'].shift(1)
    
    res = {'time': [], 'txn_on': [], 'txn_off': [],
           'min_cum_txn': [], 'max_cum_txn': []}
    for _, row in joined_txn.iterrows():
        # break
        # if row['time'].date() == datetime.date(2023, 3, 6):
        #     break
        is_api_return = (row['type']=='status')
        is_txn = (row['type']=='on')|(row['type']=='off')
        is_not_the_same_date = (row['date']!=row['prev_date'])
        # 不同天須將累加歸0
        if is_not_the_same_date:
                cum_txn_delta = 0
                temp_cum_txn_delta_list = []
        # count txn type
        if is_txn:
            if row['type'] == 'on':
                on_count += 1
                cum_txn_delta += -1
                temp_cum_txn_delta_list.append(cum_txn_delta)
            else:
                off_count += 1
                cum_txn_delta += 1
                temp_cum_txn_delta_list.append(cum_txn_delta)
        # api return, summit txn count
        if is_api_return:
            res['time'].append(row['time'])
            res['txn_on'].append(on_count)
            res['txn_off'].append(off_count)
            is_no_txn = (len(temp_cum_txn_delta_list)==0)
            if is_no_txn:
                res['min_cum_txn'].append(cum_txn_delta)
                res['max_cum_txn'].append(cum_txn_delta)
            else:
                max_cum_txn = pd.Series(temp_cum_txn_delta_list).max()
                min_cum_txn = pd.Series(temp_cum_txn_delta_list).min()
                res['min_cum_txn'].append(min_cum_txn)
                res['max_cum_txn'].append(max_cum_txn)
            on_count = 0
            off_count = 0
            temp_cum_txn_delta_list = []
    txn_count = pd.DataFrame(res)
    return txn_count

def count_dispatch_between_api_return(joined_dispatch):
    ''
    in_count = 0
    out_count = 0
    res = {'time': [], 'in': [], 'out': []}
    for _, row in joined_dispatch.iterrows():
        # break
        is_api_return = (row['type']=='z_status')
        is_dispatch = (row['type']=='in')|(row['type']=='out')
        # count txn type
        if is_dispatch:
            if row['type'] == 'in':
                in_count += 1
            else:
                out_count += 1
        # api return, summit txn count
        if is_api_return:
            res['time'].append(row['time'])
            res['in'].append(in_count)
            res['out'].append(out_count)
            in_count = 0
            out_count = 0
    dispatch_count = pd.DataFrame(res)
    return dispatch_count


# config
ym = '202303'
root_path = r'D:\iima\ubike分析'
txn_path = root_path+f'/DM/{ym}/prepared_data/txn'
dispatch_path = root_path+f'/DM/{ym}/prepared_data/dispatch'
status_path = root_path+f'/DM/{ym}/prepared_data/status'
idle_path = root_path+f'/DM/{ym}/閒置車'
init_hour = 6  # 一天初始時間(6 = 06:00)
min_delay_secs = 4  # 交易發生後，站點最快也要4秒才會回傳紀錄

# load 
# txb
txn = pd.read_csv(txn_path+'/txn_only_ubike.csv')
txn['on_time'] = pd.to_datetime(txn['on_time']).dt.tz_localize(None)
txn['off_time'] = pd.to_datetime(txn['off_time']).dt.tz_localize(None)
# status
status = pd.read_csv(status_path+'/unique_raw.csv')
status['source_update_time'] = pd.to_datetime(status['source_update_time'])
# dispatch
dispatch = pd.read_csv(dispatch_path+'/cleaned_raw.csv')
dispatch['txn_time'] = pd.to_datetime(dispatch['txn_time']).dt.tz_localize(None)
# stop
stop = pd.read_csv(root_path+'/DIM/ubike_stops_from_api_202303.csv')

# 結合status、txn釐清閒置車輛
idle_status = status.copy()
idle_status = idle_status.sort_values('source_update_time')
# 站點回傳會delay，目前觀察delay時間4~n秒不固定，但原則上我們只要確認不會差太多
idle_status['adjust_api_time'] = idle_status['source_update_time'] - pd.Timedelta(seconds=min_delay_secs)
# 
a = 0
results = []
stop_ids = set(status['stop_id'])
t = time.time()
for stop_id in stop_ids:
    # break
    # select data    
    target_txn = generate_target_txn(txn, target_stop_id=stop_id)
    target_status = generate_target_status(idle_status, target_stop_id=stop_id)
    target_dispatch = generate_target_dispatch(dispatch, target_stop_id=stop_id)
    # combine txn
    joined_txn = combine_status_and_txn(target_txn, target_status)
    txn_count = count_txn_between_api_return(joined_txn, init_hour)
    # combine dispatch
    joined_dispatch = combine_status_and_dispatch(target_dispatch, target_status)
    dispatch_count = count_dispatch_between_api_return(joined_dispatch)
    # add columns
    idle_data = target_status.merge(txn_count, how='left',
                                    left_on='adjust_api_time',
                                    right_on='time')
    idle_data = idle_data.merge(dispatch_count, how='left',
                                left_on='adjust_api_time',
                                right_on='time')
    idle_data = idle_data.drop(columns=['time_x', 'time_y', 'source_update_time'])
    idle_data = idle_data.sort_values('adjust_api_time')
    # 涉及到shift都應該是每站獨立執行
    idle_data['prev_adjust_api_time'] = idle_data['adjust_api_time'].shift(1)
    idle_data['actual_delta'] = idle_data['available_rent_bikes'] - idle_data['available_rent_bikes'].shift(1)
    idle_data['available_rent_bikes_in_worst_case'] = idle_data['available_rent_bikes'].shift(1) - idle_data['txn_on']
    is_na = idle_data['available_rent_bikes_in_worst_case'].isna()
    idle_data.loc[is_na, 'available_rent_bikes_in_worst_case'] = idle_data.loc[is_na, 'available_rent_bikes']
    #
    results.append(idle_data)
    if (a%100) == 0:
        print(f'{a}/{len(stop_ids)} cost {time.time() -t} seconds.')
    a += 1
results = pd.concat(results)
# cost 80 minutes

# reshape
results['date'] = results['adjust_api_time'].dt.date.astype(str)
results['time'] = results['adjust_api_time'].dt.time.astype(str)
results['weekday'] = results['adjust_api_time'].dt.weekday + 1
# 為符合使用者習慣，將init_hour視為每日起點
results['adjust_api_time_m6h'] = results['adjust_api_time'] - pd.Timedelta(hours=init_hour)
results['date_m6h'] = results['adjust_api_time_m6h'].dt.date.astype(str)
results['weekday_m6h'] = results['adjust_api_time_m6h'].dt.weekday + 1
results['txn_delta'] = results['txn_off'] - results['txn_on']
results['dispatch_delta'] = results['in'] - results['out']
is_zero = (results['dispatch_delta']==0)  # 為了powerbi在0的地方不顯示
results.loc[is_zero, 'dispatch_delta'] = nan
results['other_delta'] = results['actual_delta'] - results['txn_delta'] - results['dispatch_delta']
results = results[['date', 'weekday', 'time',
                   'adjust_api_time_m6h', 'date_m6h', 'weekday_m6h',
                   'stop_id', 'stop_name', 'service_status', 'capacity',
                   'prev_adjust_api_time', 'adjust_api_time',
                   'available_rent_bikes', 'txn_on', 'txn_off',
                   'in', 'out', 'txn_delta', 'actual_delta', 'other_delta',
                   'dispatch_delta', 'min_cum_txn', 'max_cum_txn',
                   'available_rent_bikes_in_worst_case']]

# results = pd.read_csv(idle_path+'/status_return_time_merge_txn_and_dispatch.csv')
# add columns for powerbi present
results['negative_capacity'] = -results['capacity']
results['zero'] = 0
results['capacity1.5'] = results['capacity'] * 1.5
results['negative_capacity1.5'] = results['negative_capacity'] * 1.5
results['upper_bound']  = results[['capacity1.5', 'max_cum_txn']].max(axis=1)
results['lower_bound']  = results[['negative_capacity1.5', 'min_cum_txn']].min(axis=1)

# save
file_path = idle_path+'/status_return_time_merge_txn_and_dispatch.csv'
results.to_csv(file_path, index=False, encoding='UTF-8')
