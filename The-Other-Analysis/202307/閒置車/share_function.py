# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 16:19:34 2023

@author: rz3881
"""

# import sys
# sys.path.append(r'D:\iima\ubike分析\CODE\202304\閒置車')
# from share_function import (
#     )

import pandas as pd
import datetime


def filter_solid_results(data, only_weekday, confidence_ratio_threshould,
                         is_confidence_ratio = True):
    filtered_data = data.copy()
    if only_weekday:
        is_weekday = (filtered_data['週間週末']=='weekday')
        filtered_data = filtered_data.loc[is_weekday]        
    if is_confidence_ratio:
        is_cl_big_enough = (data['資料可信度'] >= confidence_ratio_threshould)
        filtered_data = data.loc[is_cl_big_enough]
    return filtered_data

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
        if target_stop_id:
            on_txn = txn.loc[is_target_on_stop, filter_on_col_name]
            off_txn = txn.loc[is_target_off_stop, filter_off_col_name]
        else:
            on_txn = txn[filter_on_col_name]
            off_txn = txn[filter_off_col_name]
    # reshape
    on_txn.columns = result_col_name
    on_txn['type'] = 'on'
    off_txn.columns = result_col_name
    off_txn['type'] = 'off'
    target_txn = pd.concat([on_txn, off_txn])
    return target_txn

def dispatch_accumulator(init_num, capacity, numbers):
    '累加數列後，若超出上下界線，紀錄超出值並抹去'
    zero = 0
    delta = 0
    result = {'avaliabe_bike': [], 'margin_dispatch_num': []}
    for num in numbers:
        # 累加值
        init_num += num
        result['avaliabe_bike'].append(init_num)
        # 調度值
        if init_num > capacity:
            delta = init_num - capacity
            init_num = capacity
        elif init_num < zero:
            delta = init_num
            init_num = zero
        else:
            delta = 0
        result['margin_dispatch_num'].append(delta)
    return pd.DataFrame(result)


def get_weekday_type_list(file_path, exclude_date):
    '取得該月份下，weekday/weekend 的日期清單'
    dispatch_date = pd.read_csv(file_path+'/compare_detail.csv')
    weekday_map = {1: 'weekday', 2: 'weekday', 3: 'weekday', 4: 'weekday', 5: 'weekday',
                   6: 'weekend', 7: 'weekend'}
    dispatch_date['weekday_type'] = dispatch_date['weekday_m6h'].map(weekday_map)
    weekday_type_list = {'weekday': [], 'weekend': []}
    for weekday_type in ['weekday', 'weekend']:
        target_date = dispatch_date.loc[dispatch_date['weekday_type']==weekday_type, 'date_m6h']
        target_date = set(target_date) - set(exclude_date)
        weekday_type_list[weekday_type] = list(target_date)
    return weekday_type_list


def find_init_hour_bike(gdata):
    '''
    找到最接近{init_hour}:00 +8的在站車數，因date是-{init_hour}tz
    每天初始車數 = {init_hour}點後第一筆資料的在站車數
    '''
    init_row = gdata.iloc[0]
    closest_init_hour_time = init_row['adjust_api_time']
    init_hour_available_bike = init_row['available_rent_bikes']
    return closest_init_hour_time, init_hour_available_bike


def find_afternoon_hour_bike(gdata, date_m6h):
    '''
    找到最接近{afternoon_hour}:00 +8的在站車數
      = {afternoon_hour}點後第一筆資料的在站車數
    '''
    afternoon_time = datetime.datetime(
        date_m6h.year, date_m6h.month, date_m6h.day, afternoon_hour, 0, 0)
    afternoon_gdata = gdata.loc[gdata['adjust_api_time']>=afternoon_time]
    is_empty = afternoon_gdata.shape[0]==0
    if is_empty:
        last_available_bike = gdata['available_rent_bikes'].iloc[-1]
        return afternoon_time, last_available_bike
    else:
        afternoon_time_row = afternoon_gdata.iloc[0]
        closest_init_hour_time = afternoon_time_row['adjust_api_time']
        init_hour_available_bike = afternoon_time_row['available_rent_bikes']
        return closest_init_hour_time, init_hour_available_bike
