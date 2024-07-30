# -*- coding: utf-8 -*-
"""
Created on Tue May 30 16:16:03 2023

@author: rz3881
"""

import pandas as pd
import time
import pickle
import datetime
pd.options.mode.chained_assignment = None


def check_tomorrow(date_sequnce, current_dict, i, today):
    '''
    包裝error control
    本月最後一天、資料missing都會給定tomorrow=today
    '''
    is_end_date = (i==(len(date_sequnce)-1))
    if is_end_date:  # 不知道下個月第一天最適init bike，假設為今日的
        tomorrow = today
    else:
        tomorrow = date_sequnce[i+1]
        missing_tomorrow = current_dict.get(tomorrow) is None
        if missing_tomorrow:  # 明天沒有資料，假設為今日的
            tomorrow = today
    return tomorrow


def get_dispatch_capacity(dispatch_capacity, capacity, dispatch_reserve_number):
    '計算可調度量能'
    if dispatch_capacity:
        return dispatch_capacity
    else:
        return (capacity - dispatch_reserve_number)


def optimize_dispatch(today_dispatch, dispatch_capacity, max_time_interval_hour=4, is_print=False):
    '''
    將零散的調度合併，能將未來要補的提早一起補。
    合併的邏輯為線性迴圈，發生調度時先紀錄時間，往後合併需調度數量。
    包含以下例外:
    1.合併期間<= {max_time_interval_hour} 小時，太提前部屬就是浪費
    2.若調度換號，代表有潮汐反轉，不合併需求
    3.上下午視為不同調度階段，不合併需求
    4.每次最大調度數為dispatch_capacity，超過即拆分多次
    '''
    dispatch_queue = today_dispatch
    log = []
    log_col = [
        'api_time', 'dis_suggest',
        'is_change_sign', 'is_overflow', 'is_positive_overflow',
        'dispatch_number', 'dispatch_cum_after_dis_suggest'
    ]
    dispatch_suggestion = {'api_time': [], 'dis_suggest': []}
    at_least_one_dispatch = (len(dispatch_queue)>0)
    
    if at_least_one_dispatch:
        # 安全柱數=可操作數，以第一個須補的時間點開始，往後累加直到超過調度量能
        # 超過處即視為第二個操作時間點，以此往復
        dispatch_cum = 0
        prev_sign = None
        is_prev_morning = None
        # 第一筆需調度時間，往後累加需求
        dispatch_suggestion['api_time'].append(dispatch_queue['api_time'].iloc[0])
        
        for _, row in dispatch_queue.iterrows():
            api_time = row['api_time']
            dis_suggest = row['dispatch_num']
            # available_bike = row['available_bike']
            dis_num = 0
            temp_log = [api_time, dis_suggest]
            
            # 若換號必須打斷累加，避免忽略短期的觸底或觸頂
            now_sign = (dis_suggest > 0)
            is_init_sign = (prev_sign is None)
            
            if is_init_sign: # 初始特別狀態
                prev_sign = now_sign
            
            # 一些預計算
            is_change_sign = (now_sign != prev_sign)
            last_dispatch_time = dispatch_suggestion['api_time'][-1]
            time_diff_in_hour = (api_time - last_dispatch_time).seconds/60/60
            is_too_far = (time_diff_in_hour >= max_time_interval_hour)
            
            if is_change_sign:  # 若調度變號，代表中間有潮汐反轉，不該繼續累加
                dis_num = dispatch_cum
                dispatch_cum -= dis_num
                dispatch_suggestion['dis_suggest'].append(dis_num)
                dispatch_suggestion['api_time'].append(api_time)
            elif is_prev_morning:  # 上下午視為不同調度階段，過下午不可繼續累加
                is_now_afternoon = (api_time.hour >= afternoon_hour)
                is_morning_to_afternoon = is_prev_morning & is_now_afternoon
                if is_morning_to_afternoon:
                    dis_num = dispatch_cum
                    dispatch_cum -= dis_num
                    dispatch_suggestion['dis_suggest'].append(dis_num)
                    dispatch_suggestion['api_time'].append(api_time)
            elif is_too_far:  # 調度需求不應合併過長的時間，避免閒置
                dis_num = dispatch_cum
                dispatch_cum -= dis_num
                dispatch_suggestion['dis_suggest'].append(dis_num)
                dispatch_suggestion['api_time'].append(api_time)
            else:
                pass
            prev_sign = now_sign
            is_prev_morning = api_time.hour < afternoon_hour
            
            # 非提前終止，累加本次調度量
            dispatch_cum += dis_suggest
            # 若超出調度量能，產生新調度，記錄前次調度總需求調度數
            is_positive_overflow = (dispatch_cum > dispatch_capacity)
            is_negative_overflow = (dispatch_cum < -dispatch_capacity)
            is_overflow = (is_positive_overflow | is_negative_overflow)
            if is_overflow:  # 需補車已超過可操作柱，分散多次
                if is_positive_overflow:
                    dis_num = dispatch_capacity
                else:
                    dis_num = -dispatch_capacity
                dispatch_cum -= dis_num
                dispatch_suggestion['dis_suggest'].append(dis_num)
                dispatch_suggestion['api_time'].append(api_time)
                
            temp_log.extend(
                [
                    is_change_sign, is_overflow,
                    is_positive_overflow,dis_num, dispatch_cum
                ]
            )
            log.append(temp_log)
        # 最後一筆調度需求量
        dis_num = dispatch_cum
        dispatch_suggestion['dis_suggest'].append(dis_num)
    dispatch_suggestion = pd.DataFrame(dispatch_suggestion)
    log = pd.DataFrame(log, columns=log_col)
    if is_print:
        print(f'  optimize_dispatch cost {time.time()-start_time} secs.')
    return dispatch_suggestion

def add_dispatch_for_afrenoon(
    dispatch_suggestion, current_dict,
    morning_final_bikes, today_best_afternoon,is_print=False
    ):
    '上下午在站車是分開計算的，因此中間需補上'
    temp_dis_suggest = dispatch_suggestion
    dis_for_afternoon = today_best_afternoon - morning_final_bikes
    y, m, d = today.split('-')
    api_time = datetime.datetime(int(y), int(m), int(d), afternoon_hour-1, 59, 59)
    dispatch_for_afternoon_bset_init = pd.DataFrame(
        [[api_time, dis_for_afternoon]],
        columns=('api_time', 'dis_suggest'))
    temp_dis_suggest = pd.concat([temp_dis_suggest, dispatch_for_afternoon_bset_init])
    if is_print:
        print(f'add_dispatch for afrenoon: {dis_for_afternoon}, at {api_time}')
    return temp_dis_suggest


def add_dispatch_for_tomorrow(dispatch_suggestion, current_dict, today, tomorrow, is_print=False):
    'add dispatch at the end of day to match best init bike tomorrow'
    # get config
    temp_dis_suggest = dispatch_suggestion
    today_final_bikes = current_dict[today]['afternoon_final_bikes']
    tomorrow_best_init_bikes = current_dict[tomorrow]['best_init_bikes']
    dis_suggest = (tomorrow_best_init_bikes - today_final_bikes)
    y, m, d = tomorrow.split('-')
    api_time = datetime.datetime(int(y), int(m), int(d), init_hour-1, 59, 59)
    if tomorrow == today: # 月底最後一天的特殊處理
        api_time = api_time + datetime.timedelta(days=1)
    
    # do
    last_dispatch_col = ['api_time', 'dis_suggest']
    last_dispatch = [[api_time, dis_suggest]]
    last_dispatch = pd.DataFrame(last_dispatch, columns=last_dispatch_col)
    # is_at_least_one_dispatch = (temp_dis_suggest.shape[0] > 0)
    # if is_at_least_one_dispatch:
    #     is_exist = (temp_dis_suggest['api_time'].iloc[-1]==api_time)
    #     if is_exist:
    #         temp_dis_suggest = temp_dis_suggest.drop(temp_dis_suggest.index[-1])
    #         if is_print:
    #             print(f'dispatch exist, replace origin. add_dispatch bikes: {dis_suggest}, at {api_time}')
    temp_dis_suggest = pd.concat([temp_dis_suggest, last_dispatch])
    # temp_dis_suggest['api_time_m6h'] = temp_dis_suggest['api_time'] - pd.Timedelta(hours=6)
    if is_print:
        print(f'add_dispatch for tomorrow: {dis_suggest}, at {api_time}')
    return temp_dis_suggest


def merge_dispatch_and_data(today_best_init, today_data, dispatch_suggestion):
    '將新的調度方案拼回原本的資料'
    temp_data = today_data
    temp_data['best_init'] = today_best_init
    dispatch_suggestion = dispatch_suggestion.rename(columns={'api_time': 'adjust_api_time'})
    temp_data = temp_data.merge(dispatch_suggestion, how='outer', on='adjust_api_time')
    is_last_row_new_added = pd.isna(temp_data['date'].iloc[-1])
    if is_last_row_new_added:
        for j in range(temp_data.shape[1]):
            if pd.isna(temp_data.iloc[-1, j]):
                temp_data.iloc[-1, j] = temp_data.iloc[-2, j]
        last_row = temp_data.iloc[-1]
        temp_data['time'].iloc[-1] = str(last_row['adjust_api_time'].time())
        temp_data['prev_adjust_api_time'].iloc[-1] = temp_data['adjust_api_time'].iloc[-2]
        temp_data['txn_on'].iloc[-1] = 0
        temp_data['txn_off'].iloc[-1] = 0
        temp_data['in'].iloc[-1] = 0
        temp_data['out'].iloc[-1] = 0
        temp_data['txn_delta'].iloc[-1] = 0
        temp_data['actual_delta'].iloc[-1] = 0
        temp_data['other_delta'].iloc[-1] = 0
        temp_data['dispatch_delta'].iloc[-1] = 0
        temp_data['min_cum_txn'].iloc[-1] = temp_data['min_cum_txn'].iloc[-2]
        temp_data['max_cum_txn'].iloc[-1] = temp_data['min_cum_txn'].iloc[-2]
        temp_data['adjust_api_time_m6h'].iloc[-1] = last_row['adjust_api_time'] - datetime.timedelta(hours=init_hour)
        temp_data['api_m6h_hour'].iloc[-1] = temp_data['adjust_api_time_m6h'].iloc[-1].hour
        temp_data['date_m6h'].iloc[-1] = str(temp_data['adjust_api_time_m6h'].iloc[-1].date())
    return temp_data


def resimulate_with_bset_para(today_best_init, today_data, is_print=False):
    '根據新的init_bike與調度，重新模擬交易情況，算出最適模擬結果'
    temp_data = today_data
    temp_data['best_init'] = today_best_init
    # 重模擬
    simulate_bikes = []
    for txn_delta, dis_suggest in zip(temp_data['txn_delta'], temp_data['dis_suggest']):
        # break
        is_init = (len(simulate_bikes)==0)
        if is_init:
            new_available_bikes = today_best_init + txn_delta
        else:
            new_available_bikes += txn_delta
            
        if pd.notna(dis_suggest):
            new_available_bikes += dis_suggest
        simulate_bikes.append(new_available_bikes)
    temp_data['simulat_best_result'] = simulate_bikes
    if is_print:
        print(f'  resimulate_with_bset_para cost {time.time()-start_time} secs.')
    return temp_data


def deal_init_merge(today_best_init, today_data):
    is_first_row_dis_for_afternoon = pd.isna(today_data['date'].iloc[0])
    if is_first_row_dis_for_afternoon:
        # 若第一筆就是調度，也就是早上沒交易
        # 則下午起始值可當作上午起始值，此調度直接化為早上初始車數
        today_best_init = today_best_init + today_data['dis_suggest'].iloc[0]
        if today_best_init < 0:
            raise 'today_best_init < 0'
        today_data = today_data.iloc[1:] 
    return today_best_init, today_data


def fill_dispatch_na(today_data):
    '''
    為了下午、明天的調度是新增的調度，拼回大表其他欄位無值
    須補na才能正常
    '''
    y, m, d = today.split('-')
    # 為下午調度
    afternoon_end_time = datetime.datetime(int(y), int(m), int(d), afternoon_hour-1, 59, 59)
    is_dis_for_afternoon = (today_data['adjust_api_time']==afternoon_end_time)
    if is_dis_for_afternoon.sum() > 0:
        today_data.loc[is_dis_for_afternoon, 'time'] = afternoon_end_time.strftime("%H:%M:%S")
        today_data.loc[is_dis_for_afternoon, 'adjust_api_time_m6h'] = (
            today_data.loc[is_dis_for_afternoon, 'adjust_api_time'] 
            - datetime.timedelta(hours=init_hour))
        today_data.loc[is_dis_for_afternoon, 'txn_on'] = 0
        today_data.loc[is_dis_for_afternoon, 'txn_off'] = 0
        today_data.loc[is_dis_for_afternoon, 'txn_delta'] = 0
        if today_data.loc[is_dis_for_afternoon, 'dis_suggest'].iloc[0] >= 0:
            today_data.loc[is_dis_for_afternoon, 'in'] = today_data.loc[
                is_dis_for_afternoon, 'dis_suggest']
            today_data.loc[is_dis_for_afternoon, 'out'] = 0
        else:
            today_data.loc[is_dis_for_afternoon, 'in'] = 0
            today_data.loc[is_dis_for_afternoon, 'out'] = today_data.loc[
                is_dis_for_afternoon, 'dis_suggest']
        today_data.loc[is_dis_for_afternoon, 'actual_delta'] = 0
        today_data.loc[is_dis_for_afternoon, 'api_m6h_hour'] = afternoon_hour - init_hour
    # 下午、晚上調度一起調整
    cols = ['date', 'weekday', 'date_m6h', 'weekday_m6h', 'stop_id',
            'stop_name', 'service_status', 'capacity', 'available_rent_bikes',
            'min_cum_txn', 'max_cum_txn', 'available_rent_bikes_in_worst_case',
            'negative_capacity', 'zero', 'capacity1.5', 'negative_capacity1.5',
            'upper_bound', 'lower_bound', 'best_init']
    for col in cols:
        today_data[col] = today_data[col].fillna(method="ffill")
    today_data['prev_adjust_api_time'] = today_data['adjust_api_time'].shift(1)
    return today_data


print('Start [03]simulate_to_find_best_dispatch.py.')
t = time.time()

# Load
data = pd.read_csv(idle_path+'/status_return_time_merge_txn_and_dispatch.csv')
data['adjust_api_time'] = pd.to_datetime(data['adjust_api_time'])
data['prev_adjust_api_time'] = pd.to_datetime(data['prev_adjust_api_time'])
# 為符合使用者習慣，將init_hour視為每日起點
data['adjust_api_time_m6h'] = data['adjust_api_time'] - pd.Timedelta(hours=init_hour)
data['api_m6h_hour'] = data['adjust_api_time_m6h'].dt.hour 
data['date_m6h'] = data['adjust_api_time_m6h'].dt.date.astype(str)
daily_data = {}
for stop_id, temp1 in data.groupby('stop_id'):
    daily_data[stop_id] = {}
    for date, temp2 in temp1.groupby('date_m6h'):
        daily_data[stop_id][str(date)] = temp2
# init para
file_path = idle_path+'/best_init_and_raw_dispatch_queue_by_stop_by_date.pkl'
with open(file_path, 'rb') as f:
    results = pickle.load(f)

# optimize dispatch and resimulate
date_sequnce = list(set(data['date']))
date_sequnce.sort()
total_loop = len(set(data['stop_id'])) * len(date_sequnce)
loop_counter = 1
start_time = time.time()
for stop_id in set(data['stop_id']):
    current_dict = results[stop_id]
    capacity = next(iter(daily_data[stop_id].values()))['capacity'].iloc[0]
    # print(stop_id)
    for i in range(len(date_sequnce)):  # date loop
        # raise ValueError('test')
        today = date_sequnce[i]
        is_no_today_data = (current_dict.get(today) is None)
        if is_no_today_data:
            loop_counter += 1
            continue
        
        is_already_run = results[stop_id][today].get('simulat_best_result') is not None
        if is_already_run:
            loop_counter += 1
            continue
        
        # init
        today_data = daily_data[stop_id][today]
        today_best_init = current_dict[today]['best_init_bikes']
        morning_final_bikes = current_dict[today]['morning_final_bikes']
        today_best_afternoon = current_dict[today]['best_afternoon_bikes']
        today_dispatch = current_dict[today]['dispatch_queue']
        tomorrow = check_tomorrow(date_sequnce, current_dict, i, today)
        dispatch_capacity = get_dispatch_capacity(
            dispatch_capacity, capacity, dispatch_reserve_number
        )
        
        # optimize dispatch
        dispatch_suggestion = optimize_dispatch(today_dispatch, dispatch_capacity)
        dispatch_suggestion = add_dispatch_for_afrenoon(
            dispatch_suggestion, current_dict, morning_final_bikes, today_best_afternoon
        )
        dispatch_suggestion = add_dispatch_for_tomorrow(
            dispatch_suggestion, current_dict, today, tomorrow
        )
        
        # reshape
        today_data = merge_dispatch_and_data(today_best_init, today_data, dispatch_suggestion)
        today_data = today_data.sort_values('adjust_api_time')
        today_best_init, today_data = deal_init_merge(today_best_init, today_data)
        today_data = fill_dispatch_na(today_data)
        
        # 根據最適初始與優化後調度，重新生成當日資料
        simulat_best_result = resimulate_with_bset_para(today_best_init, today_data)
        
        # save
        tomorrow_best_init_bikes = current_dict[tomorrow]['best_init_bikes']
        results[stop_id][today]['tomorrow_best_init_bikes'] = tomorrow_best_init_bikes
        results[stop_id][today]['dispatch_suggestion'] = dispatch_suggestion
        results[stop_id][today]['simulat_best_result'] = simulat_best_result
        
        # conclution
        loop_counter_remainder = loop_counter % 1000
        if (loop_counter_remainder == 0):
            print(f'  {loop_counter}/{total_loop}, cost {time.time()-start_time} secs.')
        loop_counter += 1
print(f'Loop finished, cost {time.time()-start_time} sesc.')
# cost 27 minutes

# save dict
del data, daily_data
file_path = output_path+'/simulate_results_by_stop_by_date.pkl'
with open(file_path, 'wb') as f:
    pickle.dump(results, f)
print('Finished [03]simulate_to_find_best_dispatch.py.')
del results