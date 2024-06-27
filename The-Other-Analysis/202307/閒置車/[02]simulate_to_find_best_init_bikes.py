# -*- coding: utf-8 -*-
"""
Created on Tue May 23 09:29:47 2023

@author: rz3881
"""

import pandas as pd
import time
import pickle
import datetime
pd.options.mode.chained_assignment = None

def adjust_txn(data, adjust_num):
    '''
    為了猜測真實需求，車=0(觸底)或車=柱數(觸頂)時借車數會+-adjust_num，
    也就是假設觸底而不能反映的交易還有adjust_num個(觸頂亦同)
    '''
    is_empty = (data['available_rent_bikes']==0)
    data.loc[is_empty, 'txn_on'] = data.loc[is_empty, 'txn_on'] + adjust_num
    data.loc[is_empty, 'txn_delta'] = data.loc[is_empty, 'txn_delta'] - adjust_num
    data.loc[is_empty, 'min_cum_txn'] = data.loc[is_empty, 'min_cum_txn'] - adjust_num
    data.loc[is_empty, 'max_cum_txn'] = data.loc[is_empty, 'max_cum_txn'] - adjust_num
    
    is_full = (data['available_rent_bikes']==data['capacity'])
    data.loc[is_full, 'txn_off'] = data.loc[is_full, 'txn_off'] + adjust_num
    data.loc[is_full, 'txn_delta'] = data.loc[is_full, 'txn_delta'] + adjust_num
    data.loc[is_full, 'min_cum_txn'] = data.loc[is_full, 'min_cum_txn'] + adjust_num
    data.loc[is_full, 'max_cum_txn'] = data.loc[is_full, 'max_cum_txn'] + adjust_num
    return data


def simulat_init_bikes(gdata, simulate_bike_step=2, is_print=False):
    '''
    loop 初始車輛數，實際模擬每天的交易情況
    以此尋找最適初始車輛，並紀錄超過最大、最小預警值的數量，以做後續調度
    '''
    simulat_results = {}
    # 給定預設水位 & init
    for init_available_bike in range(reserve_number, safety_capacity+1, simulate_bike_step):
        available_bike = init_available_bike
        dispatch_queue = []
        dispatch_queue_col = ['api_time', 'dispatch_num', 'available_bike']
        log = []
        log_col = ['api_time', 'available_bike_before_dispatch',
                   'is_overflow', 'is_positive_overflow',
                   'dispatch_num_cause_overflow',
                   'available_bike_after_dispatch']
        # 先去除無交易row，減少運算
        is_delta = (gdata['txn_on'] + gdata['txn_off'] + gdata['in'] + gdata['out']) != 0
        gdata = gdata.loc[is_delta]
        # 模擬當日交易
        for _, target_row in gdata.iterrows():
            api_time = target_row['adjust_api_time']
            available_bike += target_row['txn_delta']
            # 若超出預警上下界
            is_positive_overflow = available_bike > safety_capacity
            is_negative_overflow = available_bike < reserve_number
            is_overflow = (is_positive_overflow | is_negative_overflow)
            temp_log = [api_time, available_bike]
            if is_overflow:  # 溢出
                if is_positive_overflow: # 正溢出=還爆
                    overflow = available_bike - safety_capacity
                else:  # 負溢出=借爆
                    overflow = available_bike - reserve_number
                # 超出額抹去，並記錄超出額
                dispatch_num = -overflow
                available_bike += dispatch_num
                dispatch_queue.append((api_time, dispatch_num, available_bike))
            else:
                dispatch_num = 0
            temp_log.extend([is_overflow, is_positive_overflow, dispatch_num, available_bike])
            log.append(temp_log)
        log = pd.DataFrame(log, columns=log_col)
        # save results
        abs_dispatch_sum = sum([abs(dis_suggest) for (api_time, dis_suggest, available_bike) in dispatch_queue])
        pos_dispatch_sum = sum([dis_suggest for (api_time, dis_suggest, available_bike) in dispatch_queue if dis_suggest>0])
        neg_dispatch_sum = sum([dis_suggest for (api_time, dis_suggest, available_bike) in dispatch_queue if dis_suggest<0])
        simulat_results[init_available_bike] = {}
        simulat_results[init_available_bike]['abs_dispatch_sum'] = abs_dispatch_sum
        simulat_results[init_available_bike]['pos_dispatch_sum'] = pos_dispatch_sum
        simulat_results[init_available_bike]['neg_dispatch_sum'] = neg_dispatch_sum
        dispatch_queue = pd.DataFrame(dispatch_queue, columns=dispatch_queue_col)
        simulat_results[init_available_bike]['dispatch_queue'] = dispatch_queue
        simulat_results[init_available_bike]['final_available_bike'] = available_bike
    if is_print:
        print(f'  init bikes been simulated, cost {time.time()-loop_start_time} sesc.')
    return simulat_results


def _find_min_dis_and_init_bike(min_dispatch_sum, best_init, v, init_bike):
    if min_dispatch_sum is None:
        min_dispatch_sum = v['abs_dispatch_sum']
        best_init = init_bike
    else:
        is_smaller_dispatch = (v['abs_dispatch_sum'] < min_dispatch_sum)
        if is_smaller_dispatch:
            min_dispatch_sum = v['abs_dispatch_sum']
            best_init = init_bike
    return min_dispatch_sum, best_init


def get_best_init_and_dispatch_queue(simulat_results):
    '''
    The selected option should satisfy single-direction dispatching,
    which means avoiding situations where both dispatch in and dispatch out,
    and minimizing the total number of dispatching and the number of bikes.
    '''
    # single-direction dispatching
    min_dispatch_sum = None
    best_init = reserve_number
    for init_bike, v in simulat_results.items():
        # break
        is_single_direction = (v['pos_dispatch_sum']==0) | (v['neg_dispatch_sum']==0)
        if is_single_direction:
            # find min by updating the optimal solution
            # which also ensures under the same dispatching,
            # it selects a smaller initial number of bikes
            min_dispatch_sum, best_init = _find_min_dis_and_init_bike(min_dispatch_sum, best_init, v, init_bike)
    # cant find single-direction dispatching
    if min_dispatch_sum is None:
        for init_bike, v in simulat_results.items():
            # break
            min_dispatch_sum, best_init = _find_min_dis_and_init_bike(min_dispatch_sum, best_init, v, init_bike)
    dispatch_queue = simulat_results[best_init]['dispatch_queue']
    return best_init, min_dispatch_sum, dispatch_queue


# load
data = pd.read_csv(idle_path+'/status_return_time_merge_txn_and_dispatch.csv')
data['adjust_api_time'] = pd.to_datetime(data['adjust_api_time'])
# 為符合使用者習慣，將init_hour視為每日起點
data['adjust_api_time_m6h'] = data['adjust_api_time'] - pd.Timedelta(hours=init_hour)
data['api_m6h_hour'] = data['adjust_api_time_m6h'].dt.hour 
data['date_m6h'] = data['adjust_api_time_m6h'].dt.date.astype(str)
fdata = data.copy()

# 觸頂或觸底時，多估算需求量
if is_add_txn_when_empty_or_full:
    fdata = adjust_txn(fdata, adjust_num=1)

# 計算建議的調度與預設車數
total_loop = len(set(fdata['stop_id'])) * len(set(fdata['date_m6h']))
loop_counter = 1
start_time = time.time()
results = {}
for (date, stop_id), gdata in fdata.groupby(['date_m6h', 'stop_id']):
    loop_start_time = time.time()
    # break
    # print(f'Into {stop_id}, {date}......')
    # 設定本站預警上下界
    capacity = gdata['capacity'].iloc[0]
    safety_capacity = (capacity - reserve_number)
    # 迴圈模擬，尋找最適初始車並記錄調度，上下午拆兩半
    is_morning = (gdata['adjust_api_time'].dt.hour < afternoon_hour) & (gdata['adjust_api_time'].dt.hour >= init_hour)
    is_afternoon = ~is_morning
    # 上午
    morning_data = gdata.loc[is_morning]
    m_simulat_results = simulat_init_bikes(morning_data, simulate_bike_step=2)
    m_best_init, m_min_dispatch_sum, m_dispatch_queue = get_best_init_and_dispatch_queue(m_simulat_results)
    m_final_bikes = m_simulat_results[m_best_init]['final_available_bike']
    # 下午
    afternoon_data = gdata.loc[is_afternoon]
    a_simulat_results = simulat_init_bikes(afternoon_data, simulate_bike_step=2)
    a_best_init, a_min_dispatch_sum, a_dispatch_queue = get_best_init_and_dispatch_queue(a_simulat_results)
    a_final_bikes = a_simulat_results[a_best_init]['final_available_bike']
    # concat 上下午資訊
    min_dispatch_sum = m_min_dispatch_sum + a_min_dispatch_sum
    dispatch_queue = pd.concat([m_dispatch_queue, a_dispatch_queue])
    
    # save
    if results.get(stop_id) is None:
        results[stop_id] = {}
    if results[stop_id].get(date) is None:
        results[stop_id][date] = {}
    results[stop_id][date]['reserve_number'] = reserve_number
    results[stop_id][date]['best_init_bikes'] = m_best_init
    results[stop_id][date]['best_afternoon_bikes'] = a_best_init
    results[stop_id][date]['min_dispatch_sum'] = min_dispatch_sum
    results[stop_id][date]['dispatch_queue'] = dispatch_queue
    results[stop_id][date]['morning_final_bikes'] = m_final_bikes
    results[stop_id][date]['afternoon_final_bikes'] = a_final_bikes
    # print(f'  result been append, cost {time.time()-loop_start_time} sesc.')
    
    # conclution
    loop_counter_remainder = loop_counter%1000
    if (loop_counter_remainder == 0):
        print(f'  {loop_counter}/{total_loop}, cost {time.time()-start_time} sesc.')
    loop_counter += 1
# cost 96.5 minutes

# save dict
file_path = idle_path+'/best_init_and_raw_dispatch_queue_by_stop_by_date.pkl'
with open(file_path, 'wb') as f:
    pickle.dump(results, f)
print('Finished [02]simulate_to_find_best_init_bikes.py.')
del results, data, fdata